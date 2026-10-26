# Branch review tasks

## Scope and execution rules

Reviewed `feature/trim-azure-client` at `9dc4d767f313ecca78f01e80e178c79bea56daba`.
The requested comparison is `main...HEAD`, with local `main` at
`278868c71dc55ffc1586f3f92ed8bc5d99587e6d`: 362 changed files, 58,235 insertions,
5,514 deletions. Local `main` is behind the available `origin/main`
(`9b21cef9b39e18fd4393a0eb6d65c453cafdcaf2`), which is also an ancestor of HEAD.
The latter comparison has 361 changed files and 57,417 insertions. Neither ref
was updated during the review.

This is a source review of the changed HTTP/Azure clients, filesystem read/write
paths, caches, public interfaces, and relevant tests/build configuration. It is
not a claim that every added line or every possible interleaving was exhaustively
verified. The tasks below distinguish source-confirmed problems, measured-work
candidates, and inherited issues. No implementation changes were made.

- File-cache and secondary-cache recommendations are excluded. Remaining tasks
  may address in-memory prefetch, buffer pools, DNS, TLS sessions, and credentials.
  Task IDs are retained, so removed tasks leave gaps.
- **Do not introduce type erasure.** No new erased callbacks, erased sinks,
  virtual interfaces, or equivalent runtime dispatch as a refactoring shortcut.
  Keep concrete templates, typed state, and existing ownership guarantees.
- Read `.github\copilot-instructions.md` before implementing. Preserve the
  host-owned executor model, public API compatibility where promised, ETag
  consistency, lease fencing, and exactly-once completion.
- P1 means a correctness/liveness fix to prioritize; P2 means a normal-priority
  correctness or performance task; P3 means an optional simplification.
- "Source-confirmed" identifies a concrete code path, not a newly executed
  reproducer. Add the specified focused regression test before fixing it.
- Benchmark candidates are **not** instructions to assume a speedup. Measure
  first, retain the current implementation if the alternative loses, and record
  allocations, bytes, latency, and concurrency rather than timing alone.
- Implement independent tasks separately. Do not combine all of these into a
  broad redesign. Suggested dependencies are stated explicitly.
- References are to the reviewed HEAD; use the named symbols if lines move.

## Correctness and liveness

### R01 - Make limiter release/cancellation safe when rearming the timer fails

- [x] **P1 | Source-confirmed | New limiter**
- **Locations:** `libs\HttpClient\src\private\OriginLimiter.hpp:124-180,225-268`;
  `libs\HttpClient\src\private\HttpClientImpl.hpp:151-165`.
- **Evidence:** `Release` and `Cancel` call `ForgetExpiryLocked` before removing
  the waiter. That helper erases the expiry and calls the allocating
  `ArmTimerLocked`. If `async_wait` setup throws, the waiter remains queued but
  its expiry is gone. On the release path, `RequestState::Finish` has already
  taken the original completion out of its cell and `ReleaseSlot` has cleared
  `holdsSlot`; unwinding destroys that completion without invoking it and loses
  the slot hand-off.
- **Action:** Separate the non-throwing queue/accounting commit from timer
  scheduling. Preserve both completions and slot ownership if rearming fails.
  Provide a reported, recoverable scheduling failure path; merely swallowing the
  exception and hoping unrelated future traffic arrives is insufficient.
- **Acceptance:** Inject a one-shot timer-handler allocation/setup failure while
  releasing the earliest-deadline waiter with another deadline queued. Also test
  cancellation of that waiter. Every accepted request completes exactly once,
  the origin remains usable, and no waiter loses its timeout.
- **Tests:** Extend `OriginLimiterTests.cpp` and the limited-client cases in
  `HttpClientRobustnessTests.cpp`, using a concrete timer/fault seam.

### R02 - Do not run DNS failure recovery on another request's strand

- [x] **P1 | Source-confirmed | New HTTP client**
- **Location:** `libs\HttpClient\src\private\RequestOperation.cpp:224-241`,
  `OnSharedResolve`, together with `Contain` and `Finish`.
- **Evidence:** If posting a shared DNS outcome fails, the catch calls
  `self->Contain([] { throw; })` immediately. This executes on the leader's
  callback thread, not necessarily the waiting request's strand. `Contain` reads
  `m_finished`, and `Finish` mutates request state, cancels its timer/resolver,
  and closes its socket. Those operations can race the waiter's timeout or
  cancellation on a multithreaded `io_context`.
- **Action:** Keep recovery serialized on the waiting operation's executor.
  Use concrete, safely owned emergency/recovery state rather than mutating the
  operation inline from a foreign callback. Coordinate with R03's delivery
  failure policy without sharing unrelated operation state.
- **Acceptance:** Deterministically fail the DNS-result post while the waiter's
  timeout/cancellation is runnable on another thread. Assert exactly-once
  completion and no concurrent stream/timer access; exercise under TSan.

### R03 - Close the remaining allocation-failure holes in async event delivery

- [x] **P1 | Source-confirmed | New Azure client and HTTP completion path**
- **Locations:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\RequestPipeline.hpp:193-235`;
  `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp:204-223,607-627,811-821`;
  `libs\HttpClient\include\AVEVA\HttpClient\HttpClient.hpp`, `SendAsync`'s final post.
- **Evidence:** The retry runner's contended `m_queue.push_back` and the download
  runner's `m_events.push_back` allocate outside their guarded dispatch code.
  A failure can discard an already-arrived completion without draining its
  in-flight entry. `PostWriteDone` catches a failed fallback enqueue and returns
  with `m_writing` still true. `CompleteFileDownload` marks `Completed`, moves the
  user's handler into a post, and silently catches failure. The HTTP public
  adapter likewise moves its handler into a final post without recovery. These
  paths can strand accepted operations or replace the promised result with
  destruction/broken-promise behavior.
- **Action:** Make terminal-event admission and handler ownership transactional.
  Consider preallocated typed terminal slots or a bounded concrete event
  representation where the event count can be proved. Keep ordinary payload
  queues growable where required. Never overwrite/drop an event to fit a bound.
  Preserve executor and buffer-lifetime guarantees when reporting failure.
- **Acceptance:** Add one-shot failure tests specifically at event admission,
  writer-to-I/O hand-off, and final completion posting, after initiation has
  succeeded. No hung future, retained work guard, orphaned partial file, lost
  in-flight count, or premature release of a live request body.
- **Dependency:** Establish the limiter completion invariant in R01 before
  modifying common HTTP completion failure handling.

### R04 - Include token copies and cache publication in fan-out failure handling

- [x] **P2 | Source-confirmed | New credential cache**
- **Location:** `libs\AzureClient\include\AVEVA\AzureClient\TokenCredential.hpp:212-243,340-363`.
- **Evidence:** `CompleteAll` constructs `AccessToken{token}` as an argument
  *outside* `deliver`'s try block. A token-string allocation failure abandons the
  remaining recipients after the refresh has been drained. Separately,
  `HandleInnerTokenResult` copies scopes/token into the cache before draining
  waiters; a throw in an asynchronously delivered result can leave the refresh
  permanently in flight. The existing throwing-recipient isolation does not
  cover either case.
- **Action:** Guard preparation as well as invocation. Cache publication must
  not prevent pending recipients from receiving an outcome. Keep a failed
  recipient's completion owned until it can be failed, and continue draining
  other recipients. Do not change the public token representation gratuitously.
- **Acceptance:** Use a long token and at least three coalesced callers. Fail a
  cache copy and a middle-recipient copy, separately, with asynchronous inner
  completion. All callers settle exactly once and a later acquisition works.

### R05 - Saturate duration arithmetic before conversion, subtraction, or rounding

- [x] **P2 | Source-confirmed | New timeout/retry code**
- **Locations:**
  `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\ReadableFileImpl.hpp:52-65`;
  `src\Azure\BasicReadableFile.ipp:90-94`;
  `libs\AzureClient\include\AVEVA\AzureClient\Detail\RequestPipeline.hpp:56-64`;
  `libs\AzureClient\src\RequestPipeline.cpp:86-93`.
- **Evidence:** The plugin's `DeadlineAfter` adds arbitrary milliseconds to
  `steady_clock::now()` without the saturation already present in
  `AzureRequestOptions`. `IOOptions::timeout = microseconds::max()` can overflow
  the finer-grained steady-clock duration. `ComputeBackoff` converts
  `milliseconds::max()` to double, where the value can round to 2^63, and then
  calls `llround`. Finally, `ParseRetryAfter` subtracts two individually valid
  system-clock timestamps before clamping; on a nanosecond clock, dates in 1700
  and 2200 fit individually but their difference does not.
- **Action:** Bound durations in a representable unit before arithmetic. Reuse
  the existing deadline policy where appropriate. Clamp retry delays before
  floating-point-to-integer conversion and timer conversion. Compute the bounded
  HTTP-date difference without first overflowing the clock representation.
- **Acceptance:** Cover maximum/minimum durations, zero, normal short timeouts,
  extreme retry delays with a deterministic generator, and widely separated
  valid Date/Retry-After values. Run the date/duration cases under Linux UBSan as
  well as Windows; preserve existing jitter and immediate-expiry semantics.

### R06 - Apply one timeout budget to a MultiRead batch; document synchronous behavior

- [x] **P2 | Source-confirmed behavior gap | New MultiRead; inherited sync omission**
- **Locations:** `src\Azure\BasicReadableFile.ipp:41-47,90-94,177-358`;
  `src\Azure\Impl\ReadableFileImpl.ipp`, `RandomRead`, `TryReadFromPrefetch`,
  `DownloadWithRetry`.
- **Evidence:** Each newly admitted MultiRead group calls `StartRead(*target,
  opts, ...)`, which creates a fresh deadline. Batches larger than the 32-request
  window can spend the full timeout repeatedly, in addition to batch setup.
  Synchronous `Read` discards `IOOptions` entirely and can spend the independent
  30-second prefetch wait plus transport/stale-read retries. Ignoring the sync
  hint predates this branch; the new async implementation now behaves differently.
  RocksDB documents these options as hints, so this is not an unconditional
  claim of violating a mandatory RocksDB guarantee.
- **Action:** Capture the batch deadline once and pass it through internal read
  admission. Expired queued groups should receive TimedOut without new network
  work. Align synchronous reads with the same budget, or explicitly document
  their unsupported hint rather than implying all reads are deadline-aware.
  Prefer preserving an absolute deadline over repeatedly rounding/restarting it.
- **Acceptance:** More than 32 separated ranges with delayed first-wave reads,
  partial successes before expiry, and expired queued groups. Include synchronous
  pending-prefetch and stale-metadata cases if adding sync support.
- **Dependency:** R05.

### R07 - Complete and explicitly test the error-to-IOStatus mapping policy

- [x] **P2 | Source-confirmed omissions | New error translation**
- **Locations:** `src\Azure\AzureErrorTranslator.cpp`, `IsLeaseLost`,
  `IOStatusFromError`, `ClassifyCurrentException`;
  `libs\AzureClient\src\BlobStorageErrorCategory.cpp`;
  `libs\HttpClient\src\private\HttpErrorCategory.cpp`.
- **Evidence:** `LeaseNotPresentWithLeaseOperation` exists in the client enum but
  is absent from `IsLeaseLost`, while the corresponding blob-operation error and
  other lease-operation failures are mapped to IOFenced. `HttpClientError::InvalidUrl`
  and `InvalidRequest` have no portable `invalid_argument` condition, so they
  fall through to generic IOError. A raw `std::system_error(timed_out)` also
  becomes generic IOError, unlike the same code in `RequestFailedException`.
- **Action:** Make these classifications deliberate and consistent. Add a
  table-driven policy covering every client error enum, with explicit generic
  fallbacks for service errors that genuinely have no more precise RocksDB
  equivalent. Keep cancellation non-retryable and lease loss fenced.
- **Acceptance:** Extend `AzureErrorTranslatorTests.cpp` for the omitted lease
  code, invalid HTTP input, and equivalent wrapped/unwrapped timeout conditions.
  Assert status category, retryability, and retained diagnostic context.

## Performance and resource use

### R08 - Budget MultiRead by bytes as well as request count

- [x] **P2 | Source-confirmed memory amplification; tune with benchmarks**
- **Locations:** `src\Azure\BasicReadableFile.hpp:26-34`;
  `src\Azure\BasicReadableFile.ipp`, `Group`, `start`;
  `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp:684-704`.
- **Evidence:** A batch can admit 32 merged groups of 8 MiB. Each group owns an
  8 MiB merge buffer and the async read separately owns an 8 MiB download lease:
  roughly 512 MiB before HTTP response bodies, caller scratch, or other batches.
  Releasing buffers per completed group helps, but does not bound this active
  window by bytes. The shared prefetch budget does not cover MultiRead.
- **Action:** Add internal byte-based admission alongside the request cap,
  accounting for actual backing capacity and merged storage. Evaluate a
  per-filesystem budget if multiple batches can otherwise multiply the peak.
  Ensure a single large valid request can progress; do not silently truncate it.
- **Acceptance:** Instrument peak outstanding bytes for small random reads,
  maximum-size merged groups, mixed sizes, and concurrent batches. Preserve
  sliding-window progress when one group stalls and release charges on all exits.

### R09 - Cancel abandoned private downloads rather than only suppressing their results

- [x] **P2 | Source-confirmed resource retention; optimization**
- **Locations:** `src\Azure\Impl\AsyncReadRequest.cpp:74-87,123-139`;
  `src\Azure\BasicReadableFile.ipp`, `StartRead`;
  `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp:669-705`.
- **Evidence:** Abort/deleting a handle changes `AsyncReadRequest` state but
  does not signal its private blob download. The download may continue through
  network retries, retaining buffers and occupying connection slots even though
  it can never publish to scratch. Suppressing the copy is correct for lifetime,
  but does not reclaim transport resources.
- **Action:** Connect private reads to concrete, thread-safe cancellation state.
  Serialize slot installation/emission as the existing StopBridge does. Do not
  cancel a shared prefetch or metadata refresh just because one waiter leaves.
  A timed-out/aborted prefetch waiter must not launch a fallback download later.
- **Acceptance:** Abort many stalled reads; verify transport cancellation,
  eventual buffer/tracker release, no further retries, exactly-once callback
  delivery, and untouched freed scratch. Preserve shared-prefetch consumers.
- **Dependencies:** R02/R03 recovery rules before expanding cancellation paths.

### R10 - Remove quadratic allocation growth from prefetch waiter registration

- [x] **P2 | Source-confirmed complexity | New prefetch code**
- **Location:** `src\Azure\Impl\ReadableFileImpl.ipp:247-264`, `PrefetchState::AddWaiter`.
- **Evidence:** `Callbacks.reserve(Callbacks.size() + 1)` before every insertion
  defeats vector's geometric growth. A popular stalled prefetch with N readers
  can cause N allocations and O(N^2) callback moves while holding its mutex.
  `Waiters.reserve(Waiters.size() + 1)` has the same pattern for retained groups;
  invalidated in-flight slots can leave groups alive beyond the visible slot cap.
- **Action:** Reserve geometrically only when capacity is exhausted, before
  moving the caller's callback. Preserve the rollback of newly created empty
  groups.
- **Acceptance:** Hundreds/thousands of interleaved waiters, allocation counts
  showing amortized growth, and an injected growth failure that leaves the
  caller's callback usable. Keep allocation-free waiter detachment.

### R11 - Give prefetch-hit and trivial MultiRead batches a cheaper path

- [x] **P2 | Benchmark candidate**
- **Done:** MultiRead now serves requests covered by one completed in-memory prefetch slot
  before sorting/merging (file cache untouched), and returns before the signal/handle setup when
  nothing is left to fetch. Measured by test: two adjacent requests each covered by a separate
  completed prefetch went from 3 downloads (2 prefetches + 1 merged-range miss) to 2 (no new
  network request); all-hit batches now allocate only `resolved`/`order` and no signal, groups,
  handles or sort. Cost on misses: one uncontended mutex acquisition per request, negligible next
  to a GET. No wall-clock microbenchmark was run; the gain is the avoided request. Tests:
  `AsyncReadTests.MultiRead_AdjacentRequestsCoveredBySeparatePrefetches_*`,
  `MultiRead_MixedPrefetchHitsAndMisses_*`. Pending prefetches are still handled by StartRead.
- **Location:** `src\Azure\BasicReadableFile.ipp:207-285`.
- **Evidence:** MultiRead allocates the resolved/order/group/active structures,
  sorts, and plans merged ranges before consulting completed prefetches through
  StartRead. All-prefetch-hit batches still allocate handles/signals. Merging requests that
  were individually covered by separate completed prefetch slots can also turn
  those hits into a merged-range miss.
- **Action:** Measure an empty/single-request fast path and serving independent
  completed-prefetch hits before sorting/merging the misses. Preserve per-request
  validation and statuses. Leave disk file-cache behavior unchanged.
- **Acceptance:** All-prefetch-hit, mixed-hit, separately prefetched adjacent requests,
  zero-length requests, overlapping requests, and error cases. Compare network
  request count and allocations as well as latency.

### R12 - Avoid recreating connection-pool origin entries on every serial request

- [x] **P2 | Source-confirmed churn; benchmark alternative**
- **Result:** Implemented (the alternative won on allocations). Serial Release/Acquire on one
  origin with a long host name: 400 pool allocations per 100 cycles before (4 per cycle: node
  entry, key strings, bucket), 0 after warm-up. Bytes follow the same drop. An emptied origin
  stays in the map as a dormant entry on an intrusive list capped at 32
  (`MaxDormantOrigins`); older ones are erased, so 1000 unique hosts leave at most 32 empty
  entries. Concurrency is unchanged (same single mutex, a few pointer writes more). Regression tests:
  `SerialKeepAliveRequestsDoNotAllocateAfterWarmUp`, `DormantOriginsAreBoundedWhenManyOriginsAreTouched`;
  the existing expiry, global-cap and allocation-failure tests still pass.
- **Location:** `libs\HttpClient\src\private\ConnectionPool.hpp`, `AcquireAged`,
  `Release`, `Recycle`.
- **Evidence:** Acquiring the last idle connection erases its origin map entry.
  Releasing that same connection calls `try_emplace(key)` again. A serial
  keep-alive workload therefore recreates the origin node/key repeatedly despite
  recycling the connection node, so the pool is not allocation-free in steady
  state for that workload.
- **Action:** Measure a bounded dormant-origin/key-reuse strategy. Preserve
  stable addresses required by intrusive nodes and cap retained origins even
  when callers touch many unique hosts. Avoid unbounded tombstones.
- **Acceptance:** One origin with one outstanding request, bursts on one origin,
  and high origin cardinality. Count pool-only allocations after warm-up and
  retain expiry/global-cap/failure-transaction tests.

### R13 - Reduce unused retry machinery and tiny-download bookkeeping

- [x] **P2 | Benchmark candidate**
- **Result:** Implemented lazy backoff-timer construction in `RetryOperation` (`std::optional` timer built on
  first backoff; cancel is a no-op until then). Measured by construction count (the fake client's retry timer):
  first-attempt success went from 1 timer to 0; a request that retries constructs exactly 1 (reused across
  retries). The alternative only removes work, so there is no losing case to retain. Regression tests:
  `RetryTests.FirstAttemptSuccessDoesNotConstructABackoffTimer`, `RetryTests.ARetryConstructsExactlyOneBackoffTimer`.
  The `BlobDownload` tiny-download bookkeeping (direct delivery, inline first entry) was not changed and has no
  benchmark yet; Linux allocation deltas were not measured.
- **Locations:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\RequestPipeline.hpp`,
  constructor and `m_timer`/`m_queue`;
  `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp`,
  `Send`, `Deliver`, `m_inFlight`, `m_ready`.
- **Evidence:** Every retry operation constructs a timer even when the first
  request succeeds or retries are disabled; the deadline timer already has a
  documented lazy-construction optimization. A one-chunk download still creates
  an in-flight map node, cancellation signal allocation, and a ready-map node
  that is usually erased immediately. Empty deque costs vary by standard
  library and should be measured, not assumed.
- **Action:** Evaluate lazy backoff-timer construction, direct delivery of the
  next contiguous chunk, and a concrete first-entry/inline-storage fast path.
  Keep the general out-of-order and reentrant paths. Do not trade correctness
  under memory pressure for lower allocation counts.
- **Acceptance:** First-attempt success, retry disabled/enabled, one/many chunks,
  out-of-order responses, synchronous fake completions, and cancellation.
  Report Windows and Linux allocation deltas.
- **Dependency:** R03; retain its failure-injection coverage.

### R14 - Measure and remove avoidable download copies without exposing scratch to late I/O

- [ ] **P2 | Source-confirmed copies; benchmark/design candidate**
- **Locations:** `libs\HttpClient\src\private\RequestOperation.cpp`, `BuildResponse`;
  `libs\AzureClient\include\AVEVA\AzureClient\Detail\DownloadSinks.hpp`, `SpanSink::Write`;
  `src\Azure\Impl\AsyncReadRequest.cpp`, `Complete`;
  `src\Azure\BasicReadableFile.ipp`, `applyOutcome`.
- **Evidence:** The transport materializes a body string, SpanSink copies it to
  the pooled read buffer, and AsyncReadRequest copies it to scratch. Merged
  MultiRead adds a split-to-members copy. Those copies can become material once
  network round trips are reduced.
- **Action:** Profile sizes and copy bandwidth. Evaluate a typed transport
  destination or concrete owned-buffer result that avoids an intermediate copy.
  Keep the final guarded publication into caller scratch: it must remain safe
  after handle deletion, abort, timeout, and stale-read retry. Do not introduce
  an erased sink API or store caller scratch in an outstanding socket operation.
- **Acceptance:** Compare representative 4 KiB, 64 KiB, 4 MiB and merged reads,
  peak bytes, and abort/retry lifetime tests. Coordinate with R08 rather than
  creating two incompatible buffer-ownership changes.

### R15 - Use fixed-capacity containers for genuinely fixed-capacity buffer free lists

- [ ] **P3 | Concrete Boost simplification**
- **Locations:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\DownloadBufferPool.hpp`,
  `SizeClass::Free`, `Return`, `EvictLargestWhileOverCap`;
  `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\WriteableFileImpl.hpp`,
  `UploadTracker::Free`.
- **Evidence:** The download pool keeps at most eight buffers per class and the
  upload tracker at most four. Dynamic free-list vectors require allocation
  handling/reservation despite those strict caps. Download eviction also builds
  a dynamic vector while trying to release memory.
- **Action:** Evaluate `boost::container::static_vector` for these free lists
  and a bounded per-class eviction batch. A `std::array` plus count is another
  option, but do not hand-write container bookkeeping if static_vector is clear.
  Keep deallocation outside class locks and account for object-size growth.
- **Acceptance:** Existing pool race/budget tests pass; returning a buffer and
  evicting over-cap buffers need no free-list allocation. Preserve the hard caps,
  destructor safety, and upload payload lifetime.

### R17 - Avoid DNS-cache wholesale eviction at the capacity boundary

- [ ] **P3 | Source-confirmed policy; benchmark before changing**
- **Location:** `libs\HttpClient\src\private\DnsCache.hpp:120-135`, `Store`.
- **Evidence:** At 256 entries, Store sweeps expired entries and then clears all
  remaining live entries if still full. Even refreshing an existing key can
  trigger this before `insert_or_assign`. Multi-origin bursts can discard useful
  resolutions and regenerate DNS traffic.
- **Action:** Handle replacement before capacity eviction; evaluate bounded
  single-entry or batched eviction if high-cardinality traffic matters. Preserve
  the five-second TTL, lookup coalescing, heterogeneous lookup, and scope rules.
  Do not add per-hit LRU mutation without measuring its lock cost.
- **Acceptance:** Refresh an existing key in a full cache without losing unrelated
  entries; churn beyond the cap and verify bounded memory and DNS request counts.

## Standard-library and Boost simplifications

### R18 - Replace straightforward hand-written searches/fills, not protocol state machines

- [ ] **P3 | Source-confirmed standard-library opportunities**
- **Locations and specific candidates:**
  - `libs\AzureClient\src\BlobResponseParsers.cpp`, `ParseMonth`: use
    `std::ranges::find_if`, then derive the month index.
  - `libs\AzureClient\src\RequestPipeline.cpp`,
    `ResolveCopySourceAuthorization`: find the first matching header with
    `std::ranges::find_if`, then retain the existing erase-or-update semantics.
  - `libs\AzureClient\include\AVEVA\AzureClient\Detail\RequestPipeline.hpp`,
    `StampDate`: express the initial header search with a ranges algorithm while
    keeping the cached-index fast path and first-match behavior.
  - `libs\AzureClient\src\SharedKeySigner.cpp`, `MakeCultureTableLevel2`: use
    constexpr `std::fill`/`std::fill_n` for the contiguous uppercase range.
  - `src\Azure\Impl\ReadableFileImpl.ipp`, `PrefetchState::DropAll`: `Drop` now
    only erases a slot, so `Slots.clear()` replaces repeated front erasure.
- **Action:** Make only changes that improve clarity with equivalent behavior.
  These are mostly maintainability improvements, not promised speedups.
- **Acceptance:** Existing parser, request-shape, signing and prefetch tests
  pass. Do not replace Azure's culture-aware signing comparator with ordinary
  lexical ordering or replace whitespace collapsing with plain trimming.

### R20 - Reuse Boost.Scope for simple local cleanup guards

- [ ] **P3 | Concrete Boost simplification**
- **Locations:** `src\Azure\Impl\ReadWriteFileImpl.ipp`, `ReleaseWave`, `TrimOnExit`;
  `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp`,
  `WaitForPending`.
- **Evidence:** Several one-off local structs only run a cleanup lambda.
  `boost::scope::scope_exit`/`scope_fail` are already used by the rename code,
  so no new dependency or erased callback is necessary.
- **Action:** Convert simple guards where shorter and clearer. Keep explicit
  types for guards with meaningful move/exactly-once ownership semantics, such
  as `ReadCompletionGuard`; do not flatten those into ad hoc lambdas.
- **Acceptance:** Preserve declaration/destruction order, lock ownership,
  conditional activation, and the rule that in-flight reads/uploads drain
  before backing buffers are released. Exercise failure paths, not just success.

### R21 - Evaluate Boost.MultiIndex for the TLS session cache

- [ ] **P3 | Optional design experiment, not a blanket rewrite**
- **Location:** `libs\HttpClient\src\private\TlsSessionCache.hpp`.
- **Evidence:** The TLS cache manually synchronizes an owning list and a lookup
  index, duplicates the origin string in the list and map, and now has explicit
  rollback to keep them consistent.
- **Action:** Prototype a concrete Boost.MultiIndex container with hashed-unique
  and sequenced indices. Compare node size, allocations, cache locality, lookup,
  LRU relocation, heterogeneous lookup, and exception guarantees. Keep the
  existing implementation if it wins materially.
- **Acceptance:** Replacement, TLS 1.3 ticket
  consume-once behavior, bounds, and failure-injection tests remain unchanged.
  Do not replace the already intrusive dual-list ConnectionPool merely for
  uniformity.

## Validation and compatibility

### R22 - Make performance evidence isolate the production client and end-to-end path

- [ ] **P2 | Source-confirmed measurement gap**
- **Locations:** `libs\HttpClient\tests\HttpClientAllocationTests.cpp:25-105`;
  `tools\db_bench\Compare-Backends.ps1`; relevant read/write test suites.
- **Evidence:** The submit allocation test intentionally excludes network work.
  The pooled-request measurement counts loopback-server allocations on the same
  thread and calls the private core, bypassing the public SendAsync adapter's
  completion post. Consequently its broad ceilings do not isolate production
  client allocation regressions or the new final hand-off.
- **Action:** Retain coarse regression checks but add isolated allocation
  measurements for the public client, with the server on an independent
  executor/thread. Measure the full Azure request/read path too. Extend the
  existing benchmark process, rather than adding an unrelated framework.
  Record exact revisions, Release configuration, compiler/dependency versions,
  request/body sizes, warm/cold state, allocations/bytes, request counts,
  throughput and tail latency. Keep credentials out of artifacts.
- **Acceptance:** Produce before/after evidence for retained R08-R17 changes.
  Distinguish loopback CPU measurements from Azure network results. Live Azure
  runs must be explicitly configured; do not spend cloud resources by default.

## Review validation and implementation checks

The following existing targeted tests were executed during this review:

```powershell
ctest --test-dir build\WindowsDebug -C Debug --output-on-failure --timeout 60 --parallel 4 -R '^(OriginLimiterTests|RetryTests|AsyncReadTests|LruFileIndexTests)\.'
```

**Result:** 131/131 passed in 74.45 seconds. These were existing Debug binaries,
not rebuilt for this review. The HTTP executable was timestamped 08:51:58 and the
Azure pipeline executable 08:51:13 on 2026-10-26, before reviewed HEAD's 08:57:05
commit; this is baseline evidence, not certification of the exact HEAD or of the
new failure scenarios above. Initial discovery without `-C Debug` reported zero
tests; this is a multi-configuration build, so always select the configuration.

No live Azure benchmark, fresh native build, Linux sanitizer run, or new
regression test was performed. The code review does not claim measured speedups
for the proposed optimizations.

For implementation, use the repository's existing tools:

```powershell
cmake --preset WindowsDebug
cmake --build build\WindowsDebug --config Debug
ctest --test-dir build\WindowsDebug --build-config Debug --output-on-failure --no-tests=error -R '<affected suites>'
```

Keep `VCPKG_ROOT` configured. If reusing a stale/inconsistent build tree, follow
the repository's fresh-configure guidance rather than editing its cache. Run
the pinned clang-format version on changed native files and the existing
relevant static checks. For concurrency and arithmetic changes, use the existing
Linux TSan and ASan/UBSan configurations. Run tests for the actual new failure
scenario, not only a similarly named success-path test.

Already-present improvements to preserve include intrusive connection-pool
lists, transparent lookups, `boost::hash_combine`, ranges-based searches/sorts,
body ownership across retries, bounded read/write windows, allocation-free
prefetch waiter detachment, ETag pinning of multi-chunk downloads, and
scope-guarded draining before buffer release. Do not undo these merely to make
the new code look more uniform.
