# Branch review execution backlog

Reviewed `feature/trim-azure-client` at `c950080d652f9c6bb14264ba8f7f07c89d7ff3b1`
against local `main` / merge base `278868c71dc55ffc1586f3f92ed8bc5d99587e6d`.
The comparison contains 357 changed files, 53,525 insertions and 5,508 deletions.
Review date: 2026-10-21.

This is a new backlog, not a request to repeat the previous passes. T01-T90 are
closed or explicitly dropped in `c950080:tasks.md`; consult that history before
reopening a previously measured optimization. IDs below continue from T91.
The working tree had `tasks.md` deleted when this review started; this file
recreates the requested handoff. No implementation files were edited.

Review concentrated on the changed production read/write, filesystem, lease,
HTTP, retry, transfer and cache paths, plus representative tests and build/CI
configuration. This is not a claim of exhaustive line-by-line coverage of every
changed test or every supported service configuration.

## Execution constraints

- **Do not introduce or recommend type erasure.** Preserve concrete templates,
  concepts, variants and statically typed continuations. Do not replace these
  paths with erased callables, erased executors, virtual dispatch or erased
  implementation objects. Existing callbacks mandated by RocksDB's API are not
  a reason to generalize erasure internally.
- Preserve public-header source compatibility, exactly-once completion,
  associated executor/allocator/cancellation behavior and error translation.
- Never wait for network completions on a thread running the host `io_context`.
  The plugin must not own or run that context.
- Keep downloads out of caller-owned scratch until the request lock confirms
  the request is live. RocksDB callbacks still run only from `Poll`/`AbortIO`.
- Keep write fencing, sticky upload errors, bounded upload concurrency and the
  existing non-atomic-rename recovery guarantees.
- Reproduce each correctness finding with a focused regression test before
  changing it. **Verify first** items are hypotheses requiring that reproduction,
  not instructions to change behavior speculatively.
- For performance changes, record before/after allocation bytes/counts,
  request counts or representative timings. Do not add complexity for an
  unmeasurable improvement. Do not infer an end-to-end speedup from static review.
- Make one focused change per commit, update its checkbox and record the result.
  Do not create a PR without explicit user approval.

## P1: correctness and completion/lifetime guarantees

### [x] T91 - Keep the default completion executor alive across off-executor work

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\AsyncInitiation.hpp:99-117`
creates `BoundHandler::Work` only when `!direct`. A plain callback using the
client executor takes the direct path and owns no work guard. That is not
equivalent to having no asynchronous work: credential acquisition may finish on
a foreign thread, and file downloads perform open/write/finalization on a writer
executor before posting back to the client executor
(`Detail\BlobDownload.hpp:720-786`). No transport operation necessarily holds
the client context alive during those intervals.

**Impact:** `io_context::run()` can return and leave the context stopped while
the Azure operation is still outstanding. Its later posted continuation or
completion then does not run without an unexpected `restart()`/second `run()`.
Existing foreign-thread tests call the fake transport's polling helper after
completion; the work-guard test covers an explicitly bound strand, not this path.

**Action:** Decouple direct invocation from outstanding-work ownership. Retain
the fast inline completion path when already on the right executor, but hold
appropriate concrete executor work until all off-executor work and final
completion dispatch have finished. Check the I/O executor as well as a different
handler executor where both are involved.

**Acceptance:** Extend `AsyncDeferredCompletionTests.cpp` and file-download
tests. With no external work guard, block a credential or writer, start `run()`
once, release the blocker and verify exactly one completion without restarting
the context. Preserve the existing explicit-executor/allocator tests.
An isolated probe using the actual binding helper confirmed `Work == nullopt`,
a stopped context before completion, and an undelivered late completion until
restart.

**Result:** Reproduced with `DefaultExecutorStaysBusyWhileCredentialIsPendingOffExecutor`
(failed before the fix: context stopped). `BoundHandler` now always holds the
executor work guard from initiation; the direct path keeps its inline fast path
and releases the guard right after the handler runs. Covers the file-download
writer path too, as the guard is taken at initiation for every operation (no
separate download test added). Full WindowsDebug ctest: 1165 passed/skipped, 0 failed.

### [x] T92 - Include payload-owner allocation in `StartFlush` rollback

**Evidence:** `src\Azure\Impl\WriteableFileImpl.ipp:192-228` swaps the original
buffer into `next` and truncates it before
`std::make_shared<PooledPayload>(...)` at line 217. The rollback `try` begins
only after that allocation.

**Impact:** If allocating the shared payload owner throws, unwinding destroys
`next`, which owns the original full pages. `m_buffer` contains only the copied
tail, but offsets and `m_flushed` still describe all the original data. A later
retry can upload missing/incorrect bytes while reporting success. T79 fixed
throws from upload initiation, not this earlier allocation.

**Action:** Make ownership preparation and the buffer swap a transaction.
Either finish fallible owner preparation before mutating buffered state or arm
a non-allocating rollback before the swap. Preserve the original full pages
and tail on every pre-acceptance exception, without adding a full-buffer copy.

**Acceptance:** Extend `WriteableFileTests.cpp` with a targeted payload-owner
allocation failure, distinct from the existing `UploadPages` fault hook. Append
distinct full-page and partial-tail patterns, fail once, then retry `Sync` and
verify exact bytes and logical size. Include a page-aligned case.

**Result:** Reproduced with two `WriteableFileTests` cases (tail and page-aligned) that
fail the `make_shared` owner allocation via a thread-local-armed `operator new` hook
(both failed before the fix: retry uploaded wrong bytes). `StartFlush` now rents the
buffer and copies the tail into it before touching `m_buffer`, and swaps into the
owner only after the owner allocation succeeded, so the swap is non-allocating. No
extra copy. Full WindowsDebug ctest: 1167 tests, 0 failed.

### [ ] T93 - Make upload-buffer recycling non-throwing

**Evidence:** `src\Azure\Impl\WriteableFileImpl.ipp:200-215` calls
`Pool->Free.push_back(std::move(Data))` from `PooledPayload::~PooledPayload`.
The free list is a dynamically growing vector without reserved slots
(`...\Impl\WriteableFileImpl.hpp:40-47`).

**Impact:** A free-list allocation failure escapes an implicitly `noexcept`
destructor and terminates the process, including when releasing a successful
upload on the host executor or unwinding another failure.

**Action:** Use bounded, allocation-free return storage, or reserve the bounded
free-list capacity safely during tracker construction. Alternatively, narrowly
handle recycling allocation failure and release the buffer rather than retain
it. Keep the existing four-upload bound; do not catch unrelated failures around
the whole upload operation. The download pool's best-effort return policy is
useful prior art.

**Acceptance:** Force the first free-list-growth allocation to fail while a
payload is released. The process must survive, upload tracking must drain, and
the upload's real result must be preserved. Check warmed steady-state allocation
counts so the fix does not add a per-upload allocation.

### [ ] T94 - Do not hide ambiguous Azure mutations inside HTTP stale-socket retries

**Evidence:** `libs\HttpClient\src\private\RequestOperation.cpp:151-191,398-400`
allows a reused connection's failed request to be resent after it may have been
sent, based only on the HTTP verb. Both PUT and DELETE qualify. Azure's separate
pipeline deliberately refuses ambiguous replay of ordinary DELETE and
conditional create requests
(`libs\AzureClient\src\RequestPipeline.cpp:126-133`,
`Detail\RequestPipeline.hpp:519-525`), but never sees the first error when the
HTTP layer retries it internally.

**Impact:** An ordinary `DeleteAsync` that actually deleted the blob but lost
its response can be silently resent and return 404 instead of the original
ambiguous transport failure. Similar replay changes conditional-mutation
outcomes. The fake-transport tests in `RetryTests.cpp` exercise only the Azure
retry layer, so they cannot detect this production-transport discrepancy.

**Action:** Make the transport respect a concrete per-request replay policy,
or leave ambiguous Azure-mutation replay to the Azure retry layer. Retrying
before any bytes could have been sent and retrying safe reads should remain
cheap. Do not solve this by disabling pooling across the client.

**Acceptance:** Add loopback tests with the real HTTP transport: warm a pooled
connection, accept a DELETE or conditional PUT, apply its side effect and drop
the response. Assert the intended error and number of requests. Include plain
DELETE versus `DeleteIfExistsAsync` and confirm safe GET stale-socket recovery
still works.

### [ ] T95 - Require complete gap reads before read/modify/write uploads

**Evidence:** `src\Azure\Impl\ReadWriteFileImpl.ipp:113-123,157-174` knows the
length of every existing-data gap, but `finishRead` discards the byte count from
`BlobOperations::FinishDownload`. The general download API legitimately permits
a short read when the blob ends earlier than the requested range.

**Impact:** A short successful gap download is not a successful preservation of
the old page. Uploading the extent can replace untouched bytes with zeros or
bytes retained in a recycled extent buffer. `WriteableFileImpl` already rejects
short reads of the reopened partial page; this mutating path needs the same
invariant.

**Action:** Track the expected length alongside each pending gap read and reject
a short result before starting any extent upload in that wave. Reuse/extract
the existing exact-read validation where sensible, without making ordinary
read-only EOF handling fail.

**Acceptance:** Extend `ReadWriteFileTests.cpp`: seed a full page with a pattern,
write a small subrange, make its preservation read succeed with too few bytes,
and assert failure with no upload for that wave. Repeat with a reused extent
buffer holding a different pattern, then restore the download and verify a retry.
Keep draining all already-started reads before releasing their buffers.

### [ ] T96 - Recover async runners when an internal continuation throws

**Evidence:** `Detail\RequestPipeline.hpp:194-218` sets `m_draining` before
dispatch with no exception cleanup; `StartAttempt:319-323` builds/copies the
request outside the narrower signing/rebuild catches. A retry or 401 token
refresh can execute that code from a transport/timer callback. The download
runner similarly sets `m_processing` and calls `Handle` without recovery
(`Detail\BlobDownload.hpp:169-189`); signal/map allocation and authorized-send
initiation at lines 250-265 are outside its request-building catch.

**Impact:** An internal setup/allocation exception after initial initiation can
escape the host event loop, leave the runner marked active and lose the expected
completion. Errors starting writer work or posting its result can also strand
the `m_writing` state. The read completion guard added in T80 does not make these
lower-level state machines exception-safe.

**Action:** Establish an explicit failure transition for internal continuation
and scheduling failures. Restore runner state, cancel/drain accepted operations
and complete once with the appropriate error. Distinguish internal setup throws
from a user's final completion throwing; do not disguise a user-handler exception
as a storage failure or invoke the handler again. Check batch continuation
initiation for the same accepted-work invariant while making the fix.

**Acceptance:** Use focused fault-injection tests in the existing retry and
parallel-download suites: throw during a second attempt, a later chunk start
and writer scheduling. Assert no exception escapes `run()`, one error completion,
no permanently active runner, and no released buffer still used by accepted I/O.

### [ ] T97 - Guard condition-variable upload waits on the host executor

**Evidence:** `src\Azure\Impl\WriteableFileImpl.ipp:98-104,303-308` waits for
upload callbacks without `AzureClient::ThrowIfRunningOn`. These waits are reached
by back-pressure during `Append`/`Flush` and by `Sync`/`Close`; the lower-level
future guards cannot help once the code is already waiting on the tracker.

**Impact:** A call made on a single-threaded host executor with pending uploads
deadlocks instead of failing promptly, despite the branch's explicit safeguards
at other blocking boundaries.

**Action:** Guard blocking public-operation waits before entering them, using
the blob's concrete executor and the repository's existing misuse error policy.
Treat destructor drainage separately: it is `noexcept` and must not throw or
release state still needed by uploads. Preserve/document its external lifetime
precondition and diagnose a violation rather than silently pretending to drain.

**Acceptance:** Hold an upload pending, call the blocking operation from its
executor and verify a prompt failure rather than a hang. Confirm normal
RocksDB-thread back-pressure and shutdown drainage still work.

## P2: cache correctness and consistency

### [ ] T98 - Roll back the new temporary LRU pin when reservation throws

**Evidence:** `src\Core\LruFileIndex.cpp:60-89` now increments the existing
entry's `pinCount`, discounts its size and calls `EvictUntilSizeLocked`.
The decrement/restoration happens only on normal return. Eviction can throw
while reserving its output vector or constructing path strings.

**Impact:** A failed replacement reservation can leave the existing entry
permanently pinned. Size discount exception safety was already weak in `main`;
the branch adds the leaked pin, making this a directly coupled regression rather
than a request for a general cache rewrite.

**Action:** Restore the temporary pin and size discount on every exit using
non-allocating scope cleanup under the existing mutex. Do not invalidate the
stored iterator or subtract already-evicted entries twice. Keep deferred disk
I/O outside the index lock.

**Acceptance:** In the existing LRU/secondary-cache tests, fail allocation before
eviction and during path preparation for a same-key replacement. Afterwards,
usage must match surviving entries, and shrinking the cache must still evict
the temporarily pinned entry. Check the existing replacement-generation pin
tests. This finding is statically traced, not runtime fault-injection verified
by this review.

### [ ] T99 - Verify that partial-prefetch reads cannot return mixed blob versions

**Verify first; medium confidence.**

**Evidence:** `src\Azure\Impl\ReadableFileImpl.ipp:157-169` copies a prefetched
prefix into caller scratch and then downloads only the suffix.
`DownloadWithRetry:661-700` can refresh metadata after a 412 and return the suffix
from a new ETag. Clearing the prefetch at that point cannot undo the prefix
already copied into scratch. The existing partial-overlap test in
`AsyncReadTests.cpp:381-402` covers only a stable blob.

**Action:** First reproduce an overwrite between prefetch completion and the
suffix download. If the read returns old-prefix/new-suffix success, bind the
whole read to one metadata version or restart the entire requested range when
the version changes. Keep the suffix-only optimization for a stable ETag and
preserve the existing bounded stale-read retry budget.

**Acceptance:** Add a deterministic deferred-transport test with distinct old
and new full-file patterns. A successful read must contain one version, not a
mixture; exhausting version changes must return the existing stale-read error.
Do not remove the stable-blob optimization without measuring its replacement.

## P2: performance and measurement reliability

### [ ] T100 - Construct the DNS leader guard without a live temporary

**Evidence:** `libs\HttpClient\src\private\RequestOperation.cpp:224-236`
constructs the shared guard with
`std::make_shared<LeaderGuard>(LeaderGuard{...})`. `LeaderGuard` has a destructor
but no move operation transferring/disarming its cleanup responsibility. The
temporary's destructor calls `DnsCache::Finish(operation_aborted)` immediately
at the end of that expression, before `async_resolve` starts.

**Impact:** The pending-leader entry is removed prematurely, so subsequent
requests can each become another DNS leader. An older leader can also publish
against a replacement pending entry. This defeats the intended burst
coalescing and makes cancellation testing less representative.

**Action:** Construct the guard directly in shared storage and make cleanup
ownership non-copyable (or explicitly transferable with the source disarmed).
Ensure failure after claiming leadership also releases that claim.

**Acceptance:** Test the actual guard/start path, not only `BasicDnsCache` in
isolation. Hold resolution pending, submit multiple same-origin requests, verify
one leader/lookup and one publication, then cover leader cancellation and
abandonment. An isolated current-header probe confirmed that a follower becomes
a second leader immediately after the existing guard-construction expression.

### [ ] T101 - Publish download-pool byte accounting with the buffer

**Evidence:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\DownloadBufferPool.hpp:118-136`
pushes a returned buffer under its size-class mutex, unlocks, then increments
`m_pooledBytes`. `Acquire:69-75` can remove that buffer and decrement the counter
between those operations.

**Impact:** Starting from an empty pool, the counter transiently wraps below zero
to a huge `size_t` value. Concurrent returns/evictions can interpret that value
as severe over-capacity and discard otherwise useful buffers, undermining reuse.
The existing pool tests in `BlobOperationsTests.cpp:505-577` are single-threaded
and only check settled accounting.

**Action:** Update byte ownership inside the same size-class critical section
that makes a buffer visible/invisible. Keep actual deallocation outside locks
and preserve the per-class and total retention bounds. Stronger atomic memory
ordering alone does not fix the publication order.

**Acceptance:** Add a controlled return/acquire interleaving or suitable narrow
test hook, plus concurrent churn across classes. Accounting must not wrap and
settled retained bytes must match free buffers. Measure allocation/reuse under
contention, and run the relevant tests under TSan on Linux.

### [ ] T102 - Make failed or unparsed benchmark runs explicitly incomplete

**Evidence:** `tools\db_bench\Compare-Backends.ps1:108-126` returns `$null` for a
nonzero benchmark exit but does not propagate a final failed status. A zero-exit
process missing an expected metric returns a truthy map containing `Seconds`;
callers such as lines 155-156 divide the missing map value by 1000, which can
record a misleading zero. Results are averaged/exported at lines 213-231 without
requiring valid paired samples.

**Impact:** A failed backend can produce an apparently successful comparison
with missing samples, or an output-format change can look like a dramatic
performance improvement. This is especially problematic for the branch's
performance acceptance workflow.

**Action:** Validate expected metric keys and numeric values for each requested
benchmark, retain explicit failure records, and return a failing/incomplete
status when requested paired comparisons are unavailable. Keep partial logs and
diagnostics; never turn missing measurements into zero latency. Preserve the
existing bounded load retry policy rather than silently retrying every failure.

**Acceptance:** Test with small fake baseline/candidate executables that require
no Azure access: nonzero exit, zero exit without metrics, malformed metrics,
one missing backend, and valid paired output. Only real measurements may appear
as numeric samples in `summary.csv`.

## P3: best practices and simplifications

### [ ] T103 - Include the new template implementation files in formatting checks

**Evidence:** `.github\hooks\pre-commit:35` and
`.github\workflows\build-and-test.yml:44` match only
`c|cc|cpp|cxx|h|hh|hpp|hxx`. The branch moves substantial production
implementation into `.ipp` files, including the read/write and filesystem
definitions; those files are now outside both checks.

**Action:** Add `.ipp` consistently to the hook and CI selection, and update
documented file-selection commands where needed. Include the new benchmark C++
source under `tools` if it is meant to follow the same policy. Use the pinned
formatter version and keep hook/CI behavior aligned.

**Acceptance:** Demonstrate that an intentionally unformatted representative
`.ipp` is selected and rejected by the check, then restore/format it. Confirm
ordinary C++ sources remain selected. Do not combine this with unrelated bulk
formatting changes.

### [ ] T104 - Use concrete ownership for the HTTP pending request state

**Evidence:** `libs\HttpClient\src\private\HttpClientImpl.hpp:84` stores the
request's only possible state as `std::shared_ptr<void>`. Lines 343 and 364 cast
it back to `RequestState` unconditionally.

**Action:** Forward-declare the nested `RequestState` before `PendingRequest`
and store `std::shared_ptr<RequestState>` directly, arranging the later
definition around the existing `Limiter` alias. Remove the casts and obsolete
comment. This is a bounded type-safety simplification, not a new abstraction or
dispatch mechanism.

**Acceptance:** Existing limiter cancellation, failed-start, failed-post and
client-destruction tests pass, with unchanged submission allocation counts.

### [ ] T105 - Remove the obsolete upload-error accessor and stale wait wording

**Evidence:** `HasUploadError` is declared at
`src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\WriteableFileImpl.hpp:89`
and defined at `WriteableFileImpl.ipp:90-96`, but has no callers after the shared
`WaitForUploads` error path was adopted. The comment at lines 161-164 still says
`Flush` waits, although it only uses a nonblocking error-check predicate unless
starting another upload requires back-pressure.

**Action:** Remove the unused private helper and describe the actual distinction
between asynchronous flushing, bounded back-pressure and durability waits.
Do not reintroduce duplicate upload-error checks or change the public API.

**Acceptance:** Search for remaining references, build the implementation target
and keep the existing flush/upload-error tests passing. No new behavior test is
needed for removing an unused helper.

## Dependencies and recommended order

T91-T98 are independently actionable. T92 and T93 touch the same buffer-lifecycle
code and should be coordinated, but neither is a prerequisite for the other.
Implement T99 only after reproducing it. T100-T105 do not require the correctness
changes. T96 must preserve T91's work ownership while recovering failed
continuations. No performance task justifies dropping a correctness guarantee.

## Validation performed for this review

- Passed:
  `cmake --build build\WindowsDebug --config Debug --target aveva-http-client-tests aveva-azure-client-pipeline-tests aveva-rocksdb-plugin-azure-impl-tests -- /nologo /v:minimal`
- Passed, **169/169 tests**:
  `ctest --test-dir build\WindowsDebug -C Debug --output-on-failure --parallel 4 -R 'DnsCache|SharedDns|ReadWriteFileImplTests|WriteableFileTests|Retry|AsyncBehaviorTests.*(ForeignThread|WorkGuard)' -E Integration`
- Isolated C++ probes using current headers confirmed T91 and the guard-lifetime
  mechanism in T100. These were review artifacts outside the repository, not
  committed regression tests or end-to-end throughput benchmarks.
- Live Azure integration, Linux sanitizer runs and baseline/candidate
  performance comparisons were **not run**. The passing existing tests do not
  cover the newly identified failure/interleaving scenarios.

For subsequent native changes, use the focused selectors above or the owning
suite, then expand as warranted:

```powershell
cmake --build build\WindowsDebug --config Debug
ctest --test-dir build\WindowsDebug --output-on-failure --build-config Debug -E Integration
```

Configure with `cmake --preset WindowsDebug` only if needed; use the repository's
Linux presets/CI for sanitizer verification. Azure integration tests require
their documented opt-in and credentials. Do not print credentials or add them
to task notes, logs or commits.
