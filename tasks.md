# Review backlog (pass 4): `feature/trim-azure-client` vs `main`

Review scope: merge-base `278868c`, 509 commits, 357 files (+53.1k / −5.5k). This pass re-covered `libs\HttpClient`,
`libs\AzureClient`, the plugin read path and write/filesystem/lease code in `src\Azure`, `src\Core`, build/CI and test
infrastructure, and docs. Every finding below was checked against the code at `HEAD` (`34947f4`). Findings marked
**Verify first** are medium confidence: confirm or measure them before changing anything. Passes 1–3 (T01–T78) are
closed; their text is in git history at `HEAD:tasks.md` (commit `34947f4`), summarised at the end.

## Hard constraints for the executor

- **No type erasure.** Do not introduce `std::function`, `std::move_only_function`, `std::any`,
  `any_io_executor`, `any_completion_handler`, virtual interfaces that replace templates, or erasing pimpls. The
  branch exists to build zero-overhead, template-based async paths. Removing erasure that is already there is
  welcome.
- The RocksDB API boundary is fixed. For example, `FSRandomAccessFile::ReadAsync` takes a `std::function`, so
  `AsyncReadRequest::Callback` stays as it is.
- Never block with `.get()` or a wait on a thread that runs the host-supplied `io_context`. Guard any new blocking
  point with `AzureClient::ThrowIfRunningOn(executor)`.
- Keep the `AsyncReadRequest` invariants:
  - downloads go into a pooled buffer, never into the caller's scratch;
  - the scratch copy happens under the request lock;
  - RocksDB callbacks run only from `Poll`/`AbortIO`.
- Map every Azure client error to a `rocksdb::IOStatus`, and never swallow errors.
- Public headers in `include\AVEVA\RocksDB\Plugin\` must stay source-compatible.

## Executor rules

- Build: `cmake --build build\WindowsDebug --config Debug` (or the `LinuxDebug` preset).
- Test: `ctest --test-dir build\WindowsDebug --output-on-failure --build-config Debug -E Integration`
  (Integration tests need `AZURE_*` env vars).
- Run `clang-format` on touched files. Add new sources and tests to the owning `CMakeLists.txt`.
- Performance tasks: measure before and after, with a micro-benchmark, an allocation count (see
  `libs\HttpClient\tests\HttpClientAllocationTests.cpp`), or `tools\db_bench\Compare-Backends.ps1`. Put the numbers
  in the commit message. If a change shows no measurable gain *and* adds complexity, drop it and record why here.
- Do one task per commit, and tick its box when done (`[x]`; `[!]` blocked, with a reason; `[-]` dropped, with a
  reason).
- Never create or publish a PR without explicit user approval.

---

## P1: correctness

### [x] T79 – `StartFlush` rollback zeroes the buffered tail (silent data corruption on retry)
- **File:** `src\Azure\Impl\WriteableFileImpl.ipp:179-212` (`BasicWriteableFileImpl::StartFlush`).
- **Problem:** On an asynchronous flush (`includePartialPage == false`) with a partial-page tail:
  1. the tail `[fullBytes, m_bufferOffset)` is copied to the start of a rented buffer `next`;
  2. `std::swap(m_buffer, next)` makes the rented buffer current;
  3. `next.resize(bytesToWrite)` with `bytesToWrite == fullBytes` **truncates the tail off the original buffer**.

  If `StartUpload` then throws synchronously (`bad_alloc`, or any throw from `client.UploadPagesAsync` initiation;
  fencing is reported through the callback, not thrown), the catch block does
  `owner->Data.resize(m_bufferSize)`, which zero-fills `[fullBytes, m_bufferSize)`, and swaps it back.
  `m_bufferOffset` is unchanged, so the buffer now holds zeros where the tail bytes were. A synchronous throw does
  not set the sticky `m_uploads->Error`, so the next `Flush`/`Sync` succeeds and uploads zeros in place of real
  data. The comment ("Nothing was accepted, so the data stays buffered for a retry") is false for the tail. The
  catch also drops the rented buffer instead of returning it to `m_uploads->Free`.
- **Change:** Make the rollback restore the exact pre-call state. Simplest option: in the catch, after resizing,
  copy the tail back from the current `m_buffer` (`[0, tail)`) to `owner->Data[fullBytes, fullBytes + tail)` before
  the swap, then hand the rented buffer back to the pool. A cleaner alternative is to keep the original buffer
  untouched until `StartUpload` has accepted the payload, for example by building the payload owner first and only
  swapping and truncating after `StartUpload` returns. That works because the request views a `shared_ptr` to the
  vector, so `resize` must happen before the call. Pick whichever keeps the no-copy fast path.
- **Validation:** New `WriteableFileTests` case: a blob fake whose `UploadPagesAsync` throws synchronously once.
  Append `N*PageSize + 100` bytes, call `Flush()` and expect the exception, then `Sync()`. Assert that the uploaded
  bytes equal the appended bytes (the last 100 bytes must not be zero). The test must fail before the fix.

### [x] T80 – A read restarted from inside a completion can be lost (hang plus an exception into the host `io_context`)
- **Files:**
  - `src\Azure\Impl\ReadableFileImpl.ipp:459-523` (`ReadAsyncAttempt`, `RefreshMetadataAndReadAsync`)
  - `src\Azure\Impl\ReadableFileImpl.ipp:563-600` (`CoalescedMetadataAsync`, waiter loop)
  - `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp:597-634` (`DownloadAsync`),
    `:636-650` (`GetMetadataAsync`)
- **Problem:**
  - `ReadAsyncAttempt` is re-entered from completion handlers: the metadata-refresh waiter, and the
    `DownloadAsync` completion after a 412.
  - `DownloadAsync` can throw synchronously after it has taken ownership of the callback: `TransferOptions`,
    `DownloadBufferPool::Acquire` (`bad_alloc`), or the client initiation.
  - At the top level, `BasicReadableFile::ReadAsync` catches this and returns an `IOStatus`. In the nested case
    nothing catches it, with three effects:
    1. The read's callback is destroyed without being invoked, so the `AsyncReadRequest` never finishes. `Poll`
       never reports it, and `AsyncReadTracker` never drains, so filesystem teardown waits the full
       `ShutdownWaitTimeout`.
    2. The exception escapes into `io_context::run()` on a host thread.
    3. In `CoalescedMetadataAsync`, the remaining waiters in the loop are skipped, so their reads are lost too.
- **Change (no erasure):**
  1. In `BasicReadableFileImpl::ReadAsync`, wrap the callback in a small move-only template guard,
     `ReadCompletionGuard<TCallback>`. If the guard is destroyed without having been invoked and was not moved
     from, it invokes the callback with an `IOError`-mapped exception, for example
     `RequestFailedException(..., "Read abandoned")`. This is safe because the callback only completes the
     `AsyncReadRequest`; RocksDB callbacks still run from `Poll`. Mark the destructor `noexcept` and swallow
     anything the callback throws.
  2. In the `CoalescedMetadataAsync` completion and in the catch fallback, invoke each waiter under its own
     `try/catch` so that one waiter cannot skip the others. Once (1) is in place, a waiter that throws still
     completes its read through the guard.
  3. Make sure nothing propagates out of the `GetMetadataAsync`/`DownloadAsync` completion lambdas into the
     `io_context`.
- **Validation:** New `ReadableFileTests` cases:
  - A fake blob returns 412 on the first download and its second `DownloadToAsync` throws synchronously. Assert
    that `Poll` reports a non-OK status and that `AsyncReadTracker::InFlight()` returns to 0.
  - Two reads join one metadata refresh, and the first one's continuation throws. Assert that the second read
    still completes.

  Both must hang or fail before the fix. Do not reintroduce erasure: the guard is a template over `TCallback`.

### [x] T81 – `MultiRead` can leak an in-flight read that writes into freed memory
- **File:** `src\Azure\BasicReadableFile.ipp:155-315`.
- **Problem:** In the `start` lambda, `StartRead` succeeds and only then is the handle stored with
  `active.push_back(Active{handle, &group})`. If `push_back` throws (`bad_alloc`), the handle is in neither `active`
  nor `HandleCleanup`, so it is never deleted or aborted. The function returns through the function-try-block while
  the download is still running, and its completion later copies into `group.Buffer` (freed with `groups`) or into
  the caller's `reqs[i].scratch` (which RocksDB may have reused). Separately, on any batch-level exception, requests
  that never received an outcome keep stale `status`/`result` values from the caller.
- **Change:**
  - Call `active.reserve(std::min(kMultiReadMaxInFlight, groups.size()))` before the loop, so `push_back` can never
    allocate (and so never throw) after a read has started.
  - In the function-try-block handler (`reqs` and `num_reqs` are in scope), set every request that has no outcome
    to the returned status and an empty `Slice`. A `std::vector<bool>`, or a sentinel status assigned up front,
    tracks which requests have an outcome; keep per-request errors that were already assigned.
- **Validation:** `ReadableFileTests` MultiRead cases. Optionally add a fault-injection hook (the HttpClient
  `FaultPolicy` pattern) that throws after the first `StartRead`, and assert that every request has a non-OK status
  and that no read is still running after return (`AsyncReadTracker::InFlight() == 0` once the context is pumped).

### [x] T82 – HttpClient: a request aborted by the limiter can lose its completion
- **Files:** `libs\HttpClient\src\private\HttpClientImpl.hpp:360-364` (`AbortLimited`), `:113-118`
  (`LimiterSink::Abort`); callers in `libs\HttpClient\src\private\OriginLimiter.hpp:84, 167, 189, 263`.
- **Problem:** `AbortLimited` moves `pending.completion` into an `asio::post` lambda without a fallback. If `post`
  throws (allocation), the completion is destroyed unrun. The handler is never called and its work guard is never
  released, so the caller's `run()` can hang. The exception also propagates:
  - out of `OriginLimiter::Close()`, skipping every remaining queued abort, which loses those handlers too;
  - out of the deadline-timer handler, into the host `io_context`.

  `FailLater` (`:258-270`) already solves this for the submit path: it is `noexcept`, posts through the
  `RequestState` cell, and completes inline as a last resort.
- **Change:** `AbortLimited(PendingRequest&& pending, std::error_code error) noexcept` →
  `FailLater(std::static_pointer_cast<RequestState>(std::move(pending.state)), error)`. The guarded completion in
  `pending.completion` only forwards to `cell->Finish`, so dropping it is correct; check that `pending.state` is
  never null on this path. This removes a second completion path and makes the abort exactly-once by construction.
- **Validation:** `OriginLimiterTests`, `HttpClientRobustnessTests` (limiter close, queued deadline, cancellation).
  Add a `FaultPolicy` hook that throws inside the abort post, and assert that the handler still runs exactly once.

### [x] T83 – Idle sweeper can stop scheduling for good after one failed `Schedule`
- **Files:** `libs\HttpClient\src\private\IdleSweeper.hpp:92-96` (`Notify`), `:203-212`
  (`SweepScheduler::Schedule`); `libs\HttpClient\src\private\ConnectionPool.hpp:365-369` (`Release` calling
  `m_notifier()`); `libs\HttpClient\src\private\RequestOperation.cpp:420-435` (`ReleaseOrClose`).
- **Problem:** `Notify` sets `m_scheduled` with `exchange(true)` *before* calling `Schedule`. `Schedule` can throw:
  `queue.emplace` can throw `bad_alloc`, and creating the `std::jthread` can throw `system_error`. When it does,
  `m_scheduled` stays `true` forever, every later `Notify` is a no-op, and idle connections are never swept again
  (no idle timeout, sockets held until the client is destroyed). The exception then leaves
  `ConnectionPool::Release` *after* the pool has taken the connection, and `ReleaseOrClose` reports
  "Returning the connection to the pool failed" for a connection that was in fact pooled.
- **Change:**
  - In `Notify`, catch exceptions from `Schedule`, reset `m_scheduled = false`, and report through the
    diagnostics sink. The next release then retries scheduling.
  - Make `SweeperNotifier::operator()` `noexcept`, and add a `static_assert(std::is_nothrow_invocable_v<const
    Notifier&>)` in `ConnectionPool`. `Release` then cannot fail after taking ownership.
- **Validation:** `IdleSweeperTests` and `ConnectionPoolTests`. Add a case with an injected scheduler failure on the
  first `Schedule`, and assert that a later release schedules a sweep and that the idle connection expires.

### [x] T84 – `LockFileImpl::Lock` reports success without a lease when the acquire timeout is not positive
- **File:** `src\Azure\Impl\LockFileImpl.ipp:48-63` (constructor), `:94-145` (`Lock`).
- **Problem:** With `acquireTimeout <= 0` (or `leaseLength <= 0` and no explicit timeout, since the timeout defaults
  to the lease length), `while ((end - start) < m_acquireTimeout)` never runs. `lastError` is empty, so `Lock()`
  sets `m_held = true`, publishes a lease deadline and returns `true`, although no lease was acquired and `m_leaseId`
  is unset. Writes would then proceed with no fencing. This is latent today because the values come from
  `Configuration` (`LeaseLength = 20s`), but nothing enforces it and tests construct the class directly.
- **Change:** In the constructor, throw `std::invalid_argument` when `leaseLength <= 0s` or
  `m_acquireTimeout <= 0ms`. Also check that Azure accepts the lease length: it must be 15–60 s or −1 for
  infinite. Validate that range too, unless infinite is a supported mode, in which case document it. Defensively,
  at the end of `Lock()`, require `m_leaseId.has_value()` before setting `m_held`.
- **Validation:** `LockFileImplTests`: zero and negative timeouts throw at construction. Existing
  `LeaseSafetyTests` still pass.

---

## P2: performance

### [x] T85 – `Flush` uploads every completed page at once, which can block the WAL writer (**Verify first**: measure)
- **Files:** `src\Azure\Impl\WriteableFileImpl.ipp:140-212` (`Flush`, `RangeSync`, `StartFlush`); constants in
  `WriteableFileImpl.hpp:50` (`MaxInFlightUploads = 4`). Check whether `ReadWriteFileImpl.ipp` has the same pattern.
- **Problem:** RocksDB calls `Flush` after every WAL record. Whenever at least one complete page is buffered,
  `StartFlush(false)` starts a PutPage for those pages. A WAL with records of a few hundred bytes therefore sends
  roughly one request per 512 B page boundary crossed. Every fifth such flush blocks the RocksDB write thread for a
  network round trip in `WaitForUploads(MaxInFlightUploads - 1)`. Readers cannot see that data until `Sync` publishes
  the size metadata (`BlobHelpers::FileSizeFromProperties` reads only the `size` metadata key), so the early uploads
  give no visibility benefit. Sync durability is unchanged either way.
- **Change:** Start an upload from `Flush`/`RangeSync` only when the full-page bytes reach a threshold. Fair options
  are half the buffer, or a new `Configuration::PageBlob::MinAsyncFlushBytes` such as 256 KiB. `Append` (buffer
  nearly full) and `Sync`/`Close` keep their current behaviour. Keep the sticky-error and fencing checks in `Flush`.
- **Validation:** Use `Compare-Backends.ps1` with a WAL-heavy workload (`fillrandom` with `--sync=0`, small values),
  and count requests per MB written before and after with the fake blob server's request log. Run
  `WriteableFileTests` (including `Flush_ReusesBuffersAndKeepsTheTailAcrossTheSwap`, which may need its thresholds
  adjusted). Drop the change if throughput does not improve.
- **Done:** Flush/RangeSync start an upload only when at least half the buffer (64 KiB by default) is filled. Measured with
  a temporary 300 B append+Flush loop of 16 MiB against the fake blob server (Debug): 32768 -> 256 uploads (2048 -> 16 per
  MB) and 5407 ms -> 84 ms. Tests that relied on an upload after a single page now use a small buffer.

### [-] T86 – Batch delete rebuilds each sub-request from scratch on every attempt
- **File:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobBatch.hpp:189-210` (`BuildChunkBody`).
- **Problem:** For each of up to 256 sub-requests, on every retry, the loop builds a new `HttpRequest` (header
  vector), formats the `x-ms-date` value again with `BuildDateHeaderValue(system_clock::now())`, and appends to a
  `body` string that started with no reserved capacity, so the body reallocates about log2(size) times. Within one
  call the date has one-second resolution and is effectively the same for every part.
- **Change:**
  - Format the date once per `BuildChunkBody` call.
  - Reuse one `HttpRequest` across iterations by clearing its headers, so the vector keeps its capacity.
  - Before the loop, call `body.reserve(...)` with an estimate: per part, the fixed header bytes plus
    `urls[i].size()` plus the boundary length.

  Each sub-request must still be signed separately.
- **Validation:** `BatchDeleteTests` (body shape and the retry tests). Count allocations for a 256-blob chunk before
  and after. Batch delete is not on the read/write hot path, so drop the change if it adds complexity for less than
  a 2x allocation reduction.
- **Dropped:** Measured with a counting `operator new` on a 256-blob chunk (Debug, token auth, 1100 B token): the
  current loop makes 11285 allocations per attempt; one date/bearer string, a reused `HttpRequest` and a reserved body
  make 5903 (1.9x, about 44 -> 23 per part). That is under the 2x bar, the remaining cost is the per-header string
  copies in `AddHeader`/`AppendBatchDeletePart`, and batch delete is off the hot path, so the extra state is not worth it.

---

## P3: cleanups / simplifications / header hygiene

### [x] T87 – `Flush` duplicates `WaitForUploads`' error reporting
- **File:** `src\Azure\Impl\WriteableFileImpl.ipp:144-153` and `:274-283`.
- **Problem:** `Flush` repeats the "rethrow sticky upload error, then `ThrowIfWritesFenced`" sequence that
  `WaitForUploads` already contains.
- **Change:** `Flush()` → `StartFlush(false); WaitForUploads(std::numeric_limits<size_t>::max());`. The predicate is
  immediately true, so it never waits. Alternatively, extract a `ThrowIfUploadFailed()` helper and use it in both.
  Do this after T85 if T85 changes `Flush`.
- **Validation:** `WriteableFileTests` (`Flush_AfterAnEarlierUploadFailed_ReportsTheFailure`, fencing cases).

### [x] T88 – Move the download-writer pool state out of the public `HttpClient.hpp`
- **File:** `libs\HttpClient\include\AVEVA\HttpClient\HttpClient.hpp:23-25, 147-152`; `libs\HttpClient\src\HttpClient.cpp`.
- **Problem:** T76 added `std::mutex m_writerMutex` and `std::unique_ptr<boost::asio::thread_pool> m_writerPool` as
  members of the public class, which pulls `<mutex>` into every consumer. `HttpClient` already has a private
  `Impl`, and `GetDownloadWriter()` is out-of-line and not on a hot path.
- **Change:** Move both members into `HttpClient::Impl`, or into a small struct owned by it. Keep the destructor's
  ordering: the pool is joined before `m_impl`'s transport is torn down. `<boost/asio/thread_pool.hpp>` must stay,
  because `GetDownloadWriter()` returns `thread_pool::executor_type`. Drop `<mutex>` if nothing else in the header
  uses it. This adds no erasure and no extra indirection on a request path.
- **Validation:** `HttpClientConstructionTests`, `DownloadToFileTests`, the `PublicHeaderSelfContainedTests`, and
  the T72 install-consume job.

### [-] T89 – Lighter `AsyncInitiation.hpp`: do not pull `<future>`/`<thread>` into every client header (**Verify first**)
- **Files:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\AsyncInitiation.hpp:8`;
  `libs\AzureClient\include\AVEVA\AzureClient\BlockOn.hpp`.
- **Problem:** `AsyncInitiation.hpp` only needs `BlockOnToken`, `IsBlockOnToken` and `ThrowIfRunningOn`, but it
  includes all of `BlockOn.hpp`, which brings `<future>`, `<thread>`, `<atomic>` and `io_context.hpp` into every
  public client header.
- **Hazard:** The `async_result<BlockOnToken, …>` specialization must be visible wherever `block_on` is used, and
  users get `block_on` through the client headers today. Splitting the header without keeping the specialization
  reachable would break `client.XAsync(block_on)` for anyone who does not include `BlockOn.hpp` directly.
- **Change, only if worth it:**
  - Move `BlockOnToken`, `block_on`, `IsBlockOnToken` and `ThrowIfRunningOn` into `Detail\BlockOnToken.hpp`.
  - Keep `BlockOn.hpp` as that header plus the specialization.
  - Have the client headers (`BlobClient.hpp`, `PageBlobClient.hpp`, `BlobContainerClient.hpp`,
    `BlobServiceClient.hpp`) keep including `BlockOn.hpp`, so the public behaviour is unchanged.
  - Have only the `Detail` headers include the light header.

  This only pays off for TUs that include `Detail` headers without a client header. Measure first; if no real TU
  benefits, mark the task `[-]` with the numbers.
- **Validation:** Preprocessed line count and compile time of `src\Azure\Impl\ReadableFileImpl.cpp` and
  `libs\AzureClient\src\BlobBatch.cpp` before and after; `PublicHeaderSelfContainedTests`.
- **Dropped:** Measured with `cl /P` (MSVC 14.51, C++latest, Debug flags). `ReadableFileImpl.cpp` includes the client
  headers anyway, so it gains nothing. Only `BlobBatch.cpp` includes `Detail` headers without a client header; it
  preprocesses to 208607 lines. Only `<future>` is exclusive to `BlockOn.hpp`: `<thread>` and `<atomic>` come in
  through asio, and `ThrowIfRunningOn` itself needs `io_context.hpp`, `<thread>` and `<atomic>`, so they would stay in
  the light header. A probe TU showed `<future>` adds about 17.7k lines (162586 -> 180286, roughly 8% of that one TU).
  That is one internal TU, and the split adds a header and a second place to keep the `async_result` specialization
  reachable, for no gain to any client or plugin TU, so it is not worth the complexity. No code changed, so no build
  or tests were run.

### [x] T90 – Diagnose filesystem teardown on the client executor instead of waiting out the timeout
- **File:** `src\Azure\Impl\BlobFilesystemImpl.ipp:343-366` (`~BasicBlobFilesystemImpl`); precondition documented in
  `include\AVEVA\RocksDB\Plugin\Azure\Plugin.hpp:40-46`.
- **Problem:** Destroying the filesystem on a thread that runs the client `io_context` is documented as a host bug.
  With a single-threaded context it cannot drain, because the reads' completions need that thread. Today the
  destructor still warns every 30 s for two minutes and then carries on, so the bug looks like an unexplained
  two-minute hang followed by possible use-after-free.
- **Change:** At the top of the destructor, if `m_asyncReads->InFlight() > 0` and the client executor's
  `running_in_this_thread()` is true, log one `error` that names the precondition violation, and add an `assert`
  for debug builds. Keep waiting as before, because a multi-threaded context can still drain. Do not throw from the
  destructor.
- **Validation:** A `BlobFilesystemFakeTests` case that destroys the filesystem from a handler posted to its own
  context while a read is outstanding, with a second context thread so it can drain, and asserts that the error is
  logged.
- **Done:** The `assert` was left out: it would abort the required debug-build test, which destroys the filesystem on
  the executor on purpose and must survive. The error log carries the diagnosis instead.
---

## Reviewed in pass 4 and rejected (do not redo)

- **"`RequestOperation::Read` reserves 16 KiB for every response":** the read buffer travels with the pooled
  connection, and `TrimIdleBuffer` keeps up to `MaxRetainedBufferCapacity` (≥ `ReadChunk`, see the `static_assert`
  in `ConnectionPool.hpp:79`). The reserve is paid once per connection, not once per request, and the TLS handshake
  dwarfs it.
- **"`<functional>` is unused in `AsyncInitiation.hpp`":** this is wrong, because the header uses `std::invoke`
  (`:161`).
- **"`FileCache::WaitUntilIdle` should wait for `m_failedDeletes`":** failed deletes are retried only when other work
  wakes the worker, and they can keep failing (a file locked on Windows). Adding them to the idle predicate would
  make `WaitUntilIdle()` with no timeout hang. The documented contract (no queued or in-flight work) is the intended
  one.
- **Stale duplicate `include\AVEVA\RocksDB\Plugin\Azure\Impl\*.hpp`:** only `Configuration.hpp` remains there. The
  rest moved to `src\Azure\Impl\include`, and there are no duplicates.

## Reviewed in pass 4 and judged sound (no task)

- `FailLater` exactly-once fallback; `RequestState` slot release ordering; `ConnectionPool::Release` strong exception
  safety up to the notifier (see T83).
- `AsyncReadTracker::Registration` posting `End()` when released on the executor.
- `WriteableFileImpl` upload pooling and back-pressure, sticky upload error, fencing re-check after in-flight
  uploads, and the reopen tail treated as already flushed.
- `LockFileImpl` reuse of the proposed lease ID across acquire retries, and per-attempt deadline bounding.

---

## Prior passes (T01–T78): closed, do not redo

- **Pass 3 (T67–T78):**
  - **Done:** T67 partial-cancellation filter; T50 Direct-path dispatch off foreign threads; T68 remaining error
    codes plus one `IsLeaseLost`; T69 closed-variant `CancelState`; T58 `ClientRuntimeBase` split into
    `WriteFence`, `BlobMetadataCache` and `ClientRuntimeTransport.hpp`; T72 per-package install-consume; T73 one
    exception ladder; T74 read-path nits; T75 `DnsCache` key-view probe; T76 client-owned writer pool; T77 include
    cleanup; T78 docs.
  - **Dropped after measurement:**
    - T70: remaining erasure on `RenewAsync`, prefetch waiters and `MetadataCallback`. Making these concrete needs
      re-erasure or header weight for an unmeasurable gain.
    - T71: one allocation fewer per `ReadAsync`, which is about 0.5%.
- **Pass 3 rejected or sound:**
  - `IdleSweeper::Stop` waits correctly.
  - The ignored-range 200 body is bounded.
  - Public-config callbacks (`AssertionProvider`, `SourceFailureObserver`, `HttpDiagnosticsCallback`,
    `FileSystemFactory`) may stay erased.
  - `TokenCredential` waiters as `any_completion_handler` are deliberate.
  - `MultiRead` `active.erase` is fine.
- **Pass 2 (T28–T66):** all done except T43, which was skipped because the merged-group copy is under 1%.
- **Pass 1 (T01–T27):**
  - **Done:** T01, T03–T12, T15, T17–T21, T26, T27.
  - **Dropped after measurement:** T16, T22–T25 (T25 because the `BlockOn` stack promise is unsafe).
  - **Rejected:**
    - `LruFileIndex::RegisterEntry` erasing a pinned entry;
    - rename always copying through a buffer;
    - `LeaseRenewalLoop::Fail` race;
    - public-header `unique_ptr` pimpl;
    - AzureClient `cxx_std_23` PUBLIC→PRIVATE;
    - one strand per client;
    - ISO-8601 with more than 7 fraction digits;
    - retry pipeline body sharing on a single attempt;
    - TSan label split.
