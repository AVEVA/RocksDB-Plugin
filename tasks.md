# Review backlog: `feature/trim-azure-client` vs `main`

Review scope: merge-base `278868c`, ~428 commits, 347 files (+50.7k / −5.4k). Production code reviewed:
`libs/HttpClient`, `libs/AzureClient`, `src/Azure`, `src/Core`, plus build/CI. `src/Core/FileCache*` is out of scope. Each finding below was checked
against the code unless it says **Verify first**.

## Hard constraints for the executor

- **No new type erasure.** Do not introduce `std::function`, `std::move_only_function`, `std::any`,
  `any_io_executor`, `any_completion_handler`, virtual interfaces that replace templates, or erasing pimpls. The
  branch's goal is zero-overhead, template-based async paths. **Removing** existing erasure from hot paths is
  encouraged.
- The RocksDB API boundary is fixed. For example, `FSRandomAccessFile::ReadAsync` takes a `std::function`, so keep
  it as-is and remove erasure only *behind* it.
- Never call `.get()` / `future.get()` / blocking waits on a thread that runs the host-supplied `io_context`. Guard
  new blocking points with `AzureClient::ThrowIfRunningOn(executor)` (BlockOn.hpp).
- Preserve the `AsyncReadRequest` invariants:
  - Downloads write into a pooled buffer, never into the caller's scratch (scratch may be freed after abort).
  - The copy into scratch happens under the request lock.
  - The RocksDB callback runs only from `Poll`/`AbortIO` on RocksDB threads.
- Map every Azure client error to a `rocksdb::Status`; never swallow errors.
- Public headers in `include/AVEVA/RocksDB/Plugin/` must stay source-compatible.

## Executor rules

- Build: `cmake --build build\WindowsDebug --config Debug` (or the `LinuxDebug` preset).
- Test: `ctest --test-dir build\WindowsDebug --output-on-failure --build-config Debug -E Integration`
  (Integration tests need `AZURE_*` env vars).
- Run `clang-format` on touched files. New sources and tests must be added to the owning `CMakeLists.txt`.
- For every performance task, measure before and after. Use a micro-benchmark or allocation count, or
  `tools\db_bench\Compare-Backends.ps1` for end-to-end. Record the numbers in the commit message. Drop a change
  that shows no measurable gain *and* adds complexity.
- Do one task per commit, and tick the box when it is done.
- Never create or publish a PR without explicit user approval.

---

## P1: correctness / significant performance

### [x] T01 – Add io_context guard to `MultiRead` and async-read waits
- **Files:**
  - `src\Azure\BasicReadableFile.ipp` (MultiRead ~140-290: `signal->WaitPast`)
  - the `PollAsyncReads` / `AsyncReadSignal` wait
- **Problem:** These are blocking waits with no `ThrowIfRunningOn` guard. Every other blocking path has one
  (BlobOperations.hpp ~237/311/470, LockFileImpl.ipp ~250, AsyncInitiation.hpp ~132). If called on the host
  io_context thread, the wait deadlocks, because the completion it waits for must run on that thread.
- **Change:** Call `AzureClient::ThrowIfRunningOn(client executor)` before the first wait. Translate the
  `std::logic_error` to `IOStatus::NotSupported`/`InvalidArgument`, consistent with the other guarded paths.
- **Validation:** Add a unit test that calls MultiRead / Poll from inside an `io_context` handler and expects an
  error, not a hang.

### [x] T03 – Avoid synchronous GetProperties on every file open
- **Files:**
  - `src\Azure\Impl\ReadableFileImpl.ipp` (ctor ~65-79)
  - `ClientRuntime.hpp/.cpp` (`CachedBlobSize`)
- **Problem:** Each `NewRandomAccessFile`/`NewSequentialFile` does a blocking `BlobOperations::GetMetadata` round
  trip. RocksDB opens many SST files, so this is pure latency.
- **Change (pick one; document the choice):**
  - (a) Cache `(size, ETag)` in the runtime next to `CachedBlobSize`. Populate it on write-close/rename, and
    invalidate it on delete/rename.
  - (b) Defer lazily. The first ranged GET returns ETag + `Content-Range` total. `GetFileSize` forces the fetch.
    Note the tradeoff: `NotFound` would surface on the first read instead of at open. RocksDB tolerates that for
    reads, but check `FileExists`-style callers.
- **Validation:** The tests asserting NotFound-on-open still pass, or are updated deliberately. Count requests
  per open in a test with the fake HTTP client.

### [x] T04 – Remove zero-fill and per-group vectors in `MultiRead`
- **File:** `src\Azure\BasicReadableFile.ipp` MultiRead.
- **Problems:**
  - `group.Buffer.resize()` zero-fills up to 8 MiB (`kMultiReadMaxMergedBytes`) per merged group.
  - Each group allocates `std::vector<size_t> Members`, even though groups are contiguous runs of `order`.
  - The `active` scan calls `Finished()`, which locks each request's mutex on every pass.
- **Change:**
  - Use `DownloadBufferPool` (or `std::make_unique_for_overwrite<char[]>`) for the merged buffer.
  - Represent members as a `[begin,end)` range into `order`.
  - Track completion with a counter or atomic that the completion decrements, instead of polling each mutex.
- **Validation:** Existing MultiRead tests (merged, unmerged, partial failure). Allocation count drops.

### [x] T05 – Shrink per-read allocations in the async read path
- **Files:**
  - `src\Azure\Impl\AsyncReadRequest.cpp/.hpp`
  - `src\Azure\BasicReadableFile.ipp` (StartRead ~55-135)
- **Problems:**
  - Each read makes a `make_shared` request (mutex + cv), a `make_unique` handle, a `std::function` callback, and a
    separate `done` `std::function` per MultiRead group.
  - Completion notifies both the request's cv and the `AsyncReadSignal`.
  - The prefetch resume path stores a `std::move_only_function` and allocates `std::string bytes(length)` per
    resume. Data is copied twice (prefetch → bytes → scratch), and the `download` lambda is copied into resume.
- **Change:**
  - Make `StartRead` an internal template on the completion type, so MultiRead / ReadAsync pass concrete lambdas.
    The RocksDB `std::function` from `ReadAsync` is stored once, as received.
  - Replace the per-request cv with an atomic state plus the shared signal (wait on the cv only if needed).
  - Copy from the prefetch buffer straight into the pooled buffer / scratch, without the intermediate string.
  - Move `download`, don't copy it.
- **Validation:** The async read tests, including abort-while-in-flight and poll ordering, still pass. Allocation
  count per read drops.

### [x] T06 – Remove erasure from the download-writer path
- **Files:**
  - `libs\AzureClient\include\AVEVA\AzureClient\BlobOperationOptions.hpp:75` (`DownloadWriteScheduler`)
  - `Detail\BlobDownload.hpp:55` (`RunOnWriter`)
  - `src\Azure\Impl\ClientRuntime.cpp` (`DownloadWriter()`)
- **Problem:** `std::function<void(std::move_only_function<void()>)>` is double erasure, built per download and
  invoked per written chunk.
- **Change:**
  - Pass an executor (the runtime's `thread_pool::executor_type`) or make the scheduler a template parameter, then
    use `boost::asio::post(executor, task)` with the concrete lambda.
  - Keep the fallback "no scheduler → HTTP client writer → detached thread" ordering via `if constexpr`.
- **Validation:** BlobDownload file/stream tests and plugin download tests.

### [x] T07 – Make `DownloadSink` static
- **Files:**
  - `libs\AzureClient\include\AVEVA\AzureClient\Detail\DownloadSinks.hpp`
  - `Detail\BlobDownload.hpp` (`DownloadOperation` ctor takes `std::shared_ptr<DownloadSink>`)
- **Problem:** A virtual base and a `shared_ptr` allocation per download. `TryWrite` dispatches virtually per
  chunk, `WritesInline` is a virtual query, and `SpanSink` is the plugin's hot path.
- **Change:**
  - Template `DownloadOperation` on `TSink`, and store the sink by value inside the operation (it is already
    `shared_from_this`).
  - `WritesInline` becomes `static constexpr`.
  - `FileSink`'s `shared_ptr<ofstream>` can become a member `std::ofstream`.
- **Validation:** All download tests. Check the compile-time impact against `ClientInstantiations.cpp`.

### [x] T08 – Lock-free write-deadline check
- **File:** `src\Azure\Impl\ClientRuntime.cpp/.hpp` (`WriteTimeRemaining`, `ThrowIfWritesFenced`).
- **Problem:** Every mutation takes the global `m_deadlineMutex` and calls the `std::function` `WriteDeadlineSource`.
- **Change:**
  - Have the lease loop publish the deadline into a `std::atomic<steady_clock::rep>` (plus the fenced flag), and
    read it with acquire.
  - Keep the test-clock hook only in test builds, or as a template/static seam, not as a per-call `std::function`.
  - Do the same for `MetadataNow()`.
- **Validation:** Lease/fencing tests, including the "write after lease loss is rejected" ones.

---

## P2: improvements / cleanups

### [x] T09 – `ReadableFileImpl` in-place construction
- **Files:**
  - `src\Azure\Impl\include\...\ReadableFileImpl.hpp`
  - `BasicReadableFile` (`std::make_shared<TImpl>(std::move(file))`)
- **Problem:** Several members exist only to keep the type movable: `m_metadataMutex` is a `unique_ptr<std::mutex>`,
  and `m_prefetch`/`m_metadataRefresh` are separate `make_shared` allocations. That costs ~3 extra allocations per
  open. `ReadCallback` is a `std::function` on the read path.
- **Change:**
  - Construct the Impl in place: the factory returns `shared_ptr`, or forwards ctor args to `make_shared`.
  - Make those members direct and the type non-movable.
  - Template the internal read callback.

### [x] T10 – `DownloadBufferPool` contention and allocation
- **File:** `src\Azure\Impl\include\...\DownloadBufferPool.hpp`.
- **Problems:**
  - One process-wide pool behind one mutex.
  - A `make_shared<Lease>` per `Acquire`.
  - `EvictLargestWhileOverCap` frees buffers while holding the lock.
- **Change:**
  - Use per-size-class locks (or a small thread-local cache).
  - Use `allocate_shared` with the pool's buffer as the control block, or an intrusive lease.
  - Collect evicted buffers into a local and free them after unlock.
- **Validation:** Benchmark multi-threaded random reads.

### [x] T11 – Blob-size cache: targeted invalidation, heterogeneous lookup, smarter bound
- **Files:**
  - `ClientRuntime.cpp/.hpp` (`m_sizes`, `CacheBlobSize`, `CachedBlobSize(const std::string&)`)
  - `BlobFilesystemImpl.Rename.ipp`
- **Problems:**
  - `Rename` calls `InvalidateAllBlobSizes()`.
  - The map is keyed by `std::string` without transparent hashing, so lookups from `string_view` allocate.
  - `CacheBlobSize` `clear()`s everything at 4096 entries.
- **Change:**
  - Invalidate only the source and destination URLs.
  - Add a transparent hash/equal and take `std::string_view`.
  - Evict one entry (or use a small LRU/random eviction) instead of clearing the whole map.

### [x] T12 – Rename small-file path zero-fill
- **File:** `src\Azure\Impl\BlobFilesystemImpl.Rename.ipp`.
- **Problem:** The inline path (≤ `MaxInlineRenameSize`) allocates a zero-initialised `make_shared<vector<char>>`.
- **Change:** Use a pooled buffer or `make_shared_for_overwrite<char[]>`.

### [x] T15 – `LruFileIndex` eviction scan
- **File:** `src\Core\LruFileIndex.cpp` (`EvictUntilSizeLocked`).
- **Problem:** Each eviction runs a `find_if` from the tail. With pinned entries at the tail this is O(n·k).
- **Change:** Either keep a cursor past known-pinned entries for the duration of one eviction pass, or move pinned
  entries out of the list while pinned. Separately, consider building the path strings outside the lock
  (`RemoveEntryLocked`/`MakePath`); this is a documented tradeoff, so measure first.

### [x] T16 – `OriginLimiter` ticketing and keys (dropped: no measurable gain; allocations per limited Submit rose 119 → 120, and the cancel path needed extra ticket plumbing)
- **File:** `libs\HttpClient\src\private\OriginLimiter.hpp`.
- **Problems:**
  - The ticket is an atomic issued outside the lock, which forces a sorted insert (`upper_bound`) into a deque.
  - `m_origins` is keyed by a `std::string` origin that is built per request.
- **Change:**
  - Issue the ticket inside `Run` under `m_mutex`, then use plain `push_back` and drop the atomic.
  - Use transparent lookup with a `string_view` origin, or cache the origin string on the `Route`.

### [x] T17 – Wake queued requests at their deadline
- **Files:** `libs\HttpClient\src\private\HttpClientImpl.hpp` (`SendLimited`), `OriginLimiter.hpp`.
- **Problem:** This is a documented limitation: a request queued behind the per-origin limit is not woken when its
  deadline passes, so it fails late.
- **Change:** One steady timer per limiter, armed for the earliest queued deadline (min-heap or ordered set).
  Expired entries complete with `timed_out`.
- **Validation:** A new test with limit=1, a long-running first request, and a short-deadline second request.

### [x] T18 – `CancelState::Target` erasure and copies
- **Files:**
  - `libs\HttpClient\src\private\CancelState.hpp`
  - `HttpClientImpl.hpp` (`SendLimited` captures `origin` by copy)
- **Problems:**
  - `Target` is a `std::function<void()>`.
  - `Cancel()` copies it under the lock, and `SetTarget` copies it again into the member.
- **Change:**
  - Pass the target by value and move it into the member.
  - In `Cancel`, move it out, or keep a shared_ptr snapshot.
  - Better: the targets are a small closed set (limiter dequeue, running operation cancel), so represent them
    with a `std::variant` of concrete target structs and `std::visit`.
  - Capture a `shared_ptr`/key instead of copying the origin string.

### [x] T19 – Retry pipeline per-attempt copies
- **File:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\RequestPipeline.hpp` (~265-310, 418-445).
- **Problems:**
  - Every non-final attempt copies the URL and all headers.
  - `StampDate` linearly rescans the headers.
  - `AzureRequestOptions` is copied per `Send`.
  - `AttemptRebuilder` (BlobRequestHelpers.hpp:519) is a `std::function`.
- **Change:**
  - Remember the `x-ms-date` header index after the first stamp.
  - Template the rebuilder on its callable type.
  - Consider moving the URL/headers into the attempt and restoring them from the response path only when a retry
    actually happens (measure; retries are rare).
- **Note:** The single-attempt case already moves the request, so it does not need changing.

### [x] T20 – SharedKey signing allocations
- **File:** `libs\AzureClient\src\SharedKeySigner.cpp` (~210-385).
- **Problems:**
  - `std::format_to` is used for trivial concatenations (`/{}{}`, `,{}`, `\n{}:{}`, Content-Length).
  - `ParseUrl` allocates the query vector.
  - `Authorize` builds via `operator+` temporaries.
  - `EVP_MAC_CTX_dup` runs per signature.
- **Change:**
  - Use `append`/`push_back`, and `std::to_chars` for the length.
  - Reserve the `Authorize` result once.
  - Benchmark a thread_local duplicated context, re-initialized with `EVP_MAC_init(ctx, nullptr, 0, nullptr)`, vs
    dup per call. Adopt it only if it is measurably faster, and keep thread-safety.

### [x] T21 – Response header lookups
- **Files:**
  - `libs\AzureClient\src\BlobResponseParsers.cpp` (`ParseBlobProperties` ~689-760, `FindHeaderValue` ~474)
  - `RequestPipeline.cpp` ~76-88
- **Problem:** `ParseBlobProperties` calls `FindHeaderValue` a dozen or more times, and each call is a linear
  case-insensitive scan.
- **Change:** Do a single pass over the headers, dispatching on name (switch on length + first char, then
  `IEquals`), and fill all the fields.
- **Validation:** The parser unit tests.

### [x] T22 – Response construction copies (Verify first / measure) — DROPPED: no measurable gain
- **Outcome:** Dropped. `BuildResponse` already builds each header name/value once from Beast's views (no double
  copy). The remaining body copy costs ~1 µs per 64 KiB and ~30 µs per 1 MiB (memcpy ≈ 30–60 GB/s, measured), versus
  millisecond-scale Azure round trips, so the gain is unmeasurable. A `buffer_body` sink template threaded through
  `SendAsync` would add substantial complexity, so it is not worth it.
- **File:** `libs\HttpClient\src\private\RequestOperation.cpp` ~415-424.
- **Problem:** Each header becomes two `std::string`s per response. Download bodies arrive as `std::string`
  (`string_body`), and `SpanSink` then copies them into caller memory. That is a structural extra copy per ranged
  read, as noted in DownloadSinks.hpp.
- **Change (larger, optional):**
  - Add a non-erased body-sink hook to the HTTP client: a template parameter on `SendAsync` selecting a
    `buffer_body`/span parser.
  - Ranged reads would then parse straight into the pooled buffer, removing one full copy of every byte read.
- **Do not** add a callback `std::function`.

### [x] T23 – Small per-request formatting costs (Verify first) — DROPPED: no measurable gain (date header 288 -> 270 ns/call, ~18 ns vs ms-scale network requests; BuildQueryString is list-only, Route::Of struct fields would need reshaping); adds complexity
- **Files:**
  - `libs\AzureClient\src\BlobRequestHelpers.cpp` ~401-420: the RFC1123 date is built via `std::format` per
    request. Use a fixed `char[29]` buffer and cache it per second.
  - `BlobUrl.cpp` ~31-44: `BuildQueryString` creates `UrlEncode` temporaries without a reserve. Encode in place
    into a reserved output.
  - `libs\HttpClient\src\private\Route.hpp` ~38-79: `Route::Of` makes several string allocations. Build the
    origin once.

### [x] T24 – HttpClient connection-path costs (Verify first) — DROPPED: no measurable gain; adds complexity. Idle probe is already gated by a threshold (m_probeThreshold in Acquire); DnsCache hits copy asio results (a shared_ptr refcount bump) under a few-ns lock, dwarfed by ms-scale requests; the non-pooled TLS shutdown make_shared is one allocation on a rare path; TLS session resumption works because the plugin uses one shared HttpClient (SharedHttpClient) per runtime, so one SSL_CTX cache
- **Files:**
  - `DnsCache.hpp` (~67-95, 154-218): one process-wide mutex, and `ResolveResults` is copied per hit. Consider a
    shared_mutex, or storing results as `shared_ptr<const>`.
  - `ConnectionPool.hpp` (~229-243, 400-438): a synchronous idle-probe syscall on every acquire. Probe only
    connections idle longer than a threshold.
  - `RequestOperation.cpp` (~425-438): non-pooled TLS shutdown allocates `make_shared<unique_ptr<TlsStream>>`.
    Move the stream into the shutdown handler directly.
  - `TlsContextConfigurator.cpp` (~150-190): the TLS session cache is per client. Check that resumption works
    across the plugin's clients.

### [ ] T25 – `BlockOn` / `ThrowIfRunningOn` allocations
- **File:** `libs\AzureClient\include\AVEVA\AzureClient\BlockOn.hpp`.
- **Problem:** `block_on` allocates a `shared_ptr<promise>` per call. The erased-executor probe allocates a
  `shared_ptr<atomic>` and dispatches.
- **Change:**
  - Keep the promise on the caller's stack. The caller blocks until the handler runs, so a raw pointer handler is
    safe *only if* every initiation guarantees exactly one completion before abandonment. Otherwise keep the
    shared_ptr.
  - The probe path is only hit for erased executors; leave it as-is unless the plugin uses one.

### [ ] T26 – Misc correctness nits
- `libs\AzureClient\include\AVEVA\AzureClient\Detail\AsyncInitiation.hpp` ~143-148: `PostCompletion` doesn't bind
  the handler's associated allocator/executor. Use `boost::asio::bind_executor` /
  `get_associated_allocator`-aware posting. **Verify first.**
- `Detail\BlobBatch.hpp` ~93-95: `ChunkCount` can overflow on huge inputs. Use checked arithmetic.
- `libs\AzureClient\src\TokenEndpointError.cpp`: `message`/`GetTokenEndpointFailure` mis-decode negative values
  of a hand-constructed `error_code`. Clamp or reject `value < 0`.
- `src\Azure\Impl\include\...\BlobOperations.hpp` (~235-251, 308-319, 399-435): `FinishDownload`/`FinishUploadPages`
  call `.get()` and rely on the Start-time guard. Add a debug assert / `ThrowIfRunningOn` at the `.get()` site too.

### [ ] T27 – Build hygiene
- `libs\AzureClient\src\CMakeLists.txt`: set `POSITION_INDEPENDENT_CODE ON` on the static lib so it can be linked
  into shared plugins on Linux.
- The header-only footprint is large. Move non-dependent helpers out of the `Detail\*.hpp` headers into `.cpp`
  files, and extend `ClientInstantiations.cpp` explicit instantiations. Measure build time.
- `infrastructure\vcpkg-overlays\rocksdb`: add a CI step that fails if the overlay patches no longer apply to the
  pinned port version.

---

## Reviewed and rejected (do not redo)

- `LruFileIndex::RegisterEntry` erasing a pinned entry: unpin is by id, and replacement is an atomic rename.
- Rename "always copies through a buffer": large files use `CopyPagesServerSide`, and only ≤ `MaxInlineRenameSize`
  is copied inline (with rollback).
- `LeaseRenewalLoop::Fail` race: it is always called under `m_mutex`. `Stop` waiting up to ~2 min is documented.
- Public headers leaking Impl: they use forward-declared `unique_ptr` pimpl (not erasure of hot paths).
- AzureClient `cxx_std_23` PUBLIC → PRIVATE: wrong, because the header-only templates require C++23 in consumers.
- One strand per HTTP client instead of per connection: the per-connection strand is required for stream
  serialization.
- ISO-8601 fractions beyond 7 digits: they are deliberately truncated (documented).
- Retry pipeline sharing the body on single-attempt requests: the last/only attempt already moves the request.
- TSan CI skipping unlabeled tests: `-L 'core|http-client'` and `-LE 'core|http-client'` are complementary.
