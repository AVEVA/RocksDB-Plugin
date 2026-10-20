# Review backlog (pass 2): `feature/trim-azure-client` vs `main`

Review scope: merge-base `278868c`, 452 commits, 348 files (+51.8k / −5.5k). Everything was reviewed:
`libs\HttpClient`, `libs\AzureClient`, `src\Azure`, `src\Core`, the public headers, build/CI/tooling, and the test
infrastructure. The first pass (T01–T27) is closed; see "Prior pass" at the end. Findings marked **Verify first**
are medium confidence: confirm them against the code, or measure, before changing anything.

## Hard constraints for the executor

- **No type erasure.** Do not introduce `std::function`, `std::move_only_function`, `std::any`,
  `any_io_executor`, `any_completion_handler`, virtual interfaces that replace templates, or erasing pimpls. The
  branch exists to build zero-overhead, template-based async paths. Removing erasure that is already there is
  encouraged.
- The RocksDB API boundary is fixed. For example, `FSRandomAccessFile::ReadAsync` takes a `std::function`.
- Never block with `.get()` or a wait on a thread that runs the host-supplied `io_context`. Guard any new blocking
  point with `AzureClient::ThrowIfRunningOn(executor)`.
- Keep the `AsyncReadRequest` invariants:
  - downloads go into a pooled buffer, never into the caller's scratch;
  - the scratch copy happens under the request lock;
  - RocksDB callbacks run only from `Poll`/`AbortIO`.
- Map every Azure client error to a `rocksdb::Status`, and never swallow errors.
- Public headers in `include\AVEVA\RocksDB\Plugin\` must stay source-compatible.

## Executor rules

- Build: `cmake --build build\WindowsDebug --config Debug` (or the `LinuxDebug` preset).
- Test: `ctest --test-dir build\WindowsDebug --output-on-failure --build-config Debug -E Integration`
  (Integration tests need `AZURE_*` env vars).
- Run `clang-format` on touched files. Add new sources and tests to the owning `CMakeLists.txt`.
- Performance tasks: measure before and after, with a micro-benchmark, an allocation count, or
  `tools\db_bench\Compare-Backends.ps1`. Put the numbers in the commit message. If a change shows no measurable
  gain *and* adds complexity, drop it and record why here.
- Do one task per commit, and tick its box when done.
- Never create or publish a PR without explicit user approval.

---

## P1: correctness / significant performance

### [x] T28 – `WriteableFile::Append` never terminates when the buffer is one page
- **Files:**
  - `src\Azure\Impl\WriteableFileImpl.ipp` (`Append` ~116-135, `StartFlush` ~157-170)
  - `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobFilesystemImpl.hpp:49-58` (`InvalidDataFileSizes`)
- **Problem:** `dataFileBufferSize == 512` passes validation and the ctor check (`< PageSize`). Once
  `0 < m_bufferOffset < 512`, `spaceLeft < PageSize`, so Append calls `StartFlush(false)`. That computes
  `fullBytes == 0` and returns, and the `continue` spins forever. Two small appends trigger it, and so does
  reopening a file whose last page is partial. (Verified.)
- **Change:**
  - Require `dataFileBufferSize >= 2 * PageSize` in both `InvalidDataFileSizes` and the ctor (after rounding).
  - Alternatively, make Append copy `min(spaceLeft, dataSize)` whenever `StartFlush` cannot free a page.
  - Prefer the validation fix and update the error message.
- **Validation:** new `WriteableFileTests` case: buffer = `PageSize`, append 100 B twice, Sync, verify the content.
  It hangs today, so use a test timeout.

### [x] T29 – Batch delete: data race on `cancellation_signal` between chunk start and cancel
- **File:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobBatch.hpp` (`Pump` ~100-127, `StartChunk`
  ~131-137, `OnCancel` ~141-161, `SendChunk` ~204).
- **Problem:** `Pump` publishes the chunk's signal into `m_signals` under `m_mutex`, then calls `SendChunk` outside
  the lock, and `RetryOperation::Start` assigns a slot on that signal there. Meanwhile `OnCancel`, on another
  thread, `emit`s on every signal in the map outside the lock. `StartChunk`'s own post-start `emit` can also
  overlap `OnCancel`'s. Asio `cancellation_signal` is not thread-safe, so concurrent `emit`/`assign` is UB.
  `DownloadOperation` avoids this with its event trampoline.
- **Change (pick one):**
  - (a) Insert the signal into `m_signals` only after `SendChunk` returns. Then re-check `m_cancelled` under
    `m_mutex` and emit while still holding the lock, or mark the chunk for emission.
  - (b) Add a dedicated emit mutex held around `SendChunk` and around both emit sites. Initiation never completes
    inline, because completions are posted.
  - Either way, every `emit` on a given signal must be serialized with its slot assignment.
- **Validation:** new TSan-run test: a batch delete with more than 256 blobs (several chunks) cancelled right after
  initiation, looped 200×. Run the existing `BatchDeleteTests`.

### [x] T30 – Token cache hit erases and heap-allocates the handler on every authorized request
- **File:** `libs\AzureClient\include\AVEVA\AzureClient\TokenCredential.hpp` (`GetTokenForScopesAsync` ~160-165:
  `Start(..., Waiter{std::move(handler)})`; `Start` ~320; `LookUpCachedTokenOrStartRefresh` ~220).
- **Problem:** The handler is converted to `Waiter` (`any_completion_handler`) *before* the cache is checked. On
  every request this costs a heap allocation plus a move of the `RetryOperation` continuation, which captures an
  `HttpRequest`, even when the token is cached. The cached token string, often 1–2 KB, is also copied on every hit.
- **Change:**
  - Template `Start`/`LookUpCachedTokenOrStartRefresh` on the handler type, so the cache hit completes the
    concrete handler directly.
  - Convert to `Waiter` only when the handler must queue behind an in-flight refresh (the cold path). That keeps
    the existing erasure off the hot path; do not add new erasure.
  - Optional follow-up: store the token as `std::shared_ptr<const std::string>`, or make `AccessToken` hold
    immutable shared storage, so hits don't copy the string. Measure first.
- **Validation:** `TokenCredentialTests.cpp`. Add an allocation-count test showing that a cache hit makes no
  erasure allocation.

### [x] T31 – HttpClient reads response bodies in ~0.5–2 KiB chunks (**Verify first**: measure)
- **Done:** reserve 16 KiB (`ReadChunk`) before `async_read`; added `ReadsLargeBodyIntact` (4 MiB). Read count not
  instrumented (no hook); 32/64 KiB and db_bench not tried.
- **Files:**
  - `libs\HttpClient\src\private\RequestOperation.cpp:336-344` (`Read`)
  - `RequestOperation.hpp:107,360` (`m_buffer`)
  - `ConnectionPool.hpp:66-82` (`MaxRetainedBufferCapacity = 16 KiB`, `TrimIdleBuffer`)
- **Problem:** `m_buffer` (`flat_buffer`) starts empty. Beast's `read_size(b, 65536)` returns
  `min(max(512, capacity - size), 65536)`. The parser consumes `string_body` bytes eagerly, so `size()` stays near
  0 and the buffer never grows beyond what header parsing needed. A multi-MiB page-range GET therefore needs
  thousands of `async_read_some` strand round trips: a syscall each on plain HTTP, a small `SSL_read` each on TLS.
- **Change:**
  - Add `inline constexpr std::size_t ReadChunk = 16 * 1024;` (one TLS record) and, in `Read()`, call
    `if (m_buffer.capacity() < ReadChunk) m_buffer.reserve(ReadChunk);` before `http::async_read`.
  - Keep `MaxRetainedBufferCapacity >= ReadChunk` so pooled connections keep the buffer.
  - Also try 32 and 64 KiB, and record the worst-case idle memory (max idle connections × chunk).
- **Validation:** `ConnectionPoolBuffers.*`, `ConnectionPool.IdleBuffersAboveTheRetentionCapAreReleasedAndSmallOnesKept`,
  `HttpClientRequest.ReusesConnectionForKeepAliveRequests`. Add a test that downloads a 4 MiB body and counts
  reads; expect at least a 10× drop. Run a db_bench random-read comparison.

### [x] T32 – UBSan findings don't fail CI
- **Files:** root `CMakeLists.txt:22-32` (`add_compile_options(-fsanitize=...)`), `libs\AzureClient\CMakeLists.txt`
  (`AVEVA_AZURE_CLIENT_ENABLE_UBSAN`), `.github\workflows\build-and-test.yml` (asan-ubsan Test step).
- **Problem:** GCC/Clang UBSan reports and continues unless built with `-fno-sanitize-recover`. Neither that flag
  nor `UBSAN_OPTIONS` appears anywhere, so UB stays green in CI.
- **Change:**
  - When `undefined` is in the sanitizer list, add `-fno-sanitize-recover=undefined` to both compile and link
    options, in both places.
  - Set `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1` in the job.
- **Validation:** temporarily add a test with signed overflow, confirm the job fails, then revert it.

---

## P2: correctness / improvements

### [x] T33 – File wrappers translate `invalid_argument` / cancellation / size mismatch to generic IOError
- **Files:**
  - `src\Azure\AzureErrorTranslator.cpp:21-25, 111-153` (`FromCurrentException`/`FromException`,
    `IsPermanentHttpClientError`)
  - `src\Azure\BlobFilesystem.cpp:129` (`Guard`)
  - `src\Azure\WriteableFile.cpp`, `ReadWriteFile.cpp`, `ReadableFile.cpp`, `SequentialFile.cpp`, `Directory.cpp`
- **Problem:** Only `BlobFilesystem::Guard` maps `std::invalid_argument` to InvalidArgument. Through the file
  wrappers:
  - `RangeCheck` errors and `ReadWriteFileImpl::Flush`'s "past maximum" become IOError, although the comment in
    `RangeCheck.hpp` promises InvalidArgument;
  - `std::system_error(operation_canceled)` becomes IOError instead of Aborted;
  - the `FinishDownload` "Downloaded X but server reported Y" error becomes IOError, while the equivalent
    short-download path gives Corruption.
  - `IsPermanentHttpClientError` is never used, which risks `-Wunused-function` under `-Werror`.
- **Change:**
  - Move the `invalid_argument` → InvalidArgument and `system_error` handling into `FromCurrentException`, with
    cancellation codes → Aborted, and have `Guard` call it.
  - Map the size-mismatch error to Corruption.
  - Use or delete `IsPermanentHttpClientError`.
- **Validation:** ReadWriteFile write/read past `MaxOffset` expecting `IsInvalidArgument()`. Add
  `AzureErrorTranslatorTests` cases for `system_error(operation_canceled)`.

### [x] T34 – Lease / sequence-number / permission error codes are not mapped (**Verify first**)
- **Files:** `libs\AzureClient\include\AVEVA\AzureClient\BlobStorageErrorCode.hpp`,
  `libs\AzureClient\src\BlobStorageErrorCategory.cpp:22-129` (`KnownCodes`, `default_error_condition`),
  `src\Azure\AzureErrorTranslator.cpp:68-73`.
- **Problem:** These codes fall through to a generic `ServiceError`: `LeaseIdMismatchWithLeaseOperation`,
  `LeaseIsBrokenAndCannotBeRenewed`, `LeaseAlreadyBroken`, `LeaseIsBreakingAndCannotBeAcquired`,
  `SequenceNumberConditionNotMet`, `InvalidPageRange`, `AuthorizationPermissionMismatch`. A lost lease during
  renewal therefore becomes a retryable IOError instead of IOFenced. `ConditionNotMet`, `TargetConditionNotMet`
  and `LeaseAlreadyPresent` also have no portable condition.
- **Change:**
  - Append the codes to the enum (it is append-only `uint8_t`) and insert them into `KnownCodes` in sorted order
    (a `static_assert` checks the order).
  - Map the lease codes to fenced and `AuthorizationPermissionMismatch` to permission-denied, in both the category
    and the translator.
- **Validation:** parameterised `BlobStorageErrorCategoryTests` and `AzureErrorTranslatorTests`, one case per code,
  checking the subcode and retryability.

### [x] T35 – Re-creating a blob doesn't invalidate the FileCache (**Verify first**)
- **File:** `src\Azure\Impl\BlobFilesystemImpl.FileOpen.ipp:28-38, 52-57, 99`.
- **Problem:** `CreateWriteableFile`/`ReuseWritableFile` call `CreateOrReplace` but never evict the cached copy.
  The cache entry is only marked stale at a Sync that has unsynced data, and cache entries are not checked
  against the ETag. A file that is recreated and closed empty, or read before its first Sync, can therefore serve
  the old cached bytes.
- **Change:** after `CreateOrReplace`, call `FindCache(path)->RemoveFile(realPath)`, as `DeleteFile` does.
- **Validation:** cache file A, recreate A, Close it without writing, read A, and expect empty content or NotFound.

### [x] T36 – `ReuseWritableFile` ignores `old_fname` (pre-existing on `main`)
- **File:** `src\Azure\BlobFilesystem.cpp:190-196`.
- **Problem:** RocksDB expects `old_fname` to be recycled into `fname`. The plugin creates a new `fname` instead,
  so old WAL blobs are left behind in the container.
- **Change:** either rename `old_fname` → `fname` (server-side) before reopening it, or return `NotSupported`,
  which makes RocksDB fall back to creating a new file. Document the choice.
- **Validation:** with `recycle_log_file_num > 0`, the container does not accumulate orphan logs.

### [ ] T37 – `ReadWriteFileImpl::Flush` commits `m_size` before uploads succeed; split the function
- **File:** `src\Azure\Impl\ReadWriteFileImpl.ipp:72-230` (Flush, ~155 lines), `:119` (`m_size = maxSizeNeeded`),
  `:226` (`#if _DEBUG` zero-fill), `:304` (Write threshold), move ctor.
- **Problems:**
  - `m_size` is advanced before the uploads run. A failed upload leaves it too large, so a retried Flush computes
    the wrong `sizeBefore` and gap handling, and Read may return bytes beyond what was persisted.
  - The debug-only zero-fill makes Debug and Release behave differently.
  - The Write flush threshold uses `DefaultBufferSize` instead of `m_buffer.size()`.
  - The move ctor drops `m_extentPool`.
- **Change:**
  - Split Flush into `BuildExtents` / `PrepareExtent` / `RunWave`.
  - Commit `m_size` only after every wave succeeds.
  - Remove the `_DEBUG` zero-fill and fix any test that depends on it.
  - Use `m_buffer.size()` for the threshold, and move `m_extentPool` in the move ctor.
  - Call `runtime.ThrowIfWritesFenced()` after the final wave, matching `WriteableFileImpl` (**Verify first**).
- **Validation:** inject a failure into one upload, retry Flush, then check the size and content. Run the existing
  `ReadWriteFileTests`. Fence mid-Flush and expect IOFenced.

### [ ] T38 – `LockFileImpl::Unlock` has no deadline
- **File:** `src\Azure\Impl\LockFileImpl.ipp:302-323`.
- **Problem:** `ReleaseLeaseAsync` runs with the default options and no total deadline. `~BlobFilesystem` unlocks
  the locks one after another, so shutdown can stall for each lock's full retry budget, even though the lease
  lapses on its own.
- **Change:** pass a total deadline (shutdown wait timeout, or remaining lease time, whichever is shorter), and log
  a warning on timeout. Local state is already cleared by the scope exit.
- **Validation:** with a fake ReleaseLease that never completes, Unlock returns within the bound.

### [ ] T39 – Async/inline metadata refreshes don't update the runtime (size, ETag) cache
- **File:** `src\Azure\Impl\ReadableFileImpl.ipp` 451-475 (`ReadAsyncAttempt` EOF), 510-527
  (`RefreshMetadataAndReadAsync`), 588-603 (`DownloadWithRetry` EOF), 626-635 (`RefreshBlobMetadata`).
- **Problem:** Only `RefreshBlobMetadata` calls `ClientRuntimeBase::CacheBlobMetadata`; the other three sites only
  call `SetMetadata`. Files opened afterwards start from a stale (size, ETag) and pay a 412 plus a refresh, and
  `GetCachedSize` keeps returning the stale size. When the ETag is unchanged, `m_metadataFetchedAt` is not bumped
  either, so `GetSize` makes another round trip.
- **Change:**
  - Add a private `ApplyRefreshedMetadata(size, etag, epoch)` that calls `Runtime->CacheBlobMetadata` (if
    `Runtime` is set) and then `SetMetadata`.
  - In `CoalescedMetadataAsync`, capture `BlobSizeEpoch()` before `GetMetadataAsync` and pass it through the
    callback.
  - Use the helper at all four sites, including the unchanged-ETag branches.
- **Validation:** `AsyncReadTests`, `ReadableFileTests`. New test: after a 412 refresh,
  `runtime.CachedBlobMetadata(url)` holds the new ETag.

### [ ] T40 – Async download skips the Content-Range length check (**Verify first**)
- **File:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp` 268-277 (`FinishDownload`),
  587-617 (`DownloadAsync`).
- **Problem:** The sync path throws when `ContentRange->Length != BytesWritten`. `DownloadAsync` does not check, so
  in ReadAsync, MultiRead and Prefetch the same mismatch becomes a silent short read, which RocksDB sees as EOF or
  corruption. First confirm whether the HTTP layer already rejects a body shorter than its Content-Length.
- **Change:** move the check into a shared `Detail::CheckDownloadLength(result)` and call it inside the
  `DownloadAsync` completion's `try`. Map a mismatch to Corruption (consistent with T33).
- **Validation:** `BlobOperationsTests`, `AsyncReadTests`. Add a fake-server test where Content-Range overstates the
  size.

### [ ] T41 – `TryReadFromPrefetch` copies up to 8 MiB under the prefetch mutex
- **File:** `src\Azure\Impl\ReadableFileImpl.ipp` 309-367 (`std::copy_n` ~362); prefetch completion ~219-250.
- **Problem:** The copy holds `m_prefetch.Mutex`, which blocks other readers of the file and the prefetch
  completion. That completion runs on the host io_context thread.
- **Change:**
  - Under the lock, take a snapshot: the `Owner` shared_ptr, the `Data` subview and the `Id`. The published data
    is immutable.
  - Unlock, copy, then re-lock to `Drop` by `Id` if the read reached the slot end.
  - The scratch copy in `TryCompleteInline` still runs under the request lock (invariant kept).
- **Validation:** the Prefetch and ChainedOntoPrefetch tests in `AsyncReadTests`. Optionally, a concurrent
  4 MiB-read micro-benchmark.

### [ ] T42 – Two ETag string copies per read
- **Files:** `src\Azure\Impl\ReadableFileImpl.ipp` 451, 481, 588; `BlobOperations.hpp` 248-262 (`StartDownload`),
  587-591 (`DownloadAsync`).
- **Problem:** `GetMetadata()` copies the ETag, then `DownloadAsync`/`StartDownload` copy it again into
  `Conditions.IfMatch`. Azure ETags are about 19 chars, longer than the SSO buffer, so that is two heap
  allocations per read.
- **Change:** take `std::string ifMatch` by value in `DownloadAsync`/`StartDownload`/`Download` and move it into
  the options. Bind the metadata non-const at the call sites and `std::move(etag)`. `DownloadWithRetry` copies the
  ETag once per attempt, so moving it is safe.
- **Validation:** `AsyncReadTests`, `ReadableFileTests`, `BlobOperationsTests`, and an allocation count per read.

### [ ] T43 – MultiRead merged groups: temporary buffer and double copy (**Verify first**: measure)
- **Files:** `src\Azure\BasicReadableFile.ipp` 169-275 (`Group::Buffer` ~233, `applyOutcome` memcpy ~267);
  `AsyncReadRequest.hpp/.cpp` (`Complete`).
- **Problem:** Each merged group allocates up to 8 MiB (`make_unique_for_overwrite`). Data goes from the pooled
  buffer into the group buffer, and then again into each member's scratch.
- **Change:**
  - Give `AsyncReadRequest` an optional `std::span<const ScatterTarget>` (`{relOffset, len, char* scratch,
    size_t* out}`) owned by the group. `HandleCleanup` aborts handles before the groups die, so the span stays
    valid.
  - `Complete` scatters straight from the pooled data into each member's scratch, under the request lock.
  - For the FileCache-hit path of a merged range, keep the buffer as a fallback, or skip the cache.
  - Then remove `Group::Buffer`.
- **Validation:** the `MultiRead_*` tests. Benchmark before and after.

### [ ] T44 – Blob-size cache: a global epoch blocks unrelated stores (**Verify first**: measure hit rate)
- **File:** `src\Azure\Impl\ClientRuntime.cpp` 160-232 (`BlobSizeEpoch`, `StoreBlobSize`, `InvalidateBlobSize`);
  `BlobOperations.hpp` (`SizeInvalidation`, which runs on every `SetSize`/Sync).
- **Problem:** `InvalidateBlobSize(url)` bumps the single `m_sizeEpoch`, so *any* fetch that overlaps a mutation of
  *any* blob is never stored. Under frequent WAL syncs, the open and GetFileSize caches may rarely fill. Also,
  once the map is full, `StoreBlobSize` runs an O(4096) `erase_if` sweep under `m_sizeMutex` on every store, and
  `CachedBlobSize`/`CachedBlobMetadata` duplicate their lookup and expiry logic (146-156 vs 170-184).
- **Change:**
  - Invalidate per URL: store a tombstone `{InvalidatedAt = ++m_sizeEpoch}`. A store is rejected only if that
    URL's tombstone is newer than the fetch epoch, or the epoch is older than an
    `m_evictedTombstoneWatermark` that is raised when tombstones are swept.
  - `InvalidateAllBlobSizes` sets the watermark.
  - Rate-limit the full sweep (`m_nextSweep = now + MetadataCacheTtl`) and otherwise evict a single entry.
  - Extract a shared `FindFreshLocked(url)`.
- **Validation:** existing runtime cache tests. New tests: invalidating A during a fetch of B still caches B, while
  invalidating A during a fetch of A does not.

### [ ] T45 – FileCache generation ABA can activate a re-created entry with a stale size
- **Files:**
  - `src\Core\FileCache.cpp` (BackgroundDownload post-`GetBlobSize` check ~395-420; ReadFile shortRead ~240-250)
  - `include\AVEVA\RocksDB\Plugin\Core\FileCacheEntry.hpp` (`m_generation = 0`)
  - `src\Core\FileCacheEntry.cpp`
- **Problem:** Every entry starts at generation 0.
  - **Download path:** if an entry is removed and re-created while the downloader is inside `GetBlobSize`,
    `GetGeneration() == generation` still passes. The block also doesn't check `state == QueuedForDownload`, so a
    fresh Stale entry gets the old size and becomes Active. Reads past the old size then return a false EOF.
  - **ReadFile path:** the shortRead path has the same ABA and can drop a newer, valid entry.
- **Change:**
  - Add a FileCache-wide `m_nextGeneration` (guarded by `m_mutex`) and use it for every new entry and every bump.
    Expose it additively (a defaulted ctor parameter or a new setter) so the public header stays
    source-compatible.
  - Require `state == QueuedForDownload` before applying the size.
  - In shortRead, also compare the read-handle pointer identity.
- **Validation:** new `FileCacheTests` case where `ContainerClientMock::GetBlobSize` calls `RemoveFile` then
  `ReadFile` on the same path mid-call. Run `ctest -L core`.

### [ ] T46 – Secondary cache: two zero-filled buffers and two payload copies per Insert
- **Files:** `src\Core\FileBasedCompressedSecondaryCache.cpp` (Insert ~125 `small_vector buf(dataSize)`;
  WriteToDisk ~400-410 `writeBuf(storedSize)` + memcpy). Also `src\Core\LocalFilesystem.cpp` ~64
  (`std::string contents(size, '\0')` in Lookup).
- **Change:**
  - Allocate one `storedSize` buffer with `boost::container::default_init`.
  - Write the header byte, then `saveto_cb` into `buf + 1`, and add a `WriteToDisk` overload that takes the
    prepared buffer.
  - In Lookup, use `resize_and_overwrite` to read without the zero-fill.
- **Validation:** `ctest -L core` (the secondary-cache round-trip tests). Measure the Insert cost.

### [ ] T47 – `FileCache::ReadFile` copies the relative path under `m_mutex` on every hit
- **File:** `src\Core\FileCache.cpp` ~196 (`auto relativePath = fileEntry.GetFilePath();`).
- **Change:** use the `filePath` parameter (it equals the map key), and build `m_cachePath / filePath` only when
  `!handle->Opened`.
- **Validation:** `ctest -L core`, plus an optional micro-benchmark of hot `ReadFile` calls.

### [ ] T48 – HttpClient `CompleteSuccess` can drop the completion handler on exception
- **File:** `libs\HttpClient\src\private\RequestOperation.cpp:430-484`.
- **Problem:** `m_completion` is moved into a local before `ReleaseOrClose`. In the TLS shutdown branch,
  `make_shared`, `expires_after` and `async_shutdown` run outside any `try`. If one throws, the exception escapes
  into `io_context::run()` and the handler is destroyed without being called. The pool `Release` at ~440 only
  catches `std::exception`.
- **Change:**
  - Wrap `ReleaseOrClose(keepAlive)` in `try { ... } catch (...) { Report(SwallowedException, ...); Close(); }`
    so `Detail::Complete` always runs.
  - Change ~440 to `catch (...)` and remove the `static_cast<void>(ex)`.
- **Validation:** `HttpClientTls.*`, `HttpClientRobustness.*`. Add a fault-injection test (use the existing
  `AllocationFailure` counters) asserting the handler runs exactly once.

### [ ] T49 – Concurrent 401s each invalidate a freshly refreshed token (refresh herd)
- **Files:** `libs\AzureClient\include\AVEVA\AzureClient\TokenCredential.hpp` (`Invalidate` ~169-185);
  `Detail\RequestPipeline.hpp` (401 path in `OnAttemptCompleteOnStrand` ~430-445).
- **Problem:** Every 401'd request calls `Invalidate(scopes)` unconditionally, so a late 401 carrying the *old*
  token can drop the *new* token and trigger another refresh. Under load this multiplies traffic to the token
  endpoint.
- **Change:** add `Invalidate(scopes, rejectedToken)`, or key it on a token generation, so the cache is dropped only
  when it still holds the rejected token. `RetryOperation` records the token (or its generation) used for each
  attempt.
- **Validation:** new test: two concurrent requests both get 401 with the same token. Assert exactly one inner
  refresh and that both retries succeed.

### [ ] T50 – Direct-path `BoundHandler` may run the handler off its associated executor (**Verify first**)
- **Files:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\AsyncInitiation.hpp` (`BoundHandler::operator()`,
  Direct branch ~45-70); `Detail\RequestPipeline.hpp` (`Run`/`Finish` ~170, ~520).
- **Problem:** The Direct branch calls the handler inline on the assumption that it is already on the client
  executor. But `RetryOperation::Run` drains events on whichever thread arrives first. That can be a credential's
  completion thread (a credential on another `io_context`, or a user `AssertionProvider`) or a thread that emits
  an explicit cancellation slot. The plugin is unaffected only because its credential shares the runtime's
  HttpClient.
- **Change:** invoke inline only if the executor reports `running_in_this_thread()`; otherwise `dispatch`. Or have
  `Finish` always dispatch through `m_httpClient.get_executor()`. Measure that the common path stays inline.
- **Validation:** a test with a credential completing on a separate `io_context` thread that asserts the handler
  thread id. Run `AsyncDeferredCompletionTests`.

### [ ] T51 – Per-request deque/timer allocations in `RetryOperation` / `DownloadOperation` (**Verify first**)
- **Files:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\RequestPipeline.hpp` (`m_queue`, `m_deadlineTimer`
  ~555-590); `Detail\BlobDownload.hpp` (`m_events`).
- **Problem:** A default-constructed `std::deque` allocates its map plus a node eagerly on libstdc++, so every
  request pays that for a queue that rarely holds more than two events. `m_deadlineTimer` is constructed (via
  `use_service`, which takes a registry mutex) even when no total deadline is set.
- **Change:**
  - Replace the deques with a lazily allocating FIFO (`std::vector` plus a head index, or a small inline ring of
    about 2 events).
  - Hold the timer in `std::optional` and emplace it only when there is a deadline.
- **Validation:** an allocation count per `GetPropertiesAsync` on Linux, plus the retry and pipeline tests.

### [ ] T52 – `StringSink` copies the whole body even for single-response downloads
- **Files:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\DownloadSinks.hpp` (`StringSink::Reserve/Write`
  ~75-100); `Detail\BlobDownload.hpp` (`OnProbe` → `m_sink.Reserve(total)` ~330).
- **Problem:** `Reserve(total)` always runs before the first `Write`, so `Write`'s move fast path (taken when
  `capacity < data.size()`) never fires. A single-chunk `DownloadAsync` allocates a second full buffer and copies
  the body into it. The plugin uses `SpanSink`, so for it this is P3.
- **Change:** have `Reserve` record the expected size only. On the first `Write`, move `data` in if it covers the
  expected size; otherwise reserve and append. Keep `SpanSink` unchanged.
- **Validation:** test that the single-chunk result's `data()` is the transport buffer (or count allocations), plus
  the download tests.

### [ ] T53 – CI: vcpkg cache poisoning, db-bench job never caches, duplicate builds, link-less consume test
- **Files:** `.github\workflows\build-and-test.yml` (save at ~132-134; matrix ~92-98; db-bench ~193-234;
  install-consume ~235-284), `.github\actions\setup-vcpkg\action.yml:42-53`.
- **Problems:**
  - Saving with `if: always()` can store a partial vcpkg tree under the exact key. Later runs get an exact hit,
    skip the save, and never fix it.
  - There are no `restore-keys`, so any manifest change rebuilds everything.
  - The db-bench job has a different RocksDB ABI and never saves a cache, so it rebuilds RocksDB on most PRs.
  - The `tests: false` LinuxRelease matrix entry duplicates the install-consume build.
  - The install-consume consumer is `int main() { return 0; }`, so a missing link dependency or `find_dependency`
    goes undetected.
- **Change:**
  - Save on `success()` and add `restore-keys: vcpkg-${{ runner.os }}-`.
  - Give db-bench its own cache key plus a save on main, or narrow its path filter.
  - Drop the redundant matrix entry.
  - Make the consumer reference a real symbol behind a runtime guard (for example
    `if (argc > 100) Plugin::Register(...)`).
- **Validation:** CI runs. Removing a `find_dependency` should now make install-consume fail.

### [ ] T54 – Library types / PIC consistency
- **Files:** `src\Core\CMakeLists.txt`, `src\Azure\Impl\CMakeLists.txt`, `src\Azure\Models\CMakeLists.txt`,
  `libs\HttpClient\src\CMakeLists.txt`.
- **Problem:**
  - Core, Impl and Models use `add_library` without a type and set `WINDOWS_EXPORT_ALL_SYMBOLS`, while
    azure-client hard-fails on `BUILD_SHARED_LIBS`.
  - Only azure-client sets PIC (T27). http-client and the plugin libraries can't be linked into a Linux `.so`.
- **Change:** declare the libraries `STATIC` explicitly, drop `WINDOWS_EXPORT_ALL_SYMBOLS`, and set PIC on all of
  them (or `CMAKE_POSITION_INDEPENDENT_CODE ON` in the presets).
- **Validation:** on Linux, build a tiny shared library that links `aveva::rocksdb-plugin-azure`.

### [ ] T55 – Flaky wall-clock test assertions and busy-polling fake pump
- **Files:**
  - Upper-bound timing assertions:
    - `tests\AVEVA\RocksDB\Plugin\Azure\Impl\LockFileImplTests.cpp:638` (`EXPECT_LT(gap, 200)`) and `:662`
    - `libs\HttpClient\tests\HttpClientRobustnessTests.cpp:448`
    - `ReadWriteFileTests.cpp:701`
    - `WriteableFileTests.cpp:117`
    - `libs\AzureClient\tests\RetryTests.cpp:449` (`sleep_for(40ms)` vs a 15 ms deadline)
  - `libs\AzureClient\tests\FakeHttpClient.hpp:417-430` (pump loop).
- **Problems:**
  - The upper-bound assertions will flake under sanitizers or on a loaded CI machine.
  - The fake's pump runs `scoped_lock m_pollMutex; restart(); run_for(1ms)` in a loop, so it holds the recursive
    mutex almost continuously and wakes 1000×/s. Test-thread accessors (`RequestCount`, `Requests`, `RequestAt`)
    contend for it (**Verify first**).
- **Change:**
  - Drive those tests through the existing manual-clock or timer seams. Otherwise relax the bounds and add a
    `timing` label with `RUN_SERIAL`.
  - Make the pump `run()` with a work guard and stop it through a `stop_callback` → `m_context.stop()`, and skip
    `Poll`'s lock/restart while the pump is active.
- **Validation:** `ctest --repeat until-fail:50 -R <names>` on an ASan build. Time the test binaries before and
  after.

### [ ] T56 – HttpClient allocation test measures setup, not per-request cost
- **File:** `libs\HttpClient\tests\HttpClientAllocationTests.cpp:25-58`.
- **Problem:** Each measurement uses a fresh `io_context` and client, so the ~119 allocations counted are mostly
  one-time Asio service setup. Only `limited` is asserted. The pooled-connection path is never measured.
- **Change:**
  - Warm up first (one Submit plus `RunBounded`), then count a second Submit.
  - Assert tight bounds for unlimited, limited and limited+cancellation.
  - Add an end-to-end second-request count against a keep-alive test server.
- **Validation:** the test is stable across 20 repeats. Use it to validate T31, T51 and T62.

---

## P3: cleanups / simplifications / nice-to-have

### [ ] T57 – Remove `std::function` clock from `LockFileImpl` (removes erasure)
- **File:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\LockFileImpl.hpp:46-55`
  (`using Clock = std::function<...>`).
- **Change:** make the clock a template parameter, defaulting to `steady_clock`, or use the atomic clock-pointer
  seam that `ClientRuntime` uses.
- **Validation:** `LockFileImplTests`.

### [ ] T58 – Split `ClientRuntimeBase` into concrete components
- **Files:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\ClientRuntime.hpp`,
  `src\Azure\Impl\ClientRuntime.cpp`.
- **Problem:** One class owns several unrelated concerns: the write fence, lease deadlines, the lazy download-writer
  `thread_pool`, the blob metadata cache and the test clock. Its header also holds
  `Unwrap`/`Call`/`TransferOptions`, `HttpClientFactory` and `windows.h` `#undef`s. It is included almost
  everywhere (via `BlobFileHandles.hpp`), which pulls `HttpClient.hpp`, `thread_pool` and `use_future` into the
  read path.
- **Change:**
  - Extract non-virtual components: `WriteFence` (fence, lease deadlines, `ThrowIfWritesFenced`,
    `BoundByWriteDeadline`), `BlobMetadataCache` (sizes, epoch, clock) and `DownloadWriterPool`.
  - Move `Unwrap`/`Call`/`CallResponse`/`ThrowRequestFailed`/`TransferOptions` to `RequestHelpers.hpp`.
  - Keep forwarding members (or `Fence()`/`MetadataCache()` accessors) so callers can migrate step by step.
  - Check whether `virtual ~ClientRuntimeBase()` is needed. Do T44 first.
- **Validation:** full build plus all Azure tests. Measure build time.

### [ ] T59 – Read-path smaller items
- **Short prefetch at EOF keeps its full budget:** `ReadableFileImpl.ipp` 187-270, 362-366. On success, if
  `data.size() < slot->Length`, `Budget->fetch_sub(slot->Length - data.size())` and shrink `Length`. Add a test
  that the budget returns to 0.
- **Prefetch that hits a 412 throws away its own result** (**Verify first**): `SetMetadata` → `ClearPrefetch` drops
  the pending slot the prefetch itself is refreshing. Record the slot's ETag, make `ClearPrefetch` drop only
  completed slots, and publish a pending slot only if `HasETag(slotEtag)`. Don't widen the `ReadCompletion`
  concept.
- **`AsyncReadTracker` costs a mutex plus an allocation per read:** `AsyncReadTracker.cpp` 12-58. Make the in-flight
  count atomic, lock and notify only on the transition to zero, and make `Token` a move-only RAII type holding
  `shared_ptr<AsyncReadTracker>` instead of a per-read `make_shared<Registration>`.
- **`DownloadBufferPool::Lease` dtor can `terminate`:** `Return` calls `push_back`, which can throw `bad_alloc`.
  Reserve per-class capacity at construction, make `Return` `noexcept`, and free the buffer on failure.
- **Duplicated prefetch helpers:** `ReadableFileImpl.ipp` 219-300, 371-420; `BasicReadableFile.ipp` 321-325.
  - Add `TakeWaitersLocked(id)` and `FindPendingCovering(offset, len)`.
  - Merge the double lock in the catch block.
  - Make `TryReadFromCache` delegate to `TryReadFromCompletedPrefetch`.
  - Collapse `BasicReadableFile::Prefetch`'s two OK branches.
- **Async metadata refresh ignores `IOOptions::timeout`:** `ReadableFileImpl.ipp` 532-575. Give
  `GetMetadataAsync` an optional timeout, and use the smallest waiter timeout.
- **`SequentialFileImpl` refills synchronously** (**Verify first**: measure WAL/MANIFEST replay):
  `SequentialFileImpl.ipp` 34-66. After each refill, call `Prefetch(next range)` so the next refill overlaps the
  I/O.
- **Validation:** `AsyncReadTests`, `SequentialFileTests`, and the DownloadBufferPool tests.

### [ ] T60 – Write/filesystem smaller items
- **Duplicated lifecycle:** `WriteableFileImpl.ipp` and `ReadWriteFileImpl.ipp` both repeat the GetInfo probe,
  Close/dtor/`LogIfUnclosed`, and Sync's mark-stale → flush → publish-size. Move these into `PageBlobWriter.hpp`
  as non-virtual template helpers.
- **`LeaseRenewalLoop::NextRetryDelay`** (`LeaseRenewalLoop.hpp:323-350`) re-implements
  `ClampToEarliestDeadline`. Compute the backoff, then call the clamp.
- **`EnsureLiveness` logs fatal on every call after fencing** (`BlobFilesystemImpl.ipp:386-391`). Log fatal once
  (via an `atomic<bool>` exchange), then debug or rate-limited.
- **Logger never closes its sink** (**Verify first**): `src\Azure\Logger.cpp`, `LoggerImpl.cpp`. Without a
  `CloseImpl` override, the LOG tail after the last Flush can be lost. Add a private `CloseImpl()` override (the
  header stays source-compatible) that Syncs and Closes, and remove the unused `m_logLevel` or document it.
- **`Plugin.cpp` Register overloads** repeat an identical designated-initializer block six times. Forward them to
  one internal template helper; the public signatures stay the same.
- **Extra round trips** (**Verify first**; `BlobFilesystemImpl.Paths.ipp:19`, `.Delete.ipp`):
  - A missing file costs exists plus `HasChildren`.
  - `IsDirectory` → `FileExists` can call `HasChildren` twice.
  - `DeleteDir` invalidates the whole size cache instead of just the deleted names.
- **`OwnedIoContext` can self-join or join during static destruction** (**Verify first**;
  `PluginRegistry.cpp:84-138`). If the dtor runs on the worker thread, detach and log. Document that `Unregister`
  must run before exit.
- **Validation:** the respective unit tests. Count requests with the fake client for the round-trip item.

### [ ] T61 – Core smaller items
- `src\Core\FileCacheMaintenance.cpp` (`PruneUndownloadedStaleEntriesUnsafe`, `EvictBytesUnsafe`) and
  `FileCache.cpp` (`RemoveFileUnsafe`):
  - Add `RemoveEntryUnsafe(iterator)` using `m_cache.extract(it)` and move `node.key()` into `PendingRelease`, to
    avoid repeated path copies and the second hash.
  - Set `RemoveFromDisk = false` for never-downloaded entries.
- `FileCache.cpp` ~470-475: BackgroundDownload pushes a duplicate `{filePath, nullptr, true}` release after
  `RemoveFileUnsafe` has already queued one. Remove the push and keep the log line.
- `FileCache.cpp` BackgroundDownload catch: stop-token cancellation at shutdown is logged as an error. If
  `stopToken.stop_requested()`, log at debug instead.
- `src\Core\LruFileIndex.cpp` ~207-219: the returned path strings are copied from `const` locals under the
  exclusive lock. Make them non-const, `reserve` and `append`, then `std::move` them out.
- `FileBasedCompressedSecondaryCache.cpp` Impl ctor ignores the `CreateDir` result. Check it, log, and throw or
  disable the cache.
- `src\Core\RocksDBHelpers.cpp` `IsFile`: work on a `string_view` instead of copying to `std::string`.
- **Validation:** `ctest -L core`.

### [ ] T62 – HttpClient smaller items
- **Replace the `CancelState::Target` `std::function`** with a closed `std::variant` (removes erasure;
  **Verify first**: libstdc++ allocates for these captures).
  - Files: `CancelState.hpp:20-55`, `HttpClientImpl.hpp:316-324`, `RequestOperation.cpp:26-38`.
  - Variant alternatives: `monostate`, a queue target, and `weak_ptr<RequestOperation<PlainStream>>` /
    `<TlsStream>`.
  - The queue target depends on the `FaultPolicy`-templated limiter. Either template `CancelState` on it, or hold
    a `weak_ptr` to the per-request state and call a fixed non-template hook. No virtuals and no `std::function`.
  - (T18 only fixed the copies.)
- **`AcceptTruncatedTlsBody`'s `bodyless` (204/304) term is unreachable**, because Beast completes those at the end
  of the header. Remove it and fix the doc in `RequestOperation.hpp:254-260`.
- **Stale docs:**
  - `HttpClientOptions.hpp:204-211` says queued requests are not woken at their deadline; T17 changed that.
  - `HttpClient.hpp:74-80` says initiation failures map to `InvalidRequest` "otherwise", but
    `ClassifyBuildFailure` returns `state_not_recoverable` for anything else.
- **`DnsCache::Shared()`** (`DnsCache.hpp:116-119`) can be used after static destruction, via
  `LeaderGuard::~LeaderGuard` when a host's static `io_context` outlives it (**Verify first**). Make it immortal
  (`static auto* c = new ...;`) and comment why.
- **Dead or duplicated code:**
  - `ConnectionKeyEqual` duplicates `operator==`.
  - The `Waiter&` overload of `JoinOrLead` is test-only; move the tests to the factory overload.
  - `Close()` runs in both `~BasicHttpClientCore` and `~BasicHttpClientImpl`; keep the handle's call.
  - `m_maxPerOrigin` is stored only for `Initialize`; pass it as an argument instead.
- **Validation:** `ConnectionPool.*`, `DnsCacheTests.*`, `HttpClientTls.*`, `HttpClientRobustness.*`, and T56.

### [ ] T63 – AzureClient smaller items
- **`ReleaseLeaseAsync` can report a real 409 `LeaseNotPresentWithLeaseOperation` as success** (**Verify first**;
  `BlobRequestHelpers.hpp` ~690-715).
  - The retried-attempt counter is also bumped by 401 refresh repeats and by unambiguous failures.
  - Count only attempts that followed an ambiguous outcome (a transport error after send, 5xx, or timeout).
  - Test both 401 → 409 (should be an error) and reset → 409 (should be success).
- **Duplicated integer parsers:** `ParseUnsigned`, `ParseInt`, `ParseUnsignedComponent` and the inline
  `from_chars` calls in `ParseContentRange` (`BlobResponseParsers.cpp`), `ParseHeaderUnsigned`
  (`RequestPipeline.cpp`) and `ParseInteger` (`Credentials.cpp`). Replace them with one
  `template<class T> std::optional<T> ParseDecimal(std::string_view)` in a private header.
- **Failure-to-details conversion is duplicated:** `BlobDownload.cpp:38-39` uses `value_or(MakeClientError(...))`,
  which builds the error eagerly. The same logic is in `BlobBatch.cpp:188-189` and `Detail\BlobBatch.hpp:247-248`.
  Add one `ToDetails(RequestFailure&&)` helper.
- **List Blobs XML parsing does ~20 linear child scans per blob** (`ParseBlobPropertiesXml`, `TryReadXml`).
  Classify the children in a single pass, like `ClassifyBlobHeader`, and return `std::optional<XmlDocument>`
  instead of a `unique_ptr`. Benchmark a 5000-entry list.
- **`ParseGetPageRangesResult`:** `End - Start + 1` wraps when `End == UINT64_MAX`. Reject that case and add a
  malformed-XML test.
- **Key hygiene in `SharedKeySigner.cpp`:**
  - `Base64Decode` doesn't `OPENSSL_cleanse` its buffer on the throw path; add a scope guard.
  - `AuthorizeRequest(const SharedKeyCredentialOptions&)` (`BlobRequestHelpers.cpp:700`) re-decodes the key on
    every call. Move it to test code or cache the signer.
- **Loopback checks disagree:** `IsLoopbackEndpoint` accepts only `127.0.0.1`, while
  `IsAcceptableIdentityEndpoint` accepts 127/8. Share one predicate. Also pass `ApplyLeaseRequestConditions`'s
  conditions by `const&`.
- **Unused includes:** in `BlobResponseParsers.cpp` (OpenSSL, `boost/uuid`, `<mutex>`, `<locale>`, the
  AsyncInitiation and client headers), `BlobRequestHelpers.cpp` and `SharedKeySigner.cpp`. Check with
  `misc-include-cleaner`.
- **`RunOnWriter` fallback** spawns a detached `std::thread` per batch when no scheduler is given, which is
  unbounded and never joined. Require a scheduler, or reuse a single worker.
- **Misc:**
  - `CredentialVariant::Type()` dereferences a possibly-null credential.
  - `CachingTokenCredential` checks for a null `m_inner` on every call; validate it once in `Create`.
  - The `RetryOperation` ctor copies `requestOptions`; move it.
  - `InitiateClientOperation` snapshots the client `shared_ptr` even for eagerly started tokens.
  - The `extern template class Basic*Client<HttpClient>` declarations buy little, because the operations are
    member templates. Measure, then drop them or move the non-template members into `src`.
- **Validation:** the AzureClient test suite.

### [ ] T64 – Build/CMake smaller items
- **Unused Boost links:**
  - `boost/algorithm` in `libs\AzureClient\src\CMakeLists.txt:7,101` and
    `aveva-azure-client-config.cmake.in:13`.
  - Boost.Thread in `src\Core\CMakeLists.txt:40,60` and `aveva-rocksdb-plugin-core-config.cmake.in:13`.
  - Remove the `find_package`/link/`find_dependency` entries.
- **Possibly unused vcpkg deps** (**Verify first**; they may predate the branch): `boost-endian`,
  `boost-interprocess`, `boost-iostreams[zlib]`, `zstd`.
- **`Boost::json` is linked PRIVATE** to Impl, but `TokenCredentials.hpp` includes it and is reached through public
  include paths. Make it PUBLIC/BUILD_INTERFACE, or move the include into the `.cpp`.
- **Leftover bare `install(TARGETS ...)`** in `src\Azure\Impl\CMakeLists.txt` and `src\Azure\Models\CMakeLists.txt`,
  next to `aveva_install_library`. Delete them.
- **Dead variables:** `AVEVA_AZURE_CLIENT_EXAMPLES` and `AVEVA_AZURE_CLIENT_INSTALL_TEST`
  (`infrastructure\cmake\AvevaClientLibraries.cmake:28-29`). Also fix the stale `LinuxDebugTSan` comment in
  `libs\AzureClient\tests\CMakeLists.txt`.
- **Warning flags:** http-client has none, and no clang-tidy hook, unlike azure-client. Share one warnings helper.
- **Test CMake:**
  - The Impl test exe runs `gtest_discover_tests` 4×; discover once and set labels via `PROPERTIES`.
  - The suite→label map is hand-maintained; derive it from a naming convention.
  - Replace `../../../../../../src/Azure` with `${PROJECT_SOURCE_DIR}`.
  - Scale `TIMEOUT 30` under sanitizers.
  - Apply sanitizer labels and instrumentation to the HttpClient and plugin test trees too.
  - List `TempDirectory.hpp` in the Mocks CMakeLists.
- `tools\db_bench\README.md:62` contains a literal `\r\n`; replace it with a real newline.
- **Validation:** clean configure and build of all presets, install-consume, `ctest -N` counts unchanged.

### [ ] T65 – Test infrastructure consolidation
- **Temp file/dir helpers:** create one UUID-based RAII `TempDirectory`/`TempFile` in a shared test-support target,
  and replace:
  - `HttpClientTestHelpers` `TempFile`;
  - `ParallelDownloadTests.cpp:89` (test-name based, so it can collide);
  - `DownloadToFileTests.cpp:74/106/130` (fixed names). Line 150 scans the whole system temp dir; scan the test's
    own dir instead.
  - `BlobOperationsTests.cpp:57` (no RAII);
  - `PluginSecondaryCacheIntegrationTests.cpp:44` and `BlobFilesystemIntegrationTests.cpp:1431`.
- **io_context helpers:** `AsyncReadTests.cpp:968` `HostIoContext` duplicates `IntegrationTestHelpers` `TestIoContext`.
  Share one.
- **gtest listeners:** `FakeHttpClient.hpp:1012-1021` registers them through a static initializer, while
  `TestEventListener.cpp:27` has a custom `main` written to avoid exactly that. Use one
  `InstallFakeHttpClientListeners()` called from every test `main`; `ImplTests.cpp` currently lacks the
  poll-drain listener.
- **FakeBlobServer:** its public `Data`/`Size`/`ETag` references, `SetCapacity` and `FakeBlobEnvironment::Fill`
  mutate state without `m_mutex` while the pump thread runs `Respond`, which is a TSan race. Replace them with
  locked setters and snapshots.
- **`AsyncBehaviorSupport.hpp`:** about 45 of its 54 helpers are used at most once (**Verify first**). Move those
  into the using test file.
- **`IntegrationTestHelpers.hpp:109`:** the doc says it "skips" but the code calls `FAIL()`. Also,
  `CreateContainerClient` reports the failure twice; return after the helper.
- **Validation:** all three test trees, including `ctest -j` and a TSan run of the azure-impl tests.

### [ ] T66 – Test gaps to add alongside the fixes above
- MultiRead merged group crossing EOF: members wholly past EOF get OK and empty; a straddling member gets a
  partial result.
- A prefetch that completes short at EOF returns its budget (T59).
- An async refresh updates the runtime metadata cache (T39).
- File wrappers return InvalidArgument for out-of-range offsets (T33).
- FileCache invalidation on re-create (T35).
- Unlock against a service that never responds (T38).
- Flush failure followed by a retry (T37).

---

## Prior pass (T01–T27): closed, do not redo

**Done:**
- T01 io_context guards on MultiRead/Poll.
- T03 (size, ETag) open cache.
- T04 MultiRead zero-fill / per-group vectors.
- T05 per-read async allocations.
- T06 download-writer erasure.
- T07 static `DownloadSink`.
- T08 lock-free write deadline.
- T09 in-place `ReadableFileImpl`.
- T10 per-size-class `DownloadBufferPool` locks.
- T11 targeted blob-size invalidation and transparent lookup.
- T12 rename zero-fill.
- T15 `LruFileIndex` pinned-tail scan.
- T17 limiter deadline wakeups.
- T18 `CancelState` target copies.
- T19 retry per-attempt copies.
- T20 SharedKey allocations.
- T21 single-pass property headers.
- T26 misc nits.
- T27 PIC on azure-client, overlay patch CI.

**Dropped after measurement (no measurable gain):**
- T16 OriginLimiter ticketing / string_view keys.
- T22 response body copy (`buffer_body` sink).
- T23 RFC1123 date caching, `BuildQueryString` reserve, `Route::Of`.
- T24 DnsCache `shared_mutex`, idle-probe threshold, TLS shutdown `make_shared`, TLS session cache.
- T25 BlockOn stack promise. This is unsafe: an io_context destroyed with queued work would hang.

**Reviewed and rejected:**
- `LruFileIndex::RegisterEntry` erasing a pinned entry.
- Rename always copying through a buffer.
- `LeaseRenewalLoop::Fail` race.
- Public-header unique_ptr pimpl.
- AzureClient `cxx_std_23` PUBLIC→PRIVATE.
- One strand per client.
- ISO-8601 >7 fraction digits.
- Retry pipeline body sharing on single attempt.
- TSan label split.

**Reviewed in pass 2 and judged sound (no task):**
- `RetryOperation` event trampoline.
- `DownloadOperation` reorder window / `CheckedAdd` / If-Match.
- Token refresh coalescing with pre-expiry window.
- HttpClient handler lifetimes, exactly-once completion, keep-alive reuse rules.
- XML parser DTD/entity rejection.
- Header-injection guards.
- FileCache lock ordering and deferred-release ordering.
- `AsyncReadRequest` abort races.
- Rename rollback ordering.
- Lease renewal accounting.
