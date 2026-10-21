# Review backlog (pass 3): `feature/trim-azure-client` vs `main`

Review scope: merge-base `278868c`, 494 commits, 352 files (+53.0k / −5.5k). This pass covered `libs\HttpClient`,
`libs\AzureClient`, `src\Azure` (with the public headers), `src\Core`, build/CI/tooling, test infrastructure and docs.
Passes 1 and 2 (T01–T66) are summarised under "Prior passes" at the end. Every finding below was checked against the
code. Findings marked **Verify first** are medium confidence: confirm or measure them before changing anything.

## Hard constraints for the executor

- **No type erasure.** Do not introduce `std::function`, `std::move_only_function`, `std::any`,
  `any_io_executor`, `any_completion_handler`, virtual interfaces that replace templates, or erasing pimpls. The
  branch exists to build zero-overhead, template-based async paths. Removing erasure that is already there is
  encouraged (see T69 and T70).
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

### [x] T67 – Download and batch-delete abort on *partial* cancellation
- **Files:**
  - `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp:206-214` (`DownloadOperation::Begin`,
    `m_parentSlot.assign`)
  - `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobBatch.hpp:78-87` (`BatchDeleteOperation::Start`)
  - Reference behaviour: `Detail\RequestPipeline.hpp:112-119`
- **Problem:** `RetryOperation` ignores cancellation unless the type includes `terminal` or `total`, which is the
  documented contract. `DownloadOperation` and `BatchDeleteOperation` ignore the `cancellation_type_t` argument, so
  a `partial` emit (or any other type) aborts the whole operation with `operation_canceled`. Composed operations and
  `asio::experimental` parallel groups emit partial cancellation, so the same slot gives different results
  depending on which client call is behind it.
- **Change:** In both slot handlers, return early when
  `(type & (terminal | total)) == none`, using the same filter as `RequestPipeline`. Factor the predicate into one
  `inline bool IsAbortingCancellation(cancellation_type_t)` in `Detail\AsyncInitiation.hpp` (or a small new
  detail header) and use it from all three sites.
- **Validation:** Add one test each to `ParallelDownloadTests.cpp` and `BatchDeleteTests.cpp`: emit
  `cancellation_type::partial` mid-operation and assert success; then emit `terminal` and assert
  `operation_canceled`. `RetryTests.cpp:683` already does this for the pipeline and can serve as the template.

### [!] T50 – Direct-path `BoundHandler` may run the handler off its associated executor (carried over, still blocked)
- **Done:** FakeHttpClient now completes on its executor (infra commit), then the Direct branch invokes inline only when unning_in_this_thread() (executors without it, e.g. use_future's, stay inline) and dispatches otherwise. Test: CredentialCompletingOnForeignThreadRunsTheHandlerOnTheClientExecutor (fails without the change). Inline path adds no allocation: it is a thread-local check only.
- Previously: still open, still blocked on test infrastructure. Re-confirmed in this pass:
  `AsyncInitiation.hpp:45-70` invokes inline, and `RetryOperation::Run` can be entered from a credential's
  completion thread.
- **Unblocking plan (do this first, as its own commit):**
  1. In `libs\AzureClient\tests\FakeHttpClient.hpp` and `ScriptedTokenCredential`, make `CompleteNext` post the
     completion to the fake's executor instead of calling it inline, and have every test that calls `CompleteNext`
     drive `Poll()`/`run_one()` afterwards. A shared helper such as `CompleteNextAndPump()` keeps the churn small.
  2. Re-run the ~12 tests listed in pass 2 (`AsyncBehaviorTests` reentrancy/move-only/download-destroy,
     `A3_DownloadIntoSpanTests`, `AsyncReadTests`, `WriteableFileTests`) and fix any that relied on inline
     completion.
  3. Then land the production change: the Direct branch invokes inline only when
     `executor.running_in_this_thread()`, and dispatches otherwise.
- **Validation:** a test whose credential completes on a separate `io_context` thread asserts that the handler runs
  on the client executor. Allocation tests must show no new allocations on the inline path.

---

## P2: performance / improvements

### [x] T68 – Map the remaining Azure error codes; one lease-lost predicate
- **Done:** `AzureErrorTranslator::IsLeaseLost` is now the single lease-lost list, used by the translator and `IsFatalLeaseFailure`. `InvalidPageRange` → `InvalidArgument`; 412 condition codes → non-retryable `IOError` with the "Blob changed concurrently: " prefix (not Corruption, since nothing on disk is corrupt); `LeaseAlreadyPresent` → `IOFenced`. `LockFileImpl` checks the raw 409, so lock contention is unchanged. Tests added to `AzureErrorTranslatorTests`.
- **Files:**
  - `src\Azure\AzureErrorTranslator.cpp:60-101`
  - `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\LeaseRenewalLoop.hpp:116-129` (`IsFatalLeaseFailure`)
  - `libs\AzureClient\include\AVEVA\AzureClient\BlobStorageErrorCode.hpp`
- **Problem:**
  - T34 added the codes to the enum and category, but the translator still sends several of them to a generic
    `IOError`:
    - `InvalidPageRange` (416) is a caller or offset bug, not an I/O failure;
    - `SequenceNumberConditionNotMet`, `ConditionNotMet` and `TargetConditionNotMet` (412) say that the blob
      changed underneath us;
    - `LeaseAlreadyPresent` is contention for the lock file and ownership loss for a write.
  - The translator and `LeaseRenewalLoop::IsFatalLeaseFailure` each keep their own list of "lease lost" codes, and
    the lists differ. The behaviour is correct today, because `!IsTransient` catches the codes missing from the
    renewal list, but the lists will drift.
- **Change:**
  - `InvalidPageRange` → `IOStatus::InvalidArgument`.
  - The 412 condition codes stay non-retryable, but get a distinguishing prefix, for example
    `"Blob changed concurrently: "`. Prefer `IOStatus::IOError` over `Corruption` because nothing on disk is
    corrupt. Document the choice.
  - `LeaseAlreadyPresent` on a data-blob write → `IOFenced`. Check `LockFileImpl::ShouldRetryAcquire` first: it
    sees the raw `BlobStorageError` and must keep treating a 409 on acquire as contention.
  - Add `bool IsLeaseLost(std::error_code)` to `AzureErrorTranslator`, or to `RequestFailedException.hpp`, and use
    it from both the translator and `IsFatalLeaseFailure`.
- **Validation:** One parameterised `AzureErrorTranslatorTests` case per code, asserting the status kind and
  retryability. `LeaseSafetyTests` and `LockFileImplTests` must still pass.

### [x] T69 – `CancelState` stores a `std::function` target and copies it under the lock (removes erasure)
- **Files:** `libs\HttpClient\src\private\CancelState.hpp:20-60`; callers `private\RequestOperation.cpp:33-37` and
  `private\HttpClientImpl.hpp:319-323`.
- **Problem:** There are exactly two kinds of target: a running `RequestOperation`, captured as a `weak_ptr`, and a
  limiter queue entry, captured as a `weak_ptr<Limiter>` plus the origin `std::string` and a ticket. The limiter
  lambda is larger than the libstdc++ `std::function` small buffer (16 bytes) and probably MSVC's as well, so
  `SetTarget` heap-allocates for every limited request that has a connected cancellation slot. `Cancel()` copies the
  `std::function` under the mutex, which allocates again and copies the origin string. It is erased for no reason.
- **Change:** Replace `Target` with a closed
  `std::variant<std::monostate, std::weak_ptr<RequestOperation>, LimiterTarget>`, where `LimiterTarget` is
  `{std::weak_ptr<Limiter>, std::string origin, Ticket}`, and add a `std::visit`-based invoke. `Cancel()` moves the
  weak handles out under the lock. A repeated `Cancel` must still reach the target, so copy only the `weak_ptr`
  part or keep the state, but never copy the string while holding the lock. If including `RequestOperation` from
  `CancelState.hpp` creates a cycle, template `CancelState` on the two target types instead.
- **Validation:** `HttpClientRobustnessTests` (cancellation cases), `OriginLimiterTests`. Extend
  `HttpClientAllocationTests` with a request that carries a connected cancellation slot and passes through the
  limiter, and record the allocation count before and after.

### [-] T70 – Remaining internal erasure on the plugin read and lease paths (**Verify first**)
- **Dropped (verified, no code change):**
  - `RenewAsync`: its definition is in `LockFileImpl.ipp` and is only explicitly instantiated for `PageBlobClient`.
    A `template <class TCallback>` member needs its body visible to `LeaseRenewalLoop.hpp` and the tests, which means
    moving it, with `TotalDeadlineFor`, `Now`, `Unwrap` and the private lease state, into the header. That adds header
    weight and breaks the `.ipp` plus `extern template` structure. The gain is one `std::function` allocation (a
    40-byte capture) per lease renewal, once per renewal interval and next to a network round trip, so it is not
    measurable.
  - Prefetch waiters: the waiter embeds `makeDownload()`, an unnameable lambda local to `StartRead`. A concrete struct
    would need that type, so it would have to be a template, and a homogeneous `Waiters` vector in the non-template
    `ReadableFileImpl` state then needs erasure again. The path only runs while a prefetch is pending.
  - `MetadataCallback`: waiters come from several `ReadAsyncAttempt<TCallback>` instantiations with no common
    continuation type, so erasure stays (as the task allowed).
- **Files:**
  - `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\LockFileImpl.hpp:69-72` (`RenewCallback =
    std::function<void(std::exception_ptr, bool)>`); `src\Azure\Impl\LockFileImpl.ipp:229`
  - `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\ReadableFileImpl.hpp:69` (prefetch `Waiters` of
    `std::move_only_function<void()>`), `:139` (`ChainOntoPendingPrefetch`), `src\Azure\BasicReadableFile.ipp:124`
  - `ReadableFileImpl.hpp:81-89` (`MetadataCallback`, `CoalescedMetadataAsync`)
- **Problem / change:**
  - `RenewAsync` has a single caller, `LeaseRenewalLoop`, and lives in an `.ipp`. Make it
    `template <class TCallback> void RenewAsync(TCallback callback) const`. This is straightforward erasure removal.
  - Prefetch waiters have one producer, the `resume` lambda in `BasicReadableFile::StartRead`. Replace the erased
    waiter with a concrete struct: `{shared_ptr<const TImpl> file, int64_t offset, length,
    shared_ptr<AsyncReadRequest> request, <download params>, logger}`, plus a member `operator()` that contains the
    current lambda body. `ChainOntoPendingPrefetch` then takes that type. This path only runs while a prefetch is
    pending, so the change matters for erasure hygiene, not throughput.
  - `MetadataCallback` waiters come from more than one `ReadAsyncAttempt<TCallback>` instantiation, so a
    homogeneous vector needs a common type. Erasure here is acceptable unless every caller can share one concrete
    continuation type. Check that, and leave it alone if they cannot.
- **Validation:** `LockFileImplTests`, `LeaseSafetyTests`, `ReadableFileTests` (prefetch chaining),
  `BlobFilesystemFakeTests`.

### [-] T71 – One allocation fewer per `ReadAsync` (**Verify first**: measure)
- **Dropped:** measured with a temporary global `operator new` counter around `ReadableFile::ReadAsync` (deferred
  download, fake blob server, Debug): about 207 allocations per call (221 on the first call). The handle is one of
  them, so removing it saves about 0.5%, which is below noise next to a network round trip. An intrusive refcount or
  a free-list would add complexity to the `AsyncReadRequest` lifetime for no measurable gain. No code was changed.
- **File:** `src\Azure\BasicReadableFile.ipp:84-86`.
- **Problem:** Every async read allocates both a `shared_ptr<AsyncReadRequest>` (`make_shared`) and a separate
  `unique_ptr<AsyncReadHandle>`, which only wraps that `shared_ptr` for RocksDB's `void*`.
- **Change:** Either give `AsyncReadRequest` an intrusive reference count, so that `io_handle` is the raw request
  pointer and `DeleteAsyncReadHandle` drops one reference, or embed the handle state in the request and hand out
  `new`'d storage from a small free-list. Keep the `AsyncReadRequest` invariants, and keep `DeleteAsyncReadHandle`
  aborting the request.
- **Validation:** Count allocations per `ReadAsync` and run `ReadableFileTests`/`BlobFilesystemFakeTests`, then
  `Compare-Backends.ps1` with a random-read workload. Drop the change if it is below noise.

### [x] T58 – Split `ClientRuntimeBase` into concrete components (carried over, partial)
- **Done:** request helpers moved to `RequestHelpers.hpp`.
- **Remaining (refined):**
  1. `ClientRuntime.hpp:4-31` still includes `HttpClient.hpp`, `thread_pool.hpp`, `use_future.hpp`,
     `io_context.hpp` and `post.hpp` for every user of `ClientRuntimeBase`. Move `HttpClientFactory` and
     `BasicClientRuntime` to a new `ClientRuntimeTransport.hpp` that only factories and `PluginClients` include.
     Forward-declare `boost::asio::thread_pool` in the base header; `DownloadWriter()` returns
     `thread_pool::executor_type`, so keep that include in the base header or return through a small concrete
     wrapper.
  2. Extract `WriteFence` (`m_writesFenced`, `m_writeDeadline`, `m_leaseMutex`, `m_leaseDeadlines`,
     `ThrowIfWritesFenced`, `SetLeaseDeadline`, `WriteTimeRemaining`) and `BlobMetadataCache` (sizes, tombstones,
     epoch, sweep, test clock) as non-virtual members. Keep forwarding functions so callers can migrate gradually.
  3. In `ClientRuntime.cpp:142-184`, `CachedBlobSize` and `CachedBlobMetadata` repeat the lock-and-find step. Put it
     in one helper with a projection, so that the size path never copies the ETag.
  4. Check whether `virtual ~ClientRuntimeBase()` is needed (search for deletion through a base pointer) and make
     it non-virtual if not.
- **Validation:** full build and all Azure tests. Compare clean-build time for `aveva-rocksdb-plugin-azure-impl`
  before and after.

### [x] T72 – CI install-consume only covers the aggregate plugin package
- **File:** `.github\workflows\build-and-test.yml:258-302`.
- **Problem:** The consumer job only uses `find_package(aveva-rocksdb-plugin-azure)`. `libs\README.md` documents
  `aveva-azure-client`, `aveva-http-client`, `aveva-rocksdb-plugin-core` and `aveva-rocksdb-plugin-azure-impl` as
  separately installable packages, and none of them is configured or linked on its own. A broken
  `find_dependency` in one of their `*.cmake.in` files would go unnoticed.
- **Change:** Extend the generated consumer `CMakeLists.txt` with one tiny executable per package. Each one calls
  `find_package(<pkg> CONFIG REQUIRED)`, includes a representative public header, and references one non-inline
  symbol behind the existing `argc > 100` guard so the link is real. This stays inside the same job and costs a few
  seconds.
- **Validation:** The job passes on both Linux and Windows. Deliberately removing a `find_dependency` from
  `aveva-azure-client-config.cmake.in` locally must make it fail.

---

## P3: cleanups / simplifications / docs

### [ ] T73 – `AzureErrorTranslator` keeps two copies of the exception-to-status ladder
- **File:** `src\Azure\AzureErrorTranslator.cpp:111-130` (`FromException(exception_ptr)`) and `:132-162`
  (`FromCurrentException(logger, …)`).
- **Problem:** The catch ladders (`RequestFailedException`, `WritesFencedException`, `invalid_argument`,
  `system_error` with cancellation, `exception`, `...`) are written out twice. The non-logging overload is public
  API, so it must stay, but its body can be shared. A future mapping change, such as T68, has to be made twice.
- **Change:** Add a private
  `template <class TLog> IOStatus ClassifyCurrentException(TLog&& log, std::string_view operation)` that contains
  the ladder and calls `log(severity, message)`. Pass a no-op lambda from `FromException(exception_ptr)` and a
  `BOOST_LOG_SEV` lambda from `FromCurrentException`. A lambda template parameter is not erasure.
- **Validation:** `AzureErrorTranslatorTests` unchanged and passing.

### [ ] T74 – Read-path small items
- `src\Azure\BasicReadableFile.ipp:69-70` and `:82-83`: `ReadAsync` runs `RangeCheck::IsValid` and then
  `StartRead` runs it again. `MultiRead` already filters invalid ranges before calling `StartRead`. Keep the check
  in `StartRead` only, and keep `ReadAsync`'s `cb` null-check.
- `src\Azure\BasicReadableFile.ipp:321-326` (`Prefetch`): `if (!TImpl::Prefetch(...)) return OK; return OK;` has
  two identical branches. Collapse it to `(void)TImpl::Prefetch(...)` (or `std::ignore = ...`) and keep the
  comment that explains why a declined hint is not `NotSupported`.
- **Validation:** `ReadableFileTests`, `SequentialFileTests`.

### [ ] T75 – `DnsCache::JoinOrLead` builds an owning key on every pending-lookup probe
- **File:** `libs\HttpClient\src\private\DnsCache.hpp:163-173`.
- **Problem:** `m_pending.try_emplace(Key{std::string(host), std::string(service), scope})` allocates two strings
  even when the caller is only joining a lookup that is already in flight. The class comment says lookups use
  views. This only affects concurrent cold-cache resolves, so the gain is small.
- **Change:** Probe with `m_pending.find(KeyView{host, service, scope})`, and on a hit push the waiter and return
  false. Only on a miss, `emplace` with an owning `Key`. `try_emplace` with a heterogeneous key needs C++26, so do
  not rely on it.
- **Validation:** `DnsCacheTests`. Drop the change if no allocation difference is measurable on the join path.

### [ ] T76 – `RunOnWriter` falls back to a detached thread per task
- **File:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp:52-62`; docs at
  `BlobOperationOptions.hpp:86, 111` and `BlobClient.hpp:134`.
- **Problem:** When there is no `WriteScheduler` and the transport has no `GetDownloadWriter()`, every blocking
  sink write runs on a new `std::thread(...).detach()`. The plugin is not affected, because it always passes
  `ClientRuntimeBase::DownloadWriter()` (`AzureContainerClient.hpp:45`). A direct library user doing
  download-to-file gets unbounded thread creation, and threads that can outlive the client at shutdown.
- **Change:** Pick one:
  - (a) add `GetDownloadWriter()` to `::AVEVA::HttpClient`, backed by a lazily created, client-owned
    `boost::asio::thread_pool` that is joined in the client's destructor;
  - (b) make `WriteScheduler` required for file and stream sinks, and fail with `invalid_argument` if neither is
    available.

  Option (a) keeps the API ergonomic. In either case, update the three doc comments.
- **Validation:** `DownloadToFileTests` with `::AVEVA::HttpClient` and no scheduler, asserting a bounded thread
  count.

### [ ] T77 – Stale and unused includes in hot template headers
- `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp:35`: `<functional>` is unused there. Check
  `<map>`, `<fstream>` and `<filesystem>` as well, and move anything that only file sinks need to
  `DownloadSinks.hpp`.
- `libs\HttpClient\src\private\CancelState.hpp:7`: `<functional>` becomes unused after T69.
- `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\ClientRuntime.hpp:22`: `<functional>` is only needed for
  `std::hash`/`std::equal_to`. Keep it, but recheck after T58.
- **Validation:** builds on both presets. Optionally run clang-tidy `misc-include-cleaner` on the touched headers.

### [ ] T78 – Docs: `libs\README.md` lists a dependency the Azure client no longer has
- **File:** `libs\README.md:22`, and the consumer guidance around line 36.
- **Problem:** The table lists `boost_algorithm` as a private dependency of `aveva-azure-client`, but only
  `src\Core\CMakeLists.txt:35,55` links `Boost::algorithm`. `libs\AzureClient\src\CMakeLists.txt` no longer does.
- **Change:** Remove it from the `aveva-azure-client` row and from any surrounding text. Cross-check the other rows
  against each `libs\*\src\CMakeLists.txt` and `*.cmake.in` in the same commit.
- **Validation:** Read the docs against the CMake files. The T72 consumer will also catch packaging drift.

---

## Reviewed in pass 3 and rejected (do not redo)

- **"`IdleSweeper::Stop` does not wait for an in-flight sweep":** this is wrong. `RunSweep`
  (`IdleSweeper.hpp:104-113`) holds `m_sweepMutex` for the whole sweep, so taking that mutex in `Stop` already
  waits. Any later `RunSweep` then sees `m_stopped`.
- **"An ignored-range `200` body is copied or truncated at unbounded size":** the response-body limit bounds the
  body, and `resize` only shrinks it without reallocating (`BlobDownload.hpp:331-344`).
- **Public-config callbacks:** `Credentials.hpp` `AssertionProvider`, `ChainedTokenCredential`
  `SourceFailureObserver`, `HttpDiagnosticsCallback` and `PluginRegistry` `FileSystemFactory` are user-supplied
  configuration hooks off the hot path. Their erasure is acceptable.
- **`TokenCredential` waiters as `any_completion_handler`:** this is documented and deliberate, because waiters of
  different handler types share one refresh. It is the only erasure on that path.
- **`MultiRead` `active.erase(it)` on a vector:** `n ≤ kMultiReadMaxInFlight`, so it does not matter.

## Reviewed in pass 3 and judged sound (no task)

- `RequestOperation` completion ownership (T48 fix), cancellation posting, stale pooled-connection retry rules and
  the exactly-once `Finish`.
- `OriginLimiter` FIFO ordering, cancellation and deadline removal; connection-pool exception safety and
  executor-aware disposal; DNS-cache leaked singleton; bounded TLS shutdown.
- `CachingTokenCredential` conditional invalidation (T49), lazy total-deadline timer (T51), `RetryOperation` event
  serialisation.
- `FinishDownload` Content-Range validation (T40), prefetch copy outside the mutex (T41), per-URL size tombstones
  (T44), `ThrowIfRunningOn` guards in `BlobOperations` and `LockFileImpl`.
- FileCache intrusive LRU, generation checks (T45), handle lifetimes and lock separation; `LruFileIndex` pinning
  and deferred eviction.
- CMake target visibility, Core PIC, CI cache-key separation, sanitizer split.

---

## Prior passes (T01–T66): closed, do not redo

**Pass 2 (T28–T66):** everything is done except:
- **T43:** skipped after measurement. The merged-group copy is under 1% of download time.
- **T50:** done in pass 3.
- **T58:** partial; carried over above with a refined plan.

The full pass-2 text is in git history at `HEAD:tasks.md` (commit `b292244`).

**Pass 1 (T01–T27):**
- **Done:** T01 io_context guards; T03 (size, ETag) open cache; T04 MultiRead zero-fill; T05 per-read async
  allocations; T06 download-writer erasure; T07 static `DownloadSink`; T08 lock-free write deadline; T09 in-place
  `ReadableFileImpl`; T10 per-size-class `DownloadBufferPool` locks; T11 targeted blob-size invalidation; T12 rename
  zero-fill; T15 `LruFileIndex` pinned-tail scan; T17 limiter deadline wakeups; T18 `CancelState` target copies (see
  T69 for the remaining erasure); T19 retry per-attempt copies; T20 SharedKey allocations; T21 single-pass property
  headers; T26 misc nits; T27 PIC on azure-client, overlay patch CI.
- **Dropped after measurement:** T16, T22, T23, T24, T25 (BlockOn stack promise is unsafe).
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
