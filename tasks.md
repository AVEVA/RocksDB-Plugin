# Branch review execution backlog

Reviewed `feature/trim-azure-client` at
`46b93bb` against local `main`, using merge base
`278868c71dc55ffc1586f3f92ed8bc5d99587e6d`.
The committed comparison contains 358 changed files, 54,386 insertions and
5,507 deletions. Review date: 2026-10-21.

This is a new handoff, not an instruction to repeat completed optimization
passes. T91-T105 are recorded in `46b93bb:tasks.md`; earlier dispositions are in
`c950080:tasks.md` and the corresponding commits. IDs continue at T106.
The working tree had `tasks.md` deleted at review start; this file recreates the
requested backlog. No production, test or configuration files were changed.

The review covered changed HTTP connection/DNS/scheduling paths, Azure client
retry/download/batch/credential paths, plugin read/write/lease/filesystem/cache
integration, Core cache changes and representative build/benchmark tooling.
It was not an exhaustive line-by-line review of every changed test or every
Azure deployment configuration.

## Execution constraints

- **Do not introduce or recommend type erasure.** Preserve concrete templates,
  concepts, variants, executor types and statically typed continuations. Do not
  replace these paths with erased callbacks/executors, virtual dispatch or
  erased implementation objects. Existing API-mandated callbacks are not a
  reason to generalize erasure internally.
- Preserve public-header source compatibility, exactly-once completion,
  associated executor/allocator/cancellation behavior and meaningful error
  translation. Do not turn operational failures into success.
- The plugin must not own or run the host-supplied `io_context`. Never wait for
  network completions on a thread running that context.
- Keep downloads out of caller-owned scratch until the request lock confirms
  that the request is live. RocksDB callbacks still run only from `Poll` or
  `AbortIO`.
- Preserve write fencing, sticky upload errors, bounded upload concurrency,
  ETag consistency and the documented non-atomic-rename recovery guarantees.
- For correctness tasks, write a focused regression and demonstrate the
  failure before changing production code. **Source-traced** means the failure
  mechanism was inspected, not that a new runtime regression was run.
  **Verify first** items require reproduction/measurement before implementation.
- Performance work must record before/after request counts, peak concurrency,
  allocation counts/bytes or representative timings, as appropriate. Retain no
  added complexity for an unmeasurable improvement. Do not claim end-to-end
  speedups from static review or fake-transport timings.
- Keep each task focused, record its result beside its checkbox, and make one
  focused commit per task when execution/commits are authorized. If a hypothesis
  does not reproduce or a proposed optimization is not worthwhile, record that
  disposition rather than changing behavior speculatively.
- Follow `.github\copilot-instructions.md`. Do not create a PR without explicit
  approval. This review only creates the handoff; it does not implement it.

## P1: correctness, ownership and completion guarantees

### [x] T106 - Stop caching connection-pool map iterators across rehash

**Result:** Done. Regression `SweepAfterOriginMapRehashClosesEveryExpiredConnection` (512 origins) failed before the fix (Sweep did not close every expired connection); `Origin` now keeps a pointer to its stable map key and `EraseOrigin` does a fresh lookup. Added a global-cap churn test; all 1181 tests pass on WindowsDebug. ASan was not run (MSVC debug preset); no allocation-path change.

**Status:** Source-traced; high confidence.

**Evidence:** `libs\HttpClient\src\private\ConnectionPool.hpp:339-343,459-466`
stores an insertion iterator in each origin; expiry/global eviction erase
through it at lines 364 and 504. `boost::unordered_node_map` preserves element
addresses across rehash, not iterators. The installed Boost iterator contains
pointers into slot/group arrays that rehash replaces.

**Impact:** Enough distinct origins invalidate cached iterators. A later sweep
or global-cap eviction can use invalid bucket storage, despite the origin
objects themselves remaining address-stable.

**Action:** Retain stable origin pointers and intrusive lists, but erase using
a fresh lookup or a stable reference to the map-owned key. Do not rely on a
guessed reserve limit to suppress rehash.

**Acceptance:** Exercise at least 128 distinct origins with a generous total
cap, expire/sweep them, then separately churn origins through a small global
idle cap. Check exact stream destruction, idle accounting and continued reuse.
Run these cases under ASan and preserve warmed pool allocation behavior.

### [x] T107 - Keep both I/O and handler executors alive during composed operations

**Result:** Done. Regression `DistinctHandlerExecutorKeepsTheClientContextAliveUntilCompletion` failed before the fix (client context stopped while the distinct handler context held the bound handler); `BoundHandler` now also holds a work guard on the client executor when the handler executor differs (none when they are equal; direct fast path preserved). All 1182 tests pass on WindowsDebug. The full credential/writer `run()`-once matrix and `use_future` (its associated executor is the default one, so it takes the single-guard path) were not added as separate end-to-end tests.

**Status:** Reproduced at the actual binding-helper level; follow-up to T91.

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\AsyncInitiation.hpp:99-117`
takes work only on the associated handler executor. If that executor belongs
to a different context, nothing here keeps the client's context alive during
foreign-thread credential acquisition or writer work. An isolated probe using
`BindAssociationsAndAdoptCancellationOn` printed
`client_stopped=1 handler_stopped=0` while the operation's bound handler existed.

**Impact:** The client's one `run()` can finish before a continuation posts
back, even though the completion context remains alive. `use_future` also
needs investigation because its associated executor is not the client executor.

**Action:** Own work for the concrete I/O executor as well as a distinct
completion executor for the required lifetime. Avoid redundant guards when
they are the same. Preserve the direct inline-completion fast path.

**Acceptance:** Without external guards, start each context's `run()` once,
hold/release a successful foreign-thread credential so HTTP must subsequently
run on the client context, and repeat with a blocked file writer.
Verify one completion without `restart()`, for default callbacks, an explicitly
different executor and `use_future`. Retain allocator/cancellation tests.

### [x] T108 - Recalculate the write budget immediately before every server-side copy

**Result:** Done. Regressions `ServerSideCopySendsNothingWhenPlanningExhaustsTheWriteBudget` (a copy was sent after planning exhausted the lease) and `ServerSideCopyBoundsLaterRequestsByTheRemainingWriteBudget` (later copies got a 30 s timeout instead of the ~2 s left) failed before the fix. `CopyPagesServerSide` now takes the fence decision and remaining budget inside the request-start lambda, so every copy, including the first after planning, is checked and bounded afresh; source ETag conditions and drain behavior are unchanged. The fake server now records each copy's timeout. Full WindowsDebug suite passes.

**Status:** Source-traced; high confidence.

**Evidence:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp:492-543`
captures `writeBudget` before `PlanPageCopy` and reuses that duration in every
`UploadPagesFromUrlAsync`. Later iterations call `ThrowIfWritesFenced()` but
discard its newly calculated budget. `ClientRuntime.cpp:133-145` turns the
duration into a fresh relative request deadline.

**Impact:** Planning or an earlier wave can consume the lease budget. The first
copy can still start after planning exhausted it; later copies can receive an
obsolete, overly long timeout and continue beyond the write deadline.

**Action:** Obtain the current fence decision and remaining budget at each
actual copy initiation, including the first one after planning. Do not renew a
relative duration by reusing an earlier snapshot. Keep source ETag conditions
and existing drain/rollback behavior.

**Acceptance:** Expire/fence during page-range planning and assert no copy is
sent. Delay an earlier copy and verify each subsequent request's total deadline
is bounded by the remaining lease time, not the original duration.

### [x] T109 - Apply one absolute total deadline across download and batch children

**Result:** Done. New `Detail\AggregateDeadline.hpp` records the absolute end of the budget; `DownloadOperation::Send` and `BatchDeleteOperation::SendChunk` pass each child only the remaining time and, once it is spent, start no further request and fail once with `timed_out` (in-flight children drain as before). File downloads start the clock before the writer hand-off/open. Regressions (failed before the fix, pass after): `T11_ParallelDownloadAcceptanceTests.SequentialChunksShareOneAggregateDeadline`, `...FileDownloadDelayedBeforeTheFirstGetStartsNoRequestAfterTheDeadline`, `BatchDeleteTests.SequentialBatchesShareOneAggregateDeadline`. Full WindowsDebug ctest passes (integration tests skipped as before).

**Status:** Source-traced; high confidence.

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp:249-281`
copies unchanged `m_requestOptions` into every chunk.
`Detail\BlobBatch.hpp:227-240` does the same for every batch.
`Detail\RequestPipeline.hpp:116-127` starts a new relative deadline timer for
each child. `AzureRequestOptions.hpp:35-44` promises a bound on the whole
operation, retries/backoff included.

**Impact:** Sequential chunks/batches can each spend the entire advertised
budget. In particular, `BlobOperations::DeleteBatch` passes a lease-limited
budget that later delete batches effectively restart.

**Action:** Establish an aggregate steady-clock deadline and pass only its
remaining budget to each child. Stop admitting work after expiration, cancel
and drain accepted children, then complete once with `timed_out`. Include the
file-open/writer handoffs in the defined deadline semantics; do not release
buffers or streams merely to report a timeout promptly.

**Acceptance:** With concurrency one, make each of several chunks/batches fit
individually but exceed the aggregate budget. Verify no late child starts and
one timeout is reported. Cover a file download delayed before its first GET and
a lease-bounded delete spanning multiple batches.

### [x] T110 - Secure pending-operation ownership before container growth can throw

**Result:** Done. Added `BlobOperations::PushAccepted`, which drains an already-started operation if the container's `push_back` throws; used at the gap-read and upload sites in `ReadWriteFileImpl.ipp` and the `CopyPages`/`CopyPagesServerSide` queues (concurrency bounds unchanged). Tests `BlobOperationsPushAcceptedTests.*` inject a throwing container and assert the operation is drained before unwinding. The regression was written against the helper after the sites were edited, so no pre-fix runtime failure was demonstrated (the pre-fix pattern has no seam for injection); ASan was not run (MSVC debug preset). All 1189 tests pass on WindowsDebug.

**Status:** Source-traced; high confidence.

**Evidence:** `src\Azure\Impl\ReadWriteFileImpl.ipp:137-138` starts a download
into an extent before `reads.push_back` can allocate. Lines 197-198 likewise
start an upload before growing its pending vector.
`src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp:466,543`
has analogous start-then-insert patterns in copy-upload queues.

**Impact:** If insertion throws after initiation succeeds, the just-started
future is destroyed without being drained. Asio `use_future` futures do not
wait in their destructor. A gap download can subsequently write into a freed
extent; uploads/copies can outlive the client or rollback that assumes all work
has drained. Existing cleanup only sees operations already in the container.

**Action:** Preallocate sufficient bookkeeping before initiation, or hold each
accepted operation in a non-throwing temporary drain guard until insertion
commits. Cover all listed sites without changing the concurrency bounds.

**Acceptance:** Inject insertion/allocation failures with the newly accepted
operation deferred. Assert that unwinding does not finish or release its buffer
or client until that operation drains. Include both gap reads and copy/upload
queues; run the buffer-lifetime case under ASan.

### [x] T111 - Make mutation-scope metadata invalidation safe during unwinding

**Result:** Done. Regression `BlobMetadataCacheAllocationTests.InvalidationSurvivesAllocationFailureAndRejectsOlderFetches` (all allocations failed during invalidation of a short-new and a 200-char URL) failed before the fix (`InvalidateBlobSize` was not `noexcept`; the insert throws `bad_alloc` from the `SizeInvalidation` destructor). `Invalidate`/`InvalidateAll` (and the runtime wrappers) are now `noexcept`: existing tombstones are advanced in place without allocating, and if a new tombstone cannot be allocated the watermark is raised to the new epoch and tombstones and cached sizes are cleared (non-allocating), so older in-flight fetches are rejected and fresh metadata can be stored afterwards. All 1190 tests pass on WindowsDebug. ASan not run.

**Status:** Source-traced; high confidence.

**Evidence:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp:107-116`
calls `InvalidateBlobSize` from the implicitly `noexcept` `SizeInvalidation`
destructor. `src\Azure\Impl\BlobMetadataCache.cpp:92-104` constructs a string
and inserts a tombstone, both potentially allocating. Rename cleanup also
invalidates metadata from a scope guard.

**Impact:** Allocation failure while recording invalidation can terminate the
process after a successful mutation or while unwinding its real failure.
Simply suppressing the allocation error could allow a pre-mutation metadata
fetch to repopulate stale state.

**Action:** Provide a non-allocating invalidation fallback that preserves the
epoch/tombstone safety invariant, for example an appropriately advanced
watermark plus clearing affected cache state. Reuse existing tombstones without
constructing another owned URL when possible. Do not silently skip invalidation.

**Acceptance:** Fail allocation for a new URL tombstone and for a long URL in a
mutation scope, including during error unwinding. The process must survive,
preserve the real operation outcome, reject older in-flight metadata and permit
fresh metadata to be stored afterwards.

### [x] T112 - Reset credential refresh state when inner initiation throws

**Result:** Done. Two throw-once regressions (empty cache with a joiner; background refresh inside the refresh window with a joiner) failed before the fix and pass now. `Start` catches an inner initiation throw, releases the refresh claim and fails admitted waiters once (`not_enough_memory` for `std::bad_alloc`, else `io_error`); a `Drained` flag rethrows when the exception came from a user completion after delivery. In the background case the caller still gets the cached token. Later calls retry the inner credential. All 1192 tests pass on WindowsDebug.

**Status:** Reproduced with the current `CachingTokenCredential` template.

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\TokenCredential.hpp:253-274`
publishes `m_inFlightRefresh` before `Start` calls the inner credential at line
350. There is no rollback if that call throws. An isolated inner credential
throwing `std::bad_alloc` once produced
`inner_calls=1 subsequent_request_completed=0`: the next matching request
joined a refresh that no longer had a producer.

**Impact:** One failed allocation/initiation can permanently hang subsequent
requests for those scopes. The fixed null-inner path does not cover this case.

**Action:** Release the failed refresh claim and give all already-admitted
waiters an error exactly once. Separate initial rejection, accepted waiters and
user-completion exceptions; do not catch a user's completion and call it again.
Keep the existing concrete cache-hit fast path.

**Acceptance:** Add throw-once tests for an empty cache and a background refresh
of a still-valid token, including concurrent joiners. Later calls must retry
the inner credential or receive the documented failure/backoff, never hang.

### [x] T113 - Release a DNS leader claim even when caching its answer fails

**Result:** Done. Regression `FinishPublishesAndReleasesTheLeaderEvenWhenCachingFails` (leader + 3 followers, throwing clock fails `Store`) failed before the fix; `BasicDnsCache::Finish` now treats caching as best-effort so the answer is published to all waiters, the pending claim is released and a later lookup can lead. All 1193 tests pass on WindowsDebug.

**Status:** Source-traced; follow-up to T100.

**Evidence:** `libs\HttpClient\src\private\RequestOperation.cpp:250-255`
sets `guard->finished = true` before publishing the result.
`libs\HttpClient\src\private\DnsCache.hpp:174-191` calls allocating `Store()`
before removing the pending lookup.

**Impact:** If caching throws, the resolver callback unwinds onto the host event
loop, the pending lookup remains without a resolver, and the disarmed guard
cannot release it. Later requests join the abandoned lookup and time out.

**Action:** Reliably detach the pending entry and publish the usable DNS answer
to followers/leader even if optional cache population fails. Disarm the guard
only after its cleanup responsibility has been discharged. Preserve concrete
waiter types and exactly-once continuation.

**Acceptance:** Establish a leader and several followers, fail successful-answer
cache insertion, and verify no exception escapes `run()`, all requests complete
once, and a later uncached request can become leader. A valid DNS answer must
not become a timeout solely because caching it failed.

### [x] T114 - Guard internal download work on both sides of writer handoffs

**Result:** Done. Three regressions (`T114_DownloadWriterHandoffTests`, using a concrete throwing-executor test client) failed before the fix: failing a writer's return post or the file-start handoff aborted the process (exception escaped the pool worker) and failing finalization scheduling lost the completion and leaked the `.partial-*` file. Now a file download's completion, temp path and sink live in a shared `FileDownloadState`; the return post falls back to queueing the failed write-done event from the writer, every handoff failure becomes one error completion with temp-file cleanup on a writer (old destination preserved), and the finalization adapter is a concrete internal `FileFinalizer` that runs inline and never throws. All 1196 tests pass on WindowsDebug. Last-resort limits: if posting the user's completion itself fails the handler is consumed and cannot be told; a failed hop to a writer for cleanup runs cleanup inline.

**Status:** Source-traced; follow-up to T96's incomplete writer coverage.

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp:555-570`
catches scheduling the writer but not the writer lambda's post back to I/O.
The file-open/start/finalize handoffs at lines 766-818 also execute internal
setup outside `DownloadOperation::Guarded`. `Finish` marks `m_done` before
invoking the internal finalization continuation.

**Impact:** An allocation/setup/post failure on a writer thread can escape its
pool worker, terminate execution, strand `m_writing`, lose completion or leave
a `.partial-*` file. A finalization-scheduling exception is not a user handler
exception merely because the network phase has finished.

**Action:** Give internal writer/open/finalization steps an explicit failure
transition and temporary-file ownership/cleanup. Keep disk calls off I/O
threads, retain resources until accepted work drains, and distinguish the actual
user completion from internal completion adapters. Do not add an erased scheduler.

**Acceptance:** Inject one-shot failures in a write's return post, file-start
handoff and finalization scheduling. Use concrete test hooks/executors or narrow
allocation injection. Assert one error completion, no escaping worker exception,
no retained writing state, preserved old destination and no leaked partial file.

### [x] T115 - Roll back batch admission when signal, map or child initiation fails

**Result:** Done. Regressions `ChildInitiationFailureAfterACompletedChunkCompletesOnceWithTheFailure` and `ChildInitiationFailureDrainsTheChunksAlreadyAdmitted` failed before the fix (initiation exception escaped / aggregate never completed). `Pump` now registers the signal before advancing `m_nextChunk`/`m_inFlight` and records an allocation failure as the batch failure; `StartChunk` routes pre-acceptance child-start exceptions through `FailChunk` (inline fallback if the post throws). Admitted chunks drain and the aggregate completes once. Full WindowsDebug suite passes (integration tests skipped). Signal-allocation/map-insertion faults are covered by code path only (no allocation-injection seam).

**Status:** Source-traced; follow-up to T96's conclusion that batch needed no change.

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobBatch.hpp:111-115`
advances `m_nextChunk` and `m_inFlight` before allocating/registering the signal.
`SendChunk:230-240` can also throw before the child's retry operation is
established, including allocating the cached-URL vector.

**Impact:** A later `Pump()` from a completed chunk can throw with an unmatched
in-flight count and no future completion to reduce it. The previous child's
retry pipeline has already marked itself finished, so its T96 guard does not
repair the parent batch. Earlier concurrently accepted deletes can still run.

**Action:** Make admission transactional and handle pre-acceptance child-start
failures in the batch owner. Stop new admissions, drain accepted chunks and
complete once with the real failure. Never hold a permanently incremented count
for a child that was not started.

**Acceptance:** Fault the signal allocation, signal-map insertion and child
setup after another batch is pending or has completed. All admitted chunks
must drain; no exception may escape the host event loop and the aggregate
completion must run exactly once.

## P2: consistency, retry limits and reliable validation

### [x] T116 - Invalidate local cached bytes after ambiguous mutation failures

**Result:** Done. `DeleteFile`, `CreateWriteableFile` and `ReuseWritableFile` now drop the cached copy with a
`scope_exit`, so it is removed even when the mutation throws. Regression tests
`CachedFailureTests.DeleteThatFailsStillDropsTheCachedCopy` and
`OverwritingCreateThatFailsStillDropsTheCachedCopy` failed before the fix and pass after it (a `Create` fault
operation was added to `FakeBlobServer`). Full ctest: 1200/1200 passed.

**Status:** Source-traced; concrete extension of T35 and T94.

**Evidence:** `src\Azure\Impl\BlobFilesystemImpl.Delete.ipp:10-19` removes the
local cached file only after `BlobOperations::Delete` returns successfully.
`BlobFilesystemImpl.FileOpen.ipp:38-45,110-116` similarly invalidates only after
an overwriting create. A service-applied mutation whose response is lost can
throw without reaching these invalidations. Rename already uses scope cleanup
because failures can leave the service changed.

**Impact:** If deletion/replacement occurred despite the reported error, a
later reader can still serve the old cached file by name. Metadata-cache
invalidation alone does not evict its bytes.

**Action:** Apply failure-safe local-cache invalidation around potentially
destructive mutations, consistent with rename/truncate, including overwrite
creation. Preserve the original operation's error and existing API behavior.

**Acceptance:** Cache an old pattern, apply a fake delete/overwrite remotely
and then return an ambiguous transport failure with retries disabled/exhausted.
Recreate or inspect the path and prove no subsequent read returns old cached
bytes. Retain the existing successful-recreation regression.

### [x] T117 - Make prefetch budget and slot admission one transaction

**Result:** Done. Regression `Prefetch_AllocationFailureWhileAdmitting_RestoresBudgetAndLeavesNoPendingSlot` (slot-vector growth and name copy failed separately) failed before the fix (orphaned pending slot, no download). `Prefetch` now reserves slot capacity and copies the name before touching budget or slots, so admission cannot throw; the prior finished slot and budget are preserved and a later prefetch succeeds. Full WindowsDebug suite passes (1201).

**Status:** Source-traced; high confidence.

**Evidence:** `src\Azure\Impl\ReadableFileImpl.ipp:274-287` reserves shared bytes
before the allocating `Slots.push_back`; the exception cleanup starts only at
line 297. Copying `m_h.Name` at line 292 also happens after a pending slot was
published but before that cleanup.

**Impact:** A failed insertion leaks budget with no slot to return it. A failed
name copy leaves a permanently pending slot with no download, so future hints
are falsely treated as covered and synchronous reads wait for the 30-second
prefetch timeout.

**Action:** Perform fallible preparation before publishing state, or arm
non-allocating rollback as soon as budget/slot ownership changes. Preserve a
previous useful slot when a new hint is not accepted, and do not disturb other
files' contributions to the shared budget.

**Acceptance:** Fail slot-vector growth and the long-name copy separately.
Verify exact shared-budget restoration, absence of an orphaned pending slot,
successful later prefetch, and no artificial wait on the rejected range.

### [x] T118 - Verify multi-chunk downloads reject an unusable version validator

**Result:** Done; reproduced. A first 206 with an empty ETag and no caller `IfMatch` let the download admit unconditional chunks (second request was sent; regression `FirstResponseWithoutETagRejectsFurtherChunks` failed before the fix). `OnProbe` now fails with `InvalidResponse` before admitting further chunks when more bytes remain and no exact validator exists. Caller-provided `IfMatch` and single-response transfers without an ETag still succeed (tests added); all WindowsDebug tests pass.

**Status:** Verify first; malformed-response robustness, not a claim that Azure
normally omits ETags.

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobDownload.hpp:335-343,432-435`
accepts an empty response ETag and copies it into the remaining chunks'
`IfMatch`. The common request-condition helper then has no ETag condition to
send. The documented chunked-download contract promises version consistency.

**Action:** Reproduce with a valid-looking first 206 without an ETag and a
different version in the next range. If it succeeds with mixed data, reject the
unusable validator before admitting further chunks unless a concrete caller
condition already provides a valid exact-version constraint. Do not fail a
single-response transfer unnecessarily or weaken normal ETag checking.

**Acceptance:** A changing multi-chunk fake cannot report mixed-version success;
check absent/empty ETag, caller-provided exact `IfMatch`, and ordinary
single-chunk success. Return the standard invalid-response error where appropriate.

### [x] T119 - Recover an unscheduled idle sweeper while the pool remains nonempty

**Result:** Done. Regression `IdleSweeper.ReleaseSchedulesAgainAfterAFailedSchedule` (now releases a second origin without emptying the pool) failed before the fix and passes after. `ConnectionPool::Release` now notifies on every successful release, and `IdleSweeper::Notify` does an acquire load before the exchange so the already-pending case adds no read-modify-write. Coalescing tests unchanged; `ConnectionPool.NotifiesOnEverySuccessfulRelease` updated. Full WindowsDebug suite passes (1204). Release-path overhead is one `weak_ptr::lock` plus one atomic load per release; no allocations added. A warmed release-throughput/allocation benchmark was not run, so no timing figure is claimed.

**Status:** Source-traced; follow-up to T83.

**Evidence:** `libs\HttpClient\src\private\IdleSweeper.hpp:158-165` resets
`m_scheduled` after scheduling fails, but
`ConnectionPool.hpp:326,368-371` only notifies on an empty-to-nonempty transition.
The existing `IdleSweeperTests.cpp:82-96` regression empties the pool before the
next release, masking the remaining recovery case.

**Impact:** After a schedule failure, subsequent releases need not retry
scheduling. When traffic stops, the nonempty pool can retain sockets past the
configured idle timeout indefinitely.

**Action:** Let a successful release retry an unscheduled sweep without
requiring an empty pool. Reuse the existing scheduling-coalescing state; measure
the release-path overhead of any additional notifications.

**Acceptance:** Fail the first schedule, release another connection without
acquiring the first, stop traffic and verify both expire automatically. Keep
coalescing tests and compare warmed allocations/release throughput.

### [x] T120 - Validate load metrics before treating a database load as successful

**Result:** Done. `New-LoadedDb` now passes `fillrandom` as the expected metric to `Invoke-Bench`; wall-clock `Seconds` and the 3-attempt retry are unchanged. Three new Pester regressions (valid load, no metrics, malformed metric) — the two invalid-load cases failed before the fix (exit 0, numeric sample) and all 8 Pester tests pass now. No C++ changed, so no CMake build was needed.

**Status:** Reproduced at the script-function level; follow-up to T102.

**Evidence:** `tools\db_bench\Compare-Backends.ps1:138-143` calls `Invoke-Bench`
from `New-LoadedDb` without expected metric names. Lines 127-134 then permit a
zero-exit run with no parsed metrics to return a map containing only `Seconds`.
An isolated function probe accepted that map with zero failures. The new
Pester regressions cover `-Scenarios write`, not a load-only comparison.

**Impact:** `-Scenarios load` can mark an empty or malformed executable's output
as a complete numeric benchmark comparison.

**Action:** Require the expected load metric, at least `fillrandom`, before
accepting a loaded database. Keep elapsed wall-clock seconds as the load
measurement and preserve the bounded load-retry policy.

**Acceptance:** Extend the existing Pester file with missing metrics, malformed
metrics and valid load output. Invalid loads must never produce numeric samples;
exhausted retries must mark the summary incomplete and return nonzero.

### [x] T121 - Do not let SIGPIPE turn relevant benchmark changes into a CI skip

**Result:** Detection moved to `.github\scripts\detect-relevant-changes.sh`,
which captures the full `git diff` output (so a diff failure fails the step
under `set -e`) before matching with non-quiet `grep`. The workflow calls it.
Pester tests (`detect-relevant-changes.Tests.ps1`): early match plus 3,000 paths
selects the build; no match skips; a failing diff exits nonzero. Before the fix
the failing-producer test failed (old logic reported `relevant=false`); the
SIGPIPE ordering itself did not reproduce under Git-for-Windows bash, only on
Linux semantics, so that case is a guard rather than a demonstrated failure here.

**Status:** Failure mechanism reproduced; current branch's smaller diff selected
the benchmark build correctly.

**Evidence:** `.github\workflows\build-and-test.yml:209-214` uses
`git diff ... | grep -qE ...` in a Bash `pipefail` environment. `grep -q` can
exit after the first match, causing the still-writing producer to exit with
SIGPIPE. A relevant filename followed by 3,000 paths returned pipeline status
141 and selected `relevant=false` in a shell probe.

**Impact:** A sufficiently large PR can silently skip the db_bench build
despite changing relevant files, while the job appears successful.

**Action:** Consume the complete input, e.g. with non-quiet grep redirected to
`/dev/null`, retaining `pipefail`. Distinguish a real diff-command failure from
the ordinary no-match result instead of treating either as a skip.

**Acceptance:** Check an early relevant match followed by more than a pipe
buffer, a true no-match diff, and a failing producer. Select the build for the
first case, skip only the second, and explicitly fail the third.

### [x] T122 - Share one stale-read refresh budget across partial-prefetch recovery

**Result:** Done. Regression `Read_OverwrittenAfterPrefetchAndAgainDuringRecovery_SharesOneRefreshBudget` (suffix 412 twice then success, whole-range re-read 412 every time) failed before the fix (9 downloads vs bound kMaxStaleReadRetries+2 = 7, and the stale-read error). `RandomRead` now passes one `refreshesLeft` budget into both `DownloadWithRetry` calls; stable-ETag suffix-only path unchanged and existing error returned on exhaustion.

**Status:** Source-traced; follow-up to T99's untested exhaustion case.

**Evidence:** `src\Azure\Impl\ReadableFileImpl.ipp:169-172` can invoke
`DownloadWithRetry` for a suffix and then for the whole range after detecting a
version change. Each call initializes a fresh `kMaxStaleReadRetries` budget at
lines 668-674. T99 explicitly required preserving the existing bounded
stale-read budget, but its added test covers one overwrite only.

**Impact:** A single logical read can spend two independent refresh budgets
under churn, multiplying requests and latency beyond the previous per-read
limit. Mixed-version protection must not be removed to avoid those requests.

**Action:** Carry one concrete per-read refresh budget through the suffix and
full-range recovery paths. Preserve the suffix-only fast path for stable ETags
and return the existing stale-read error on exhaustion.

**Acceptance:** Script 412/refresh cycles around both phases, including a second
overwrite during recovery. Assert one-version success or the existing error,
an explicit total refresh/request bound, and unchanged stable-prefetch request
counts. This review did not run a new exhaustion reproduction.

## P2: performance opportunities requiring measurement

### [ ] T123 - Bound and coalesce read/modify/write gap downloads

**Status:** Source-traced request-amplification opportunity; measure first.

**Evidence:** `src\Azure\Impl\ReadWriteFileImpl.ipp:113-150` starts one download
for every uncovered interval in an extent. `RunWave:177-183` admits all of them
before waiting. Limiting a wave to four extents does not bound the number of
gap requests: many small noncontiguous writes to one page/run create many gaps
inside a single merged extent.

**Impact:** Sparse small writes can generate many tiny GETs and pending
download/future states despite the advertised bounded-wave structure. The
transport's connection limit is not a bound on allocated queued operations.

**Action:** First record request count, transferred bytes, peak pending reads
and allocations for clustered sub-page and sparse-page writes. Evaluate reading
a partially covered page/run once and overlaying buffered writes afterwards,
or another simple concrete coalescing/bounded-admission approach. Avoid
downloading fully overwritten extents and preserve last-write-wins ordering.

**Acceptance:** Demonstrate reduced requests/peak pending state on the sparse
case without regressing aligned full-page writes. Verify untouched bytes,
overlap ordering, EOF zeros, short-read failure and retry after a failed wave.
Coordinate ownership changes with T110; do not retain a speculative rewrite.

### [ ] T124 - Measure eager page-copy planning on large fragmented blobs

**Status:** Verify first; scalability opportunity, not a measured speedup.

**Evidence:** `src\Azure\Impl\include\AVEVA\RocksDB\Plugin\Azure\Impl\BlobOperations.hpp:365-385`
materializes every page-range result as `PageChunk` entries before any transfer
starts. Both copy implementations wait for the full `PlanPageCopy` result.
Transfer concurrency bounds do not bound this planning memory or listing delay.

**Action:** Measure peak plan allocation, list request counts and time to first
copy on representative large contiguous and highly fragmented sparse blobs.
Only if material, use concrete incremental planning or bounded batches while
preserving source ETag conditions, fallback semantics and rename rollback.
Do not add a new abstraction merely to avoid a small vector.

**Acceptance:** Preserve exact copied bytes, sparse holes, final-page padding,
listing pagination and failure drainage. Record before/after planning memory
and time to first copy; if gains are immaterial for supported workloads, close
with measurements and no production change. Reuse T108's deadline coverage.

## P3: bounded simplifications and contract cleanup

### [ ] T125 - Share the strict download-length check used by writer recovery paths

**Status:** Concrete duplication introduced by the recent correctness fixes.

**Evidence:** `src\Azure\Impl\WriteableFileImpl.ipp:17-25` and
`ReadWriteFileImpl.ipp:23-31` duplicate the same expected/actual byte-count check
and `RequestFailedException` construction. Strict copy/rename reads elsewhere
also check counts, but currently use less specific generic errors.

**Action:** Reuse a narrowly scoped, concrete helper for reads that must fill a
known range, with file/range context and the existing invalid-response mapping.
Keep it internal and separate from normal EOF-tolerant random reads. Do not
introduce a general operation-policy framework or change every read call site.

**Acceptance:** Retain constructor and gap short-read regressions; add only
missing coverage needed for any copy/rename sites migrated. Short mandatory
reads must remain corruption/invalid-response failures, and legitimate EOF
reads must still succeed with fewer bytes.

### [ ] T126 - Remove the obsolete stream-download declaration and correct its contracts

**Status:** Source-verified dead declaration/documentation mismatch.

**Evidence:** `libs\AzureClient\include\AVEVA\AzureClient\Detail\BlobRequestHelpers.hpp:562-574`
still declares `DownloadBlobToAsync(..., std::ostream&, ...)` and documents the
stream lifetime. No definition/caller remains in the Azure client sources;
`BlobClient.hpp:128-179` exposes the file/span destination overloads instead.
`BlobOperationOptions.hpp` also says the in-memory `MaxSize` rejects a large
blob before any body is buffered, although the first ranged response must
already have arrived for `OnProbe` to inspect its size.

**Action:** Remove the stale internal prototype and stream-specific wording.
Document the existing destination lifetimes and distinguish an output-size
limit from the transport's per-response buffering bound. Do not restore an
unused stream API or add a preliminary HEAD request just to match stale prose.

**Acceptance:** Confirm no remaining references to the removed prototype,
compile public-header/self-contained tests and preserve file/span overload
resolution. Documentation must accurately describe initial-probe buffering,
`MaxSize`, and the deadline semantics established by T109.

## Dependencies and recommended execution order

Fix P1 items before performance refactors. Independent correctness tasks can be
implemented separately; the ordering below prevents overlapping rewrites:

| Task | Prerequisite / coordination |
|------|----------------------------|
| T106 | Independent connection-pool lifetime fix; keep origin-list design. |
| T107 | Add both executor-context regressions before altering work ownership. |
| T108 | Per-copy fence/budget fix is independently required, even after T109. |
| T109 | Coordinate download and batch aggregate deadlines with T114/T115. |
| T110 | Establish accepted-operation ownership before changing gap admission. |
| T111 | Preserve invalidation epochs before expanding failure-scope invalidation in T116. |
| T112-T113 | Independent credential/DNS leader-claim rollback fixes. |
| T114-T115 | Keep real user-handler exceptions distinct from internal handoff failures. |
| T116 | Depends on safe scope invalidation where it uses the T111 cleanup path. |
| T117-T122 | Independent regressions; T119 additionally requires a hot-path measurement. |
| T123 | Depends on T110; re-run T95's short-gap and retry cases. |
| T124 | Measure first; preserve T108's per-copy budget checks. |
| T125 | After T110/T123 to avoid rewriting the same gap path twice. |
| T126 | Finalize deadline wording after T109; dead prototype removal is independent. |

## Review evidence and limits

- Read the committed branch diff and surrounding implementations, plus the
  completed-task ledgers and related commits. Follow-ups above name the missed
  scenarios; they do not reopen T01-T105 wholesale.
- Compiled an isolated MSVC C++ probe against the current Azure client headers
  and `HttpRequestOptions.cpp`. The actual binding helper left the client
  context stopped while the distinct handler context stayed busy (T107).
  The actual credential-cache template stranded the next request after an
  inner `std::bad_alloc` (T112).
- Ran the existing WindowsDebug selection:
  `ctest --test-dir build\WindowsDebug --build-config Debug --output-on-failure --timeout 120 -j8 -R 'AsyncBehaviorTests|RetryTests|BlobOperationsFakeBlobTests|LeaseDeadlineTests|TokenCredentialTests|T11_ParallelDownloadAcceptanceTests'`
  -- **142/142 passed**. The existing build was not rebuilt for this review;
  this is baseline evidence, not proof of the missing regressions.
- HTTP/Core review also exercised selected existing tests, and isolated script/
  shell probes established the T120 and T121 validation gaps. No production
  fixes or committed regression tests were made during review.
- No live Azure integration, new ASan/TSan run, full rebuild, representative
  end-to-end benchmark or before/after performance comparison was performed.
  Performance tasks explicitly require those measurements before keeping changes.
- Do not redo prior rejected/dropped micro-optimizations without new evidence:
  per-read handle allocation, merged-read memcpy, per-request formatting,
  batch-body rebuild, moving the BlockOn promise to the stack, and header-only
  include trimming were already investigated. Conditional-write replay tests
  intentionally surface ambiguous failures; do not suppress a subsequent
  409/412 as success.
