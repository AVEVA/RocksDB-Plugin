<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# Secondary cache v1: correctness review and implementation to-do list

**Date:** 2026-10-20.
**Reviewed implementation:** `41f8390`, including region-engine commit `ba73504`.
**Scope:** the secondary block cache, its configuration, tests, and design documents.
The SST-file download cache (`Core/FileCache`), RocksDB, and other plugin services
are not implementation targets. The already-approved Azure Release-build fix was
inspected; no further Azure changes are proposed.
**Status:** review complete; first minimal synchronous implementation now applied
on `paul-pirogovsky/ssd-secondary-cache` (2026-10-21). Original findings below
describe `41f8390`; implementation progress and remaining gates are recorded here.
Keep `kLegacy` as the default. Do not describe `kRegion` as production-ready.

Start new sessions from the [handoff/background index](secondary-cache-handoff.md).

## Implementation progress: first minimal milestone

The [cache-local README](../../src/Core/SecondaryCache/README.md) describes files,
methods, ownership, record layout and data flow. ADR v0.4 is authoritative where
it amends the historical design. No RocksDB, Azure or SST-file cache changes are
part of this milestone.

| Task | Implemented in this milestone | Remaining acceptance work |
|---|---|---|
| T00 | ADR amendments, public API docs and cache-local README; historical overclaims corrected | Keep future benchmark/production claims evidence-based |
| T01 | Checked geometry/record arithmetic before filesystem creation; zero capacity; owned child/marker; exclusive owner lock; link/reparse rejection; bounded final-region search | Broader native create/marker-write fault injection |
| T02 | Allocating internals may throw; allocation-free exception translation; failed-create output ignored; nullable deleter; success guard survives handle allocation; no reader pins. Permanent isolated OOM executable proves successful creation is cleaned exactly once if handle allocation fails | Expand deterministic system-OOM injection at additional allocator sites |
| T03 | One manager mutex owns append, tentative index publication, commit, reclaim and usage; exact-location/incarnation cleanup; 32-bit generation wrap guard | Full engine/DB TSan run and longer mixed resize/read/write stress |
| T04 | RAM reverse hash lists replace every footer/parser/scan; reclaim never reads corrupt storage; clear directly clears index/buffer/lists | Long churn/heap profiling at deployment scale |
| T05 | One active buffer; direct writes for large records; lazy 64-handle pool; checked transfers/flush; write failure invalidates mappings; injectable memory device | More native partial-transfer/seek/close faults. ADR explicitly removes physical preallocation promise; files grow on write |
| T06 | Valid resize invalidates all entries and builds a new layout; post-invalidation failure disables cache; invalid geometry preserves old state | Native filesystem failure matrix and longer concurrent control-operation stress |
| T07 | Useful admit-all FIFO with force-false turnover; reject unsupported second chance; no ghosts | Optional policies stay in T13 |
| T08 | Additive stats snapshot; counted metadata allocator includes layout/buckets/reverse capacity and rehash overlap; bounded transient allocator; failure logging/counters; optional RAM APIs return NotSupported | Measure OS page cache/RSS/physical blocks separately. Node map can hit budget before disk fills; T12 remains a production gate |
| T09 | Permanent regression, shared contract, default RocksDB LRU turnover and local DB data/hit tests; Windows Debug/Release and Linux compile/probe validation | Full native Linux linked RocksDB suite, complete OOM/fault matrix and applicable full-engine sanitizers |
| T10-T13 | Not implemented in this minimal milestone | Sequence below; do not combine speculative optimizations into this PR |

### Next implementation sequence

1. Finish remaining T09 platform/fault gates, including a full native Linux build,
   broader system-OOM sites and mixed resize/read/write sanitizer coverage. Keep the
   permanent deterministic result-handle OOM test as a release regression gate.
2. **T10:** repair/extend the benchmark before drawing new performance conclusions:
   verify payloads, run beyond several capacities, report p50/p95/p99, hit ratio,
   disk writes, metadata/transient allocations, RSS, page cache and physical blocks.
   The old Debug benchmark is historical evidence only, not a v0.4 result.
3. **T11:** add one bounded writer queue without changing this reference lifecycle.
   Prove foreground spill does not wait for device completion and bound queued
   memory/shutdown/resize behavior. This remains the Q6 production gate.
4. **T12:** measure the simple node map against the accepted metadata goal; change
   to existing Boost flat map only with comparable results. Then address duplicate
   serialization copies/CRC or lock contention one measured change at a time.
5. **T13:** consider optional policies/asynchronous reads/native positioned I/O only
   after evidence warrants them. Keep legacy default until release gates pass.

Task checklists and release gates below deliberately remain acceptance checklists:
implemented mechanisms are not a claim that every originally requested platform,
fault-injection, fuzz or deployment-scale experiment has passed.

### Validation of this milestone (2026-10-21)

Commands below ran with VS 18's CMake/CTest in a Developer Command Prompt.
Existing `build\WindowsDebug\vcpkg_installed` dependencies were reused; no package
changes/restoration were required. Release uses that installed directory too.
Cache test directories include the process ID so simultaneous Debug/Release/ASan
runs do not delete each other's files.

| Exact build/test command | Result |
|---|---|
| `cmake --build build\WindowsDebug --config Debug --target aveva-rocksdb-plugin-core-tests` | Passed, including isolated allocation-failure executable |
| `ctest --test-dir build\WindowsDebug -C Debug -R SecondaryCache --output-on-failure` | **89 passed, 4 skipped, 0 failed** (93 discovered) |
| `cmake --build build\WindowsRelease --config Release --target aveva-rocksdb-plugin-core-tests aveva-secondary-cache-benchmark` | Passed; benchmark was built, not used to claim a new performance result |
| `ctest --test-dir build\WindowsRelease -C Release -R SecondaryCache --output-on-failure` | **89 passed, 4 skipped, 0 failed** (93 discovered) |
| `cmake --build "$asan" --config RelWithDebInfo --target aveva-rocksdb-plugin-core-tests -- /p:TrackFileAccess=false` | Passed with `/fsanitize=address /Zi /D_DISABLE_VECTOR_ANNOTATION /D_DISABLE_STRING_ANNOTATION`; installed RocksDB/dependency libraries are not sanitizer-instrumented |
| `ctest --test-dir "$asan" -C RelWithDebInfo -R SecondaryCache --output-on-failure` | **89 passed, 1 skipped, 0 failed** (90 discovered); includes the permanent result-handle OOM executable |

`$asan` was the session artifact build directory `files\minimal-cache-windows-asan`,
configured from this worktree with `VCPKG_MANIFEST_INSTALL=OFF` and the existing
installed-dependency directory. MSBuild file tracking was disabled for that build
only after FTK1011 on a long artifact path; no project warning flags or source
files outside the cache were changed to make it build.

Windows skips: the symlink test needs a permission unavailable on this host;
three existing Azure secondary-cache integration tests need Azure credentials.
The new local DB/default LRU adapter tests **ran and passed**, including explicit
data checks, turnover, admissions and secondary-hit assertions. The ASan build
only built the Core target, so Azure tests were not discovered there.

The public callback/record tests are permanent. The isolated OOM executable
replaces `operator new` only in its own process, arms failure after successful
`create_cb`, and checks miss/kept=false/exactly-once destruction/no hit/no transient
leak. This does not alter allocations in the main GTest process or the plugin.

**Linux (WSL Ubuntu 24.04, GCC 13.3):**

- Passed `g++ -std=c++23 $flags -fsyntax-only -Iinclude -Isrc/Core
  -isystem build/WindowsDebug/vcpkg_installed/x64-windows/include
  src/Core/SecondaryCache/BlockDevice.cpp src/Core/SecondaryCache/RecordFormat.cpp
  src/Core/SecondaryCache/RegionManager.cpp src/Core/FileBasedCompressedSecondaryCache.cpp
  tests/AVEVA/RocksDB/Plugin/Core/SecondaryCacheAllocationFailureTests.cpp`,
  with `$flags` taken directly from the `LinuxRelease` preset. The corresponding
  cache-source check also passed with the `LinuxDebug` warning flags.
- Passed a native Linux **ASan+UBSan** probe (`-fsanitize=address,undefined`,
  `ASAN_OPTIONS=detect_leaks=1`) and a separate **TSan** probe
  (`-fsanitize=thread -fPIE -pie`). Both compile the real `BlockDevice`,
  `RecordFormat` and `RegionManager` sources plus linked-version RocksDB `status.cc`;
  no substitute cache implementation or altered RocksDB source is used.
- Each probe verifies 600 file-backed region identities, at most 64 region
  handles, payload copies/decoding, four writers/readers performing 4,000
  append/read/conditional-erase iterations with FIFO turnover, exact quiescent
  usage, grow/shrink/zero transitions, stale-incarnation cleanup and directory
  symlink rejection. Probe sources/builds/logs are retained in session artifacts.
- TSan initially failed before entering the program with `unexpected memory
  mapping`; invoking it as `setarch $(uname -m) -R <probe> <test-owned-directory>`
  fixed its address-layout requirement. The final run passed without reports.
  No machine-global ASLR or kernel configuration was changed.

These native probes do **not** replace the outstanding full Linux plugin/linked
RocksDB test suite or full-engine TSan gate. No production-scale memory/latency,
fuzzing, or physical-space-reservation claim is made. Final `clang-format --dry-run
--Werror` on changed/new C++ files and `git diff --check` passed.

## 1. Verdict

The region-log direction is appropriate: eliminate per-entry files, append
variable-length self-validating records, and reclaim whole regions in FIFO order.
That is the main performance opportunity, and it does not require sophisticated
concurrency or persistent eviction metadata.

The current implementation has correctness and resource-scaling blockers:

- Capacity growth can index beyond the allocated region array.
- Internal allocation failures can terminate the process despite public catch blocks.
- Callback ownership is mishandled on both failed creation and result-handle allocation failure.
- Index publication, eviction, and live-byte accounting are separate transactions.
- Reclaim can leave stale index entries when on-disk metadata is missing or corrupt.
- Default buffer reservations and eagerly opened C++ streams do not scale to the
  accepted 16-256 GiB range.

**Recommendation:** first build a small, synchronous correctness reference with one
state mutex, one append buffer, and in-memory per-region eviction lists. Keep the
existing record checksums and full-key validation. Remove the disk footer/parser
and the split reservation/publication protocol. Then add a bounded single-writer
queue and measured optimizations in separate PRs.

The synchronous reference is an intermediate review milestone, **not** fulfillment
of the accepted foreground nonblocking-insert goal. The bounded writer step is
required before production enablement unless a later ADR explicitly changes that
goal. Synchronous `Lookup(wait=false)` returning a ready handle is legal under the
linked RocksDB interface; true asynchronous lookup can remain a later optimization.

### What is already useful

- Existing constructors remain available; the new engine is opt-in.
- The 32-byte record header, full key, and separate header/key and payload CRC32C
  checks give a good foundation for converting storage corruption into cache misses.
- Fixed-size regions and a compact hash-to-location index are suitable for variable
  block sizes without per-record filesystem metadata.
- `MemoryBlockDevice` is a useful fault-injection seam.
- No new compression or warm-restart machinery is necessary.

## 2. Findings

Line references below refer to the reviewed commit, not future edited files.
Use the named methods when implementing; do not rely on line numbers remaining fixed.
`Engine` means `src/Core/FileBasedCompressedSecondaryCache.cpp`;
`Manager`, `Device`, and `Index` mean the corresponding files under
`src/Core/SecondaryCache/`.

| ID | Severity | Location | Finding and evidence | Tasks |
|---|---|---|---|---|
| R01 | High | `RegionManager.cpp:163-179, 250-259` | `SetCapacity` changes the logical region count but never grows `m_regions` or the device. A one-region cache grown to two regions produces an ASan heap-buffer-overflow on the next rollover. Shrinking an open region also returns OK with usage above the new capacity. | T03, T06 |
| R02 | High | `Engine:675-689`; `ShardedIndex.hpp:66-76` | `Peek`, `Publish`, accounting, and `Upsert` are separately synchronized. A writer paused before `Upsert` can publish a stale location after reclaim. Two first inserts of the same key can charge twice. `Upsert` itself returns the replacement, not the previous location. Lookup cleanup also removes by hash instead of the exact location read. `CapacityBytes` and the DRAM-budget fields have unsynchronized readers/writers. | T02, T03 |
| R03 | Medium | `RegionManager.cpp:197-213, 326-361, 424-452` | Reclaim depends on reading a footer or scanning disk records. A missing footer plus a bad first header leaves later mappings behind. Clearing an unflushed region at capacity zero also leaves its mappings behind while reporting usage zero. The available `RemoveAllInRegion` helper is not used as a completeness fallback. | T04, T06 |
| R04 | High | `RegionManager.cpp:43-45`; `BlockDevice.cpp:38-51` | Every region reserves `max(flushBlockSize,maxEntrySize)` bytes, not just the open region. Every region also opens an `fstream`. Defaults imply 4.08-65.03 GiB of requested buffer allocations; 32 GiB already needs 521 C++ streams. A probe hit this process's CRT stream limit of 512. | T05, T08 |
| R05 | High | `ShardedIndex.hpp:66`; `RegionManager.hpp:45-54`; `AdmissionPolicy.cpp:23` | Allocating functions are marked `noexcept`: map insertion, vector growth/copy, footer allocation, scans, and ghost insertion. `bad_alloc` invokes `terminate` before the public catch block can run. Injecting one allocation failure in `Upsert` reproduced termination. The manual reader-release lambda is also not exception-safe if internal `noexcept` is merely removed. | T02, T03, T04 |
| R06 | High | `Engine:562-589` | `create_cb` errors are required by RocksDB to leave cleanup to the callback and make the caller ignore `out_obj`. This code instead calls `del_cb` on a non-null error output, risking invalid/double deletion. On success, it disables cleanup before allocating `ResultHandle`, leaking the created object if allocation fails. Both behaviors were reproduced. | T02 |
| R07 | High | `BlockDevice.cpp:40`; `Engine:455-467` | Construction recursively deletes the caller's directory before validating the logger and maximum-entry sizing. A deliberately invalid options object threw only after deleting a diagnostic sentinel file. Cold-start invalidation is not permission to delete arbitrary directory contents. | T01 |
| R08 | Medium | `RegionManager.cpp:84-115, 365-379`; `BlockDevice.cpp:23-34` | Failed data flushes are ignored and `Publish` returns OK without failure accounting. A later discontinuous append can change the retained buffer's base offset after a failed flush. File sizing does not check seek/write/close success, and it is not physical preallocation: on Linux, a 64 MiB file occupied only 4096 bytes. | T01, T05, T08 |
| R09 | Medium | `RegionManager.cpp:241-243`; `Engine:491-493` | After the first complete pass through the regions, non-forced inserts cannot recycle space, even after an explicit erase reduced live usage. The linked RocksDB default LRU adapter sent 30 non-forced evictions and zero forced evictions in the probe; only initial cache contents remained. This is an effectiveness/integration problem, not a violation of RocksDB's permission to decline admission. Changing it requires an explicit new-engine policy decision. | T00, T07, T09 |
| R10 | Medium | `AdmissionPolicy.cpp:29-38` | Second-chance ghosts have no bound or expiration. Unique one-hit keys consume memory while being rejected from disk. A 10,000-key probe allocated 736,240 bytes cumulatively and still remembered the first key. This is not a measurement of exact retained RSS, but demonstrates growth independent of admitted data. | T07, T08 |
| R11 | Medium | `Engine:625-632`; `CacheStats.hpp` | `Deflate`/`Inflate` change an unused scalar and return success; no memory is actually released/reserved. `Inflate` addition can wrap. Cache statistics have no public snapshot; write bytes omit footers, and I/O misses have no dedicated counter. Neither bounded DRAM nor write amplification can currently be established from this API. | T08 |
| R12 | Medium | `Engine:481-493, 658-665`; `RecordFormat.hpp:57-60` | `Insert` allocates and serializes the whole value before checking admission size. The size arithmetic and several narrowing casts are unchecked. `RecordSize(SIZE_MAX,1)` evaluates to 64. Defaults mitigate some oversized configurations, but the public options do not enforce the format's representable bounds. | T01, T02 |
| R13 | Medium | `RegionManager.cpp:86, 142` | GCC 13.3 with the repository's Linux warning policy rejects two redundant casts under `-Werror=useless-cast`. The changed cache code is not Linux-build-clean as submitted. | T09 |
| R14 | Medium | `SecondaryCacheRegionManagerTests.cpp:76-96`; `SecondaryCacheRegionEngineTests.cpp:104-139`; `SecondaryCacheBenchmark.cpp:57-99` | The footer test corrupts an unused final byte after clearing the cache, not a populated checksummed footer, and never asserts index removal. The concurrency test has one writer, tiny data, no rollover, and no payload/accounting assertions. The benchmark never reaches steady-state reclaim and does not verify returned payload bytes. The old contract suite still constructs only the legacy engine. | T09, T10 |
| R15 | Low | `docs/design/secondary-cache-performance.md:41, 1193-1218`; new cache files | Documentation overstates test coverage and phase completion; code is not a `fetch_add` allocator or native positioned-I/O implementation. Public class documentation still describes only the legacy format. Unused hit-count/ghost/tombstone scaffolding, duplicated hash mixing, and unformatted code increase review cost without delivered functionality. | T00, T03, T09 |

R03 and R08 usually cause missed opportunities and stale metadata, not delivery of
incorrect SST data: the full-key and CRC checks are valuable defenses. Do not
inflate these findings into a demonstrated database-corruption claim. R01, R05,
and R06 can nevertheless affect process safety, not merely cache hit rate.

### Resource arithmetic at default region settings

Current usable bytes per file are `64 MiB - 1 MiB = 63 MiB`.
The implementation allocates `ceil(capacity / 63 MiB)` files and reserves 16 MiB
of vector capacity for each.

| Requested capacity | Region files | Requested append-buffer allocations alone | Logical file lengths combined |
|---|---:|---:|---:|
| 16 GiB | 261 | 4.08 GiB | 16.31 GiB |
| 32 GiB | 521 | 8.14 GiB | 32.56 GiB |
| 256 GiB | 4,162 | 65.03 GiB | 260.12 GiB |

These are allocation/file-length calculations, **not measured resident memory or
physically allocated disk space**. Index buckets/nodes, retained footer vectors,
ghosts, temporary records, file buffers, and the OS page cache are additional.
The 16-byte `Location` is not the cost of an index entry. The current 40-byte
estimate omits bucket/allocator overhead and already uses almost the entire
0.5%-of-capacity index target at an 8 KiB mean record size.

## 3. Evidence gathered in this review

Production and repository test sources were not edited. Diagnostic executables
were built in the session artifact directory against the unchanged source/library.
Reproduction descriptions below are sufficient to turn them into permanent tests;
the implementation must not depend on access to this review's local artifacts.

### Build and existing tests

- Windows Release cache target rebuilt successfully.
- `ctest --test-dir build\WindowsRelease --build-config Release -R SecondaryCache --output-on-failure`:
  **64 passed, 3 Azure-dependent tests skipped, 0 failed**.
- The 64 passing tests include **14 new region/component tests**. This is not a
  complete parameterized region-engine run of the original contract suite.
- The cached build originally referenced an uninstalled PowerShell 7.6.6 executable.
  Refreshing it attempted an unnecessary dependency rebuild; that was stopped.
  The successful reconfigure reused the intact dependencies under
  `build\WindowsDebug\vcpkg_installed`, with `VCPKG_MANIFEST_INSTALL=OFF`.
  No dependency manifests were changed. This is an environment detail, not a
  cache-code defect.
- The linked headers identify RocksDB **11.1.2**. Its `secondary_cache.h`,
  `advanced_cache.h`, `secondary_cache_adapter.{h,cc}`, and `lru_cache.cc` were
  inspected. Do not substitute the research note's unverified 11.12.0 version.
- GCC 13.3 syntax compilation of the changed cache sources using the repository's
  Linux warning flags failed at R13. This used available dependency headers; it
  was **not** a full native Linux dependency build or test run.
- `clang-format --dry-run --Werror` on the new cache/component/benchmark files
  reported **49 formatting diagnostics**. No automatic formatting was applied.

### Deterministic reproductions

The main Windows diagnostic reproduced 17 conditions. Separate subprocesses
reproduced the ASan capacity error and allocation-failure termination. A native
Linux device probe demonstrated sparse sizing. The important sequences are:

| Probe | Setup and operation | Observed result |
|---|---|---|
| Capacity growth | One allocated region with 64 usable bytes; reserve 64; `SetCapacity(128)`; reserve another 64. | SetCapacity returns OK, then ASan reports heap-buffer-overflow at `EnsureWritableRegion`'s next-region mutex. |
| Capacity shrink | Open region with two 64-byte live records; shrink capacity to 64. | OK, usage 128, capacity 64. No pinned reader is involved. |
| Clear buffered data | One 64-byte record below the flush threshold; set capacity to zero. | Usage zero but one live index mapping remains; its generation is stale. |
| Broken reclaim metadata | Four 64-byte records in one region; corrupt first record magic and fail the footer read; insert a fifth record with forced recycling. | Five index mappings and usage 320 for capacity 256; four mappings refer to the recycled generation. |
| Late publication | Pause writer A after manager `Publish` but before index `Upsert`; let writer B recycle the region; resume A's `Upsert`. | Stale A mapping is reintroduced after reclaim; usage is also incorrect. This deterministic interleaving uses the same separate calls as the engine. |
| Same-key accounting | Two reservations for an absent key, both capture `Peek == nullopt`; then publish/upsert both in order. | One index entry, usage 128 instead of 64. |
| Upsert result | Replace location in region 0 with location in region 1. | Returned optional contains region 1, not the previous region 0. |
| Flush failure | Device fails all writes; publish a record at the flush threshold. | `Publish == OK`, written bytes zero, no I/O failure counter. |
| Allocator failure | Fail the next allocation in `ShardedIndex::Upsert`. | `std::terminate` executes, rather than the exception reaching a caller's catch. The probe's terminate handler exits with code 86. |
| Callback error | `create_cb` returns non-OK with a non-null output the caller must ignore; use a counting-only deleter. | The cache invokes that deleter. A real callback may already have freed the output. |
| Result-handle allocation | Successful `create_cb` allocates an object; fail the next allocation. | Lookup returns null, deleter count zero, and `kept` remains true. The probe explicitly frees the leaked object afterward. |
| Invalid configuration | Diagnostic-owned directory contains a sentinel; pass `maxEntrySize` larger than usable region bytes. | Constructor throws after deleting the sentinel. |
| Eager buffers | Four regions, valid 64 KiB maximum entry, 64-byte flush threshold. | Constructor requests 262,956 bytes, including four 64 KiB buffers. |
| Open-file ceiling | Ask FileBlockDevice to open 520 tiny region files, without allocating a large cache. | Fails to create/open a region with this process's CRT stream limit of 512. |
| Real RocksDB adapter | Two-entry LRU primary via `LRUCacheOptions.secondary_cache`, four-record region secondary, 32 primary inserts. | 30 spill calls with `force_insert=false`, zero true; early data remains, later evicted blocks are not admitted. |
| Ghost growth | Offer 10,000 unique non-forced keys to second-chance admission. | Memory requests grow although no values were admitted; oldest ghost remains. |
| Early size rejection | Serialize a 1024-byte value with max encoded entry size 64. | Save callback runs before the insert is rejected. |
| Size overflow | Evaluate `RecordSize(SIZE_MAX, 1)` without constructing an invalid Slice. | Returns 64 after unsigned wrap. |
| Linux allocation | Construct a single 64 MiB FileBlockDevice region on the WSL Linux filesystem; inspect `stat`. | Logical size 67,108,864; allocated bytes 4,096. |

Some rows cover multiple related assertions in one probe; they are evidence, not
a claimed comprehensive fuzzing or concurrency test campaign. No new production
latency, RSS-at-256-GiB, or SSD-endurance claim is made.

## 4. Recommended smaller design

### First reviewable milestone: synchronous correctness reference

Keep the existing public wrapper/legacy engine and use these private rules:

1. **One state mutex** owns region selection, append state, the index, the FIFO,
   reverse lists, live-byte accounting, and capacity transitions. Do not retain
   the current mixture of manager lock, region locks, index locks, manual reader
   counters, and split publication in this reference implementation.
2. **One active append buffer** belongs to the cache, not each region. Flush before
   moving to the next region. An already-encoded record larger than the buffer
   can be written directly in this reference path; do not grow every region's buffer.
3. **An in-memory vector of published key hashes per region** drives reclaim.
   This replaces the current 16-byte per-record footer descriptor retained in RAM
   with an 8-byte hash. On reclaim, remove only mappings still naming that region
   and generation. Duplicate hashes in an old reverse list are harmless.
4. **No serialized region footer or disk scan for index eviction.** This cache
   always starts cold. Current code already retains the footer descriptors in
   memory after sealing, so using them as the authority does not introduce an
   entirely new memory cost. Account and bound that memory explicitly.
5. **One append operation publishes bytes, index state, reverse membership, and
   usage as a transaction.** No externally visible `Reserve` / `Publish` /
   `Upsert` sequence may be interleaved with recycle or resize.
6. **Copy record bytes while holding the state lock**, then release it before
   decoding and invoking `create_cb`. No pin is needed once the owned copy exists.
   Serialization callbacks likewise run outside the state lock. Record a cache
   incarnation and exact location for conditional post-callback cleanup.
7. **No unused hit-count mutation on lookup.** FIFO does not need it until
   reinsertion exists. Prefer four explicit 32-bit fields for the 16-byte
   location: region, offset units, length units, generation. Guard generation
   wrap; use a separate 64-bit cache incarnation across full layout resets.
8. **Keep configuration small.** Admit-all is sufficient for this milestone.
   Preserve public enum declarations, but reject unsupported region-engine policy
   selections before filesystem side effects rather than pretending they work.
9. **Bound open handles independently of region count.** A small handle pool
   keeps the portable implementation viable without changing process-global
   CRT settings or Linux resource limits. Native positioned I/O is a later
   optimization, not a reason to tolerate the current startup failure.

This deliberately trades concurrency for an auditable state machine. It preserves
the major benefit: no per-block file creation, deletion, or rename. It does not
establish acceptable p99 latency by itself; that must be measured.

### Explicit proposed amendments to the ADR

These are recommendations for the next implementation, **not retrospective claims
that the previously accepted design already said this**:

| Decision | Recommended treatment |
|---|---|
| Persistent footer plus scan fallback | Remove for cold-only v1; use bounded resident reverse lists. Preserve record CRCs. |
| Non-forced admission | For `kRegion`, use `force_insert` to bypass an admission filter, not to prohibit FIFO recycle after admission. Leave legacy semantics unchanged and document the difference. |
| Second-chance and hit-count reinsertion | Defer until bounded metadata and hit-ratio measurements exist. Do not retain unbounded ghosts as a placeholder. |
| Deflate/Inflate | Return `NotSupported` in the minimal region milestone rather than report a RAM reduction that did not occur. Preserve signatures and the no-disk-eviction requirement. Full RAM rebalancing is a separate feature. |
| Nonblocking foreground Insert | Synchronous reference first, bounded single-writer queue next; the latter remains a production gate under Q6. |
| Async Lookup | Defer; always-ready synchronous results are a valid initial contract. |
| Physical preallocation | Keep the requirement explicit. Implement and check actual allocation in the storage task; do not rename sparse sizing into a guarantee. |

Returning `NotSupported` is allowed by RocksDB's SecondaryCache interface, but it
is a narrower implementation than the accepted RAM-rebalancing sketch. Do not
attach this version to a tiered configuration that assumes these methods succeed.
The user explicitly said this cache is not behind `NewTieredCache`.

## 5. Implementation tasks

All boxes below are intentionally unchecked. Each task is an implementation unit
for a later coding pass, and must include its regression tests in the same change.
Do not postpone correctness tests until the final performance PR.

### Dependency and PR map

| Task | Depends on | Suggested PR |
|---|---|---|
| T00 - Correct the decision/status record | none | A: review scope and evidence |
| T01 - Validate configuration and directory ownership | T00 | B: correctness reference |
| T02 - Exception and callback ownership | T00 | B |
| T03 - One coherent state transaction | T02 | B |
| T04 - Resident-only eviction bookkeeping | T03 | B |
| T05 - Bounded buffer and checked device operations | T01, T03, T04 | B |
| T06 - Safe capacity changes | T01, T03, T04, T05 | B |
| T07 - Useful bounded admission | T03, T04, T05 | B |
| T08 - Honest memory APIs and observable counters | T03, T05, T06, T07 | B |
| T09 - Contract, concurrency, and platform gates | T01-T08 | B |
| T10 - Trustworthy steady-state measurements | T08, T09 | C: measured baseline |
| T11 - Bounded foreground handoff | T03-T10 | D: foreground latency |
| T12 - Compact index and copy/CRC optimizations | T09, T10 | E: measured efficiency |
| T13 - Optional policies and asynchronous reads | T10, T11; T12 if memory-sensitive | separate, evidence-gated PRs |

PR B is one coherent simplification; its internal commits can follow the task
order for review. Do not submit intermediate commits with newly broken tests as
independent ready-for-production PRs. PRs D and E can be independent after the
baseline, but their combined state must pass the release gates.

### T00 - Correct the decision/status record

- [x] Update the ADR alongside the subsequent implementation, preserving its
  history and recording the explicit amendments above.

**Files:** `docs/design/secondary-cache-performance.md`; public header documentation;
the research contract note only to add a clearly labeled linked-version verification.

**Steps**

1. Distinguish accepted requirements, implemented behavior, and proposed future
   behavior. Do not mark a phase complete because a smoke benchmark ran.
2. Replace the claim of a fully ported test suite with actual named coverage.
   `SecondaryCacheRegionManagerTests` currently has two tests, not the extensive
   race/overflow/pinning matrix claimed in the completion description.
3. Record RocksDB 11.1.2 as the linked contract checked by this review. Keep older
   prior-art notes as provenance, not as a replacement for installed headers.
4. Describe the current mutex/iostream implementation honestly. Separate existing
   single-thread mean latency measurements from unmeasured p99, reclaim, memory,
   concurrency, and 16-256 GiB claims.
5. Scope `kFileHeaderSize == 1` and file-per-entry/LRU documentation to legacy.
   Explain the new engine's aligned record length, capacity units, failure/drop
   semantics, synchronous lookup, and actual support of optional APIs.

**Acceptance:** every implementation-complete claim names a present test or measured
artifact. The historical benchmark numbers are retained with their limitations;
they are not upgraded to production evidence.

### T01 - Validate configuration and directory ownership before side effects

- [ ] Introduce one validation/layout calculation used by construction and resize.

**Files:** public options header, `Engine` constructor, `RecordFormat`,
`BlockDevice` constructor, and new/expanded region configuration tests.

**Steps**

1. Validate the logger, selected engine/policy, region geometry, maximum record
   size, nonzero flush size, and supported shard/count bounds before creating a
   device or allocating region buffers.
2. Use checked addition/alignment for `header + key + payload`; reject fields not
   representable by their on-disk integers. Use quotient/remainder ceiling division,
   not `capacity + usable - 1` without overflow checks.
3. Define capacity as the usable encoded-record byte budget; report physical file
   allocation separately. No active record may cross its region's byte budget.
   When footers are removed, remove the 1 MiB subtraction consistently, not at
   just one call site. A short final region is allowed; skip it for records it
   cannot contain using a bounded candidate search.
4. Make zero capacity a disabled empty cache, with no data-file/buffer allocation
   needed. Insert attempts are counted drops; lookups miss.
5. Treat the supplied directory as a parent, not a recursive-deletion target.
   Use a dedicated cache-owned child directory/ownership marker, reject unsafe
   root/empty paths and unexpected links or contents, and invalidate only owned
   region files. Do not follow a reparse point/symlink into an unrelated tree.
6. An invalid configuration must leave the supplied directory untouched. A
   partially failed valid initialization cleans only files it created.

**Tests:** null logger; unknown enum; zero/too-small region; max-entry boundary;
record arithmetic at `SIZE_MAX`/`UINT32_MAX`; zero capacity; partial final region;
sentinel preservation on invalid options; owned cold restart; unrelated files and
directory links. Never test destructive input against a real user directory.

**Acceptance:** invalid input produces a clear error without I/O side effects,
integer truncation, unbounded allocation, or deletion outside owned cache data.

### T02 - Make exception boundaries and callback ownership correct

- [ ] Keep the public `noexcept` contract; remove unsafe internal `noexcept`.

**Files:** `Engine`, `ShardedIndex`, `RegionManager`, `AdmissionPolicy`, and any
new helper introduced by the simplification.

**Steps**

1. Allow internal allocating operations to throw to the public API boundary.
   Do not label allocation/container-growth functions `noexcept` merely to match
   RocksDB's boundary requirement.
2. For OOM, return the allocation-free `rocksdb::Status::MemoryLimit()` overload.
   Do not allocate a message string while handling allocation failure.
   Error counters/logging must not introduce a second exception escaping a boundary.
3. Check size/admission limits before allocating and invoking `saveto_cb`.
   Reserve a bounded temporary-memory allowance before serialization where needed.
4. For non-OK `create_cb`, ignore `out_obj`: the callback owns failure cleanup.
   Only establish a cache-side cleanup guard after successful creation of a
   non-null object. Respect a nullable deleter when no destruction is required.
5. Construct `ResultHandle` while that success-path cleanup guard is still active;
   transfer ownership only after allocation succeeds. Set hit counters and
   `kept_in_sec_cache` for the successfully returned result, not a failed lookup.
6. If any reader pin survives T03 temporarily, make it an actual RAII guard that
   decrements exactly once on every exit, including allocation exceptions.

**Tests:** failing size/save/create callbacks; a non-OK create callback that has
already cleaned its output; successful creation followed by handle-allocation
failure; null deleter with a non-owned/static successful object; allocation failures
in index growth, read-buffer growth, and eviction bookkeeping. Death-test/subprocess
coverage must prove that the API returns an error/miss instead of terminating.

**Acceptance:** no exception crosses the public API, no invalid/double deletion,
no created-object leak, and `kept` is false on a miss.

### T03 - Make append, index publication, and accounting one transaction

- [ ] Replace the split concurrent protocol with a single documented lock domain.

**Files:** `Engine`, `RegionManager.{hpp,cpp}`, `ShardedIndex.hpp`.

**Steps**

1. Choose the engine state mutex as the owner of all mutable cache state. Replace
   independently lockable append phases with a private `AppendLocked` operation.
   Mark lock preconditions in short comments; do not add another locking framework.
2. Keep serialization and object creation outside this mutex. For lookup, copy
   owned record bytes and a location/incarnation snapshot while protected.
3. Publish only a complete valid record. Update reverse membership, the index,
   and live usage in the same state transaction. An allocation or I/O failure
   must leave either the old mapping or a miss, not a partially published entry.
4. Fix `Upsert` to return the actual old location, or simplify the API so the owner
   explicitly reads/replaces under the same lock. Remove the separate stale
   `Peek` snapshot used for accounting.
5. Define live usage as the sum of lengths of currently indexed records. On replace,
   subtract the old length exactly once and add the new length once. Do not use
   saturating subtraction to conceal invariant violations.
6. Add conditional removal by exact identity: hash, region, generation, offset,
   length, and cache incarnation where applicable. Use it for corruption cleanup
   and `adviseErase`; an old lookup must not remove a later replacement.
7. Synchronize capacity getters, reset, and statistics snapshots with their writers.
   Remove the reader spin/pin machinery if reads are fully copied under the mutex.
8. Remove unused hit-count mutation and unused ghost-location flags from the
   minimal index. Preserve a sufficiently wide generation and prevent wrap/ABA.

**Tests:** use barriers/hooks, not sleeps, for two same-key first inserts;
publish-versus-recycle; lookup-copy versus overwrite/erase/resize; and `create_cb`
reentrancy. Assert payloads, exact live-entry counts, and usage, not just no crash.

**Acceptance:** after quiescence, `usage == sum(indexed record lengths)`, every
indexed location belongs to a live generation, and stale cleanup cannot erase
a newer mapping. Run the new tests under ASan and TSan when available.

### T04 - Evict using resident metadata, not a disk parser

- [ ] Remove serialized footers and the scan fallback from cold-only v1.

**Files:** `RegionManager`, `ShardedIndex`, `RecordFormat` only for obsolete format
references; region-manager/corruption tests; ADR layout documentation.

**Steps**

1. Replace `footerEntries` with a bounded per-region list of published key hashes.
   Append only under T03's lock; handle allocation failure before making a new
   mapping visible. Track allocation/capacity of these vectors as metadata memory.
2. On FIFO eviction, visit that region's list and remove only current mappings
   still naming that region and generation. Duplicates are safe; replacements in
   other regions survive. Clear the list and advance the generation.
3. Delete footer encode/decode, footer CRC, footer overflow mode, and
   `ScanRegionEntries`. Keep the record-level CRC/key checks used for lookup.
4. `ClearAll` must clear the entire index, all reverse lists, buffers, FIFO state,
   and usage directly. It must not discover entries by reading storage.
5. Reset/reuse must work even when every byte of the old region is unreadable.
   Do not require physical zeroing; production Reset currently leaves old bytes,
   whereas the memory test device clears them.
6. Keep full-index sweeps only as a debug invariant check or an exceptional repair
   fallback. An O(total-cache-entries) sweep for every 64 MiB recycle is not an
   acceptable normal path at 256 GiB.

**Tests:** corrupt first/middle/last records; injected read errors; clear below the
flush threshold; repeated same-key replacements within/across regions; explicit
erase before reclaim; many wraps; fake Reset that leaves bytes unchanged.

**Acceptance:** reclaiming a region leaves zero index entries for its retired
generation regardless of disk content. No cold-restart index recovery is added.

### T05 - Bound buffers/handles and propagate device errors

- [ ] Keep resource usage proportional to configured budgets, not one maximum
  payload buffer plus one CRT stream per region.

**Files:** `BlockDevice.{hpp,cpp}`, `RegionManager`, fault-injection tests.

**Steps**

1. Allocate one active flush buffer. Release/transfer it on rollover rather than
   retaining a maximum-sized allocation on every sealed region.
2. For the synchronous reference, flush the active buffer before a record larger
   than the buffer, then write that already-owned encoded record directly. Keep
   transient record memory bounded; never allocate a 16 MiB buffer for every region.
3. Implement a bounded, lazy handle pool, initially an internal fixed bound such
   as 64 handles. Reopening a region must use a non-truncating mode. Close only a
   handle with no active operation. Under the reference state mutex this requires
   no independent asynchronous handle-lease protocol.
4. Check creation, sizing/allocation, seek, write, read length, and flush outcomes.
   Give device operations an explicit error result usable by the public boundary.
   A failed flush must not advance/reset the buffer offset as though it succeeded.
5. Fail closed on an unrecoverable buffer/device state: invalidate the affected
   mappings and report an I/O error, or disable the disposable cache with an
   observable reason. Do not keep appending to an ambiguously positioned buffer.
6. Implement actual checked disk allocation on supported Windows/Linux if retaining
   the preallocation guarantee. Use the platform allocation API, not just a last-byte
   write. Do not require elevated privileges or change process-global file limits.
   If a filesystem cannot satisfy the guarantee, fail explicitly rather than
   silently treating sparse sizing as reserved storage.
7. Bounds checks must use `offset <= size && length <= size - offset`, including
   narrowing to native offset/length types. Test partial transfers and EOF.
8. Add a private device factory/injection constructor usable by the real engine
   tests. Do not expand the public filesystem abstraction or modify Azure code.

**Tests:** injected create/allocate/seek/write/read/flush failures; threshold flush;
large record; discontinuous writes during a failed flush; more than 512 region
identities using small test files; memory peak as region count increases; native
file size and allocation checks on both platforms.

**Acceptance:** only the configured active/transient buffers consume payload DRAM;
open handles remain within the bound; failed required I/O is observable and never
misreported as completed storage. Large capacity need not be physically provisioned
in unit tests to verify the handle-count bound.

### T06 - Make capacity changes safe and explicit

- [ ] Replace scalar-only resizing with a coherent cache reset/layout transition.

**Files:** `RegionManager`, `Engine`, capacity tests.

**Recommended simple behavior:** capacity changes may discard all cache entries.
This is an infrequent control operation on disposable data; preserving entries
while incrementally resizing is not worth a second state machine in v1.

**Steps**

1. Validate the new layout using T01 before touching live state.
2. Under T03's mutex, invalidate the index/buffers and advance the cache incarnation.
   Construct the correctly sized region metadata/device layout; do not change a
   count used by placement without changing the backing containers.
3. On initialization/allocation failure, return a non-OK status and leave an
   explicitly empty/disabled usable object, or retain the complete old layout if
   construction was genuinely side-effect-free. Choose and document one failure
   contract; recommended baseline is disabled empty state after destructive reset.
4. Zero capacity is empty/disabled; re-enabling builds a new valid layout. Getters
   must report the actual state, including after failure.
5. Ensure a callback finishing after a reset cannot erase a newly created entry
   with coincidentally identical region/offset values; compare incarnation.
6. Do not silently return success for an unsupported growth. A temporary
   `NotSupported` guard is a valid containment patch, not completion of this task.

**Tests:** grow across a region boundary then write beyond the old capacity; shrink
with only open-buffer data; shrink with multiple sealed regions; zero/re-enable;
repeated grows/shrinks; OOM/device failure during reconfiguration; concurrent
getters and lookups; pending create callback across a reset.

**Acceptance:** no out-of-range access under ASan; usage never exceeds the new
successful capacity; zero really has no indexed entries; no stale-incarnation
cleanup affects the new layout.

### T07 - Define useful admission without an unbounded ghost cache

- [ ] Separate admission choice from the ability to recycle FIFO storage.

**Files:** `Engine`, `RegionManager::EnsureWritableRegion`, `AdmissionPolicy`,
public options documentation, adapter-level tests.

**Steps**

1. For the minimal region engine, use `kAdmitAll`: an admissible record may recycle
   the oldest region irrespective of `force_insert`. It can still be dropped on
   resource-pressure or size grounds. Preserve the legacy engine's documented gate.
2. Treat `force_insert` as bypassing a future admission filter, not bypassing safety
   limits. It must never override max-record, memory, disk, or shutdown bounds.
3. Keep `kSecondChance` in the public enum for compatibility but reject it explicitly
   during region construction until the bounded implementation is supplied in T13.
   Remove inactive ghost-map construction and locks from the admit-all hot path.
4. Record this semantic difference in the ADR/header; do not port the old
   force-false-full-cache test unchanged to the new default policy.
5. When skipping a too-small final region, search only a bounded number of regions;
   do not fail every future insert or spin indefinitely.

**Tests:** fill, erase, and refill with `force_insert=false`; insert several cache
capacities of unique keys; verify newest entries replace old entries under FIFO.
Run through `LRUCacheOptions.secondary_cache`, not only direct forced insert calls.
Check that force true cannot admit an oversized or memory-budget-exceeding entry.

**Acceptance:** the default RocksDB integration continues to admit useful new blocks
after initial fill, with bounded metadata. Intentional drops have distinct counters.

### T08 - Make memory and observability honest

- [ ] Expose a small statistics snapshot and stop reporting fake RAM rebalancing.

**Files:** public header (additive snapshot only), `CacheStats`, `Engine`, index
allocation/accounting helpers.

**Steps**

1. In the minimal region milestone, return `NotSupported()` from `Deflate`/`Inflate`;
   remove the unused arithmetic fields. Update the engine-specific test to expect
   unsupported status and unchanged disk data/capacity. Do not change legacy
   behavior in this task.
2. Add an additive `GetStats` snapshot reporting live entries, live encoded bytes,
   configured disk budget, physical allocation, index/reverse-list allocation,
   active/transient buffer memory, open handles, admission/drop reasons, I/O
   failures, corruption misses, and reclaim counts.
3. Count bytes at the device boundary for all writes, including metadata if any
   remains. Define write amplification's denominator explicitly as admitted payload
   bytes; separately expose encoded bytes including keys/headers/padding.
4. Add a hard metadata-memory budget using counted allocator/container capacity,
   not just `entryCount * 40`. Include reverse lists and retained bucket arrays.
   Under pressure, reclaim an old region early or decline admission; do not create
   unbounded ghost/history state. Account for temporary rehash peaks.
5. Count temporary serialization/read buffers separately. Do not represent OS
   page-cache residency as heap/index memory; collect it separately in benchmarks.
6. Surface operational failures with rate-limited logging plus counters. Avoid
   per-hit/per-miss logs and OOM-path allocations. Do not hide failed disk access
   behind `insertsDroppedNoBuffer`.

**Tests:** all counter categories; actual allocated-capacity accounting including
rehash and clears; tiny-entry and repeated-overwrite churn under a small budget;
no growth after rejected one-shot keys; Deflate/Inflate result and invariants.

**Acceptance:** an operator can distinguish full/pressure drops from device errors
and inspect actual cache-owned memory. If the simple map cannot fit a full 8 KiB
working set within the ADR's 0.5% index target, report that limit honestly and make
T12 a pre-production gate, not a silently waived target.

### T09 - Replace smoke coverage with contract and platform gates

- [ ] Add missing regression coverage and run it against the region engine.

**Files:** `tests/AVEVA/RocksDB/Plugin/Core/*SecondaryCache*`, owning CMakeLists,
and the two GCC-rejected expressions in `RegionManager.cpp`.

**Steps**

1. Parameterize storage-independent tests over legacy/region engines. Keep separate
   expected policies for FIFO vs LRU, key limits, capacity units, and optional RAM
   APIs. Do not duplicate all test bodies or rewrite unrelated SST-file cache tests.
2. Exercise compression type **and source tier**, `out_charge` vs payload length,
   helper incompatibility, empty saved values, binary/long keys, `adviseErase`,
   retained/not-retained flags, stats roles, and ready handles for both wait values.
3. Add every applicable reproduction from section 3 as a permanent failing-before/
   passing-after test. Drive corruption through the real engine and assert that
   `create_cb` is never called for a corrupt record. Do not manually remove the
   index in the test and call that evidence of engine cleanup.
4. Add deterministic multi-writer tests with barriers and tiny regions that force
   many recycles. Assert exact usage and valid payloads. Include resize/erase
   races and callback reentrancy. Avoid sleep-only or no-crash-only assertions.
5. Add a local-filesystem RocksDB DB/primary-cache integration test needing no
   Azure credentials. Force eviction, perform Get/MultiGet, validate results, and
   require secondary hits and continued admissions after a complete turnover.
6. Remove the two GCC-redundant casts without disabling warning-as-error flags.
   Run actual Windows and Linux builds/tests in CI. The syntax-only result from
   this review is not a replacement for that gate.
7. Apply the existing clang-format configuration to touched files. Remove unused
   headers/helpers and keep comments about ownership, lock order, and invariants,
   rather than narrating each line of code.

**Commands**

```powershell
cmake --preset WindowsDebug
cmake --build build\WindowsDebug --config Debug --target aveva-rocksdb-plugin-core-tests
ctest --test-dir build\WindowsDebug --build-config Debug -R SecondaryCache --output-on-failure

cmake --preset WindowsRelease
cmake --build build\WindowsRelease --config Release --target aveva-rocksdb-plugin-core-tests
ctest --test-dir build\WindowsRelease --build-config Release -R SecondaryCache --output-on-failure
```

On Linux use the repository's `LinuxDebug` / `LinuxRelease` presets and the matching
build directories. Add sanitizer jobs for the cache tests without modifying RocksDB.
Keep destructive/allocator-failure cases in isolated subprocesses.

**Acceptance:** all intended region contract tests run, not skip; Windows/Linux
warning-as-error builds succeed; sanitizers report no findings in the exercised
cache code. List skipped Azure tests separately from passed tests.

### T10 - Establish a meaningful performance baseline

- [ ] Extend the existing benchmark instead of adding a second framework.

**File:** `SecondaryCacheBenchmark.cpp`; ADR measured-results section.

**Steps**

1. Use a unique run-owned directory; reject invalid engine names and overflowed CLI
   sizes. Cleanup must be RAII-safe and must not delete another concurrent run.
2. Give entries deterministic but key-specific variable payloads. Verify returned
   bytes, size, and callback metadata outside timed sections.
3. Separate cold construction/preallocation from timed operations and report it.
   Report destructor/drain work separately too; do not hide a backlog of unwritten
   data when comparing insert completion times.
4. Add hit, miss, mixed read/write, duplicate-key update, and steady-state turnover
   phases. Make the working set larger than capacity and assert `regionsReclaimed`
   and post-turnover admissions are nonzero. Exercise force-false via a real primary.
5. Record per-operation p50/p99/p99.9/max and throughput, plus hit ratio, rejected
   inserts, bytes written, live/index/buffer memory, and open handles.
6. Start with 1 and 4 threads and 1/4/8/32 KiB plus a mixed-size distribution.
   Add 16/64 threads only after deterministic correctness is established.
7. Run legacy and region trials **sequentially**, with the same seed, build,
   filesystem, workload, and capacity. Do not run tests or the other engine's
   benchmark concurrently on the same device during a comparison.
8. Distinguish buffer hits, warm OS-page-cache reads, and actual device reads.
   Do not label a warm sequential lookup loop an SSD p99 result.
9. Record raw trials and variance. Revisit the ADR's H1-H6 claims using the metric
   each hypothesis names; a mean speedup cannot confirm a p99 hypothesis.

**Acceptance:** successful runs cannot consist of dropped inserts or unchecked
wrong payloads. The baseline includes real FIFO reuse and reports both performance
and memory; 16/256 GiB tests are deferred only with an explicit hardware limitation.

### T11 - Meet the foreground-insert requirement with one bounded writer

- [ ] Add a single writer only after the synchronous core is correct and measured.

**Files:** `RegionEngine`/append implementation, small private queue helper if
needed, device injection tests. No IOCP/io_uring or worker-pool framework.

**Steps**

1. Use one bounded queue of owned encoded records and one writer thread invoking
   the already-correct synchronous append transaction. Bound bytes and item count.
   Acquire a transient-memory allowance before serialization; a full budget/queue
   causes a counted drop without waiting for disk or a free buffer.
2. Never queue the borrowed RocksDB `obj` or callback context. `saveto_cb` must
   finish synchronously before `Insert` returns.
3. Preserve immediate read-your-insert behavior with a bounded pending-record
   map referencing the same immutable queued bytes. Do not make a second payload
   copy or an unbounded parallel index. Lookup prefers a current pending record.
4. Assign each pending insertion a unique sequence plus cache incarnation.
   Overwrite, Erase, and SetCapacity invalidate old tokens. The writer checks the
   token before publishing and cannot resurrect superseded or erased entries.
5. Keep the simple state mutex initially. Foreground admission uses a non-waiting
   acquisition/drop policy when it would otherwise wait behind storage work.
   The writer may block on disk; the foreground spill caller must not.
6. Remove pending state only when the disk/buffer mapping is published or the
   operation is explicitly failed/cancelled. Release budget via RAII after all
   readers/work items release the immutable bytes.
7. Account/log asynchronous failures; an earlier OK admission response is not a
   persistence guarantee. Shutdown stops admission and joins the worker after
   draining or explicitly discarding disposable pending data.

**Tests:** hold device I/O behind a test event and prove inserts still complete
until the bounded queue fills, then return counted drops; pending lookup; erase/
overwrite/resize while queued; shutdown under load; failure after queueing.

**Acceptance:** deterministic evidence that foreground spill does not wait on
device completion, bounded peak queued/transient memory, and unchanged lifecycle
invariants. This is a production gate under the currently accepted Q6.

### T12 - Improve memory/CPU efficiency one change at a time

- [ ] Optimize only measured costs after T10; retain the same functional tests.

**Candidate order**

1. Replace node-based `std::unordered_map` with the already available
   `boost::unordered_flat_map`, measuring occupied/allocated bytes and rehash peaks.
   `Boost::unordered` is already linked; no dependency addition or bespoke hash
   table is necessary. Combine the measured index and reverse-list costs when
   checking the 0.5% target at 8 KiB.
2. Serialize directly into a single bounded encoded-record allocation. Avoid the
   current intermediate payload allocation and repeated full-record zero fills;
   initialize only the header and alignment padding that require initialization.
3. Profile CRC32C CPU. Use an already available, verified implementation with
   hardware dispatch only if the measurement warrants it; retain known vectors
   and portable fallback tests. Do not remove integrity checks for speed.
4. If the coarse lock dominates p99, split read concurrency from writer lifecycle
   in a separate change with explicit lifetime/lock-order tests. Do not restore
   the old split publication protocol.

**Acceptance:** each optimization has a before/after result with the same workload,
no correctness or hit-ratio regression, and an explicit memory/latency tradeoff.
If the metadata target is otherwise unmet, the compact-index portion is required
before claiming the accepted capacity/memory envelope.

### T13 - Keep optional complexity in later, evidence-gated PRs

- [ ] Do not start these merely because the original proposal listed them.

| Feature | Evidence required first | Minimum tests/constraints |
|---|---|---|
| Bounded second-chance admission | Admit-all hit-ratio/write-rate baseline | Hard byte/entry bound, eviction/aging, unique-key flood, force bypass, no leak when admission fails |
| Hit-count reinsertion | FIFO hot-key miss-rate regression worth its write cost | Bounded reinsertion budget, correct replacement accounting, no infinite reclaim loop, measured write amplification |
| Dynamic write-rate admission | Device/write-rate budget and measured excess writes | Deterministic bounded state, convergence tests, no hidden user-workload stalls |
| Async lookup / real WaitAll | MultiGet p99/throughput shows material serialized-I/O cost | Pending/ready ownership, exactly-once create/delete, erase/reclaim/resize/cancel/shutdown races |
| Native positioned I/O | Handle-pool churn or per-file seek locks dominate | Full/short transfers, errors, offsets, resource bounds on both supported OS families |
| Direct I/O / IOCP / io_uring | Buffered/native-sync baseline is insufficient | Alignment, fallback/error visibility, queue lifetime, memory/page-cache measurements |
| URI factory registration | Harness needs runtime construction and linked API supports it | Parsing/validation, selectable engine, no changes to RocksDB or unrelated plugin code |

Do not add compression, restart recovery, a second small-object engine, or changes
to the Azure filesystem as part of these tasks.

## 6. Release gates

- [ ] R01-R08 process-safety/lifecycle/storage issues resolved with regression tests.
- [ ] Repeated real RocksDB evictions admit new entries after full turnover.
- [ ] Index, reverse metadata, buffers, ghosts/pending work, and handles have actual
  measured bounds; no per-region maximum-payload allocation.
- [ ] Foreground insert behavior meets Q6, or an explicit later ADR changes the
  requirement. Do not call the synchronous reference production-complete.
- [ ] Windows and Linux Release/Debug tests and applicable sanitizers pass.
- [ ] Device failures and corrupt data are observable misses/errors, never invalid
  callback input or a process termination.
- [ ] At least one steady-state benchmark and local RocksDB DB test demonstrate
  correct data and useful secondary hits. Scale tests distinguish logical file
  length, physical allocation, heap allocations, RSS, and OS page cache.
- [ ] Accepted 16-256 GiB and 0.5%-index targets are measured on suitable hardware,
  or release scope is explicitly limited instead of claiming untested support.
- [ ] Only then consider flipping the default. Keep legacy removal separate from
  the first enabling PR so rollout remains reversible.

## 7. Implementation-agent guardrails

Work on one task at a time, starting only after its prerequisites are met. Read the
named current implementation and tests; implement the specified behavior rather
than preserving this branch's incidental method boundaries.

For each task:

1. Add a focused regression test showing the old failure.
2. Make the smallest cohesive fix/simplification and run that test.
3. Run the shared cache contract suite and relevant build/sanitizer checks.
4. Record the exact commands and passed/failed/skipped results in the PR.
5. Update the ADR's current-state section only for work actually implemented.

Do not weaken assertions, turn device errors into silent success, remove a test
because it exposes a race, disable warning flags, or introduce new global process
settings to make this cache appear to work. Do not modify RocksDB or unrelated
plugin components. Do not create or publish a PR without the required explicit
user approval.
