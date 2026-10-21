<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# SSD secondary cache: background, plan and session handoff

This is the entry point for future work on the local-SSD RocksDB `SecondaryCache`
(`FileBasedCompressedSecondaryCache`, region engine). Start a new session from
this file; everything needed to continue is in this branch.

## Reading order

1. **This file** — goal, decisions, current state, constraints, next task.
2. [`research/README.md`](research/README.md) — background research index:
   - [`research/rocksdb-secondary-cache-contract.md`](research/rocksdb-secondary-cache-contract.md):
     how RocksDB calls the cache (Insert/Lookup/Erase/handles, LRU spill, ownership).
   - [`research/prior-art-survey.md`](research/prior-art-survey.md): CacheLib
     Navy, RocksDB persistent cache, Kangaroo, Ceph/bcache, ScyllaDB, etc.
3. [`secondary-cache-performance.md`](secondary-cache-performance.md) — ADR.
   The **v0.4 amendment near the top is authoritative**; later sections are the
   historical/target design and research rationale.
4. [`secondary-cache-v1-review-todo.md`](secondary-cache-v1-review-todo.md) —
   review findings R01–R15, tasks T00–T13, progress table, next sequence,
   exact validation commands/results, release gates and agent guardrails.
5. [`../../src/Core/SecondaryCache/README.md`](../../src/Core/SecondaryCache/README.md) —
   how the implemented cache works: files, methods, record layout, data flow.

## Goal

Improve performance of the local-disk secondary cache used by RocksDB, informed by
disk caches in other high-performance systems, while staying aligned with how
RocksDB uses `SecondaryCache`.

## Accepted decisions (owner: Paul Pirogovsky)

| Q | Decision |
|---|---|
| Q1 | Buffered I/O (OS page cache) is acceptable; no O_DIRECT requirement |
| Q2 | FIFO region eviction |
| Q3 | One cache per process, 16–256 GiB |
| Q5 | No added compression (yet) |
| Q6 | Foreground spill must not wait for device completion (production gate; T11) |
| Q8 | Not wired behind `NewTieredCache`; used as the LRU cache's secondary cache |
| Q9 | Storage is ephemeral; always cold start, no warm restart/recovery |
| Q10 | Recent Windows and Linux only |
| Others | Proposed defaults in the ADR accepted |

## Current state

- Branch `paul-pirogovsky/ssd-secondary-cache`; milestone commit `2891ac7`
  (`fix(cache): simplify SSD secondary cache to a synchronous baseline`).
- `kRegion` is opt-in; `kLegacy` stays the default. `kRegion` is a **synchronous
  correctness baseline, not production-ready**.
- T00–T08 implemented; T09 partially (Windows Debug/Release/ASan suites pass,
  Linux syntax checks and native ASan/UBSan/TSan probes pass).

## Next task sequence

1. Finish T09: full native Linux build/test, broader OOM/fault injection, full
   engine TSan and mixed resize/read/write stress.
2. T10: trustworthy benchmark (Release, payload verification, p50/p95/p99, hit
   ratio, disk writes, memory/RSS/page cache) — no performance claims before this.
3. T11: one bounded writer queue so foreground insert does not wait for I/O.
4. T12: compact index / CPU efficiency, one measured change at a time.
5. T13: optional evidence-gated features (admission policies, async reads, native
   positioned I/O).

Keep each step a separate, reviewable commit/PR.

## Constraints

- Change only the secondary cache, its tests/benchmark/docs, and the configuration
  needed to load and use it. Do not modify RocksDB, Azure filesystem, SST-file
  `Core/FileCache` or other plugin components.
- Keep the public header backward compatible (additive changes only).
- Never create a pull request without explicit approval from the user.
- Build/test commands and environment notes: see "Validation of this milestone"
  in the plan. On Windows reuse `build\WindowsDebug\vcpkg_installed` via
  `-DVCPKG_INSTALLED_DIR=... -DVCPKG_MANIFEST_INSTALL=OFF`; long ASan paths need
  MSBuild `/p:TrackFileAccess=false`; Linux TSan needs `setarch $(uname -m) -R`.
