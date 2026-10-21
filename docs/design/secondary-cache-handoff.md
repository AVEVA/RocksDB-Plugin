<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# SSD secondary cache handoff

This file is the current handoff for the local-SSD RocksDB `SecondaryCache`.
The library source is the region-only `RegionBasedSecondaryCache`; the former
`FileBasedCompressedSecondaryCache` API, file-per-entry implementation, and LRU
index have been removed in this branch.

## Current implementation

- Public API: `include/AVEVA/RocksDB/Plugin/Core/RegionBasedSecondaryCache.hpp`.
- Implementation: `src/Core/RegionBasedSecondaryCache.cpp` and
  `src/Core/SecondaryCache/`.
- The cache stores serialized block-cache entries, not SST files. It uses FIFO
  region recycling, checksummed records, bounded metadata/transient allocations,
  a 64-handle device pool and synchronous lookup/append.
- `SetCapacity` is a cold reset. `Deflate` and `Inflate` return `NotSupported`.
- `wait=false` is still synchronous; foreground spill can block on device I/O.
  This is a correctness baseline, not a production-ready implementation.
- Region mode's directory ownership, cold-start behavior, failure handling,
  accounting and remaining release gates are documented in
  [`src/Core/SecondaryCache/README.md`](../../src/Core/SecondaryCache/README.md).

## Decisions and constraints

Accepted decisions include FIFO region eviction, no added compression, ephemeral
cold-start storage, no RocksDB changes, and use as the LRU block cache's secondary
tier. The accepted Q6 requirement that foreground spill not wait for device
completion remains an unmet production gate; a bounded writer queue is future work.
The cache is not wired behind `NewTieredCache`.

The user explicitly approved a breaking rename and removal of the legacy API.
Do not restore a compatibility alias, engine selector, second-chance policy, or
legacy file-index implementation.

## Reading order

1. This handoff for current scope and decisions.
2. [`secondary-cache-v1-review-todo.md`](secondary-cache-v1-review-todo.md) for
   historical review findings, regression evidence and remaining validation gates.
3. [`secondary-cache-performance.md`](secondary-cache-performance.md) for the
   accepted design rationale; its original dual-engine proposal is historical.
4. [`research/README.md`](research/README.md) for the research index.

## Validation

Windows Debug/Release regression and native WSL Debug/Release secondary-cache
suites passed after the replacement. Native ASan+UBSan and TSan suites each
passed all 40 cache tests; prebuilt dependencies were not instrumented.
WSL dependencies are configured; no user action is required.
Reproduction commands and caveats are in the review/to-do document.

## Remaining work

1. Complete remaining native filesystem fault injection, dependency-instrumented
   sanitizer coverage and longer stress runs. T09 is not entirely complete.
2. Measure throughput, tail latency, hit ratio, metadata, RSS/page-cache and
   physical disk allocation. Do not make performance or deployment-scale claims
   based on the earlier dual-engine benchmark.
3. Implement and validate bounded asynchronous writes before treating Q6 as met.
   Revisit index representation and optional policies only with measured evidence.

Never create a pull request without explicit user approval.
