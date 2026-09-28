<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# RocksDB `SecondaryCache` contract — findings from the reference tree

> **Provenance.** Distilled from a background exploration agent that traced the `SecondaryCache`
> contract through the local RocksDB **11.12.0** tree (`C:\dev\rocksdb`) on 2026-09-28.
>
> **Fidelity warning.** The agent's raw transcript was lost to temp-file cleanup before it could be
> committed. This file records the findings that were actually incorporated into §4 of
> [`../secondary-cache-performance.md`](../secondary-cache-performance.md). The facts below were
> each derived from reading the named files, but **line numbers are approximate and must be
> re-verified before being cited as authoritative.** The RocksDB tree is the source of truth.

## Version

RocksDB 11.12.0. The vcpkg-resolved version used by this repository was **not** verified to match;
see Q-list in the proposal.

## Files read

- `include/rocksdb/secondary_cache.h`
- `include/rocksdb/cache.h`
- `cache/secondary_cache_adapter.{h,cc}`
- `cache/compressed_secondary_cache.{h,cc}`
- `cache/tiered_secondary_cache.{h,cc}`
- `cache/cache.cc`
- `include/rocksdb/advanced_cache.h` (for `CacheItemHelper`)

## Findings that drove the design

### F1 — `Insert` is called inline on a foreground thread

`CacheWithSecondaryAdapter::EvictionHandler` (`cache/secondary_cache_adapter.cc` ≈L130–160) invokes
`Insert` synchronously, **after** the shard mutex has been released, on whichever thread caused the
eviction.

**Consequence:** `Insert` must never block. Any design that waits on I/O inside `Insert` adds
latency directly to a user-facing `Get`/`MultiGet`. This is the single strongest constraint on the
write path and is why the proposal drops entries rather than stalling when write buffers are
saturated.

### F2 — The `obj` passed to `Insert` is borrowed

The object pointer is only valid for the duration of the call. `helper->saveto_cb` must therefore be
invoked **synchronously** inside `Insert`. Only the resulting *bytes* may be deferred.

**Consequence:** serialization cost is unavoidable on the foreground thread; only the device write
can be made asynchronous.

### F3 — `Status::OK()` does not imply admission

`include/rocksdb/secondary_cache.h` documents that `Insert()` "may or may not" actually insert, even
when returning OK. `force_insert` is a **hint**, not a command.

**Consequence:** admission control is explicitly legal and requires no API deviation.

### F4 — `advise_erase` is `found_dummy_entry`

In `Lookup`, `advise_erase` is passed as the adapter's `found_dummy_entry` (≈L170–250): the primary
cache holds a recency marker for the key, so the secondary copy may be dropped. The out-parameter
`kept_in_sec_cache` selects whether the adapter promotes using `helper` or
`helper->without_secondary_compat`.

**Consequence:** `Erase` and `advise_erase` must be cheap. In the proposed design they are
index-only operations with no I/O at all.

### F5 — `CompressedSecondaryCache` uses dummy entries to record recency

On the first eviction of a key, a zero-size "dummy" is inserted into the primary cache and the real
bytes are kept compressed; on a subsequent hit, the presence of the dummy is the signal to promote.
Symmetrically on eviction (`cache/compressed_secondary_cache.h` ≈L63–79).

**Consequence:** this is upstream precedent for the proposal's `kSecondChance` admission policy —
admit on the *second* sighting of a key, not the first. It costs one index slot and zero bytes.

### F6 — The async path is real and currently unimplemented by us

`StartAsyncLookupOnMySecondary` → `Lookup(wait=false)` → `secondary_cache_->WaitAll(...)`
(`cache/secondary_cache_adapter.cc` ≈L350–480; `cache/cache.cc` ≈L153–185).

**Consequence:** because the current plugin ignores `wait` and implements `WaitAll` as a no-op,
RocksDB's `MultiGet` path degrades to fully serialized synchronous I/O. Recovering parallelism here
is one of the larger available wins.

### F7 — No single-flight guarantee

The same key may be inserted concurrently by several threads. `create_cb` must be assumed reentrant.

**Consequence:** index publication must be atomic and last-writer-wins, and records must be
self-validating so that a stale location can never be mistaken for a fresh one.

### F8 — Exceptions must not escape

`include/rocksdb/secondary_cache.h` ≈L60–65 states RocksDB is not exception-safe.

**Consequence:** every override is `noexcept` with an internal catch-all.

### F9 — `Deflate`/`Inflate` are a RAM-reservation signal

`include/rocksdb/secondary_cache.h` ≈L129–153, and the routing through
`ConcurrentCacheReservationManager` in `cache/secondary_cache_adapter.cc`, establish that these
adjust the secondary cache's **memory** reservation as the tiered cache rebalances. They are
deliberately lighter-weight than `SetCapacity`.

**Consequence:** the current implementation's deletion of on-disk data in response to `Deflate` is a
genuine semantic bug. See Q8 in the proposal.

### F10 — No enumeration or introspection API is required

`SecondaryCache` has no method that requires listing contents.

**Consequence:** the index representation is entirely our choice; nothing outside the cache can
observe it.

### F11 — `CacheItemHelper` supports chunked serialization

`saveto_cb` takes `from_offset`/`length`, so a value can be serialized piecewise.

**Consequence:** a future optimization can serialize directly into the write buffer without an
intermediate contiguous allocation. Not exploited in v1.

### F12 — Statistics tickers the cache is expected to feed

`SECONDARY_CACHE_HITS`, `SECONDARY_CACHE_{DATA,INDEX,FILTER}_HITS`, and the
`COMPRESSED_SECONDARY_CACHE_*` family.

### F13 — Tiered admission policies

```
kAdmPolicyAuto, kAdmPolicyPlaceholder, kAdmPolicyAllowCacheHits,
kAdmPolicyThreeQueue, kAdmPolicyAllowAll, kAdmPolicyMax
```

`kAdmPolicyAllowCacheHits` sets `force_insert=true` only for blocks that were actually hit in the
primary cache before eviction — an upstream hotness signal the proposal's admission policy honours.
`kAdmPolicyThreeQueue` is auto-selected when an `nvm_sec_cache` is present and wraps both caches in
`TieredSecondaryCache`, which performs **no demotion** between tiers.
