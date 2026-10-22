<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# SSD secondary cache: minimal synchronous implementation

This directory implements `RegionBasedSecondaryCache`, a region-based local
secondary cache for RocksDB's serialized block-cache entries, not SST files.
The former file-per-entry/LRU implementation and its public API have been removed.

For a review-oriented walkthrough of state, class/function responsibilities and
worked insert/large-entry/FIFO examples, read the
[developer guide](DEVELOPER-GUIDE.md).

This is a deliberately synchronous correctness baseline. One mutex protects
placement, the index, FIFO reclaim, file access and accounting. Callbacks run
outside that mutex. It is not the final nonblocking spill implementation.

The [ADR](../../../docs/design/secondary-cache-performance.md) records accepted
requirements and the amendments for this milestone. The
[review and implementation plan](../../../docs/design/secondary-cache-v1-review-todo.md)
records completed work and remaining release gates.

## Construction and supported behavior

Supply `RegionBasedSecondaryCacheOptions`, a non-null logger and a dedicated
local parent directory. Pass the resulting shared cache to
`rocksdb::LRUCacheOptions::secondary_cache`. No RocksDB changes or NewTieredCache
integration are required.

| Option | Default | Meaning in region mode |
|---|---|---|
| `capacity` | 512 MiB | Total logical region space, including obsolete records and unused tails; not reserved physical space |
| `regionSize` | 64 MiB | Maximum bytes per region; multiple of 64, at most `UINT32_MAX` |
| `flushBlockSize` | 1 MiB | Capacity of the single active append buffer; nonzero multiple of 64, at most region size |
| `maxEntrySize` | 16 MiB | Entire encoded record limit, including header, full key and alignment |
| `metadataBudget` | 0 | Zero selects `max(1 MiB, capacity / 200)`; otherwise explicit allocator-requested metadata byte limit |
| `transientBudget` | 64 MiB | Shared limit for concurrent serialization, encoding and lookup-copy allocations |

The 16-256 GiB deployment range is a target, not a measured capacity/performance
claim. The simple node map can reach its metadata limit before filling disk.
Metadata pressure drops admissions; it does not allocate beyond the limit.

`forceInsert=false` **does permit FIFO recycling** in region mode. Neither value
bypasses size or memory limits. This replaces the former file-per-entry admission behavior.
Empty saved values and binary keys are supported. The cache adds no compression:
`Insert` saves callback bytes as uncompressed/volatile; `InsertSaved` preserves
both supplied compression and source-tier tags for `create_cb`.

Both `wait` values perform synchronous lookup and return an immediately ready
handle on a hit. `WaitAll` is a no-op. `Deflate`/`Inflate` return `NotSupported`,
not a fake successful RAM-budget adjustment. Zero disk capacity disables the cache.

## Files and main methods

| File | Responsibility and main methods |
|---|---|
| `../RegionBasedSecondaryCache.cpp` | Public forwarding and region-cache operations. `Insert` checks size before serialization; `InsertSaved` accepts existing bytes; `InsertEncoded` validates/encodes/appends; `Lookup` verifies bytes and invokes `create_cb`; `ReportFailure` counts errors and logs the first and powers of two |
| `RegionManager.hpp/.cpp` | Mutable storage-state owner. `Append` is the publication transaction; `SelectRegionLocked` advances the FIFO ring; `ReclaimLocked` removes retired mappings; `FlushLocked` writes the active buffer; `Read` copies a record and identity; `Remove` performs conditional cleanup; `Erase` removes the current mapping; `SetCapacity` resets the layout; `Snapshot`, `CapacityBytes`, `UsageBytes` provide synchronized observations |
| `Index.hpp` | An externally locked, lazily constructed `unordered_map<uint64_t, Location>`. `Upsert` returns the **old** location; `Find`, `Remove`, `Clear` do not mutate any independent lifecycle. `Clear` destroys the map without allocating, including MSVC's empty-map sentinel/buckets |
| `RecordFormat.hpp/.cpp` | Checked `RecordSize`, `EncodeRecord`, portable CRC32C and `DecodeRecord`. Decode checks magic/version, exact bounds, header/key CRC, full key, then payload CRC before any callback receives bytes |
| `MemoryBudget.hpp` | Atomic allocation reservation and a stateful allocator. Accounts container allocator requests, retained capacity and overlapping allocations during rehash/growth. `BudgetExceeded` is an internal admission-pressure signal, not an unsafe internal `noexcept` termination |
| `BlockDevice.hpp/.cpp` | Serialized device interface, real local file implementation, and injectable memory device. `Configure` clears owned region files/layout; `Read`/`Write` check offset/length and transfer results; `Open` uses a lazy round-robin pool capped at 64 region handles. The memory device injects configure/read/write failures |
| `CacheStats.hpp` | Atomic operational counters shared with the wrapper; the public `RegionBasedSecondaryCacheStats` combines these with a locked layout/resource snapshot |

There is no footer format, disk scanner, ghost cache, per-region payload buffer,
reader pin, asynchronous worker, recovery path or custom hash table.

## Record and index layout

Each append is:

```text
32-byte packed header | full key | serialized payload | 0..63 zero pad bytes
```

The header stores format version, compression/source-tier tags, 64-bit key hash,
32-bit key/payload lengths, payload CRC32C and header-plus-key CRC32C. Encoded size
is `align_up(32 + key_length + payload_length, 64)` with checked arithmetic and
checked field widths. Records never cross a region boundary.

The in-memory index maps a 64-bit FNV-1a key hash to a 16-byte `Location`:

```text
uint32 region_id | uint32 offset/64 | uint32 length/64 | uint32 generation
```

A hash collision can cause a miss or replace another mapping, but **cannot return
the wrong key's payload**, because lookup compares the stored full key. Lookup
cleanup carries the location plus a 64-bit cache incarnation. Old callbacks cannot
erase a replacement or a record created after resize, even at identical offsets.
Generation/incarnation exhaustion produces an explicit error rather than wrapping.

Each region keeps a vector of published hashes. Repeated hashes are allowed.
Reclaim removes a hash only if the index still points to that region/generation,
then clears the vector. Thus newer copies in other regions survive, and corrupt
or unreadable old records do not prevent eviction. The vectors and index share
the metadata budget; history cannot grow without bound.

## Data flow

### Insert

1. Reject incompatible helpers or oversized/unrepresentable records before saving.
   Disabled/pressure/oversize admissions are best-effort drops with distinct counters.
2. Allocate payload/encoded bytes against the transient budget and call `saveto_cb`
   without holding the state mutex. `InsertSaved` skips the payload-copy stage.
3. Encode the full-key/checksummed record, then enter `RegionManager::Append`.
4. Under the state mutex, choose the current region if it fits. Otherwise flush its
   buffer, walk the FIFO ring, skip an undersized final region and reclaim the next
   suitable region. Candidate search is bounded by region count.
5. Allocate reverse-list and index publication state before I/O. A tentative map
   update is invisible to other threads while the mutex is held.
6. Append small records to the one reserved buffer. When necessary, flush that
   buffer first. Records larger than the buffer write directly from their owned
   encoded bytes; they do not enlarge every region's buffer.
7. Commit cursor, live-byte replacement accounting and admission counters together.
   Usage is the sum of indexed encoded lengths, not every obsolete byte on disk.

The lock covers device I/O, so a foreground spill can block. A future bounded
single-writer queue must preserve this reference transaction and failure behavior.

### Lookup

1. Under the state mutex, find the location, copy its encoded bytes from the active
   buffer or disk into transient-budgeted memory, and capture its identity.
2. Release the mutex. Validate bounds, checksums and full key before `create_cb`.
3. On corruption, conditionally remove only the captured mapping. A key mismatch
   is a miss without deleting a potentially valid collision mapping.
4. Invoke `create_cb` outside the mutex, allowing reentrancy. A failed callback owns
   its cleanup and its output is ignored. After successful creation, keep a cleanup
   guard active until result-handle construction and postprocessing succeed. A null
   deleter is allowed for objects requiring no destruction.
5. On `adviseErase`, conditionally remove the captured mapping. Otherwise report
   whether that same identity is retained at the final locked check, not a promise
   of retention after that check. The returned charge is callback
   `out_charge`, not payload length. Record RocksDB role-specific hit statistics.

The caller consumes/owns the created value, following RocksDB's result-handle
contract. Destroying a ready result handle does not itself destroy the value.

### Erase, FIFO reclaim and resize

`Erase` is index-only. It subtracts the current encoded length; dead disk bytes
remain until overwritten. FIFO reclaim uses the resident hashes and never reads
old disk contents or physically zeroes a region.

`SetCapacity` validates geometry before changing live state. A valid resize
discards all entries and buffered bytes, advances the incarnation, releases the
old layout, and creates a correctly sized empty layout. A post-invalidation failure
returns an error and leaves the cache disabled/empty; a subsequent resize can retry.
Even a same-capacity request is an explicit cold reset.

## Directory, I/O and failure contract

The supplied path is a **parent**, never a recursive deletion target. The engine
owns only its `ssd-secondary-cache-v1` child, containing an ownership marker/lock
named `owner` and `region_<number>.dat` files. Existing child contents must match
this format and marker. Directory/file links and Windows reparse points are
rejected. An exclusive OS lock prevents two cooperating cache instances from
sharing the child. The caller must prevent external changes to that child and
its path while in use. Unrelated files in the parent are untouched.

Cold initialization removes only validated owned region files. There is no warm
restart scan or persistence promise. Region files and handles are created lazily;
files grow on write, **without physical preallocation or disk-space reservation**.
Disk exhaustion is a cache I/O error, not a database correctness failure.

The C streams are unbuffered: the one explicit append buffer batches writes, and
the OS page cache supplies buffered I/O. There is no `fsync`/durability barrier.
Short transfer, seek, write or flush failure is not acknowledged as success.
A write/flush returning false invalidates all cache mappings and buffered state;
a read returning false removes that mapping. Exceptions during preliminary flushes
or reads do not take those invalidation paths. A future insert can retry after
the underlying error clears. Callbacks run only after a successful read,
full-key match and both CRC checks.

Allocating private functions may throw. RocksDB's public methods catch exceptions:
real allocation failures return `MemoryLimit` or lookup miss, while configured
budget pressure is an intentional drop/miss. Failures increment counters and are
logged at exponentially spaced occurrences; no per-hit/per-miss logging is used.

## Observability and remaining work

`GetStats` reports live entries/encoded bytes, capacity, tracked logical file
lengths, allocator-requested metadata, active buffer capacity, current transient
allocations, region handles, admissions/drop reasons, errors/corruption, reclaim,
and device traffic. Counters are lifetime totals; layout fields share a state-lock
snapshot, while concurrent callback counters/transient usage are observational.
Write amplification's denominator is **admitted payload bytes**; encoded bytes
include keys, headers and padding. Buffered admissions may be discarded before
reaching the device. The owner lock adds one OS handle beyond `openHandles`.

Allocator counters exclude allocator bookkeeping, fixed object/mutex sizes,
C-runtime handle internals and OS page-cache residency. `fileBytes` is **not**
physical allocation, RSS, durability, or a reservation guarantee. Collect those
separately in benchmarks.

Before production enablement: complete cross-platform/sanitizer/fault-injection
coverage; measure steady-state FIFO turnover, hit ratio, p99 and heap/RSS/page-cache
usage; add the bounded writer queue required by the accepted foreground-spill goal;
measure/replace the node map if needed to achieve the 0.5% metadata target. Keep
second-chance admission, asynchronous reads, native positioned I/O and compression
out of this baseline.

`tests/AVEVA/RocksDB/Plugin/Core/SecondaryCache*Tests.cpp` covers the record format,
index, manager, device faults, region contract and RocksDB integration. The separate
`SecondaryCacheAllocationFailureTests` CTest executable injects allocation failure
after successful object creation without replacing allocators in the GTest process.
Building `aveva-rocksdb-plugin-core-tests` also builds this executable.
