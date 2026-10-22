<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# Inside the SSD secondary cache

This is a code-review walkthrough of the **synchronous, region-only baseline**
introduced in commit `2de1967`. It explains what the code does today, not what the
performance plan hopes it will do later. The [README](README.md) is the shorter
reference; the [implementation plan](../../../docs/design/secondary-cache-v1-review-todo.md)
tracks validation and future changes.

## 1. The short version

Think of the cache as a circular notebook. We append records to one page
(a region), move to the next page when a record does not fit, and eventually
reuse the oldest page. A RAM index tells us which page and offset hold a key.
When we reuse a page, we remove its live index entries. No treasure hunt through
old disk bytes is required.

The objects are **serialized RocksDB block-cache entries**, not complete SST
files and not individual application key/value pairs. A payload might represent
a data block, index block or filter block. RocksDB supplies the key and callbacks
that convert between its in-memory object and saved bytes.

The cache is attached through `rocksdb::LRUCacheOptions::secondary_cache`. It is
not a `NewTieredCache` implementation. RocksDB's primary cache controls when to
spill and look up objects; this cache controls local placement and reclamation.
A miss means RocksDB must obtain the block elsewhere. The SSD copy is expendable.

```text
RocksDB primary LRU cache
       |
       | Insert(object + callbacks) / InsertSaved(bytes) / Lookup(key)
       v
RegionBasedSecondaryCache::Impl   serialization, validation, callback ownership
       |
       v
RegionManager                    index + placement + FIFO + accounting
       |                              |
       v                              v
Index: hash -> Location           one active append buffer
                                      |
                                      v
                                 FileBlockDevice
                                      |
                                      v
                                 OS page cache -> local storage
```

Everything below `RegionManager` runs under its one mutex. Serialization,
record decoding and RocksDB callbacks run outside it. Simple to reason about,
but not a promise of low tail latency: disk I/O holds that mutex too.

## 2. Who does what?

Paths below are relative to this directory unless marked otherwise.

| Class or component | Responsibility | Important functions |
|---|---|---|
| `RegionBasedSecondaryCache`, in the public header and `../RegionBasedSecondaryCache.cpp` | Implements RocksDB's `SecondaryCache` interface; owns an opaque `Impl` | Public methods forward to `Impl`; `SupportForceErase` returns true |
| `RegionBasedSecondaryCache::Impl` | Translates RocksDB operations into saved records, owns temporary-memory budgeting and error reporting | `Insert`, `InsertSaved`, `InsertEncoded`, `Lookup`, `Erase`, `CanEncode`, `ExceptionStatus`, `ReportFailure` |
| `RegionManager` | Owns the cache's mutable storage lifecycle and the only state lock | `Append`, `Read`, `SelectRegionLocked`, `ReclaimLocked`, `FlushLocked`, `InvalidateLocked`, `SetCapacity` |
| `Index` | Maps a key fingerprint to the currently visible location; does not lock itself | `Find`, `Upsert`, `Remove`, `Clear`, `Size` |
| Record-format functions | Define record size, encoding and integrity validation; know nothing about eviction | `RecordSize`, `EncodeRecord`, `TryReadHeader`, `DecodeRecord`, `ComputeCrc32c` |
| `MemoryBudget` / `BudgetAllocator<T>` | Reserve and release allocation bytes, including container growth | `Acquire`, `Release`, `allocate`, `deallocate` |
| `BlockDevice` | Small storage interface, called serially by the manager | `Configure`, `Read`, `Write`, `FileBytes`, `OpenHandles` |
| `FileBlockDevice` | Owns the child directory/owner lock, region files, logical lengths and bounded handle pool | `Open`, `InBounds`, `CloseHandles`, `RemoveOwnedFiles`, plus the device interface |
| `MemoryBlockDevice` | Test substitute with mutable region bytes and injectable failures | `SetFaultHook`, `MutableRegion`, plus the device interface |
| `CacheStats` / public `RegionBasedSecondaryCacheStats` | Lifetime counters and resource observations | `RegionManager::Snapshot`, `Impl::GetStats` |
| `ResultHandle`, in `../ResultHandle.*` | Returns an already-created object and its RocksDB charge | `IsReady` is true; `Wait` is a no-op; `Value` and `Size` expose the result |

`HashKey` is a local 64-bit FNV-1a fingerprint function. The index then uses
`std::unordered_map<uint64_t, Location>` with `std::hash<uint64_t>` for bucket
placement. These are separate hashing jobs. We store only the fingerprint in
RAM, but keep the **full key in each record** to reject collisions on lookup.
The choice of FNV-1a is not a measured performance win.

## 3. What state lives where?

### The wrapper

`Impl` keeps the options, shared logger, atomic operational counters, the
transient-memory budget and a `RegionManager`. It does not maintain a second
index or an independent eviction policy.

### The manager

| Member | Meaning |
|---|---|
| `m_mutex` | Serializes index, region state, buffer, device operations and live-byte accounting |
| `m_options` | Geometry and configured limits; current capacity is tracked separately |
| `m_metadata` | Budget shared by the index, region layout, reverse lists and device length vector |
| `m_index` | Current fingerprint-to-location mapping |
| `m_regions` | Vector of region descriptors, in FIFO ring order |
| `m_device` | File device in normal use; injectable device in manager tests |
| `m_buffer` | One reserved byte vector holding the not-yet-written suffix of the active region |
| `m_current` | Active region ID; `UINT32_MAX` means none |
| `m_incarnation` | Cache-wide identity epoch, advanced when mappings are invalidated |
| `m_capacity` | Configured logical region-space budget; zero disables admission |
| `m_usage` | Sum of the encoded lengths of currently indexed records |

Each `Region` contains:

```text
generation : uint32       starts at 1, advances whenever the region is selected/reclaimed
cursor     : size_t       next append offset, including bytes still in the buffer
hashes     : vector<u64>  fingerprints published during this region's current use
```

The reverse list is a publication history, **not** a list of guaranteed-live
entries. Overwrites and erases can leave stale hashes in it. That is intentional:
reclaim checks the index before deleting anything. `clear()` retains vector
capacity for reuse; the metadata budget counts that retained allocation.

### An index entry and a lookup identity

```text
fingerprint (8 bytes) -> Location (16 bytes):
    regionId           uint32
    offsetInAlignUnits uint32    multiply by 64 for the byte offset
    lengthInAlignUnits uint32    multiply by 64 for the encoded length
    generation         uint32

Identity = Location + cache-wide uint64 incarnation
```

The hash-table allocation costs more than these field sizes: nodes, buckets and
allocator overhead are separate concerns. This baseline uses a standard node
map, not a tightly packed custom table.

`Identity` lets a lookup say "remove exactly the record I read." Location protects
against ordinary replacement and region reuse. Incarnation also protects against
resetting the whole cache and later reusing the same region/generation/offset.
Counter exhaustion is rejected rather than silently wrapping.

### Disk and buffer representation

```text
record = 32-byte header | full key | saved payload | 0..63 zero padding bytes
length = align_up(32 + key length + payload length, 64)
```

The packed header contains magic, format version, compression, source tier,
flags, key hash, key length, payload length, payload CRC32C and header-plus-key
CRC32C. Padding makes placement and location encoding simple; it is not useful
payload. The header is copied as a packed native structure, not a specified
cross-endian interchange format. Cold-start-only operation needs no portable
recovery format.

Records have variable length, but none crosses a region boundary. There are no
fixed-size per-entry slots, continuation records, footers or disk scanners.
The default maximum **encoded** entry is 16 MiB, within a default 64 MiB region.
The default buffer is 1 MiB.

The device owns only this child beneath the supplied parent:

```text
<parent>
    ssd-secondary-cache-v1
        owner
        region_0.dat
        region_1.dat
        ...
```

`owner` holds a marker and an exclusive OS lock. Initialization validates owned
contents and rejects links/reparse points, then discards old region files.
Files are created lazily and grow on write; there is no physical preallocation,
warm restart or durability barrier. The handle pool holds at most 64 region
streams, replacing handles round-robin, plus a separate owner-lock handle.
The C streams are unbuffered; the OS page cache is not.

## 4. Adding a new entry

`Insert` handles an object; `InsertSaved` handles bytes already serialized.

1. `Insert` counts the attempt and checks the helper is secondary-cache-compatible.
   `size_cb` supplies the saved payload length.
2. `CanEncode` checks encoded-size arithmetic, maximum entry size and current
   capacity before allocating payload storage or calling `saveto_cb`.
3. `Insert` allocates a transient-budgeted payload vector and calls `saveto_cb`.
   `InsertSaved` skips this stage and preserves the supplied compression/tier.
   Neither path adds compression.
4. `InsertEncoded` fingerprints the key and builds a transient-budgeted record
   with the full key and checksums.
5. `RegionManager::Append` takes the state mutex and rechecks admission limits.
   The earlier check is not sufficient: another thread may have resized the cache.
6. `SelectRegionLocked` chooses the active region if the record fits; otherwise
   it flushes the old buffer and advances around the ring.
7. Before writing the new record, `Append` pushes its hash into the reverse list
   and tentatively updates the index. If index allocation fails, it pops the
   reverse-list entry. Other threads cannot see tentative state under the mutex.
8. The record is buffered or written directly. On success, cursor, live usage
   and admitted-byte counters are updated before releasing the lock.

`Index::Upsert` returns the previous location. Replacing a key adds the new
encoded length and subtracts the old one from `m_usage`. The old disk record
remains physically present but is no longer reachable.

The transaction protects **publication**, not every prior cache entry from
eviction: selecting a region may already reclaim it before a later allocation
fails. A failed admission need not restore evicted data. This is a disposable
cache, not a transactional database wearing a smaller hat.

Both `forceInsert` values allow FIFO reuse. Neither overrides size or memory
limits. An OK status may mean a best-effort admission was dropped; it is not
proof that the entry is retained.

### Worked example: small entries, then a large one

Use deliberately tiny geometry so we can see the arithmetic:

```text
capacity = 3,072 bytes   regionSize = 1,024 bytes
flushBlockSize = 256     maxEntrySize = 768
all keys are 16 bytes   header + key = 48 bytes
```

| Entry | Payload | Encoded size | Placement and result |
|---|---:|---:|---|
| A | 80 | 128 | Select region 0; buffer `[0,128)`; cursor 128 |
| B | 80 | 128 | Buffer now `[0,256)`; cursor 256; still no device write |
| C | 144 | 192 | C cannot join that buffer; flush A+B, then buffer C at `[256,448)`; cursor 448 |
| D | 528 | 576 | Flush C; D is larger than the buffer, so write directly at `[448,1024)`; cursor 1,024 |
| E | 208 | 256 | Region 0 is full; select region 1 and buffer E at `[0,256)` |

Important details:

- Filling the buffer exactly does **not** automatically flush it. Another append
  that needs space, a direct write with pending buffered bytes, or a region
  transition triggers the flush. There is no timer or background flusher.
- A record is "large" here when its **encoded size exceeds the buffer capacity**.
  It is not a separate record format. It still must fit one region and the
  maximum-entry limit. Exactly 256 bytes uses the buffer in this example.
- Direct write avoids copying D into an enlarged append buffer. It does not
  bypass the OS page cache, avoid the encoded-record allocation or make I/O async.
- A large `Insert` temporarily holds both payload and encoded-record vectors.
  `InsertSaved` avoids that extra payload vector. Concurrent operations share
  the transient budget, so an individually legal entry can still be dropped.
- A 769-byte payload with a 16-byte key needs 832 encoded bytes and is rejected
  by this example's 768-byte limit before `saveto_cb`. We do not split it.

## 5. Moving between regions

Placement is by append order, not by hashing the key to a region.
`SelectRegionLocked(length)` follows this rule:

```text
if the active region has enough remaining bytes:
    keep appending there
else:
    flush the active buffer
    start at the next region, wrapping at the end
    inspect at most region_count candidates
    select/reclaim the first region whose total budget can hold this record
```

If a 192-byte record arrives with only 128 bytes left, those 128 bytes are
unused for this pass. There is no backfill into old gaps.

Capacity need not be a multiple of region size. With 2,560 bytes of capacity and
1,024-byte regions, the budgets are `[1024, 1024, 512]`. A 768-byte record cannot
use the last region; the search skips it and may wrap to reclaim region 0.
A later smaller record can use region 2 when traversal reaches it.

That means FIFO is **region-granular ring reuse**, allowing skipped undersized
regions. It is not a strict global per-entry insertion-order queue, and reads do
not move entries to the back.

## 6. FIFO reclamation: delete the addresses, reuse the space

Suppose we have reached region 2 and the next append must wrap to region 0:

```text
region 0: A(old), B     reverse list [hash(A), hash(B)]
region 1: A(new), C     index points A here, not to region 0
region 2: D, E          currently active
```

`ReclaimLocked(0)` walks region 0's reverse list:

1. Find `hash(A)` in the index. Its location names region 1, so leave it alone.
2. Find `hash(B)`. Its location names region 0 and that region's current
   generation, so remove it and subtract its encoded length from live usage.
3. Clear region 0's hash list, reset its cursor to zero and advance generation.
4. Append the new record at offset zero and publish its new location.

No bytes are copied out to rescue hot records. No old record is decoded.
No file needs to be read, zeroed or truncated for ordinary reclaim. Even corrupt
old storage cannot prevent index eviction. A previously longer region file may
retain unreachable tail bytes until later writes or a reset.

Duplicate hashes in the reverse list are harmless. After the current mapping
is removed once, a later duplicate finds nothing to remove. Conversely, a
replacement in another region survives because the region/generation check fails.

`Erase(key)` is the smaller operation: remove the current hash mapping and adjust
usage, but leave the cursor, reverse history and disk bytes alone. Hash collisions
can therefore evict another key's mapping, just as replacement can. They cost
cache effectiveness, not a wrong-value hit.

## 7. Looking up and reconstructing an object

```text
Lookup(key)
  -> hash key
  -> RegionManager::Read(hash): capture identity + copy record under mutex
  -> unlock
  -> DecodeRecord(record, full expected key)
  -> helper.create_cb(saved payload, compression, source tier, ...)
  -> ready ResultHandle(object, callback-supplied charge)
```

The manager reads from the active buffer when the location lies in its suffix;
otherwise it reads the region file. For example, after inserting C above, C is
read from RAM while A is read through the device. A flush is not required for a
buffered entry to become visible.

The lookup owns a transient copy, not a pointer into mutable region storage.
That is why decoding and callbacks can safely run without reader pins while
another thread recycles the region.

`DecodeRecord` checks magic/version, exact record bounds, header-plus-key CRC,
the full expected key, and payload CRC, in that order. `TryReadHeader` alone
only copies the header; it does not validate it. A collision causes a key
mismatch and a miss; that path preserves the other key's index mapping.
Corruption conditionally removes the captured mapping. Invalid bytes never
reach `create_cb`.

Callbacks can reenter the cache. If a callback replaces the same key or resets
capacity, the old lookup's `Remove(hash, identity)` cannot erase that new entry.
`Contains(hash, identity)` similarly reports whether the exact record read is
still retained, not merely whether some record now has that hash.

After successful object creation, a cleanup guard owns destruction until handle
construction and postprocessing succeed. If handle allocation fails, the guard
calls `del_cb` exactly once when a deleter exists. On callback failure, callback
cleanup is the callback's responsibility; the wrapper ignores its failure output.

With `adviseErase=true`, lookup removes only the captured identity; otherwise
it checks whether that identity remains. Hits also update RocksDB's total and
role-specific secondary-hit counters through `RecordHitStats`.

`ResultHandle::Size()` returns callback `out_charge`, not saved-payload length.
The successfully created value transfers to the caller under RocksDB's contract:
destroying the handle does **not** delete it. Both values of `wait` are synchronous;
`WaitAll` has nothing left to wait for.

## 8. Capacity, memory and failure rules

### There is more than one kind of "bytes used"

| Quantity | What it means |
|---|---|
| Live encoded bytes (`GetUsage` / `liveBytes`) | Sum of current index locations, including entries still buffered |
| Region cursors | Space consumed by all appends in this pass, including overwritten/erased records |
| `fileBytes` | Sum of device-tracked logical file lengths; can include dead bytes and excludes buffered-only data |
| Metadata bytes | Allocator requests for the index/layout/reverse lists/device lengths, including retained capacity and rehash overlap |
| Active buffer bytes | Reserved capacity of the single append buffer, separate from metadata and transient budgets |
| Transient bytes | Current allocations for serialization, encoding and lookup copies |

None of these is process RSS or physical disk allocation. The OS page cache,
allocator bookkeeping, fixed objects and C runtime internals are not all captured
by the allocation budgets. A metadata limit is not a total-process-memory limit.

`MemoryBudget::Acquire` uses an atomic compare/exchange loop to reserve bytes.
`BudgetAllocator::allocate` rolls that reservation back if the underlying
allocator fails; `deallocate` returns it. A zero configured metadata budget
selects `max(1 MiB, capacity / 200)`. The simple index can exhaust that budget
before region space fills.

Configured budget pressure produces a counted best-effort drop or lookup miss.
A real `std::bad_alloc` becomes `MemoryLimit` for status-returning operations.
`ExceptionStatus` translates other exception categories; `ReportFailure` counts
failures and attempts logging at counts 1, 2, 4, 8, ... . Ordinary pressure drops
increment their dedicated counter without going through that logging path.

### Failure scope matters

| Event | Behavior |
|---|---|
| Missing key / incompatible helper | Miss or no admission |
| Disabled cache / oversized record / configured budget pressure | Counted best-effort drop or miss |
| Record read returns failure | Remove that mapping; return miss |
| Record fails integrity validation | Conditionally remove the captured mapping; return miss |
| Full key differs | Return miss without removing the collision mapping |
| Buffered flush or direct write returns failure | Invalidate all mappings and buffered state; return an I/O error |
| Exception during the record write/buffer-copy stage | Invalidate cache mappings, then translate the exception |
| Successful reset followed by reconfiguration failure | Remain empty/disabled; return error; a later resize can retry |

These are the explicit manager paths, not a claim that every conceivable native
filesystem fault has been tested. A successful file write reaches the OS cache,
not durable media. Losing buffered entries on shutdown or failure is acceptable.
There is no destructor-time persistence promise.

`InvalidateLocked` advances incarnation and clears mappings, cursors, histories,
buffer contents and usage. For an ordinary write failure it retains the layout
and configured capacity, so a later insert can retry. `SetCapacity` does more:
validate first, invalidate, disable, release layout/buffer, clear owned files and
configure a new empty layout. Even setting the same capacity is a cold reset.
Invalid geometry is rejected before invalidation.

`GetStats` combines a state-locked layout snapshot with atomic counters and a
separately sampled transient usage. It is useful telemetry, not a globally
atomic accounting transaction. `Deflate` and `Inflate` return `NotSupported`;
they do not pretend to rebalance a RAM tier.

## 9. A practical code-review route

Start with `RecordFormat` and `Index`: both are small. Then follow one
`InsertSaved -> InsertEncoded -> Append` and one `Lookup -> Read -> DecodeRecord`
path. Read region selection/reclaim before diving into filesystem ownership.

The most useful invariants to keep in mind:

- One visible location per hash, and live usage equals the sum of those lengths.
- Records fit entirely inside one region.
- Buffered bytes are a contiguous suffix ending at the active cursor.
- Publication, reclaim and accounting share one lock.
- Reverse history is evidence to check against the index, not authority to delete.
- Cleanup after callbacks uses exact identity, never just a key hash.
- Full-key and integrity checks precede object reconstruction.

Concrete examples in tests live under
[`tests/AVEVA/RocksDB/Plugin/Core`](../../../tests/AVEVA/RocksDB/Plugin/Core):
`SecondaryCacheRegionManagerTests.cpp` covers reclaim, stale identities, failure
invalidation and concurrency; `SecondaryCacheBaselineTests.cpp` covers admission,
short final regions, callback ownership and actual RocksDB integration;
`SecondaryCacheRecordFormatTests.cpp` covers record validation; the separate
`SecondaryCacheAllocationFailureTests.cpp` executable exercises system-OOM paths.

The deliberate omissions are just as important: no background writer, no async
reads, no per-entry LRU, no compaction, no added compression and no recovery.
Nonblocking foreground spill remains future work. This version favors a lifecycle
humans can review before making it faster.
