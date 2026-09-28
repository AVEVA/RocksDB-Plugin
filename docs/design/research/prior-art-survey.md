<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# Local-SSD Block Cache Design: Technical Survey for a RocksDB `SecondaryCache`

> **Provenance.** Verbatim output of a background research agent, 2026-09-28. Committed unedited as
> the evidence base for [`../secondary-cache-performance.md`](../secondary-cache-performance.md).
> Read the "Notable Gaps" section at the end before quoting any figure.

All citations verified against source (repo:file:line) or paper text as noted. Items explicitly flagged "unverified/approximate" in the sub-agent reports are marked accordingly below — treat everything else as directly confirmed against source/paper text.

---

## 1. Meta CacheLib Navy (`facebook/CacheLib`, commit `0bf766c2321a2cf2ecc138aefb03db17e2f5028b`)

### 1.1 BlockCache — region-based large/medium-object engine

- **Region layout.** Device space is divided into fixed-size `Region`s (`cachelib/navy/block_cache/Types.h:29-141`, `Region.h:59-238`). Writes are strictly **append-only bump-allocation** within a region (`openAndAllocate`, `Region.h:98-101,216-221`); an attached in-memory `Buffer` batches writes before flush (`Region.h:143-190`).
- **RegionManager** (`RegionManager.h:66-334`) explicitly documents itself as a "Size class or stack allocator" (`RegionManager.h:65`) and owns a pool of pre-reclaimed "clean" regions. Reclaim is **whole-region**, distinct from item-level eviction — the doc comment (`RegionManager.h:299-303`) states this distinction exists specifically to amortize flash erase/GC cost and avoid per-item write amplification. `onRegionReclaim` (`BlockCache.cpp:673-736`) walks every entry tail-to-head, applies the reinsertion policy, then erases the whole region as one unit.
- **Eviction policies** (`EvictionPolicy.h` interface): `FifoPolicy` (plain `std::deque` FIFO over regions, `FifoPolicy.h:44-78`), `SegmentedFifoPolicy` (N ratio-partitioned FIFO segments for pseudo-LRU tiering, `FifoPolicy.h:80-160`), `LruPolicy` (doubly-linked list over regions with hit counters, `LruPolicy.h:39-104`). Eviction operates on **regions**, not individual items.
- **In-memory index** (`Index.h`): key hash → `{region/offset address, size hint, hit count}`, **not the raw key**. Two struct layouts:
  ```cpp
  // Index.h:49-67 — 8 bytes
  struct ItemRecord { uint32_t address; uint16_t sizeHint; uint8_t totalHits/itemHistory; uint8_t currentHits; };
  // Index.h:184-193 — 5 bytes (packed, exponential size encoding)
  struct PackedItemRecord { uint32_t address; struct{uint8_t curHits:2; uint8_t sizeExp:6;} info; };
  ```
  **BlockCache's DRAM index entry is 5–8 bytes**; full key is validated against a copy in the on-flash header on lookup (matches OSDI's LOC description).
- **Reinsertion policies** used during reclaim to keep hot items alive past their region's eviction: `PercentageReinsertionPolicy` (flat probability, `PercentageReinsertionPolicy.h:34-46`) and `HitsReinsertionPolicy` (reinsert if index hit-count exceeds threshold, approximating LRU inside a FIFO-region scheme, `HitsReinsertionPolicy.h:38-58`).
- **Stack-alloc vs size-classed regions**: default is sequential append ("stack" allocation); `CombinedEntryBlock`/`CombinedEntryManager` (referenced in `NavySetup.cpp:170-224`, not read in depth) pack multiple small entries into one block for size-class-like behavior.

### 1.2 BigHash — bucket-based small-object engine

- Explicitly documented as having **no in-memory per-item index**, only per-bucket metadata (`bighash/BigHash.h:47-62`). `bucketSize` default **4KB** (`BigHash.h:66`); `numBuckets = cacheSize/bucketSize`; key → bucket via `hash % numBuckets` (`BigHash.h:78,207-209`).
- Each `Bucket` is a FIFO bump-allocator (`BucketStorage.h:29-114`) of variable-size entries (16-byte header: keySize/valueSize/keyHash) plus a per-bucket `checksum_` and `generationTime_` (`Bucket.h:41-113,105-107,154-155,177`).
- **Bloom filter per bucket**: sized at **16 bytes per ~25 entries, 4 hash functions** (`NavySetup.cpp:127-134`), giving <10% false-positive rate and matching OSDI's claim of preventing >90% of unnecessary flash reads.
- **Size-based routing** between BlockCache and BigHash: `isItemLarge(keySize, valueSize, smallItemMaxSize)` (`navy/common/Types.h:114-121`), threshold set via `BigHashConfig::getSmallItemMaxSize()`. Device layout places BlockCache regions first, BigHash at the tail (`NavySetup.cpp:216-217`).

### 1.3 Device abstraction (`cachelib/navy/common/`)

- Base `Device` enforces IO alignment (`ioAlignmentSize_`, constructor asserts `maxWriteSize_ % ioAlignmentSize_ == 0`, `Device.h:79-232,110-121`).
- **O_DIRECT with fallback**: opens `O_RDWR|O_CREAT|O_DIRECT`, retries without `O_DIRECT` only on `EINVAL` (e.g., tmpfs) (`Device.cpp:1163-1173`).
- **RAID0 striping** across multiple files/block devices, explicit stripe-splitting arithmetic in `Device.cpp:657-671`; factory takes `stripeSize` explicitly (`Device.h:257-296`).
- **Both io_uring and libaio** supported, selectable via `IoEngine` enum and `useIoUring_` flag using `folly::IoUringOp` (`Device.cpp` ~824-958; `qDepth==0` forces sync IO).
- **Checksums**: CRC32C-family (`navy/common/Hash.h:29`); BlockCache checks both a header self-checksum and a value checksum per entry on read/reclaim (`BlockCache.cpp:535,560,682-694,1025-1033`); BigHash `Bucket` has its own `checksum_` (`Bucket.h:105-107`).

### 1.4 JobScheduler — ordering guarantees

- `ThreadPoolJobScheduler` shards jobs across reader/writer thread pools by key hash (`ThreadPoolJobQueue.h:31-95`, `ThreadPoolJobScheduler.h:75-104`). `OrderedThreadPoolJobScheduler` additionally spools same-key jobs so a concurrent insert/remove pair for one key cannot race and land out of order in the on-flash region/index (`ThreadPoolJobScheduler.h:117-201,154-198`) — necessary because BlockCache/BigHash apply writes asynchronously.

### 1.5 Admission policies

- `RejectRandomAP`: flat-probability reject.
- `DynamicRandomAP` (`admission_policy/DynamicRandomAP.h/.cpp`): a **multiplicative feedback controller**.
  ```cpp
  // base curve penalizes larger items: prob ≈ (baseProbabilityMultiplier/log2(size))^probabilityExponent
  // per-window (60s default) update:
  probabilityFactor = clamp(probabilityFactor * clamp(target/observed, minChange_, maxChange_),
                             lowerBound_=0.001, upperBound_=10.0)
  accept: genF(key) < clamp(baseProb * probabilityFactor, 0, 1)
  ```
  (`DynamicRandomAP.cpp:159-224,243-248,99-129`). Target write rate is tracked per-day (`kSecondsInDay=86400`), damped ±25%/interval by default. Uses a **deterministic hash of the key** (not RNG) so repeated admission decisions for the same key are consistent.

### 1.6 NvmCache integration (DRAM↔Navy)

- On DRAM eviction: `NvmCache::put` builds an `NvmItem` (packed blob(s)+metadata via `FOLLY_PACK_ATTR`, `NvmItem.h:59-193`), calls `navyCache_->insertAsync(...)` async, with tombstone/in-flight-put tracking to abort races with concurrent gets (`NvmCache.h:1220-1318`).
- On Navy read hit: `onGetComplete` reconstructs the item and **promotes it back into DRAM** (`insertFromNvm`, `markWentToNvm()`, `NvmCache.h:1341-1432`).

### 1.7 Crash recovery & durability

- **Both engines persist the index rather than rebuild by scan.** `BlockCache::persist/recover` (`BlockCache.cpp:1574-1613,1616-1623`) validates `cacheBaseOffset/cacheSize/allocAlignSize/checksum-config/version` before trusting recovered state; **any mismatch or exception → full reset (wipe), not a scan-rebuild**. `RegionManager::persist/recover` serializes per-region metadata + optionally eviction-policy ordering (`RegionManager.h:236-241,96-97,297,305-311`). `BigHash::persist/recover` includes a `ValidBucketChecker` bitmap using per-bucket `generationTime` to lazily invalidate stale buckets after a bad recovery (`BigHash.h:155-160,262-330`).
- **Torn-write handling**: header self-checksum failure during reclaim **aborts the remaining items in that region** rather than trusting corrupted data; value-only checksum failures are non-fatal (entry iteration continues, value just unusable) (`BlockCache.cpp:673-736,654-661,657-660`).

### 1.8 OSDI'20 paper numbers ("The CacheLib Caching Engine", usenix.org/system/files/osdi20-berg.pdf)

| Metric | Value | Location |
|---|---|---|
| DRAM per-item metadata overhead (whole DRAM cache) | **31 bytes/item** | Table 1, p.775 |
| DRAM cache total overhead (fragmentation+metadata) | **2.6%–7%** (workload-dependent) | Appendix B, p.780 |
| LOC (BlockCache) DRAM index overhead | **0.01%–0.61%** of cache size (4B key hash + 4B flash offset + ~2.5B/item B-tree pointer) | p.775, p.780 |
| SOC (BigHash) app-level write amplification | **≈6.5×** inserted bytes (always writes full 4KB) | p.780 |
| SOC device-level write amplification | **1.1×–1.4×**, with **50% flash overprovisioning** | p.780 |
| LOC device-level WA, LRU→FIFO sequential writes | **1.5× → 1.05×** (15% fewer NAND writes/sec) | p.780 |
| Advanced (Flashield-inspired) admission policy | **44% reduction** in flash write rate, no hit-ratio loss | p.774, Appendix C p.781 |
| Excess write rate w/o admission control | **50% above** sustainable device lifespan rate | Appendix C, p.781 |
| Hit ratio, CacheLib vs RocksDB-as-cache (SocialGraph) | **76% vs 53%**, RocksDB needs 50% more CPU | body text |
| AMAT lookup penalties | DRAM ≈500ns; LOC ≈1µs; SOC ≈2.6µs (10% Bloom FP → 16µs flash read) | Appendix A |
| Lookaside P99 flash read latency, after readmission+write-buffer changes | **−25%** | §5.3 |

No p999 numbers found in the paper; AMAT model explicitly justifies lookup order DRAM→LOC→SOC ("reversing would add several microseconds").

---

## 2. Kangaroo, Flashield, and the CacheLib admission-control appendix

### 2.1 Kangaroo (SOSP'21 Best Paper, pdl.cmu.edu/PDL-FTP/NVM/McAllister-SOSP21.pdf)

- **Hybrid KLog (log-structured, default 5% of flash) + KSet (set-associative, bulk capacity)**: pure log-structured caches need one DRAM index entry/object (too much DRAM at scale); pure set-associative caches rewrite an entire 4KB set per small-object admission (**40× alwa** for a 100B object) (p.243,245-246).
- **Flush mechanism**: objects only flush KLog→KSet once **≥n=2 objects** collide on the same KSet set, amortizing the set-rewrite over multiple objects (p.244,247).
- **Theorem 1 (Appendix A)** — worked example (2TB drive, 5% KLog, n=2): **alwa_Kangaroo ≈ 5.8×** vs **alwa_pure-set-associative ≈ 17.9×** (3.08× improvement) (p.247).
- **DRAM index**: naive log index 190 bits/object → KLog's partitioned index (2²⁰ tables, 16-bit offsets) = **48 bits/object** (3.96× reduction, p.246). Total system DRAM ≈ **7.0 bits/object**, cited as **4.3× less than Flashield** (p.246-247).
- **Direct critique of Flashield**: "Flashield needs 20 bits/object indexing + ~10 bits/object Bloom filters... would need 75GB of DRAM to track 2TB of 100B objects" (p.246).
- **Comparison to CacheLib's BigHash/SOC**: "requires no index and only ≈3 bits of DRAM per object for per-set Bloom filters" but suffers 40× alwa and runs with 2× physical overprovisioning in production (p.246).
- **Hit ratio**: simulation on 7-day FB trace — Kangaroo **reduces misses 29% vs SA** (CacheLib's set-associative SOC), 56% vs pure log-structured; miss ratio **0.29→0.20** (abstract, p.244). Production deployment: with FB's ML admission policy, **42.5% fewer flash writes at similar miss ratio** (p.253, Fig.13).
- Test drive: WD SN840 1.92TB, **3 DWPD**, 62.5 MB/s sustained write budget (p.250); device-level WA at 100% utilization ≈10× vs ≈1× at 50% (Fig.2, p.245).

### 2.2 Flashield (**NSDI'19**, not ATC — correcting task's venue assumption; usenix.org/system/files/nsdi19-eisenman.pdf)

- **ML-based admission**: DRAM acts as a "proving ground" — objects accumulate access-timing features after their first read; a periodically-run classifier (SVM) predicts future read count and only admits predicted-worthy objects, batched into large sequential writes (segment size e.g. 512MB) (p.1-4, §3). This is the first system framed as using DRAM explicitly as an admission filter and ML for admission decisions.
- **DRAM index cost**: **<4 bytes/object** (abstract, p.1,13).
- **Write amplification (CLWA)**: median **0.5–0.54×** for Flashield vs **2.85× (RIPQ)** and **3.67× (victim cache)** on Memcachier production traces (Table 5, p.9-10).

### 2.3 Cross-paper WA/endurance table

| System | Metric | Value |
|---|---|---|
| Kangaroo | alwa, naive in-place 100B/4KB | 40× |
| Kangaroo | alwa, Kangaroo config | 5.8× |
| Kangaroo | alwa, pure set-assoc | 17.9× |
| Flashield | CLWA median | 0.5–0.54× |
| CacheLib | SOC alwa | ~6.5× |
| CacheLib | LOC dlwa before/after FIFO | 1.5×→1.05× |
| CacheLib | overprovisioning needed | 50% |

No standalone "CacheSack" paper was found — its numbers correspond to the CacheLib OSDI'20 admission-control appendix (§1.8/2.1 above).

---

## 3. RocksDB's own `SecondaryCache` ecosystem (`facebook/rocksdb`, HEAD `4052fccde9d9533fa9c57749b63ba3cda2c9b134`)

### 3.1 The interface (`include/rocksdb/secondary_cache.h`)

```cpp
class SecondaryCacheResultHandle {
  virtual bool IsReady() = 0; virtual void Wait() = 0;
  virtual Cache::ObjectPtr Value() = 0; virtual size_t Size() = 0;
};
class SecondaryCache : public Customizable {
  virtual Status Insert(const Slice& key, Cache::ObjectPtr obj,
                         const Cache::CacheItemHelper*, bool force_insert) = 0;
  virtual Status InsertSaved(const Slice& key, const Slice& saved,
                              CompressionType, CacheTier source) = 0;
  virtual std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper*, Cache::CreateContext*,
      bool wait, bool advise_erase, Statistics*, bool& kept_in_sec_cache) = 0;
  virtual void Erase(const Slice& key) = 0;
  virtual void WaitAll(std::vector<SecondaryCacheResultHandle*>) = 0;
  virtual Status SetCapacity/GetCapacity/Deflate/Inflate(...);
};
```
**Documented contract**: exceptions must never propagate (RocksDB is not exception-safe); `Insert()` "may or may not" actually insert even returning OK (admission control is explicitly allowed to silently drop); `advise_erase` lets the primary cache tell the secondary "I'll own this, you can drop it."

### 3.2 `CompressedSecondaryCache` (`cache/compressed_secondary_cache.{h,cc}`)

- **Second-chance/one-hit-wonder filter**: on a hit, if a 0-size "dummy" placeholder already exists in the primary cache for that key, promote to primary and erase from the compressed cache; otherwise install a dummy in primary and keep the block compressed, returning a standalone handle. Symmetric logic on primary eviction: dummy exists → store real compressed bytes; else insert a 0-size dummy marking "seen once" (`compressed_secondary_cache.h:63-79`).
- Value chunks are sized to fit **jemalloc bins** (128B–16KB) to reduce fragmentation (`compressed_secondary_cache.h:97-99`).
- Compression via `GetBuiltinV2CompressionManager()`; compression is rejected if the result isn't smaller than the original.
- Capacity changes via `Deflate/Inflate` route through a `ConcurrentCacheReservationManager` — lighter-weight than `SetCapacity`.

### 3.3 `TieredCache`/`CacheWithSecondaryAdapter` composition

```cpp
enum TieredAdmissionPolicy { kAdmPolicyAuto, kAdmPolicyPlaceholder, kAdmPolicyAllowCacheHits,
                              kAdmPolicyThreeQueue, kAdmPolicyAllowAll, kAdmPolicyMax };
struct TieredCacheOptions {
  ShardedCacheOptions* cache_opts; PrimaryCacheType cache_type;
  TieredAdmissionPolicy adm_policy; CompressedSecondaryCacheOptions comp_cache_opts;
  size_t total_capacity; double compressed_secondary_ratio;
  std::shared_ptr<SecondaryCache> nvm_sec_cache;  // pluggable flash/NVM slot
};
```
(`include/rocksdb/cache.h:495-547`). `EvictionHandler` in `secondary_cache_adapter.cc:136-155`: default policy (`kAdmPolicyPlaceholder`) passes `force_insert=false`, letting the compressed cache's own dummy logic decide; `kAdmPolicyAllowCacheHits` forces insertion only if the block was actually hit in primary before eviction (a hotness signal); `kAdmPolicyAllowAll` forces every eviction through. `kAdmPolicyThreeQueue` auto-selected when `nvm_sec_cache` is present, and wraps compressed+nvm caches inside `TieredSecondaryCache` (`cache/tiered_secondary_cache.h`), which explicitly states **no demotion** — "blocks evicted from a tier are just discarded."

### 3.4 No in-tree local-flash/NVM SecondaryCache exists

Confirmed gap: as of current HEAD, RocksDB upstream provides only the interface, the in-memory `CompressedSecondaryCache`, and the `TieredSecondaryCache` stacking adapter — the `nvm_sec_cache` slot is left for external implementation. ToplingDB carries an essentially-identical synced fork (no independent NVM design). The 2021 design blog (rocksdb.org/blog/2021/05/27/rocksdb-secondary-cache.html) states explicitly: **"For FB internal usage, we plan to use Cachelib with a wrapper to provide the plug-in implementation."** No Speedb/TerarkDB NVM cache was confirmed (searches incomplete/rate-limited — flagged gap). RocksDB's deprecated `PersistentCache` predecessor (table-reader-exposed, no admission control) was explicitly rejected in favor of the current hidden-behind-`Cache` design — worth studying as an anti-pattern.

### 3.5 `secondary_cache_uri` flag

Not defined in `db_bench_tool.cc`; only in `db_stress_gflags.cc`, `cache_bench_tool.cc`, and used/randomized by `tools/db_crashtest.py`. No citable public benchmark numbers were found for it (flagged as gap — treat any web claims as unverified).

---

## 4. Other production systems

### 4.1 Ceph BlueStore + BlueFS

- `CacheShard` base (`BlueStore.h:1526-1608`); onode cache is **LRU-only** (`LruOnodeCacheShard`, `BlueStore.cc:1093-1230`); buffer/data cache defaults to **2Q** (`TwoQBufferCacheShard`, `BlueStore.cc:1343-1650`) — three lists `hot`/`warm_in`/`warm_out` (ghost), `warm_in` hits deliberately *not* reordered (defining 2Q behavior, `BlueStore.cc:1499-1500`). Target sizing: `kin=max*0.5`, `kout` bounded by entry count × 0.5.
- BlueFS itself has **no cache** — RocksDB's `BinnedLRUCache` is entirely separate, unified only via a shared `PriorityCache::Manager` memory budget, not one data structure.
- No canonical WA constant published; ceph.io blog benchmarks (2022, 2025) show ~2.3× byte-write differences from memtable-flush tuning, and a **2.68× reduction** in `bluefs db_used_bytes` from enabling LZ4 (now default since Squid). (docs.ceph.com/en/latest/rados/configuration/bluestore-config-ref/, ceph.io/en/news/blog/2022/rocksdb-tuning-deep-dive/, ceph.io/en/news/blog/2025/rocksdb-compression-ftw/)

### 4.2 ScyllaDB/Seastar

- IO scheduler is a **weighted 2-D (IOPS+bandwidth) fair-queue** normalized to a token-bucket budget: `bw_r/bw_r_max + bw_w/bw_w_max + iops_r/iops_r_max + iops_w/iops_w_max ≤ 1.0` (seastar `doc/io-scheduler.md`), driven by an empirically-measured `io-properties.yaml` (via `iotune`). `shared_token_bucket` is lock-free (two-rover design, no CAS loop, `doc/shared-token-bucket.md`).
- Scylla's row cache caches **parsed mutation objects** (not raw pages), tracks negative "continuity," single global LRU (`utils/lru.hh`). **Explicit design choice to bypass the Linux page cache entirely** (pure O_DIRECT), citing 4KB-granularity waste, weak negative caching, LSM read-amp from multi-SSTable merges, and re-parse CPU cost (scylladb.com/2024/01/08/inside-scylladbs-internal-cache/ — flagged first-party/promotional).

### 4.3 Aerospike hybrid memory

- **Primary index: 64 bytes/record** in DRAM (documented, no version ambiguity found) (aerospike.com/docs/develop/data-modeling/record-sizing/).
- Append-only write-blocks (**8MiB** hard-coded since 7.1.0, was 1MiB); direct I/O (`O_DIRECT`+`O_DSYNC`) on raw devices since 4.3.1.
- **Post-write-cache** (renamed from post-write-queue), default 256MiB, caches recently-flushed blocks for read-back, excluded from defrag eligibility.
- **Documented WA formula vs low-water-mark**: **2× at 50% lwm, 4× at 75%, 10× at 90%** (aerospike.com/docs/.../defrag). This is the cleanest, most explicit published WA-vs-utilization curve found across all systems surveyed.

### 4.4 Apache Ignite / Alluxio

- Ignite `PageMemoryImpl` default page **4KB**, 48-byte page header; replacement policies: `RandomLruPageReplacementPolicy`, `SegmentedLruPageReplacementPolicy` (true LRU), `ClockPageReplacementPolicy` (second-chance) — no Clock-Pro variant exists. Checkpoint every 180s, WAL 64MB×10 segments (classic WAL-then-checkpoint durability) (github.com/apache/ignite: `PageMemoryImpl.java`, `FilePageStore.java`, `*PageReplacementPolicy.java`).
- Alluxio `LocalCacheManager` default page size **1MB**, default evictor `LRUCacheEvictor` (via Java `LinkedHashMap` access order trick); best-effort, no durability layer.

### 4.5 Netflix EVCache/Moneta

- Rend (Go memcached-protocol proxy, open source) fronting Memcached (L1)/Mnemonic (L2, RocksDB-based, closed source). Full-stack throughput on `i2.xlarge` (800GB SSD): **~22K inserts/sec, ~21K reads/sec** worst case. Mnemonic latency: **p99 ≈9ms during batch load, ≈600µs after**, switched Level→**FIFO-style compaction** to cap background SSD read traffic (netflixtechblog.com/application-data-caching-using-ssds-5bf25df851ef). "70% cost savings" is unconfirmed beyond a conference-slide abstract.

### 4.6 Twitter Segcache (NSDI'21) — **DRAM-only, no disk tier** (explicitly confirmed)

- 1MB segments grouped by approximate TTL; **5 bytes/object metadata** (8-bit keysize+24-bit valsize+8-bit flags) vs Memcached's 56B, Pelikan-slab 38B, Pelikan cuckoo-hash 6B/14B-with-CAS. Segment-granularity expiration/eviction via merge-based ("CIPHER") reclaim. >70 MQPS at 24 threads vs Memcached's 9 MQPS. Only disk-adjacent feature found is `datapool_pmem.c` — whole-heap **restart persistence**, not a tiered cache (usenix.org/system/files/nsdi21-yang.pdf; github.com/twitter/pelikan).

### 4.7 Redis Enterprise Auto Tiering (RoF/Flex)

- Keys/dict always RAM-resident; only values of warm keys move to flash; **RAM floor 10%, recommended ≥20%**. Storage engine RocksDB→**Speedb** (default since 7.2.4). Historical Optane benchmarks (2017/2019, discontinued hardware): 9.2×/9.7×/2.8× speedup at 50/85/95% RAM hit ratio; ~43% memory-cost savings at equal throughput — flagged as vendor/historical, not current guidance.

---

## 5. Low-level I/O techniques (Linux + Windows)

### 5.1 Direct I/O alignment
- **Linux O_DIRECT**: buffer address, I/O size, and file offset must each be a multiple of the logical block size (typically 512 or 4096); violation → `EINVAL`. Use `posix_memalign()`, query size via `BLKSSZGET`. (man7.org/linux/man-pages/man2/open.2.html)
- **Windows FILE_FLAG_NO_BUFFERING**: identical constraint — "File access must begin at byte offsets... integer multiples of the volume's sector size... for numbers of bytes that are integer multiples of the volume's sector size" (learn.microsoft.com/windows/win32/fileio/file-buffering). Query via `IOCTL_STORAGE_QUERY_PROPERTY`/`StorageAccessAlignmentProperty` → `BytesPerLogicalSector`/`BytesPerPhysicalSector` (modern API, supersedes `GetDiskFreeSpace`).

### 5.2 Async I/O engines
- **io_uring**: shared SQ/CQ ring buffers mmap'd user/kernel; `IOSQE_FIXED_FILE` + `io_uring_register_files()` pre-registers FDs to skip per-I/O FD-table lookup/refcounting — the primary source of syscall overhead reduction (man7.org/linux/man-pages/man7/io_uring.7.html, io_uring_registered_files(7)).
- **Windows IoRing**: introduced Win11 21H2 preview, initially **read-only**, capped at 65536 queued reads/131072 CQ entries; write/flush ops added ~1 year later (`BuildIoRingWriteFile`/`FlushFile`) (windows-internals.com/i-o-rings-when-one-i-o-operation-is-not-enough/, .../one-year-to-i-o-ring-what-changed/). Architecturally less mature than io_uring (no networking ops, narrower feature parity) — treat the head-to-head comparison as secondhand/approximate.
- Fallback on Windows without IoRing: `FILE_FLAG_OVERLAPPED` + IOCP, same alignment rules apply.

### 5.3 mmap vs pread
- RocksDB's `allow_mmap_reads` bypasses its own block cache (OS page cache serves reads instead), default `false` (github.com/facebook/rocksdb/wiki/IO).
- Seastar/ScyllaDB avoid mmap entirely: **synchronous page faults would block an entire shard** in the shard-per-core model, and mmap is largely incompatible with O_DIRECT since O_DIRECT bypasses the page cache mmap depends on — cited as architectural rationale (docs.seastar.io, deepwiki.com/scylladb/seastar/5.2-file-io — flagged approximate/secondhand).

### 5.4 Sparse files, hole-punching, TRIM
- Linux: `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)` deallocates a range, reads return zero (man7.org/linux/man-pages/man2/fallocate.2.html).
- Windows: `DeviceIoControl(FSCTL_SET_SPARSE)` then `FSCTL_SET_ZERO_DATA`; query allocated ranges via `FSCTL_QUERY_ALLOCATED_RANGES`; TRIM via `FSCTL_FILE_LEVEL_TRIM` (learn.microsoft.com/windows/win32/fileio/sparse-file-operations, .../ifs/fsctl-file-level-trim).
- TRIM lets device GC skip copying dead pages, directly reducing device-level write amplification (general SSD architecture consensus — no single canonical primary source, treat as approximate).
- **Windows sparse-file fragmentation risk is real and documented**: MS shipped a hotfix specifically because heavily-fragmented sparse files on NTFS hit hard extent-count limits (error 665) (support.microsoft.com — two KB articles cited). **Implication: prefer sequential/batched writes into sparse regions, not random scatter.**

### 5.5 Filesystem metadata overhead
- **NTFS**: MFT entry ~1KB minimum per file (even 0-byte); small files can be "resident" (stored inline, avoiding separate cluster allocation); ~1M tiny files ≈ ~1GB MFT overhead (secondary-sourced figure, flagged approximate). Directory lookups are B+-tree (`$INDEX_ROOT`/`$INDEX_ALLOCATION`/`$BITMAP`) with node-split locking on insert (community/forensic docs, not official MS internals doc).
- **Design implication (synthesized, not a direct quote)**: one large preallocated file with internal region/slot allocation avoids both the fixed ~1KB/file MFT tax and B-tree directory-lookup/lock contention that come with many small files — but if using sparse regions inside that one file, favor sequential/batched writes given NTFS's sparse-fragmentation extent-count pitfalls above.

### 5.6 512e/4Kn
- 512n = 512B physical+logical; **512e** = 4096B physical, 512B logical emulation (misaligned sub-4K writes trigger internal read-modify-write, a perf penalty); **4Kn** = native 4096B/4096B, no RMW translation but requires full-stack native-4K support (WD/Seagate Advanced Format whitepapers — summarized, not directly fetched).

---

## 6. Eviction / index algorithms

- **CLOCK/second-chance**: one reference bit/frame, O(1) amortized, classic low-metadata LRU approximation (standard OS textbook material).
- **S3-FIFO** (SOSP'23, Yang et al., doi.org/10.1145/3600006.3613147): three queues — small FIFO (~10% capacity, filters one-hit-wonders), main FIFO (frequency-counter checked lazily at eviction, not on every hit), ghost queue (evicted keys only, re-request bypasses small-queue filter straight into main). Directly confirmed: **lowest mean miss ratio on 10/14 datasets** across 6,594 traces; **6× higher throughput than optimized LRU at 16 threads**. FIFO's non-mutating hit path (no per-access pointer relinking) is explicitly cited as the reason it's attractive for concurrency/flash-friendliness. (Specific "72%/46%" reduction figures circulating are **unverified** — not found in the directly-fetched abstract.)
- **SIEVE** (NSDI'24, Zhang et al.): single FIFO queue + one "visited" bit/object; a hit sets the bit but does **not** move the object (unlike CLOCK, which conceptually requeues); a moving hand from the tail clears visited bits and evicts unvisited objects. Reported (via search-tool-retrieved abstract, not hand-verified from raw PDF): **up to 63.2% lower miss ratio than ARC**, best on 45%+ of 1559 traces vs 15% for the runner-up, **2× throughput of 16-thread LRU**, <20 LOC to integrate. DRAM overhead: effectively one bit + FIFO pointers per object (inferred minimal-overhead characterization, no explicit bytes/object figure quoted).
- **W-TinyLFU** (Caffeine): window LRU + Segmented-LRU main cache gated by a TinyLFU (count-min-sketch-like) admission filter — candidate admitted to main only if its estimated frequency beats the current victim's. Aimed at scan-resistance + skew-adaptivity (arxiv.org/html/1512.00727v2).
- **Per-entry DRAM overhead compiled across literature**:

| System | Overhead | Confidence |
|---|---|---|
| FASTER | 8 bytes/key (fixed hash-index) | direct quote, arxiv F2 paper |
| Segcache | ~5 bytes/object (vs Memcached 56B) | direct NSDI'21 paper |
| Kangaroo KSet | ~7 bits/object combined system DRAM | direct SOSP'21 quote (§2.1 above) |
| CacheLib BigHash/SOC | ~3 bits/object (Bloom-filter share) | Kangaroo's citation of CacheLib, cross-verified |
| CacheLib LOC | 31 bytes/item (whole DRAM tier average), 0.01-0.61% of cache index-specific | direct OSDI'20 quote |
| Flashield | <4 bytes/object | direct NSDI'19 quote |
| RIPQ | no single bytes/object figure published; WA-reduction focused | FAST'15, secondary summary |

---

## Distilled Design Rules for a RocksDB Local-SSD `SecondaryCache`

1. **Split by object size into two engines** — a region/log-structured path for large blocks and a bucket/set-associative path for small objects — because a single indexing strategy can't be DRAM-efficient across both size regimes. *(CacheLib Navy: BlockCache+BigHash split; Kangaroo: KLog+KSet split)*
2. **Evict at region/block granularity, not per-item.** Reclaim a whole fixed-size region/segment at once and erase it as a unit — this amortizes flash erase cost and is the single biggest lever against write amplification. *(CacheLib Navy RegionManager; Aerospike write-blocks; Segcache segments)*
3. **Never persist a full 64-bit-plus key in the DRAM index — store only a key hash plus a compact address/size encoding (aim for 5–8 bytes/entry for large objects, sub-1-byte amortized for small objects via per-bucket Bloom filters).** *(CacheLib Navy Index 5-8B struct; BigHash ~3 bits/object; FASTER 8B/key baseline)*
4. **For tiny objects, avoid a DRAM index entirely — use a fixed set/bucket keyed by hash, with a small per-bucket Bloom filter (~4 bytes/25 entries, 4 hashes) to skip flash reads on likely-misses (>90% skip rate achievable).** *(CacheLib BigHash/SOC)*
5. **If admitting many tiny objects to a log-structured area, buffer them and flush to their target set/bucket only once ≥2 objects collide on the same set — this alone can cut write amplification 3×+ vs either pure design.** *(Kangaroo KLog→KSet flush threshold)*
6. **Budget writes against the device's DWPD/endurance rating explicitly, not just capacity** — compute a target daily write budget (bytes/day) and throttle admission dynamically to stay under it, using a damped multiplicative feedback controller (adjust admission probability by target/observed ratio, clamped ±25%/interval, globally bounded). *(CacheLib DynamicRandomAP; Kangaroo's 3-DWPD/62.5MB/s budget derivation)*
7. **Default to simple random/probabilistic admission before building an ML-based admission policy** — CacheLib's fixed-probability admission is the pragmatic baseline; ML-based admission (Flashield-style, predicting future reads) is a proven ~40-45% write-rate reduction but adds real engineering complexity and needs a feature-collection window. *(CacheLib OSDI Appendix C: 44% reduction; Flashield: median WA 0.5× vs 2.85-3.67× for RIPQ/victim-cache)*
8. **Always write in full alignment units matching the device's sector/page geometry when writing to a flash-backed region, but consider sub-page write-buffering (e.g. 512B-aligned instead of always-4KB) to cut internal fragmentation** — CacheLib measured cutting CDN fragmentation from 7%→2% this way. *(CacheLib OSDI §5.2)*
9. **Prefer sequential/FIFO-ordered writes over LRU-driven scattered writes at the region level** — CacheLib's switch reduced device-level WA from 1.5× to 1.05× (15% fewer NAND writes/sec) with only a small application-level WA cost. *(CacheLib OSDI LOC eviction change)*
10. **Overprovision the physical device 50%+ beyond logical cache capacity if you need low single-digit device-level write amplification** — this is a stated, deliberate tradeoff in production, not an incidental waste. *(CacheLib SOC 50% overprovisioning; Kangaroo BigHash critique — "runs with over half the flash device empty")*
11. **Use O_DIRECT (Linux) / FILE_FLAG_NO_BUFFERING (Windows) for the cache device/file, with hard alignment on buffer address, size, and offset to the queried logical sector size (512e/4Kn) — never assume 512B.** *(Universal Linux/Windows I/O finding)*
12. **Prefer io_uring (Linux) with registered/fixed files over O_DIRECT+libaio for the hot path; on Windows, fall back to overlapped I/O + IOCP since Windows IoRing is still read-primary/immature (as of Win11/Server2022 era) and lacks feature parity.** *(io_uring man pages; windows-internals.com IoRing posts)*
13. **Avoid mmap for the cache's data path** — it reintroduces uncontrolled OS page-cache behavior, synchronous page-fault stalls incompatible with async/shard-per-core designs, and is largely incompatible with O_DIRECT on most filesystems. *(ScyllaDB/Seastar rationale; RocksDB's own mmap_reads caveat)*
14. **Use one (or a few) large preallocated file(s) with internal region/slot allocation rather than many small files** — avoids NTFS's fixed ~1KB MFT-entry tax per file and B-tree directory-lookup/lock contention; on ext4/XFS, extent-based allocation makes this less punishing but still beneficial for reducing metadata churn. *(NTFS internals findings)*
15. **If using sparse files/hole-punching for reclaimed space, punch/write in large sequential batches, not scattered random ranges** — NTFS specifically has had bugs/extent-limit failures from highly-fragmented sparse-file usage; issue TRIM/discard (`FALLOC_FL_PUNCH_HOLE` / `FSCTL_FILE_LEVEL_TRIM`) so the SSD's internal GC can reclaim without extra copy-out. *(NTFS fragmentation KBs; TRIM/GC write-amp interaction)*
16. **Checksum every on-flash entry header independently from its value payload** — validate the header first; on header-checksum mismatch during a region scan/reclaim, abort remaining items in that region rather than trusting corrupted data, but let value-only checksum failures be non-fatal (skip the item, keep iterating). *(CacheLib Navy BlockCache/BigHash checksum handling)*
17. **Persist the index across restart with a strict compatibility check (base offset, cache size, alignment size, checksum config, version) and fall back to a full wipe (never a silent partial-trust) on any mismatch** — do not attempt scan-based index rebuild as the primary recovery path; it's expensive and CacheLib deliberately avoids it. *(CacheLib Navy persist/recover contract)*
18. **Design the SecondaryCache interface so admission/insertion can silently no-op** — mirror RocksDB's own contract that `Insert()` "may or may not" actually store the object even returning `Status::OK()`, so admission-control logic composes cleanly with the cache tier above it. *(RocksDB SecondaryCache interface contract)*
19. **Gate promotion into the faster tier on an explicit hotness signal (e.g., "was this block actually hit in the primary cache before eviction"), not unconditional promotion** — this is exactly RocksDB's `kAdmPolicyAllowCacheHits` and CacheLib Navy's read-hit promotion-to-DRAM path. *(RocksDB CacheWithSecondaryAdapter; CacheLib NvmCache::onGetComplete)*
20. **When stacking multiple tiers (compressed-in-memory → local-flash), don't implement demotion** — evicted blocks from a lower tier are simply discarded, not written back up; this keeps the write path unidirectional and simple. *(RocksDB TieredSecondaryCache explicit "no demotion" design)*
21. **Order lookups from cheapest/most-selective to most-expensive tier (DRAM → compact-index tier → Bloom-filter-gated tier)** and don't reverse this even though it seems symmetric — CacheLib's own AMAT model shows reversing LOC/SOC order adds several microseconds because the more selective structure should filter first. *(CacheLib OSDI AMAT ordering)*
22. **Use per-key strict write/insert ordering via a sharded, key-hashed job queue rather than a global lock** — needed once async region/bucket writes are in flight, to prevent an insert-then-remove (or duplicate-insert) race for the same key landing in an inconsistent final on-disk state. *(CacheLib Navy OrderedThreadPoolJobScheduler)*
23. **Consider a modern single-queue/FIFO-based eviction algorithm (S3-FIFO or SIEVE) over classic LRU for the in-memory index/hot-item tracking** — both report state-of-the-art hit ratios with far cheaper hit-path bookkeeping (no per-access pointer relinking), which matters more as concurrency scales; SIEVE in particular needs only ~1 bit/object beyond existing FIFO-queue pointers. *(S3-FIFO SOSP'23; SIEVE NSDI'24)*
24. **Track write-amplification as two separate numbers — application-level (bytes-charged-to-user vs bytes-actually-written) and device-level (host writes vs NAND writes)** — they diverge significantly (e.g., CacheLib SOC: ~6.5× app-level, only 1.1–1.4× device-level) and different design levers (region packing vs sequential-write patterns) address each. *(CacheLib OSDI distinction)*
25. **Publish an explicit endurance target up front (target DWPD × device capacity → daily write budget in bytes) and derive the admission-policy's target rate from it, rather than tuning admission probability empirically** — this is the one universally-repeated methodological pattern across Kangaroo, CacheLib, and Aerospike's defrag-lwm formulas. *(Kangaroo's 3-DWPD/62.5MB/s derivation; CacheLib's "50% above sustainable rate" finding; Aerospike's explicit 2×/4×/10× WA-vs-lwm curve)*

---

## Notable Gaps (from sub-agent research, flagged rather than silently omitted)

- No public in-tree or well-documented third-party local-flash/NVM `rocksdb::SecondaryCache` implementation was found (Speedb/TerarkDB searches incomplete due to GitHub rate limits — recommend follow-up).
- CacheLib's DeepWiki page describing an actual CacheLib↔RocksDB `SecondaryCache` adaptor returned HTTP 429 and was not independently verified against source.
- No public `db_bench --secondary_cache_uri` benchmark numbers (hit ratio/throughput/latency) were found in a citable primary source.
- S3-FIFO's specific "72%/46%" improvement figures and SIEVE's exact abstract percentages were retrieved via secondary search-tool summaries, not hand-verified from raw PDF text — treat as directionally correct but not block-quotable.
- Kangaroo's, CacheLib BigHash's, and RIPQ's precise bytes/object figures compiled in the eviction-algorithm table came from search-tool synthesis in one pass rather than a second direct paper fetch — cross-checked against the Kangaroo-paper agent's independently-fetched numbers (§2.1) where possible, which agree in magnitude.
- Axboe's original io_uring paper (kernel.dk/io_uring.pdf) could not be fetched (DNS/TLS failure); io_uring claims rest on man7.org man pages only.
