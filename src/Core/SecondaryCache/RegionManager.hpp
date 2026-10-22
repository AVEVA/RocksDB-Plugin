// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "BlockDevice.hpp"
#include "CacheStats.hpp"
#include "Index.hpp"

#include "AVEVA/RocksDB/Plugin/Core/RegionBasedSecondaryCache.hpp"

#include <functional>
#include <mutex>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
/// <summary>
/// Owns all mutable cache state: the hash index, the FIFO ring of regions, the single append buffer and the block
/// device. Records are opaque, already-encoded byte spans; this class never decodes them. Every public method takes
/// m_mutex, so callers need no locking; *Locked helpers require it to be held.
/// </summary>
class RegionManager {
  public:
    /// Creates the backing device; receives the metadata budget to charge for device bookkeeping.
    using DeviceFactory = std::function<std::unique_ptr<BlockDevice>(MemoryBudget&)>;

    /// <summary>
    /// Names one specific stored copy of a record. Remove and Contains match it only while the index still maps
    /// the hash to that exact copy, so they never act on a newer record or on state from before a reset.
    /// </summary>
    struct Identity {
        /// Location the record was read from, including its region generation.
        Location location;
        /// m_incarnation at read time; any invalidation makes the identity stale.
        uint64_t incarnation{0};
    };

    /// Outcome of Read.
    enum class ReadStatus {
        /// Bytes were copied out; the caller must still validate them with DecodeRecord.
        kOk,
        /// No mapping for the hash.
        kMiss,
        /// The device read failed; the mapping has been removed.
        kIoError
    };

    /// <summary>
    /// Validates options and allocates the region layout before creating and configuring the device, so invalid
    /// geometry or an undersized metadata budget never touches the filesystem.
    /// </summary>
    /// <param name="factory">Creates the backing device; called exactly once.</param>
    /// <param name="options">Geometry, initial capacity and budgets.</param>
    /// <param name="stats">Counters to update; must outlive this object.</param>
    /// <exception cref="std::invalid_argument">options fail Validate.</exception>
    /// <exception cref="BudgetExceeded">The region layout exceeds the metadata budget.</exception>
    /// <remarks>Device creation or configuration errors propagate unchanged.</remarks>
    RegionManager(DeviceFactory factory, RegionBasedSecondaryCacheOptions options, CacheStats& stats);

    /// <summary>Checks geometry and budgets without side effects.</summary>
    /// <param name="options">Options to check, including capacity.</param>
    /// <exception cref="std::invalid_argument">
    /// regionSize, flushBlockSize or maxEntrySize is not a valid multiple of kRecordAlign within its bounds,
    /// transientBudget is zero, or capacity needs more than UINT32_MAX regions.
    /// </exception>
    static void Validate(const RegionBasedSecondaryCacheOptions& options);

    /// <summary>
    /// Admits one encoded record: selects a region (reclaiming the next one in FIFO order if needed), buffers or
    /// writes the bytes and maps hash to the new location, replacing any older mapping for hash.
    /// </summary>
    /// <param name="hash">Key fingerprint used as the index key.</param>
    /// <param name="record">Encoded record; its size must be a multiple of kRecordAlign.</param>
    /// <param name="payloadSize">Payload bytes inside record; used only for statistics.</param>
    /// <returns>
    /// OK when admitted or dropped (disabled or too large); InvalidArgument for a misaligned record; IOError when a
    /// flush or direct write fails, which also clears the cache.
    /// </returns>
    /// <exception cref="BudgetExceeded">Index or reverse-list growth exceeds the metadata budget; the record is not
    /// admitted.</exception>
    rocksdb::Status Append(uint64_t hash, std::span<const std::byte> record, size_t payloadSize);

    /// <summary>
    /// Copies the bytes currently mapped for hash, from the append buffer if they are not flushed yet, otherwise
    /// from the device. Does not validate them.
    /// </summary>
    /// <param name="hash">Key fingerprint to look up.</param>
    /// <param name="record">[out] Resized to the mapped length and filled on kOk.</param>
    /// <param name="identity">[out] The copy that was read; set on kOk and kIoError.</param>
    /// <returns>kOk, kMiss, or kIoError (the failed mapping is removed).</returns>
    /// <exception cref="BudgetExceeded">record cannot grow within its allocator's budget.</exception>
    ReadStatus Read(uint64_t hash, RecordBytes& record, Identity& identity);

    /// <summary>Removes the mapping for hash only if it still names the copy in identity.</summary>
    /// <param name="hash">Key fingerprint.</param>
    /// <param name="identity">Identity returned by Read.</param>
    /// <returns>True if removed; false if the mapping is gone, was replaced, or the cache was reset.</returns>
    bool Remove(uint64_t hash, const Identity& identity);

    /// <summary>Tests whether hash still maps to the copy in identity.</summary>
    /// <param name="hash">Key fingerprint.</param>
    /// <param name="identity">Identity returned by Read.</param>
    /// <returns>True if that exact copy is still indexed.</returns>
    bool Contains(uint64_t hash, const Identity& identity) const;

    /// <summary>Unconditionally removes the current mapping for hash, if any; disk bytes wait for reclaim.</summary>
    /// <param name="hash">Key fingerprint.</param>
    void Erase(uint64_t hash);

    /// <summary>
    /// Cold-resets to a new capacity: drops all entries and buffered bytes, deletes region files and rebuilds the
    /// layout. Zero disables the cache.
    /// </summary>
    /// <param name="capacity">New capacity in bytes.</param>
    /// <returns>OK.</returns>
    /// <exception cref="std::invalid_argument">Geometry is invalid for capacity; existing state is
    /// preserved.</exception>
    /// <remarks>Any other exception happens after the reset and leaves the cache disabled.</remarks>
    rocksdb::Status SetCapacity(size_t capacity);

    /// <returns>Current capacity in bytes; 0 means disabled.</returns>
    size_t CapacityBytes() const;

    /// <returns>Encoded bytes of indexed records, including buffered ones.</returns>
    size_t UsageBytes() const;

    /// <summary>Reads layout/resource values under the lock and copies the counters.</summary>
    /// <returns>A snapshot with transientBytes left at 0 for the caller to fill.</returns>
    RegionBasedSecondaryCacheStats Snapshot() const;

  private:
    /// <summary>FIFO state of one region.</summary>
    struct Region {
        explicit Region(MemoryBudget& budget) : hashes(BudgetAllocator<uint64_t>(budget)) {}
        /// Fill number: bumped on every reclaim, reset to 1 by invalidation (which bumps m_incarnation instead).
        /// Makes copies written at the same offset in different fills compare unequal.
        uint32_t generation{1};
        /// Next append offset in bytes. For m_current, the last m_buffer.size() bytes before it exist only in
        /// m_buffer.
        size_t cursor{0};
        /// Reverse list of hashes appended during this generation, used to unmap them on reclaim. May hold stale or
        /// duplicate hashes; reclaim unmaps only those still mapped to this region and generation.
        std::vector<uint64_t, BudgetAllocator<uint64_t>> hashes;
    };

    /// <summary>Allocates ceil(capacity / regionSize) empty regions, reserves the append buffer and sets
    /// m_capacity.</summary>
    /// <param name="capacity">Capacity to lay out; m_regions must be empty.</param>
    /// <exception cref="BudgetExceeded">The layout exceeds the metadata budget.</exception>
    void ConfigureLocked(size_t capacity);

    /// <summary>
    /// Drops every mapping, region fill and buffered byte, and bumps m_incarnation so outstanding identities go
    /// stale. Keeps the layout and capacity.
    /// </summary>
    /// <exception cref="std::overflow_error">m_incarnation is exhausted.</exception>
    void InvalidateLocked();

    /// <summary>
    /// FIFO eviction of one region: unmaps hashes still pointing at its current generation, clears its reverse
    /// list, rewinds its cursor and bumps its generation.
    /// </summary>
    /// <param name="region">Region to recycle.</param>
    /// <exception cref="std::overflow_error">The region's generation is exhausted.</exception>
    void ReclaimLocked(uint32_t region);

    /// <summary>Writes the append buffer at its region offset and clears it.</summary>
    /// <returns>True on success or if the buffer is empty; false if the write failed, which also invalidates the
    /// cache.</returns>
    bool FlushLocked();

    /// <summary>
    /// Ensures m_current has at least length free bytes: keeps the current region if it fits, otherwise flushes and
    /// reclaims the next region in ring order that is large enough (a short final region may be skipped).
    /// </summary>
    /// <param name="length">Encoded record size in bytes.</param>
    /// <returns>False if the flush failed or no region can hold length.</returns>
    bool SelectRegionLocked(size_t length);

    /// <param name="region">Region index.</param>
    /// <returns>Usable bytes in region: regionSize, or the remainder of capacity for the final region.</returns>
    size_t RegionBudget(uint32_t region) const noexcept;

    /// <summary>Unmaps hash and subtracts its length from m_usage; the reverse list is left alone.</summary>
    /// <param name="hash">Key fingerprint.</param>
    void RemoveLocationLocked(uint64_t hash);

    /// The only state lock: index, layout, reverse lists, buffer, device, usage.
    mutable std::mutex m_mutex;
    /// Construction options. capacity is the initial value only; m_capacity is authoritative.
    RegionBasedSecondaryCacheOptions m_options;
    /// Counters shared with the facade; atomic, so readers need not hold m_mutex.
    CacheStats& m_stats;
    /// Budget for long-lived metadata: index, region array, reverse lists and device file lengths.
    MemoryBudget m_metadata;
    /// hash -> newest Location.
    Index m_index;
    /// Region ring of ceil(m_capacity / regionSize) entries; the last region may be short.
    std::vector<Region, BudgetAllocator<Region>> m_regions;
    /// Backing store for flushed bytes.
    std::unique_ptr<BlockDevice> m_device;
    /// Unflushed tail of m_current, ending at its cursor; capacity is flushBlockSize. Records larger than
    /// flushBlockSize bypass it.
    std::vector<std::byte> m_buffer;
    /// Region receiving appends; UINT32_MAX when none is selected (startup or after invalidation).
    uint32_t m_current{UINT32_MAX};
    /// Reset counter, bumped by InvalidateLocked so stale identities never match.
    uint64_t m_incarnation{0};
    /// Active capacity in bytes; 0 means disabled.
    size_t m_capacity{0};
    /// Sum of encoded lengths of indexed records.
    size_t m_usage{0};
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
