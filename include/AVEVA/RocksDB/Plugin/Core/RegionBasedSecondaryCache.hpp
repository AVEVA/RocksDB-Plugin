// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <rocksdb/secondary_cache.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core {
/// Configuration for the region-based secondary cache.
struct RegionBasedSecondaryCacheOptions {
    /// Maximum encoded record bytes retained across all regions.
    size_t capacity{512ULL * 1024 * 1024};
    /// Maximum size of one region; a multiple of 64, at most UINT32_MAX.
    size_t regionSize{64ULL * 1024 * 1024};
    /// Capacity of the single append buffer; a nonzero multiple of 64, at most regionSize.
    size_t flushBlockSize{1ULL * 1024 * 1024};
    /// Maximum encoded record size, including key, header and alignment; a nonzero
    /// multiple of 64, at most regionSize.
    size_t maxEntrySize{16ULL * 1024 * 1024};
    /// Allocator-requested metadata limit; zero selects max(1 MiB, capacity / 200).
    size_t metadataBudget{0};
    /// Nonzero shared limit for serialization, encoding and lookup-copy allocations.
    size_t transientBudget{64ULL * 1024 * 1024};
};

/// <summary>
/// Point-in-time view of the cache. Layout/resource fields (liveEntries .. openHandles, deviceWriteBytes,
/// deviceReadBytes) are read together under the cache lock; transientBytes and the cumulative counters are read
/// individually, so they are not mutually consistent with each other or with the layout fields.
/// </summary>
struct RegionBasedSecondaryCacheStats {
    /// Number of indexed records (one per distinct key hash).
    size_t liveEntries{0};
    /// Encoded bytes of indexed records, including records still in the append buffer.
    size_t liveBytes{0};
    /// Current capacity; 0 means the cache is disabled.
    size_t capacityBytes{0};
    /// Sum of logical region-file lengths, not reserved disk space or page-cache residency.
    size_t fileBytes{0};
    /// Allocator-requested metadata bytes: index nodes/buckets, region array, reverse-list capacity, file lengths.
    size_t metadataBytes{0};
    /// Current metadata budget limit.
    size_t metadataLimit{0};
    /// Reserved capacity of the append buffer (not the bytes currently buffered).
    size_t activeBufferBytes{0};
    /// Transient-budget bytes in use for serialization, encoding and lookup copies.
    size_t transientBytes{0};
    /// Region files currently held open.
    size_t openHandles{0};
    /// Insert and InsertSaved calls.
    uint64_t inserts{0};
    /// Records appended and indexed.
    uint64_t admitted{0};
    /// Inserts dropped because the record is unrepresentable or exceeds maxEntrySize, regionSize or capacity.
    uint64_t droppedTooLarge{0};
    /// Inserts or lookups abandoned because the transient or metadata budget was exhausted.
    uint64_t droppedPressure{0};
    /// Inserts dropped because capacity was 0.
    uint64_t droppedDisabled{0};
    /// Lookup calls.
    uint64_t lookups{0};
    /// Lookups that returned a handle.
    uint64_t hits{0};
    /// Lookups that found a corrupt record (bad magic, version, length or CRC); the record is removed.
    uint64_t corruptions{0};
    /// Lookups whose hash matched a record for a different key; treated as a miss.
    uint64_t keyMismatches{0};
    /// Device read/write failures and filesystem/device exceptions.
    uint64_t ioErrors{0};
    /// Failures passed to the rate-limited failure log (one per failure, logged or not).
    uint64_t errors{0};
    /// Non-empty regions recycled by FIFO reclaim.
    uint64_t reclaims{0};
    /// Cumulative payload bytes of admitted records.
    uint64_t payloadBytes{0};
    /// Cumulative encoded bytes (header + key + payload + padding) of admitted records.
    uint64_t encodedBytes{0};
    /// Cumulative bytes written to the device.
    uint64_t deviceWriteBytes{0};
    /// Cumulative bytes read from the device (append-buffer hits are not counted).
    uint64_t deviceReadBytes{0};
};

/// <summary>
/// Synchronous region-based secondary cache with bounded memory and FIFO region recycling.
/// It owns a dedicated child directory beneath the supplied parent and starts empty.
/// All RocksDB entry points are noexcept: internal exceptions are converted to a Status, counted and logged.
/// </summary>
class RegionBasedSecondaryCache final : public rocksdb::SecondaryCache {
  public:
    /// Default capacity (512 MiB); matches RegionBasedSecondaryCacheOptions::capacity.
    static constexpr size_t kDefaultCapacity = 512ULL * 1024 * 1024;

    /// <summary>
    /// Validates options, claims cacheDir/ssd-secondary-cache-v1 and starts empty. Existing owned region files are
    /// discarded; unrelated parent contents are never touched. Files grow on write, without reservation.
    /// </summary>
    /// <param name="cacheDir">Parent directory; the cache creates and exclusively locks its own child in it.</param>
    /// <param name="options">Geometry, capacity and memory budgets.</param>
    /// <param name="logger">Destination for rate-limited failure reports; must not be null.</param>
    /// <exception cref="std::invalid_argument">Null logger, invalid options, or an unsafe or foreign cache
    /// directory.</exception>
    /// <remarks>Also throws if the directory cannot be created or locked, or the metadata budget cannot hold the
    /// region layout.</remarks>
    explicit RegionBasedSecondaryCache(
        std::filesystem::path cacheDir, RegionBasedSecondaryCacheOptions options,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);

    ~RegionBasedSecondaryCache() override;
    RegionBasedSecondaryCache(const RegionBasedSecondaryCache&) = delete;
    RegionBasedSecondaryCache& operator=(const RegionBasedSecondaryCache&) = delete;
    RegionBasedSecondaryCache(RegionBasedSecondaryCache&&) = delete;
    RegionBasedSecondaryCache& operator=(RegionBasedSecondaryCache&&) = delete;

    /// <summary>Returns "RegionBasedSecondaryCache".</summary>
    const char* Name() const noexcept override;

    /// <summary>
    /// Serializes obj with helper's callbacks and appends the encoded record, replacing any older record for key.
    /// Admission is best-effort: incompatible helpers are ignored, and disabled, oversize and budget-pressure
    /// admissions are counted drops; all of these return OK.
    /// </summary>
    /// <param name="key">Cache key; stored in full so lookups can verify it.</param>
    /// <param name="obj">Object to serialize; not retained.</param>
    /// <param name="helper">Callbacks used to size and save obj; null or non-secondary-compatible is ignored.</param>
    /// <param name="forceInsert">Ignored: both values permit FIFO recycling and neither bypasses size or memory
    /// limits.</param>
    /// <returns>
    /// OK when admitted or dropped; the saveto_cb error; IOError when a device write fails (the cache is then
    /// cleared); MemoryLimit, InvalidArgument or Aborted for unexpected internal failures.
    /// </returns>
    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, bool forceInsert) noexcept override;

    /// <summary>Appends already-serialized bytes, replacing any older record for key.</summary>
    /// <param name="key">Cache key; stored in full so lookups can verify it.</param>
    /// <param name="saved">Serialized payload; copied into the record.</param>
    /// <param name="type">Compression of saved; returned to create_cb on lookup.</param>
    /// <param name="source">Tier saved came from; returned to create_cb on lookup.</param>
    /// <returns>Same as Insert, minus the helper and serialization-callback cases.</returns>
    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, rocksdb::CompressionType type,
                                rocksdb::CacheTier source) noexcept override;

    /// <summary>
    /// Synchronously reads and validates the record for key and materializes it with helper->create_cb.
    /// Misses, I/O errors, corruption and budget pressure all return null; unreadable or corrupt records are removed.
    /// </summary>
    /// <param name="key">Cache key; compared in full against the stored key.</param>
    /// <param name="helper">Callbacks used to create the value; null or incompatible helpers produce a miss.</param>
    /// <param name="create_context">Opaque context forwarded to create_cb.</param>
    /// <param name="wait">Ignored; lookups are always synchronous.</param>
    /// <param name="advise_erase">On a hit, remove the record that was read (never a newer replacement).</param>
    /// <param name="stats">Optional RocksDB statistics that receive hit tickers; may be null.</param>
    /// <param name="kept_in_sec_cache">
    /// [out] True only if the record that was read is still indexed when Lookup returns; false on a miss or when
    /// advise_erase is set.
    /// </param>
    /// <returns>
    /// A ready handle, or null on a miss. The caller owns the created value; destroying the handle does not
    /// delete it.
    /// </returns>
    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* create_context, bool wait, bool advise_erase, rocksdb::Statistics* stats,
           bool& kept_in_sec_cache) noexcept override;

    /// <summary>Returns true: Lookup honors advise_erase.</summary>
    bool SupportForceErase() const noexcept override { return true; }

    /// <summary>
    /// Removes the current index mapping for key; its disk bytes are reclaimed with their region.
    /// Failures are counted and logged, never thrown.
    /// </summary>
    /// <param name="key">Cache key to forget.</param>
    void Erase(const rocksdb::Slice& key) noexcept override;

    /// <summary>No-op: every handle returned by Lookup is already ready.</summary>
    /// <param name="handles">Ignored.</param>
    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept override;

    /// <summary>
    /// Changes capacity by cold-resetting the cache: all entries, buffers and region files are dropped.
    /// Zero disables the cache.
    /// </summary>
    /// <param name="capacity">New capacity in bytes.</param>
    /// <returns>
    /// OK on success; InvalidArgument if the geometry is invalid for capacity (entries are preserved); another
    /// non-OK status if the reset itself fails, which leaves the cache disabled.
    /// </returns>
    rocksdb::Status SetCapacity(size_t capacity) noexcept override;

    /// <summary>Reads the current capacity.</summary>
    /// <param name="capacity">[out] Capacity in bytes; 0 when disabled. Unchanged on failure.</param>
    /// <returns>OK, or a non-OK status if the value could not be read.</returns>
    rocksdb::Status GetCapacity(size_t& capacity) noexcept override;

    /// <summary>
    /// Not supported (same as RocksDB's default): RAM use is bounded by fixed budgets rather than rebalanced
    /// against the primary cache.
    /// </summary>
    /// <param name="decrease">Ignored.</param>
    /// <returns>NotSupported.</returns>
    rocksdb::Status Deflate(size_t decrease) noexcept override;

    /// <summary>Not supported; see Deflate.</summary>
    /// <param name="increase">Ignored.</param>
    /// <returns>NotSupported.</returns>
    rocksdb::Status Inflate(size_t increase) noexcept override;

    /// <summary>Reads live usage.</summary>
    /// <param name="usage">[out] Encoded bytes of indexed records, including records not yet flushed to disk.
    /// Unchanged on failure.</param>
    /// <returns>OK, or a non-OK status if the value could not be read.</returns>
    rocksdb::Status GetUsage(size_t& usage) const noexcept;

    /// <summary>Takes a statistics snapshot.</summary>
    /// <param name="stats">[out] Snapshot; see RegionBasedSecondaryCacheStats for consistency rules. Unchanged on
    /// failure.</param>
    /// <returns>OK, or a non-OK status if the snapshot could not be taken.</returns>
    rocksdb::Status GetStats(RegionBasedSecondaryCacheStats& stats) const noexcept;

  private:
    class Impl;
    /// Implementation; always non-null after construction.
    std::unique_ptr<Impl> m_impl;
};
} // namespace AVEVA::RocksDB::Plugin::Core
