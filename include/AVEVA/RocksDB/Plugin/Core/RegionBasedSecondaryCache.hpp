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

/// Operational counters and a synchronized snapshot of cache layout/resources.
struct RegionBasedSecondaryCacheStats {
    size_t liveEntries{0};
    size_t liveBytes{0};
    size_t capacityBytes{0};
    size_t fileBytes{0};     // Logical file lengths, not reserved disk space or page-cache residency.
    size_t metadataBytes{0}; // Allocator-requested bytes, including buckets and reverse-list capacity.
    size_t metadataLimit{0};
    size_t activeBufferBytes{0};
    size_t transientBytes{0};
    size_t openHandles{0};
    uint64_t inserts{0};
    uint64_t admitted{0};
    uint64_t droppedTooLarge{0};
    uint64_t droppedPressure{0};
    uint64_t droppedDisabled{0};
    uint64_t lookups{0};
    uint64_t hits{0};
    uint64_t corruptions{0};
    uint64_t keyMismatches{0};
    uint64_t ioErrors{0};
    uint64_t errors{0};
    uint64_t reclaims{0};
    uint64_t payloadBytes{0};
    uint64_t encodedBytes{0};
    uint64_t deviceWriteBytes{0};
    uint64_t deviceReadBytes{0};
};

/// <summary>
/// Synchronous region-based secondary cache with bounded memory and FIFO region recycling.
/// It owns a dedicated child directory beneath the supplied parent and starts empty.
/// </summary>
class RegionBasedSecondaryCache final : public rocksdb::SecondaryCache {
  public:
    static constexpr size_t kDefaultCapacity = 512ULL * 1024 * 1024;

    /// Constructs the cache after validating options and the non-null logger.
    /// Owns only cacheDir/ssd-secondary-cache-v1; unrelated parent contents are untouched.
    /// Existing owned region files are discarded. Files grow on write, without reservation.
    explicit RegionBasedSecondaryCache(
        std::filesystem::path cacheDir, RegionBasedSecondaryCacheOptions options,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);

    ~RegionBasedSecondaryCache() override;
    RegionBasedSecondaryCache(const RegionBasedSecondaryCache&) = delete;
    RegionBasedSecondaryCache& operator=(const RegionBasedSecondaryCache&) = delete;
    RegionBasedSecondaryCache(RegionBasedSecondaryCache&&) = delete;
    RegionBasedSecondaryCache& operator=(RegionBasedSecondaryCache&&) = delete;

    const char* Name() const noexcept override;

    /// Serializes an object with CacheItemHelper callbacks and appends its record.
    /// Both forceInsert values permit FIFO recycling; neither bypasses size or memory limits.
    /// Disabled, oversize and budget-pressure admissions are counted best-effort drops.
    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, bool forceInsert) noexcept override;
    /// Appends already-serialized bytes while preserving their compression and source-tier tags.
    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, rocksdb::CompressionType type,
                                rocksdb::CacheTier source) noexcept override;
    /// Synchronously looks up a record, validates it, and returns a ready handle on a hit.
    /// Both wait values are synchronous. The caller owns the successfully created value;
    /// destroying its handle does not delete it. advise_erase removes only the record read.
    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* create_context, bool wait, bool advise_erase, rocksdb::Statistics* stats,
           bool& kept_in_sec_cache) noexcept override;
    bool SupportForceErase() const noexcept override { return true; }
    /// Removes the current index mapping; disk bytes are reclaimed with their region.
    void Erase(const rocksdb::Slice& key) noexcept override;
    /// No-op: all returned handles are immediately ready.
    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept override;
    /// Changes capacity by cold-resetting all entries and buffers.
    /// Invalid geometry preserves entries; failure after reset leaves the cache disabled.
    rocksdb::Status SetCapacity(size_t capacity) noexcept override;
    rocksdb::Status GetCapacity(size_t& capacity) noexcept override;

    // Required by RocksDB's interface; the region cache does not rebalance RAM.
    rocksdb::Status Deflate(size_t decrease) noexcept override;
    rocksdb::Status Inflate(size_t increase) noexcept override;

    /// Returns live encoded bytes, including records not yet flushed to disk.
    rocksdb::Status GetUsage(size_t& usage) const noexcept;
    rocksdb::Status GetStats(RegionBasedSecondaryCacheStats& stats) const noexcept;

  private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};
} // namespace AVEVA::RocksDB::Plugin::Core
