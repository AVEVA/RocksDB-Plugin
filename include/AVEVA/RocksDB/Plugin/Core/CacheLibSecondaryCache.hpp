// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/SecondaryCacheOptions.hpp"

#include <rocksdb/secondary_cache.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>
/// RocksDB secondary cache backed by CacheLib's SSD engine (Navy BlockCache)
/// </summary>
/// <remarks>
/// - Reading a block back from the SSD does not remove it. Entries are only removed when the cache is full.
/// - This cache does not record RocksDB statistics. RocksDB's wrapper around it records the
/// <c>SECONDARY_CACHE_*</c> counters instead.
/// </remarks>
class CacheLibSecondaryCache : public rocksdb::SecondaryCache {
  public:
    /// <summary>Per-entry header: CompressionType and CacheTier bytes.</summary>
    static constexpr size_t kEntryHeaderSize = 2;

    /// <summary>Creates the cache.</summary>
    explicit CacheLibSecondaryCache(const SecondaryCacheOptions& options);

    ~CacheLibSecondaryCache() override;

    CacheLibSecondaryCache(const CacheLibSecondaryCache&) = delete;
    CacheLibSecondaryCache& operator=(const CacheLibSecondaryCache&) = delete;
    CacheLibSecondaryCache(CacheLibSecondaryCache&&) = delete;
    CacheLibSecondaryCache& operator=(CacheLibSecondaryCache&&) = delete;

    const char* Name() const noexcept override;

    /// <summary>Serializes the object and queues it for an asynchronous SSD write.
    /// <paramref name="forceInsert"/> is ignored; keys that may already exist are skipped.</summary>
    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, bool forceInsert) noexcept override;

    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, rocksdb::CompressionType type,
                                rocksdb::CacheTier source) noexcept override;

    /// <summary>Looks up <paramref name="key"/>. With <paramref name="wait"/> false the read is issued
    /// asynchronously and the object is created on the thread that waits on the handle.</summary>
    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* create_context, bool wait, bool advise_erase, rocksdb::Statistics* stats,
           bool& kept_in_sec_cache) noexcept override;

    bool SupportForceErase() const noexcept override { return false; }

    void Erase(const rocksdb::Slice& key) noexcept override;

    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept override;

    /// <summary>Not supported: the cache file size is fixed at construction and can't be resized at runtime.</summary>
    rocksdb::Status SetCapacity(size_t capacity) noexcept override;

    /// <summary>Returns the usable size of the cache file.</summary>
    rocksdb::Status GetCapacity(size_t& capacity) noexcept override;

    /// <summary>Not supported.</summary>
    rocksdb::Status Deflate(size_t decrease) noexcept override;

    /// <summary>Not supported.</summary>
    rocksdb::Status Inflate(size_t increase) noexcept override;

    std::string GetPrintableOptions() const override;

    /// <summary>Visits every CacheLib (Navy) counter, e.g. "navy_bc_inserts".</summary>
    void VisitCounters(const std::function<void(std::string_view name, double value)>& visitor) const;

    /// <summary>Waits until all queued inserts/removes have been applied. Used by tests and by db_bench before it reads
    /// the counters.</summary>
    void Drain() noexcept;

  private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};
} // namespace AVEVA::RocksDB::Plugin::Core
