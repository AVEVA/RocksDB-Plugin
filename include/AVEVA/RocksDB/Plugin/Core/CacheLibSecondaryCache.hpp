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
/// RocksDB secondary cache backed by CacheLib's flash engine (Navy BlockCache)
/// without a CacheLib DRAM tier. Designed for the classic
/// <c>LRUCacheOptions::secondary_cache</c> integration:
/// <list type="bullet">
/// <item>Entries stay on SSD after a hit (<c>SupportForceErase()</c> is false, <c>kept_in_sec_cache</c> is true),
/// so RocksDB never writes the same block to SSD twice.</item>
/// <item>Once full, the oldest region is evicted to admit new entries.</item>
/// <item>The cache records no RocksDB statistics; the adapter records SECONDARY_CACHE_* ticks.</item>
/// <item>Contents do not survive a restart; the cache file is reset on construction.</item>
/// </list>
/// Only available when built with <c>AVEVA_ROCKSDB_WITH_CACHELIB</c> (Linux).
/// </summary>
class CacheLibSecondaryCache final : public rocksdb::SecondaryCache {
  public:
    /// <summary>Per-entry header: CompressionType and CacheTier bytes.</summary>
    static constexpr size_t kEntryHeaderSize = 2;

    /// <summary>Creates the cache.</summary>
    /// <exception cref="std::invalid_argument">Invalid options.</exception>
    /// <exception cref="std::runtime_error">The cache file could not be created.</exception>
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

    /// <summary>Not supported: the cache file size is fixed at construction.</summary>
    rocksdb::Status SetCapacity(size_t capacity) noexcept override;

    /// <summary>Returns the usable size of the cache file.</summary>
    rocksdb::Status GetCapacity(size_t& capacity) noexcept override;

    /// <summary>Not supported.</summary>
    rocksdb::Status Deflate(size_t decrease) noexcept override;

    /// <summary>Not supported.</summary>
    rocksdb::Status Inflate(size_t increase) noexcept override;

    std::string GetPrintableOptions() const override;

    /// <summary>Approximate number of bytes currently occupied on SSD.</summary>
    rocksdb::Status GetUsage(size_t& usage) const noexcept;

    /// <summary>Visits every CacheLib (Navy) counter, e.g. "navy_bc_inserts".</summary>
    void VisitCounters(const std::function<void(std::string_view name, double value)>& visitor) const;

    /// <summary>Waits until all queued inserts/removes have been applied. Intended for tests.</summary>
    void Drain() noexcept;

  private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};
} // namespace AVEVA::RocksDB::Plugin::Core
