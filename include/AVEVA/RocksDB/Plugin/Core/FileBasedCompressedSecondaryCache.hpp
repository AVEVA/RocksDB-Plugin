// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"

#include <rocksdb/secondary_cache.h>

#include <boost/log/trivial.hpp>
#include <boost/static_string.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core {
enum class FileBasedSecondaryCacheAdmissionPolicy : uint8_t {
    kAdmitAll,
    kSecondChance,
};

enum class FileBasedSecondaryCacheEngine : uint8_t {
    kLegacy,
    kRegion,
};

struct FileBasedSecondaryCacheOptions {
    size_t capacity{512ULL * 1024 * 1024};
    size_t regionSize{64ULL * 1024 * 1024};
    size_t flushBlockSize{1ULL * 1024 * 1024};
    uint32_t indexShards{64};
    size_t maxEntrySize{16ULL * 1024 * 1024};
    FileBasedSecondaryCacheAdmissionPolicy admissionPolicy{FileBasedSecondaryCacheAdmissionPolicy::kAdmitAll};
    FileBasedSecondaryCacheEngine engine{FileBasedSecondaryCacheEngine::kLegacy};
    // Region-only heap limits. Zero metadata budget selects max(1 MiB, capacity / 200).
    size_t metadataBudget{0};
    size_t transientBudget{64ULL * 1024 * 1024};
};

struct FileBasedSecondaryCacheStats {
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
/// A file-based secondary cache for RocksDB that persists evicted block cache
/// entries to disk. Entries serialized via the CacheItemHelper callbacks are
/// The default legacy engine uses one file per entry and LRU eviction.
/// The opt-in region engine uses buffered sequential records and FIFO eviction.
/// Neither engine provides durable storage.
/// </summary>
class FileBasedCompressedSecondaryCache final : public rocksdb::SecondaryCache {
  public:
    static constexpr size_t kDefaultCapacity = 512ULL * 1024 * 1024; // 512 MiB

    /// <summary>
    /// Legacy-only per-entry on-disk overhead: one byte stores the RocksDB CompressionType
    /// of the payload so it can be faithfully restored on lookup.
    /// </summary>
    static constexpr size_t kFileHeaderSize = 1;

    /// <summary>
    /// Constructs the cache.
    /// </summary>
    /// <param name="cacheDir">Cache directory; will be created if it does not already exist.</param>
    /// <param name="fs">
    /// Filesystem implementation used for all I/O.  Pass a
    /// <c>LocalFilesystem</c> for production use or a mock for testing.
    /// </param>
    /// <param name="capacity">Maximum number of bytes of entry data stored on disk.</param>
    /// <param name="logger">A logger implementation.</param>
    explicit FileBasedCompressedSecondaryCache(
        std::filesystem::path cacheDir, std::shared_ptr<Filesystem> fs, size_t capacity,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);

    /// <summary>
    /// Constructs the cache with explicit engine and region-engine options.
    /// The default engine remains the legacy file-per-entry implementation.
    /// Region mode owns only the ssd-secondary-cache-v1 child of cacheDir; it starts
    /// empty, supports admit-all only, and allocates disk space on write (not reserved).
    /// indexShards is retained for compatibility but unused by the single-lock engine.
    /// </summary>
    explicit FileBasedCompressedSecondaryCache(
        std::filesystem::path cacheDir, FileBasedSecondaryCacheOptions options,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);

    ~FileBasedCompressedSecondaryCache() override;
    FileBasedCompressedSecondaryCache(const FileBasedCompressedSecondaryCache&) = delete;
    FileBasedCompressedSecondaryCache& operator=(const FileBasedCompressedSecondaryCache&) = delete;
    FileBasedCompressedSecondaryCache(FileBasedCompressedSecondaryCache&&) = delete;
    FileBasedCompressedSecondaryCache& operator=(FileBasedCompressedSecondaryCache&&) = delete;

    /// <summary>
    /// The name of this class of Customizable.
    /// </summary>
    /// <returns>The name of this class.</returns>
    const char* Name() const noexcept override;

    /// <summary>
    /// Serializes <paramref name="obj"/> via <paramref name="helper"/>
    /// callbacks and writes it to disk.
    /// </summary>
    /// <param name="key">The key identifying the cache entry.</param>
    /// <param name="obj">Pointer to the cached object to serialize and store.</param>
    /// <param name="helper">Callbacks used to serialize the object and obtain its size/charge.</param>
    /// <param name="forceInsert">
    /// Legacy: false skips insertion when full. Region: either value permits FIFO
    /// recycling; force never bypasses size or memory bounds.
    /// </param>
    rocksdb::Status Insert(const rocksdb::Slice& key, rocksdb::Cache::ObjectPtr obj,
                           const rocksdb::Cache::CacheItemHelper* helper, bool forceInsert) noexcept override;

    /// <summary>Writes pre-serialized data (possibly already compressed by RocksDB) to disk.</summary>
    /// <param name="key">The key identifying the cache entry.</param>
    /// <param name="saved">A slice containing the pre-serialized payload to store.</param>
    /// <param name="type">The compression type of the provided payload.</param>
    /// <param name="source">The cache tier that produced the saved payload.</param>
    rocksdb::Status InsertSaved(const rocksdb::Slice& key, const rocksdb::Slice& saved, rocksdb::CompressionType type,
                                rocksdb::CacheTier source) noexcept override;

    /// <summary>Looks up <paramref name="key"/> on disk and reconstructs the object via create_cb.</summary>
    /// <param name="key">The key to look up in the secondary cache.</param>
    /// <param name="helper">Callbacks used to reconstruct the cached object from stored bytes.</param>
    /// <param name="create_context">Context provided by RocksDB, used when creating the object.</param>
    /// <param name="wait">Both values currently perform a synchronous lookup and return a ready handle.</param>
    /// <param name="advise_erase">If true, advises that the found entry should be erased after use.</param>
    /// <param name="stats">Optional RocksDB statistics object to record secondary cache metrics.</param>
    /// <param name="kept_in_sec_cache">Set to true when the object will be retained in the secondary cache.</param>
    /// <returns>An immediately-ready result handle, or nullptr if the key is not found.</returns>
    std::unique_ptr<rocksdb::SecondaryCacheResultHandle>
    Lookup(const rocksdb::Slice& key, const rocksdb::Cache::CacheItemHelper* helper,
           rocksdb::Cache::CreateContext* create_context, bool wait, bool advise_erase, rocksdb::Statistics* stats,
           bool& kept_in_sec_cache) noexcept override;

    bool SupportForceErase() const noexcept override { return true; }

    /// <summary>Erase any persisted entry identified by <paramref name="key"/> from the secondary cache.</summary>
    /// <param name="key">The key identifying the cache entry to remove.</param>
    void Erase(const rocksdb::Slice& key) noexcept override;

    /// <summary>All handles returned by Lookup() are immediately ready; this is a no-op.</summary>
    /// <param name="handles">A vector of result handles to wait on (ignored by this implementation).</param>
    void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept override;

    /// <summary>Set disk capacity. Region mode discards all entries; resize failure leaves it disabled.</summary>
    /// <param name="capacity">New capacity in bytes.</param>
    rocksdb::Status SetCapacity(size_t capacity) noexcept override;

    /// <summary>Get the current maximum capacity of the on-disk cache.</summary>
    /// <param name="capacity">Out-parameter receiving the capacity in bytes.</param>
    rocksdb::Status GetCapacity(size_t& capacity) noexcept override;

    /// <summary>Legacy decreases disk capacity; region RAM rebalancing is NotSupported.</summary>
    /// <param name="decrease">Amount to reduce capacity by in bytes.</param>
    rocksdb::Status Deflate(size_t decrease) noexcept override;

    /// <summary>Legacy increases disk capacity; region RAM rebalancing is NotSupported.</summary>
    /// <param name="increase">Amount to increase capacity by in bytes.</param>
    rocksdb::Status Inflate(size_t increase) noexcept override;

    /// <summary>Returns the number of bytes currently stored on disk.</summary>
    /// <param name="usage">Out-parameter receiving the number of bytes currently used by stored entries.</param>
    rocksdb::Status GetUsage(size_t& usage) const noexcept;

    /// <summary>Region observability snapshot; NotSupported for legacy.</summary>
    rocksdb::Status GetStats(FileBasedSecondaryCacheStats& stats) const noexcept;

  private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};
} // namespace AVEVA::RocksDB::Plugin::Core
