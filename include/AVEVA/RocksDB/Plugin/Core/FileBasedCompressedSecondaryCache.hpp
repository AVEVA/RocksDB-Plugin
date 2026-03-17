// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"


#include <rocksdb/secondary_cache.h>

#include <boost/static_string.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <memory>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>
    /// A file-based secondary cache for RocksDB that persists evicted block cache
    /// entries to disk. Entries serialized via the CacheItemHelper callbacks are
    /// written to individual files under a configurable directory and are evicted
    /// from disk using an LRU policy once the configured capacity is exceeded.
    /// </summary>
    /// <remarks>
    /// File format per entry:
    ///   [8 bytes: magic]
    ///   [1 byte:  version]
    ///   [1 byte:  CompressionType]
    ///   [8 bytes: data length]
    ///   [4 bytes: CRC32C checksum of CompressionType + data length + data]
    ///   [N bytes: data]
    /// </remarks>
    class FileBasedCompressedSecondaryCache final : public rocksdb::SecondaryCache
    {
    public:
        static constexpr size_t kDefaultCapacity = 512ULL * 1024 * 1024; // 512 MiB
        static constexpr int kDefaultZstdLevel = 1;

        /// <summary>
        /// On-disk overhead per entry.
        /// </summary>
        static constexpr size_t kFileHeaderSize = 22;

        /// <summary>
        /// Constructs the cache.
        /// </summary>
        /// <param name="cacheDir">Cache directory; will be created if it does not already exist.</param>
        /// <param name="fs">
        /// Filesystem implementation used for all I/O.  Pass a
        /// <c>LocalFilesystem</c> for production use or a mock for testing.
        /// </param>
        /// <param name="capacity">Maximum number of bytes of entry data stored on disk.</param>
        /// <param name="zstdLevel">zstd compression level (1–22); 1 is fastest, higher values trade CPU for ratio.</param>
        /// <param name="logger">A logger implementation.</param>
        explicit FileBasedCompressedSecondaryCache(std::filesystem::path cacheDir,
            std::shared_ptr<Filesystem> fs,
            size_t capacity,
            int zstdLevel,
            std::shared_ptr<boost::log::sources::severity_logger_mt<
            boost::log::trivial::severity_level>> logger);

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
        /// If true, evict existing entries to make room; if false,
        /// skip insertion when there is insufficient capacity.
        /// </param>
        rocksdb::Status Insert(const rocksdb::Slice& key,
            rocksdb::Cache::ObjectPtr obj,
            const rocksdb::Cache::CacheItemHelper* helper,
            bool forceInsert) noexcept override;

        /// <summary>Writes pre-serialized data (possibly already compressed) to disk.</summary>
        /// <param name="key">The key identifying the cache entry.</param>
        /// <param name="saved">A slice containing the pre-serialized (and possibly already compressed) payload to store.</param>
        /// <param name="type">The compression type of the provided payload.</param>
        /// <param name="source">The cache tier that produced the saved payload (for accounting or metadata purposes).</param>
        rocksdb::Status InsertSaved(const rocksdb::Slice& key,
            const rocksdb::Slice& saved,
            rocksdb::CompressionType type,
            rocksdb::CacheTier source) noexcept override;

        /// <summary>Looks up <paramref name="key"/> on disk and reconstructs the object via create_cb.</summary>
        /// <param name="key">The key to look up in the secondary cache.</param>
        /// <param name="helper">Callbacks used to reconstruct the cached object from stored bytes.</param>
        /// <param name="create_context">Context provided by RocksDB used when creating the object (may be used by the helper).</param>
        /// <param name="wait">If true, wait for the lookup to complete; otherwise return immediately if not ready.</param>
        /// <param name="advise_erase">If true, the caller advises that the found entry should be erased after use.</param>
        /// <param name="stats">Optional RocksDB statistics object to record secondary cache metrics.</param>
        /// <param name="kept_in_sec_cache">Out-parameter set to true when the object was reconstructed and will be retained in the secondary cache.</param>
        /// <returns>An immediately-ready result handle, or nullptr if the key is not found.</returns>
        std::unique_ptr<rocksdb::SecondaryCacheResultHandle> Lookup(
            const rocksdb::Slice& key,
            const rocksdb::Cache::CacheItemHelper* helper,
            rocksdb::Cache::CreateContext* create_context,
            bool wait,
            bool advise_erase,
            rocksdb::Statistics* stats,
            bool& kept_in_sec_cache) noexcept override;

        bool SupportForceErase() const noexcept override { return true; }

        /// <summary>Erase any persisted entry identified by <paramref name="key"/> from the secondary cache.</summary>
        /// <param name="key">The key identifying the cache entry to remove.</param>
        void Erase(const rocksdb::Slice& key) noexcept override;

        /// <summary>All handles returned by Lookup() are immediately ready; this is a no-op.</summary>
        /// <param name="handles">A vector of result handles to wait on (ignored by this implementation).</param>
        void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept override;

        /// <summary>Set the maximum capacity of the on-disk cache.</summary>
        /// <param name="capacity">New capacity in bytes.</param>
        rocksdb::Status SetCapacity(size_t capacity) noexcept override;

        /// <summary>Get the current maximum capacity of the on-disk cache.</summary>
        /// <param name="capacity">Out-parameter receiving the capacity in bytes.</param>
        rocksdb::Status GetCapacity(size_t& capacity) noexcept override;

        /// <summary>Decrease the configured capacity by <paramref name="decrease"/> bytes.</summary>
        /// <param name="decrease">Amount to reduce capacity by in bytes.</param>
        rocksdb::Status Deflate(size_t decrease) noexcept override;

        /// <summary>Increase the configured capacity by <paramref name="increase"/> bytes.</summary>
        /// <param name="increase">Amount to increase capacity by in bytes.</param>
        rocksdb::Status Inflate(size_t increase) noexcept override;

        /// <summary>Returns the number of bytes currently stored on disk.</summary>
        /// <param name="usage">Out-parameter receiving the number of bytes currently used by stored entries.</param>
        rocksdb::Status GetUsage(size_t& usage) const noexcept;

        /// <summary>Returns the total number of entries evicted due to capacity pressure since construction.</summary>
        uint64_t GetEvictedCount() const noexcept;
    private:
        class Impl;
        std::unique_ptr<Impl> m_impl;
    };
}
