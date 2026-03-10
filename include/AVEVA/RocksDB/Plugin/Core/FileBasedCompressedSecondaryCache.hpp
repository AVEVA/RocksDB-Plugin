// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/LruFileIndex.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"

#include <rocksdb/secondary_cache.h>

#include <boost/static_string.hpp>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

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
        static constexpr size_t kDefaultCapacity  = 512ULL * 1024 * 1024; // 512 MiB
        static constexpr int    kDefaultZstdLevel  = 1;

        /// <summary>On-disk overhead per entry: magic(8) + version(1) + compressionType(1) + dataSize(8) + checksum(4).</summary>
        static constexpr size_t kFileHeaderSize = 22;

        /// <summary>Constructs the cache.</summary>
        /// <param name="cacheDir">Cache directory; will be created if it does not already exist.</param>
        /// <param name="fs">Filesystem implementation used for all I/O.  Pass a
        /// <c>LocalFilesystem</c> for production use or a mock for testing.</param>
        /// <param name="capacity">Maximum number of bytes of entry data stored on disk.</param>
        /// <param name="zstdLevel">zstd compression level (1–22); 1 is fastest, higher values trade CPU for ratio.</param>
        explicit FileBasedCompressedSecondaryCache(std::filesystem::path cacheDir,
                                                   std::shared_ptr<Filesystem> fs,
                                                   size_t capacity  = kDefaultCapacity,
                                                   int    zstdLevel = kDefaultZstdLevel);

        ~FileBasedCompressedSecondaryCache() override = default;

        FileBasedCompressedSecondaryCache(const FileBasedCompressedSecondaryCache&) = delete;
        FileBasedCompressedSecondaryCache& operator=(const FileBasedCompressedSecondaryCache&) = delete;
        FileBasedCompressedSecondaryCache(FileBasedCompressedSecondaryCache&&) = delete;
        FileBasedCompressedSecondaryCache& operator=(FileBasedCompressedSecondaryCache&&) = delete;

        const char* Name() const noexcept override { return "FileBasedCompressedSecondaryCache"; }

        /// <summary>Serializes <paramref name="obj"/> via helper callbacks and writes it to disk.</summary>
        rocksdb::Status Insert(const rocksdb::Slice& key,
                               rocksdb::Cache::ObjectPtr obj,
                               const rocksdb::Cache::CacheItemHelper* helper,
                               bool force_insert) noexcept override;

        /// <summary>Writes pre-serialized data (possibly already compressed) to disk.</summary>
        rocksdb::Status InsertSaved(const rocksdb::Slice& key,
                                    const rocksdb::Slice& saved,
                                    rocksdb::CompressionType type,
                                    rocksdb::CacheTier source) noexcept override;

        /// <summary>Looks up <paramref name="key"/> on disk and reconstructs the object via create_cb.</summary>
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

        void Erase(const rocksdb::Slice& key) noexcept override;

        /// <summary>All handles returned by Lookup() are immediately ready; this is a no-op.</summary>
        void WaitAll(std::vector<rocksdb::SecondaryCacheResultHandle*> handles) noexcept override;

        rocksdb::Status SetCapacity(size_t capacity) noexcept override;
        rocksdb::Status GetCapacity(size_t& capacity) noexcept override;
        rocksdb::Status Deflate(size_t decrease) noexcept override;
        rocksdb::Status Inflate(size_t increase) noexcept override;

        /// <summary>Returns the number of bytes currently stored on disk.</summary>
        rocksdb::Status GetUsage(size_t& usage) const noexcept;

        /// <summary>Returns the total number of entries evicted due to capacity pressure since construction.</summary>
        uint64_t GetEvictedCount() const noexcept { return m_lruIndex.GetEvictedCount(); }

    private:
        /// <summary>Immediately-ready result handle returned by Lookup().</summary>
        class ResultHandle final : public rocksdb::SecondaryCacheResultHandle
        {
        public:
            ResultHandle(rocksdb::Cache::ObjectPtr value, size_t charge)
                : m_value(value), m_charge(charge) {}

            ~ResultHandle() override = default;
            ResultHandle(const ResultHandle&) = delete;
            ResultHandle& operator=(const ResultHandle&) = delete;
            ResultHandle(ResultHandle&&) = delete;
            ResultHandle& operator=(ResultHandle&&) = delete;

            bool IsReady() noexcept override { return true; }
            void Wait() noexcept override {}
            rocksdb::Cache::ObjectPtr Value() noexcept override { return std::exchange(m_value, nullptr); }
            size_t Size() noexcept override { return m_charge; }

        private:
            rocksdb::Cache::ObjectPtr m_value;
            size_t m_charge;
        };

        /// <summary>Hex-encodes <paramref name="key"/> and returns the result.
        /// Returns an empty string when the key is too long to encode inline.</summary>
        [[nodiscard]] static boost::static_string<LruFileIndex::kMaxFilenameLen> KeyToFilename(
            const rocksdb::Slice& key) noexcept;

        /// <summary>
        /// Returns true when the hex-encoded key would exceed the inline filename buffer.
        /// Each input byte is encoded as two hex characters, so the hex-encoded length is
        /// twice the key length; the check uses <c>key.size() * 2</c> to reflect this.
        /// </summary>
        [[nodiscard]] static bool IsKeyTooLong(const rocksdb::Slice& key) noexcept
        {
            return key.size() * 2 > LruFileIndex::kMaxFilenameLen;
        }

        /// <summary>Writes bytes to disk and updates the in-memory index.</summary>
        /// <param name="force_insert">When false, the write is skipped rather than evicting an existing entry to make room.</param>
        rocksdb::Status WriteEntry(const rocksdb::Slice& key,
                                   rocksdb::CompressionType type,
                                   const char* data,
                                   size_t dataSize,
                                   bool force_insert = true) noexcept;

        /// <summary>
        /// Phase 2 of WriteEntry — called with no lock held.
        /// Builds the on-disk header, computes the CRC32C checksum, concatenates header
        /// and payload into a single buffer, and writes it atomically to pathStr.
        /// </summary>
        [[nodiscard]] rocksdb::Status WriteToDisk(std::string_view filename,
                                                  rocksdb::CompressionType type,
                                                  const char* data,
                                                  size_t dataSize,
                                                  size_t storedSize);

        /// <summary>Phase 1 of Lookup — pins the entry to prevent eviction during I/O, then maps the file.
        /// The three outcomes are named explicitly in MapEntryResult::Status.</summary>
        struct MapEntryResult
        {
            enum class Status { Miss, Corrupt, Ok };
            Status                          status;
            std::unique_ptr<MappedFileView> view; // non-null only when status == Ok
        };

        [[nodiscard]] MapEntryResult
            MapEntryForRead(std::string_view filename, const std::string& pathStr);

        /// <summary>Removes a corrupt or missing entry from the in-memory index and commits the eviction.
        /// Best-effort; never throws.</summary>
        void CleanupCorruptEntry(std::string_view filename) noexcept;

        /// <summary>
        /// Validated and parsed fields extracted from a mapped cache entry's on-disk header.
        /// </summary>
        struct ParsedHeader
        {
            rocksdb::CompressionType compressionType;

            /// <summary>
            /// points into the mapped file, valid while the view is alive
            /// </summary>
            std::span<const char> payload;
        };

        /// <summary>Validates the on-disk header of a mapped cache entry.
        /// Returns std::nullopt on any mismatch (magic, version, bounds, or checksum).</summary>
        [[nodiscard]] static std::optional<ParsedHeader> ValidateHeader(
            const char* mapped, size_t mappedSize) noexcept;

        /// <summary>Records a secondary cache hit to RocksDB's statistics subsystem.</summary>
        static void RecordHitStats(rocksdb::Statistics* stats, rocksdb::CacheEntryRole role) noexcept;

        std::filesystem::path m_cacheDir;
        std::shared_ptr<Filesystem> m_fs;
        int m_zstdLevel;
        LruFileIndex m_lruIndex;
    };
}
