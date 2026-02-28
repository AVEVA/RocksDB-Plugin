// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <rocksdb/secondary_cache.h>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
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
    ///   [4 bytes: CRC32 checksum]
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
        /// <param name="capacity">Maximum number of bytes of entry data stored on disk.</param>
        /// <param name="zstdLevel">zstd compression level (1–22); 1 is fastest, higher values trade CPU for ratio.</param>
        explicit FileBasedCompressedSecondaryCache(std::filesystem::path cacheDir,
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
        uint64_t GetEvictedCount() const noexcept { return m_evictedCount.load(std::memory_order_relaxed); }

    private:
        /// <summary>Single data record held by both the hash index and the sequenced LRU index.</summary>
        struct Entry
        {
            /// <summary>Maximum hex-encoded key length supported inline (covers 32-byte / 256-bit keys).</summary>
            static constexpr size_t kMaxFilenameLen = 64;
            std::array<char, kMaxFilenameLen> filenameBuf{};
            uint8_t filenameLen{0};
            size_t size{0};
            std::string_view FilenameView() const noexcept
            {
                return {filenameBuf.data(), filenameLen};
            }
        };

        using LruList = std::list<Entry>;

        /// <summary>Hashes and compares LruList iterators by their FilenameView(), enabling
        /// heterogeneous lookup from a plain std::string_view without constructing a std::string.</summary>
        struct IteratorHash
        {
            using is_transparent = void;
            size_t operator()(LruList::iterator it) const noexcept { return StringHash{}(it->FilenameView()); }
            size_t operator()(std::string_view sv) const noexcept  { return StringHash{}(sv); }
        };
        struct IteratorEqual
        {
            using is_transparent = void;
            bool operator()(LruList::iterator a, LruList::iterator b) const noexcept { return a->FilenameView() == b->FilenameView(); }
            bool operator()(std::string_view sv, LruList::iterator it) const noexcept { return sv == it->FilenameView(); }
            bool operator()(LruList::iterator it, std::string_view sv) const noexcept { return it->FilenameView() == sv; }
        };

        using Index       = boost::unordered::unordered_flat_set<LruList::iterator, IteratorHash, IteratorEqual>;
        using LruIterator = LruList::iterator;

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

        /// <summary>Hex-encodes <paramref name="key"/> into <paramref name="buf"/> and returns a view of the result.
        /// Returns an empty view when the key is too long to encode inline.</summary>
        [[nodiscard]] static std::string_view KeyToFilenameView(
            const rocksdb::Slice& key, std::array<char, Entry::kMaxFilenameLen>& buf) noexcept;

        /// <summary>Writes bytes to disk and updates the in-memory index.</summary>
        /// <param name="force_insert">When false, the write is skipped rather than evicting an existing entry to make room.</param>
        rocksdb::Status WriteEntry(const rocksdb::Slice& key,
                                   rocksdb::CompressionType type,
                                   const char* data,
                                   size_t dataSize,
                                   bool force_insert = true) noexcept;

        /// <summary>Evicts LRU entries from the back of the list until <c>m_currentSize &lt;= targetSize</c>.
        /// Each evicted file is renamed to a unique <c>.del</c> path under the lock and appended to
        /// <paramref name="pathsToDelete"/>; the caller must delete those files after releasing the lock.</summary>
        /// <remarks>Must be called with m_mutex held.</remarks>
        void EvictUntilSizeLocked(size_t targetSize, std::vector<std::filesystem::path>& pathsToDelete);

        /// <summary>Removes a single entry from the in-memory index and renames its on-disk file to a
        /// unique <c>.del</c> path. Returns the renamed path, which the caller must delete after releasing
        /// the lock. Returns an empty path if the rename fails (e.g. file already missing).</summary>
        /// <remarks>Must be called with m_mutex held.</remarks>
        [[nodiscard]] std::filesystem::path RemoveEntryLocked(LruIterator it);

        std::filesystem::path m_cacheDir;
        size_t m_capacity;
        int    m_zstdLevel;
        size_t m_currentSize{0};

        // shared_mutex used as a two-level reader-writer lock:
        //   shared_lock  — concurrent readers: GetCapacity, GetUsage, Lookup phase 1.
        //   unique_lock  — brief exclusive sections: MRU splice (Lookup phase 2),
        //                  index mutations (WriteEntry, Erase, SetCapacity).
        mutable std::shared_mutex m_mutex;

        /// <summary>Sequenced LRU list: front = MRU, back = LRU.</summary>
        LruList m_lruList;
        /// <summary>Hash index: filename → iterator into m_lruList for O(1) lookup.</summary>
        Index   m_index;

        /// <summary>Monotonically increasing counter for unique staging and graveyard file names.</summary>
        std::atomic<uint64_t> m_seq{0};
        /// <summary>Total entries evicted due to capacity pressure since construction.</summary>
        std::atomic<uint64_t> m_evictedCount{0};
    };
}
