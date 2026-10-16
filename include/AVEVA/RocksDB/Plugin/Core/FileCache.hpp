// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <boost/intrusive/list.hpp>
#include <boost/log/trivial.hpp>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <queue>
#include <condition_variable>
#include <stop_token>
namespace AVEVA::RocksDB::Plugin::Core
{
    // Lock-free counters for measuring FileCache effectiveness. These are kept
    // as plain atomics (rather than behind m_mutex) since they're incremented
    // from both the calling thread (ReadFile) and the background downloader
    // thread, and approximate counts are fine for benchmarking purposes.
    struct FileCacheStats
    {
        // Plain-value copy returned by FileCache::GetStats(), safe to read
        // without any synchronization.
        struct Snapshot
        {
            uint64_t hits = 0;
            uint64_t missesNotFound = 0;
            uint64_t missesNotReady = 0;
            uint64_t bytesServedFromCache = 0;
            uint64_t downloadsCompleted = 0;
            uint64_t downloadsFailed = 0;
            uint64_t bytesDownloaded = 0;
            uint64_t evictions = 0;
            uint64_t bytesEvicted = 0;
        };

        std::atomic<uint64_t> hits{0};
        std::atomic<uint64_t> missesNotFound{0};
        std::atomic<uint64_t> missesNotReady{0};
        std::atomic<uint64_t> bytesServedFromCache{0};
        std::atomic<uint64_t> downloadsCompleted{0};
        std::atomic<uint64_t> downloadsFailed{0};
        std::atomic<uint64_t> bytesDownloaded{0};
        std::atomic<uint64_t> evictions{0};
        std::atomic<uint64_t> bytesEvicted{0};

        Snapshot GetSnapshot() const noexcept;
        void Reset() noexcept;
    };

    class FileCache
    {
        std::filesystem::path m_cachePath;
        int64_t m_maxSize;
        std::shared_ptr<ContainerClient> m_containerClient;
        std::shared_ptr<Filesystem> m_filesystem;
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

        std::mutex m_mutex;
        std::stop_source m_stopSource;
        std::condition_variable m_cv;
        std::unordered_map<std::string, FileCacheEntry, StringHash, StringEqual> m_cache;
        std::queue<std::string> m_fileDownloadQueue;
        boost::intrusive::list<FileCacheEntry, boost::intrusive::constant_time_size<false>> m_entryList;
        std::jthread m_backgroundDownloader;
        FileCacheStats m_stats;
    public:
        FileCache(std::filesystem::path cachePath,
            int64_t maxCacheSize,
            std::shared_ptr<ContainerClient> containerClient,
            std::shared_ptr<Filesystem> filesystem,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
        ~FileCache();
        FileCache(const FileCache&) = delete;
        FileCache& operator=(const FileCache&) = delete;
        FileCache(FileCache&&) = delete;
        FileCache& operator=(FileCache&&) = delete;

        [[nodiscard]] bool HasFile(std::string_view filePath);
        void MarkFileAsStaleIfExists(const std::string& filePath);
        [[nodiscard]] std::optional<int64_t> ReadFile(std::string_view filePath, int64_t offset, int64_t bytesToRead, char* buffer);
        void RemoveFile(std::string_view filePath);
        [[nodiscard]] int64_t CacheSize();
        void SetCacheSize(int64_t size);

        // Returns a point-in-time copy of the cache's hit/miss/download/eviction
        // counters, intended for benchmarking (e.g. to compare full-SST vs.
        // partial-SST download strategies). See FileCacheStats for field meanings.
        [[nodiscard]] FileCacheStats::Snapshot GetStats() const noexcept;

        // Zeroes all counters. Useful for excluding a benchmark's warm-up phase
        // from the measured counts.
        void ResetStats() noexcept;
    private:
        void BackgroundDownload(std::stop_token stopToken);
        void EntryAccessedUnsafe(FileCacheEntry& file);
        void PruneUndownloadedStaleEntriesUnsafe();
        bool EvictAtLeast(int64_t bytes);
        void RemoveFileUnsafe(std::string_view filePath);
        size_t CountUndownloadedStaleEntriesUnsafe() const noexcept;
        int64_t GetCurrentSizeUnsafe() const noexcept;
    };
}
