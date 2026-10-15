// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <boost/intrusive/list.hpp>
#include <boost/log/trivial.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Core {
class FileCache {
    std::filesystem::path m_cachePath;
    int64_t m_maxSize;
    std::shared_ptr<ContainerClient> m_containerClient;
    std::shared_ptr<Filesystem> m_filesystem;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::condition_variable m_idleCv;
    bool m_downloadInFlight = false;
    std::unordered_map<std::string, FileCacheEntry, StringHash, StringEqual> m_cache;
    std::queue<std::string> m_fileDownloadQueue;
    boost::intrusive::list<FileCacheEntry, boost::intrusive::constant_time_size<false>> m_entryList;

    // Files whose entries were removed, and handles that must be closed. They are released on the background
    // thread, outside m_mutex (disk I/O must not stall readers) and serialized with downloads so a delete can
    // never land on a file that has since been downloaded again.
    struct PendingRelease {
        std::string FilePath; // The cache key; the file is m_cachePath / FilePath.
        std::shared_ptr<CachedReadHandle> Handle;
        bool RemoveFromDisk;
    };
    std::vector<PendingRelease> m_pendingReleases;
    // Cache keys whose delete failed; retried whenever the background thread wakes, unless re-cached meanwhile.
    std::vector<std::string> m_failedDeletes;

    // Running totals kept in step with the entries (see UpdateEntryUnsafe) so neither is a list scan.
    int64_t m_activeBytes = 0;
    size_t m_undownloadedStaleCount = 0;
    std::jthread m_backgroundDownloader;

  public:
    FileCache(std::filesystem::path cachePath, int64_t maxCacheSize, std::shared_ptr<ContainerClient> containerClient,
              std::shared_ptr<Filesystem> filesystem,
              std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
    ~FileCache();
    FileCache(const FileCache&) = delete;
    FileCache& operator=(const FileCache&) = delete;
    FileCache(FileCache&&) = delete;
    FileCache& operator=(FileCache&&) = delete;

    [[nodiscard]] bool HasFile(std::string_view filePath);
    void MarkFileAsStaleIfExists(const std::string& filePath);
    [[nodiscard]] std::optional<int64_t> ReadFile(std::string_view filePath, int64_t offset, int64_t bytesToRead,
                                                  char* buffer);
    void RemoveFile(std::string_view filePath);
    [[nodiscard]] int64_t CacheSize();
    void SetCacheSize(int64_t size);
    // Blocks until the background downloader has no queued or in-flight work. Lets tests assert that
    // something did NOT happen without guessing how long to wait.
    void WaitUntilIdle();
    // Same as WaitUntilIdle, but gives up after `timeout` (a guard against hangs) and reports whether the
    // downloader went idle.
    [[nodiscard]] bool WaitUntilIdleForTesting(std::chrono::milliseconds timeout);

  private:
    void BackgroundDownload(std::stop_token stopToken);
    void EntryAccessedUnsafe(FileCacheEntry& file);
    void PruneUndownloadedStaleEntriesUnsafe();
    bool EvictAtLeast(int64_t bytes);
    void RemoveFileUnsafe(std::string_view filePath);
    // Applies `mutate` to the entry and keeps the running totals consistent with its new state and size.
    template <typename F> void UpdateEntryUnsafe(FileCacheEntry& entry, F&& mutate, bool removing = false);
    void ReleaseOnBackgroundThread(std::vector<PendingRelease>& releases);
    bool TryDeleteCachedFile(const std::string& filePath);
    void ValidateCountersUnsafe() const;
    int64_t GetCurrentSizeUnsafe() const noexcept;
};
} // namespace AVEVA::RocksDB::Plugin::Core
