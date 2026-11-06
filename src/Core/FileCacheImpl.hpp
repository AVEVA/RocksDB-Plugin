// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCacheTestAccess.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"
#include "FileCacheEntry.hpp"

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
#include <span>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Core {
// The state behind FileCache. It lives on the heap behind the public class, so the background thread's `this` never
// moves and the LRU bookkeeping stays out of the installed headers.
class FileCacheImpl {
    enum class EvictionOutcome : std::uint8_t {
        Sufficient,
        Insufficient,
    };
    enum class RemovalOutcome : std::uint8_t {
        Removed,
        NotRemoved,
    };
    enum class DeleteOutcome : std::uint8_t {
        Deleted,
        Failed,
    };
    enum class DiskFate : std::uint8_t {
        Keep,
        Delete,
    };
    enum class DownloadState : std::uint8_t {
        Idle,
        InFlight,
    };
    enum class PriorRunFiles : std::uint8_t {
        Absent,
        Present,
    };
    // Lazy skips the move when the entry was relinked recently; Always is for entries that must become the newest.
    enum class RelinkPolicy : std::uint8_t {
        Lazy,
        Always,
    };

    std::filesystem::path m_cachePath;
    ByteCount m_maxSize;
    std::shared_ptr<ContainerClient> m_containerClient;
    std::shared_ptr<Filesystem> m_filesystem;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    std::condition_variable m_idleCv;
    DownloadState m_downloadState = DownloadState::Idle;
    // Guarded by m_mutex. Every new entry and every bump takes a fresh value, so a removed and re-created entry never
    // repeats the generation a downloader or reader captured earlier.
    Generation m_nextGeneration;
    // Counts accesses; an entry touched within the last size/4 accesses is not relinked again.
    AccessEpoch m_accessEpoch;
    using CacheMap = std::unordered_map<std::string, FileCacheEntry, StringHash, StringEqual>;
    CacheMap m_cache;
    std::queue<std::string> m_fileDownloadQueue;
    boost::intrusive::list<FileCacheEntry, boost::intrusive::constant_time_size<false>> m_entryList;
    // Stale entries with no size, newest at the front, so pruning pops the back in O(1).
    boost::intrusive::list<
        FileCacheEntry,
        boost::intrusive::member_hook<FileCacheEntry, FileCacheEntry::MetadataOnlyHook, &FileCacheEntry::MetadataOnly>,
        boost::intrusive::constant_time_size<false>>
        m_metadataOnlyList;

    // Files whose entries were removed, and handles that must be closed. They are released on the background
    // thread, outside m_mutex (disk I/O must not stall readers) and serialized with downloads so a delete can
    // never land on a file that has since been downloaded again.
    struct PendingRelease {
        std::string FilePath; // The cache key; the file is m_cachePath / FilePath.
        std::shared_ptr<detail::CachedReadHandle> Handle;
        DiskFate Fate;
    };
    std::vector<PendingRelease> m_pendingReleases;
    // Cache keys whose delete failed; retried whenever the background thread wakes, until a download of that key
    // overwrites the file.
    std::vector<std::string> m_failedDeletes;
    // The most recent path popped for download; a new entry of that name may have a file being written.
    std::string m_lastDownloadPath;
    // Set at construction if the cache directory already held files; a same-named entry may then have one on disk.
    PriorRunFiles m_priorRunFiles = PriorRunFiles::Absent;

    // Running totals kept in step with the entries (see SetEntryStateUnsafe) so neither is a list scan.
    ByteCount m_activeBytes;
    size_t m_undownloadedStaleCount = 0;
    std::jthread m_backgroundDownloader;

  public:
    FileCacheImpl(std::filesystem::path cachePath, ByteCount maxCacheSize,
                  std::shared_ptr<ContainerClient> containerClient, std::shared_ptr<Filesystem> filesystem,
                  std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
    ~FileCacheImpl();
    FileCacheImpl(const FileCacheImpl&) = delete;
    FileCacheImpl& operator=(const FileCacheImpl&) = delete;
    FileCacheImpl(FileCacheImpl&&) = delete;
    FileCacheImpl& operator=(FileCacheImpl&&) = delete;

    [[nodiscard]] CachePresence HasFile(std::string_view filePath) const;
    void MarkFileAsStaleIfExists(const std::string& filePath);
    [[nodiscard]] std::optional<ByteCount> ReadFile(std::string_view filePath, ByteOffset offset,
                                                    std::span<char> buffer);
    void RemoveFile(std::string_view filePath);
    [[nodiscard]] ByteCount CacheSize() const;
    void SetCacheSize(ByteCount size);
    IdleWaitResult WaitUntilIdle(std::optional<std::chrono::milliseconds> timeout);

  private:
    void BackgroundDownload(std::stop_token stopToken);
    void PurgePartialDownloads() noexcept;
    void RetryFailedDeletesOnShutdown();
    void EntryAccessedUnsafe(FileCacheEntry& file, RelinkPolicy policy = RelinkPolicy::Lazy);
    size_t PruneUndownloadedStaleEntriesUnsafe();
    // Makes room for an incoming file by evicting `bytes`; refuses when that is more than the whole cache could hold.
    EvictionOutcome EvictToFit(ByteCount bytes);
    // Evicts least recently used files until the active size is within the current maximum. Reports Insufficient
    // (after logging) when downloads in progress keep it above the maximum.
    EvictionOutcome ShrinkToMaxUnsafe();
    EvictionOutcome EvictBytesUnsafe(ByteCount bytes);
    // NotRemoved when the entry was absent or, after an allocation failure, was only invalidated (kept Stale) and
    // will be evicted later. The path may view the entry's own key: it is not used once the entry is extracted.
    RemovalOutcome RemoveFileUnsafe(std::string_view filePath);
    void DropNeverDownloadedEntryUnsafe(std::string_view filePath);
    void ReservePendingReleaseUnsafe();
    void InvalidateEntryUnsafe(FileCacheEntry& entry) noexcept;
    // These keep the running totals and the metadata-only list consistent with the entry's new state and size.
    void SetEntryStateUnsafe(FileCacheEntry& entry, FileCacheEntry::State state) noexcept;
    void SetEntrySizeUnsafe(FileCacheEntry& entry, ByteCount size) noexcept;
    // Takes the entry out of the totals and the metadata-only list ahead of its removal.
    void UntrackEntryUnsafe(FileCacheEntry& entry) noexcept;
    void SubtractContributionUnsafe(const FileCacheEntry& entry) noexcept;
    void AddContributionUnsafe(FileCacheEntry& entry) noexcept;
    void ReleaseOnBackgroundThread(std::vector<PendingRelease>& releases);
    DeleteOutcome TryDeleteCachedFile(const std::string& filePath);
    void ValidateCountersUnsafe() const;
    ByteCount GetCurrentSizeUnsafe() const noexcept;
};
} // namespace AVEVA::RocksDB::Plugin::Core
