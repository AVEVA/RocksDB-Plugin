// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
#include <algorithm>
#include <boost/log/trivial.hpp>
#include <cassert>
#include <stdexcept>
#include <utility>
#include <vector>
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Core {
namespace {
enum class PartialDownloadName : std::uint8_t { Other, Partial };
enum class CachedReadHealth : std::uint8_t { Intact, Dropped };
enum class WorkerExit : std::uint8_t { Continue, Stop };
enum class InFlightArming : std::uint8_t { Disarmed, Armed };

// The id is a lowercase UUID (8-4-4-4-12 hex digits), as produced by CreateClientRequestId.
PartialDownloadName ClassifyDownloadName(const std::string& name) {
    constexpr std::string_view marker = ".partial-";
    constexpr size_t idLength = 36;
    if (name.size() <= marker.size() + idLength) {
        return PartialDownloadName::Other;
    }
    const auto idStart = name.size() - idLength;
    if (name.compare(idStart - marker.size(), marker.size(), marker) != 0) {
        return PartialDownloadName::Other;
    }
    for (size_t i = 0; i < idLength; ++i) {
        const char ch = name[idStart + i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (ch != '-') {
                return PartialDownloadName::Other;
            }
        } else if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
            return PartialDownloadName::Other;
        }
    }
    return PartialDownloadName::Partial;
}
} // namespace

FileCache::FileCache(
    std::filesystem::path cachePath, ByteCount maxCacheSize, std::shared_ptr<ContainerClient> containerClient,
    std::shared_ptr<Filesystem> filesystem,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_cachePath(std::move(cachePath)), m_maxSize(maxCacheSize), m_containerClient(std::move(containerClient)),
      m_filesystem(std::move(filesystem)), m_logger(std::move(logger)) {
    PurgePartialDownloads();
    // Start the background thread after all members are initialized
    m_backgroundDownloader = std::jthread([this](std::stop_token stopToken) { BackgroundDownload(stopToken); });
}

FileCache::~FileCache() {
    {
        std::scoped_lock lock(m_mutex);
        m_backgroundDownloader.request_stop();
    }

    m_cv.notify_all();
    m_backgroundDownloader.join();
}

// The Azure client downloads into `<name>.partial-<id>` and renames on success, so a crash leaves only such files
// behind. They are never read, so deleting them is always safe. Only the exact staging name is matched, so a user
// file such as `report.partial-backup.txt` is left alone.
void FileCache::PurgePartialDownloads() noexcept {
    std::error_code error;
    if (!std::filesystem::is_directory(m_cachePath, error)) {
        return;
    }
    std::filesystem::recursive_directory_iterator it(m_cachePath,
                                                     std::filesystem::directory_options::skip_permission_denied, error);
    const std::filesystem::recursive_directory_iterator end;
    while (!error && it != end) {
        const auto& path = it->path();
        std::error_code entryError;
        if (it->is_regular_file(entryError)) {
            if (ClassifyDownloadName(path.filename().string()) == PartialDownloadName::Other) {
                m_priorRunFiles = PriorRunFiles::Present;
            } else {
                std::filesystem::remove(path, entryError);
                if (entryError) {
                    BOOST_LOG_SEV(*m_logger, warning)
                        << "Could not delete partial download '" << path.string() << "': " << entryError.message();
                }
            }
        }
        // An iteration error ends the walk, but the files already handled stay deleted.
        it.increment(error);
    }
    if (error) {
        BOOST_LOG_SEV(*m_logger, warning)
            << "Scanning '" << m_cachePath.string() << "' for partial downloads stopped early: " << error.message();
    }
}
CachePresence FileCache::HasFile(std::string_view filePath) const {
    std::scoped_lock lock(m_mutex);
    auto it = m_cache.find(filePath);
    // The intrusive hook answers "is it in the entry list" in O(1); a list scan per call made Debug runs with large
    // caches quadratic.
    assert((it == m_cache.end() || it->second.LinkState() == ListLink::Linked) &&
           "FileCacheEntry should also be in the entry list");
    return it != m_cache.end() ? CachePresence::Present : CachePresence::Absent;
}

void FileCache::MarkFileAsStaleIfExists(const std::string& filePath) {
    std::scoped_lock lock(m_mutex);
    auto it = m_cache.find(filePath);
    if (it != m_cache.end()) {
        // A downloader that already popped this entry is still QueuedForDownload; the generation is how it learns
        // the blob changed under it.
        it->second.SetGeneration(++m_nextGeneration);

        // Mark as stale if exists. This should prevent reads from
        // accessing the file while it's being downloaded.
        if (it->second.GetState() != FileCacheEntry::State::QueuedForDownload) {
            BOOST_LOG_SEV(*m_logger, debug) << "Marking file '" << filePath << "' as stale";

            // If the file still in the download queue we don't have to worry about marking as stale.
            // This is because when the file is downloaded, it will get the most current state from
            // azure.
            SetEntryStateUnsafe(it->second, FileCacheEntry::State::Stale);

            // The file is about to be replaced, so its open handle must not serve any more reads. The release is
            // prepared before the handle is taken so a failed allocation cannot lose it.
            try {
                ReservePendingReleaseUnsafe();
                if (auto handle = it->second.TakeReadHandle()) {
                    m_pendingReleases.push_back(PendingRelease{filePath, std::move(handle), DiskFate::Keep});
                    m_cv.notify_one();
                }
            } catch (const std::exception& e) {
                // The entry is already Stale, so it cannot serve reads; dropping the handle closes the file.
                (void)it->second.TakeReadHandle();
                BOOST_LOG_SEV(*m_logger, error) << "Could not queue the release of '" << filePath << "': " << e.what();
            }
        }
    }
}

std::optional<ByteCount> FileCache::ReadFile(const std::string_view filePath, const ByteOffset offset,
                                             const ByteCount bytesToRead, char* buffer) {
    // If there are extensions that should be filtered on then we need to process that first.
    const auto fileType = RocksDBHelpers::GetFileType(filePath);
    if (fileType != RocksDBHelpers::FileClass::SST) {
        return std::nullopt;
    }

    std::unique_lock lock(m_mutex);
    auto it = m_cache.find(filePath);
    if (it == m_cache.end()) {
        // File not found in cache. The first access only records an entry and
        // marks it stale. A subsequent access to the same SST queues download.
        auto [inserted, _] = m_cache.emplace(std::piecewise_construct, std::forward_as_tuple(std::string(filePath)),
                                             std::forward_as_tuple(filePath, ByteCount{}));
        inserted->second.SetGeneration(++m_nextGeneration);
        // Without files from an earlier run, and with no download or failed delete of this name pending, nothing of
        // this entry can be on disk, so pruning it needs no delete.
        inserted->second.Disk = m_priorRunFiles == PriorRunFiles::Absent && m_lastDownloadPath != filePath &&
                                        std::ranges::find(m_failedDeletes, filePath) == m_failedDeletes.end()
                                    ? DiskPresence::KnownAbsent
                                    : DiskPresence::PossiblyOnDisk;
        m_entryList.push_front(inserted->second);
        // A new entry is stale with no size yet.
        m_metadataOnlyList.push_front(inserted->second);
        ++m_undownloadedStaleCount;
        const auto pruned = PruneUndownloadedStaleEntriesUnsafe();
        lock.unlock();
        BOOST_LOG_SEV(*m_logger, debug) << "File not found in cache on first access '" << filePath << "'";
        if (pruned != 0) {
            BOOST_LOG_SEV(*m_logger, debug) << "Pruned " << pruned << " stale metadata-only entries";
        }
        return std::nullopt;
    } else {
        auto& fileEntry = it->second;
        if (fileEntry.GetState() != FileCacheEntry::State::Active) {
            // File is stale, do not read, but queue in the background for download.
            const auto state = fileEntry.GetState();
            if (state == FileCacheEntry::State::Stale) {
                m_fileDownloadQueue.emplace(filePath);
                fileEntry.Disk = DiskPresence::PossiblyOnDisk;

                // A fresh download must not sit at its old, possibly least recent, LRU position.
                EntryAccessedUnsafe(fileEntry, RelinkPolicy::Always);
                // Mark as downloading now so we don't queue it again.
                SetEntryStateUnsafe(fileEntry, FileCacheEntry::State::QueuedForDownload);

                lock.unlock();
                BOOST_LOG_SEV(*m_logger, debug) << "File is stale. Queueing for download: '" << filePath << "'";
                m_cv.notify_one();
            }

            return std::nullopt;
        }

        EntryAccessedUnsafe(fileEntry);

        if (buffer == nullptr) {
            return ByteCount{};
        }

        const auto handle = fileEntry.GetOrCreateReadHandle();
        // Taken under the lock: the entry may be replaced or evicted once it is released.
        const ByteCount entrySize = fileEntry.GetSize();
        const auto generation = fileEntry.GetGeneration();
        const ByteCount expectedBytes = std::min(bytesToRead, BytesAfter(entrySize, offset));

        // The read is disk I/O, so it must not hold the cache-wide mutex. If an eviction closes the handle after the
        // lock is released, the read fails and the caller falls back to the blob.
        lock.unlock();
        auto health = CachedReadHealth::Intact;
        std::optional<ByteCount> result;
        try {
            // The handle lock only covers open/close bookkeeping; the read itself is positional and lock-free.
            std::shared_ptr<File> file;
            {
                std::scoped_lock handleLock(handle->Mutex);
                if (handle->State == detail::HandleState::Closed) {
                    return std::nullopt;
                }
                if (!handle->Opened) {
                    handle->Opened = m_filesystem->Open(m_cachePath / filePath);
                }
                file = handle->Opened;
            }
            const auto dropFile = [&handle, &file] {
                std::scoped_lock handleLock(handle->Mutex);
                if (handle->Opened == file) {
                    handle->Opened.reset();
                }
            };
            ByteCount bytesRead;
            try {
                bytesRead = file->Read(buffer, offset, bytesToRead);
            } catch (...) {
                // Do not keep a handle that just failed; the next read reopens the file.
                dropFile();
                throw;
            }
            if (bytesRead < expectedBytes) {
                // A truncated or corrupt cached file must not be returned as if it were the blob's data.
                dropFile();
                BOOST_LOG_SEV(*m_logger, error)
                    << "Cached read of '" << filePath << "' returned " << bytesRead.Value() << " bytes but "
                    << expectedBytes.Value() << " were expected. Dropping the cached file.";
                health = CachedReadHealth::Dropped;
            } else {
                result = bytesRead;
            }
        } catch (const std::exception& e) {
            // Same recovery as a short read: an Active entry that cannot be opened would otherwise fail every read
            // and keep counting against the cache size without ever being downloaded again.
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                << "Cached read of '" << filePath
                << "' failed, dropping the cached file and falling back: " << e.what();
            health = CachedReadHealth::Dropped;
        }

        // Done after the handle lock is released so the two locks are never nested.
        // Only the entry this read used is dropped: a newer valid one may have replaced it meanwhile.
        if (health == CachedReadHealth::Dropped) {
            std::scoped_lock relock(m_mutex);
            if (const auto current = m_cache.find(filePath);
                current != m_cache.end() && current->second.GetGeneration() == generation &&
                current->second.OwnsReadHandle(handle) == HandleOwnership::Owned) {
                (void)RemoveFileUnsafe(filePath);
            }
        }
        return result;
    }
}

void FileCache::RemoveFile(const std::string_view filePath) {
    std::scoped_lock lock(m_mutex);
    (void)RemoveFileUnsafe(filePath);
}

ByteCount FileCache::CacheSize() const {
    std::scoped_lock lock(m_mutex);
    ValidateCountersUnsafe();
    return GetCurrentSizeUnsafe();
}

void FileCache::SetCacheSize(const ByteCount size) {
    std::scoped_lock lock(m_mutex);
    // The new maximum is set first so eviction is measured against it, not against the size being replaced.
    m_maxSize = size;
    (void)ShrinkToMaxUnsafe();
}

IdleWaitResult FileCache::WaitUntilIdle(const std::optional<std::chrono::milliseconds> timeout) {
    std::unique_lock lock(m_mutex);
    const auto idle = [this] {
        return m_fileDownloadQueue.empty() && m_pendingReleases.empty() && m_downloadState == DownloadState::Idle;
    };
    if (!timeout) {
        m_idleCv.wait(lock, idle);
        return IdleWaitResult::Idle;
    }
    return m_idleCv.wait_for(lock, *timeout, idle) ? IdleWaitResult::Idle : IdleWaitResult::TimedOut;
}

// A delete that failed earlier gets one last attempt at shutdown; anything still left would otherwise sit in the cache
// directory unnoticed, so it is logged for the operator.
void FileCache::RetryFailedDeletesOnShutdown() {
    std::vector<std::string> leftovers;
    {
        std::scoped_lock lock(m_mutex);
        leftovers.swap(m_failedDeletes);
    }
    for (const auto& key : leftovers) {
        if (TryDeleteCachedFile(key) == DeleteOutcome::Failed) {
            BOOST_LOG_SEV(*m_logger, error)
                << "Cached file '" << key << "' could not be deleted and was left in '" << m_cachePath.string() << "'";
        }
    }
}
void FileCache::BackgroundDownload(std::stop_token stopToken) {
    while (true) {
        try {
            // Declared before any lock in this iteration so it runs last and can safely take the mutex.
            struct InFlightGuard {
                FileCache& cache;
                InFlightArming armed = InFlightArming::Disarmed;
                ~InFlightGuard() {
                    if (armed == InFlightArming::Armed) {
                        std::scoped_lock lock(cache.m_mutex);
                        cache.m_downloadState = DownloadState::Idle;
                        cache.m_idleCv.notify_all();
                    }
                }
            } inFlight{*this};

            std::string filePath;
            Generation generation;
            std::vector<PendingRelease> releases;
            std::vector<std::string> retryDeletes;
            auto exit = WorkerExit::Continue;
            {
                std::unique_lock lock(m_mutex);
                if (m_fileDownloadQueue.empty() && m_pendingReleases.empty() && !stopToken.stop_requested()) {
                    BOOST_LOG_SEV(*m_logger, debug) << "File cache has no work. Waiting for condition variable.";
                    m_cv.wait(lock, [this, &stopToken]() {
                        return !m_fileDownloadQueue.empty() || !m_pendingReleases.empty() || stopToken.stop_requested();
                    });
                }

                // Take the pending releases first: a download of a path must never start before an earlier delete of
                // the same path has run.
                releases.swap(m_pendingReleases);
                // A failed delete is dropped if the path was cached again, since the new download overwrites it.
                std::erase_if(m_failedDeletes, [this](const std::string& key) { return m_cache.contains(key); });
                retryDeletes.swap(m_failedDeletes);
                if (!releases.empty() || !retryDeletes.empty()) {
                    m_downloadState = DownloadState::InFlight;
                    inFlight.armed = InFlightArming::Armed;
                }

                // We could have been woken up because it's time to close.
                if (stopToken.stop_requested()) {
                    BOOST_LOG_SEV(*m_logger, debug) << "File cache should close. Exiting thread.";
                    exit = WorkerExit::Stop;
                } else if (!m_fileDownloadQueue.empty()) {
                    filePath = m_fileDownloadQueue.front();
                    m_fileDownloadQueue.pop();
                    m_lastDownloadPath = filePath;
                    m_downloadState = DownloadState::InFlight;
                    inFlight.armed = InFlightArming::Armed;

                    auto it = m_cache.find(filePath);
                    if (it != m_cache.end()) {
                        BOOST_LOG_SEV(*m_logger, debug) << "Downloading '" << filePath << "' into cache";
                        if (it->second.GetState() != FileCacheEntry::State::QueuedForDownload) {
                            // The file was marked as stale while we were waiting for the condition variable.
                            // We should not download it.
                            BOOST_LOG_SEV(*m_logger, debug)
                                << "File '" << filePath
                                << "' is no longer marked as queued for download. Skipping download.";
                            filePath.clear();
                        } else {
                            generation = it->second.GetGeneration();
                        }
                    } else {
                        // The file was likely deleted while we were waiting for the condition variable.
                        BOOST_LOG_SEV(*m_logger, debug)
                            << "File pending download '" << filePath << "' was deleted. Skipping download.";
                        filePath.clear();
                    }
                }
            }

            // Disk I/O for removed entries runs here, outside the mutex.
            ReleaseOnBackgroundThread(releases);
            for (const auto& key : retryDeletes) {
                if (TryDeleteCachedFile(key) == DeleteOutcome::Failed) {
                    std::scoped_lock lock(m_mutex);
                    m_failedDeletes.push_back(key);
                }
            }

            if (exit == WorkerExit::Stop) {
                RetryFailedDeletesOnShutdown();
                return;
            }
            if (filePath.empty()) {
                continue;
            }

            ByteCount fileSize;
            try {
                fileSize = m_containerClient->GetBlobSize(filePath, stopToken);
            } catch (std::exception& e) {
                BOOST_LOG_SEV(*m_logger, error)
                    << "Failed to get file size for '" << filePath << "'. Error: " << e.what();

                // Without this the entry stays QueuedForDownload and is never queued again.
                std::scoped_lock lock(m_mutex);
                if (const auto failed = m_cache.find(filePath);
                    failed != m_cache.end() && failed->second.GetState() == FileCacheEntry::State::QueuedForDownload) {
                    SetEntryStateUnsafe(failed->second, FileCacheEntry::State::Stale);
                }
                continue;
            }

            // Files evicted to make room; deleted before the download so disk use never exceeds the maximum.
            std::vector<PendingRelease> evicted;

            // Set the cache entries size and evict
            {
                std::unique_lock lock(m_mutex);
                auto it = m_cache.find(filePath);
                if (it != m_cache.end()) {
                    if (it->second.GetGeneration() != generation ||
                        it->second.GetState() != FileCacheEntry::State::QueuedForDownload) {
                        // The blob was replaced while we asked for its size, so that size and any download are stale.
                        // A later read queues the entry again.
                        BOOST_LOG_SEV(*m_logger, debug)
                            << "File '" << filePath << "' was replaced before download began. Not activating.";
                        if (it->second.GetState() == FileCacheEntry::State::QueuedForDownload) {
                            SetEntryStateUnsafe(it->second, FileCacheEntry::State::Stale);
                        }
                        continue;
                    }
                    SetEntrySizeUnsafe(it->second, fileSize);
                } else {
                    // The file was likely deleted while we were waiting for the condition variable.
                    BOOST_LOG_SEV(*m_logger, error) << "Could not find file entry '" << filePath
                                                    << "' in cache after getting file size. Skipping download.";
                    continue;
                }

                const auto currentSize = GetCurrentSizeUnsafe();
                if (currentSize + fileSize > m_maxSize) {
                    if (fileSize <= m_maxSize) {
                        BOOST_LOG_SEV(*m_logger, debug)
                            << "Cache is full at " << currentSize.Value() << " (bytes). Max " << m_maxSize.Value()
                            << " (bytes). Evicting files to make room for '" << filePath << "' of size "
                            << fileSize.Value() << " (bytes)";

                        const auto bytesToEvict = (currentSize + fileSize) - m_maxSize;
                        if (EvictToFit(bytesToEvict) == EvictionOutcome::Insufficient) {
                            BOOST_LOG_SEV(*m_logger, error)
                                << "Couldn't evict enough space to fit new file '" << filePath << "'";
                            (void)RemoveFileUnsafe(filePath);
                            continue;
                        }
                    } else {
                        BOOST_LOG_SEV(*m_logger, debug)
                            << "Skipping eviction from file cache because the file '" << filePath << "' of size "
                            << fileSize.Value() << " (bytes) is greater than the maximum of " << m_maxSize.Value();

                        (void)RemoveFileUnsafe(filePath);
                        continue;
                    }
                }

                // Mark the file as downloading now so we don't queue it again.
                SetEntryStateUnsafe(it->second, FileCacheEntry::State::Downloading);
                evicted.swap(m_pendingReleases);
            }
            ReleaseOnBackgroundThread(evicted);

            try {
                const auto actualFilePath = m_cachePath / filePath;

                // Blob paths are nested (db directory/file), and the downloader will not create the parents.
                std::filesystem::create_directories(actualFilePath.parent_path());

                // No need to download the _whole_ blob. There could be lots of padding
                // at the end of the file. We can just download the actual size.
                m_containerClient->DownloadBlobTo(filePath, actualFilePath, ByteOffset{}, fileSize, stopToken);
            } catch (std::exception& e) {
                // NOTE: We're not putting the filePath back on the download queue because
                // this operation could _also_ throw. Let the next read be a cache miss which
                // will queue up the download again.
                BOOST_LOG_SEV(*m_logger, error)
                    << "Failed to download file '" << filePath << "'. Removing entry from cache. Error: " << e.what();

                std::scoped_lock lock(m_mutex);
                // A newer queued request (the entry was marked stale during the download and read again) must
                // survive: it is downloaded afresh when popped. Anything else is the failed attempt's to drop.
                if (const auto failed = m_cache.find(filePath);
                    failed != m_cache.end() && failed->second.GetState() != FileCacheEntry::State::QueuedForDownload) {
                    (void)RemoveFileUnsafe(filePath);
                }
                continue;
            }

            BOOST_LOG_SEV(*m_logger, debug) << "Finished downloading file '" << filePath << "'";

            // Mark the file as active in the cache.
            std::unique_lock lock(m_mutex);
            auto it = m_cache.find(filePath);
            if (it != m_cache.end()) {
                // Only the state this thread set may become Active. Anything else means the entry was marked stale
                // while we were downloading, and possibly queued again by a read since: the bytes just written may
                // predate the replacement, and a queued entry will be downloaded afresh when it is popped. The
                // generation check is redundant while a single thread downloads, but keeps this correct if that ever
                // changes.
                if (it->second.GetState() != FileCacheEntry::State::Downloading ||
                    it->second.GetGeneration() != generation) {
                    BOOST_LOG_SEV(*m_logger, debug)
                        << "File '" << filePath
                        << "' was marked as stale while we were downloading it. Will not mark as active.";
                    continue;
                }

                BOOST_LOG_SEV(*m_logger, debug) << "Marking file '" << filePath << "' as active";
                SetEntryStateUnsafe(it->second, FileCacheEntry::State::Active);
                // The download took a while; the entry is the newest one, so a trim below must not pick it first.
                EntryAccessedUnsafe(it->second, RelinkPolicy::Always);

                // SetCacheSize may have lowered the maximum while this file was downloading, and it was not yet
                // counted then.
                if (ShrinkToMaxUnsafe() == EvictionOutcome::Insufficient) {
                    (void)RemoveFileUnsafe(filePath);
                }
            } else {
                // RemoveFileUnsafe already queued the on-disk delete when the entry was removed.
                BOOST_LOG_SEV(*m_logger, debug) << "File '" << filePath << "' was deleted. Removing file from cache";
            }
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_logger, stopToken.stop_requested() ? debug : error)
                << "Error in file cache background downloader thread: '" << e.what() << "'";
        }
    }
}

void FileCache::ReservePendingReleaseUnsafe() {
    if (m_pendingReleases.size() == m_pendingReleases.capacity()) {
        m_pendingReleases.reserve(std::max<size_t>(8, m_pendingReleases.capacity() * 2));
    }
}

// Everything that can throw (the path copy and the vector growth) happens before the entry is touched, so a failure
// leaves it fully cached. The commit that follows cannot throw. The map node is extracted so its key moves into the
// PendingRelease instead of being copied and rehashed; `filePath` may view that key and is not used afterwards.
FileCache::RemovalOutcome FileCache::RemoveFileUnsafe(const std::string_view filePath) {
    const auto it = m_cache.find(filePath);
    if (it == m_cache.end()) {
        return RemovalOutcome::NotRemoved;
    }
    auto& fileEntry = it->second;
    try {
        ReservePendingReleaseUnsafe();

        fileEntry.unlink();
        // The entry leaves the totals now; the file itself is deleted (and its handle closed) later on the
        // background thread, outside this mutex.
        UntrackEntryUnsafe(fileEntry);
        auto node = m_cache.extract(it);
        // Capacity was reserved above, so this cannot throw.
        m_pendingReleases.push_back(
            PendingRelease{std::move(node.key()), node.mapped().TakeReadHandle(), DiskFate::Delete});
        m_cv.notify_one();
    } catch (const std::exception& e) {
        InvalidateEntryUnsafe(fileEntry);
        try {
            BOOST_LOG_SEV(*m_logger, error)
                << "Could not queue the removal of '" << it->first << "'; invalidated the entry instead: " << e.what();
        } catch (...) {
            // Logging is best effort: this runs from cleanup paths that must not throw.
        }
        // A metadata-only entry has nothing on disk to delete, so it can still be dropped without allocating.
        if (fileEntry.GetState() == FileCacheEntry::State::Stale && fileEntry.GetSize() == ByteCount{}) {
            fileEntry.unlink();
            UntrackEntryUnsafe(fileEntry);
            m_cache.erase(it);
            return RemovalOutcome::Removed;
        }
        return RemovalOutcome::NotRemoved;
    }
    return RemovalOutcome::Removed;
}

// Allocation-free and noexcept: an entry that never had a download has no file or handle to release, so it only
// leaves the accounting and the lists.
void FileCache::DropNeverDownloadedEntryUnsafe(const std::string_view filePath) {
    const auto it = m_cache.find(filePath);
    assert(it != m_cache.end() && it->second.Disk == DiskPresence::KnownAbsent);
    it->second.unlink();
    UntrackEntryUnsafe(it->second);
    m_cache.erase(it);
}

// Allocation-free fallback: the entry stays cached but Stale, so it never serves old bytes and a later read
// downloads it afresh. Eviction removes it (and its file) once an allocation succeeds.
void FileCache::InvalidateEntryUnsafe(FileCacheEntry& entry) noexcept {
    entry.SetGeneration(++m_nextGeneration);
    SetEntryStateUnsafe(entry, FileCacheEntry::State::Stale);
    (void)entry.TakeReadHandle();
}

namespace {
struct Contribution {
    ByteCount Bytes;
    size_t UndownloadedStale = 0;
};

// Downloads in progress are not counted: their size is the blob's, not yet what is on disk.
Contribution ContributionOf(const FileCacheEntry& entry) noexcept {
    const auto state = entry.GetState();
    Contribution contribution;
    if (state != FileCacheEntry::State::Downloading && state != FileCacheEntry::State::QueuedForDownload) {
        contribution.Bytes = entry.GetSize();
    }
    if (state == FileCacheEntry::State::Stale && entry.GetSize() == ByteCount{}) {
        contribution.UndownloadedStale = 1;
    }
    return contribution;
}
} // namespace

void FileCache::SubtractContributionUnsafe(const FileCacheEntry& entry) noexcept {
    const auto contribution = ContributionOf(entry);
    m_activeBytes -= contribution.Bytes;
    m_undownloadedStaleCount -= contribution.UndownloadedStale;
}

void FileCache::AddContributionUnsafe(FileCacheEntry& entry) noexcept {
    const auto contribution = ContributionOf(entry);
    m_activeBytes += contribution.Bytes;
    m_undownloadedStaleCount += contribution.UndownloadedStale;
    if (contribution.UndownloadedStale != 0 && !entry.MetadataOnly.is_linked()) {
        m_metadataOnlyList.push_front(entry);
    } else if (contribution.UndownloadedStale == 0) {
        entry.MetadataOnly.unlink();
    }
}

void FileCache::SetEntryStateUnsafe(FileCacheEntry& entry, const FileCacheEntry::State state) noexcept {
    SubtractContributionUnsafe(entry);
    entry.SetState(state);
    AddContributionUnsafe(entry);
}

void FileCache::SetEntrySizeUnsafe(FileCacheEntry& entry, const ByteCount size) noexcept {
    SubtractContributionUnsafe(entry);
    entry.SetSize(size);
    AddContributionUnsafe(entry);
}

void FileCache::UntrackEntryUnsafe(FileCacheEntry& entry) noexcept {
    SubtractContributionUnsafe(entry);
    entry.MetadataOnly.unlink();
}

} // namespace AVEVA::RocksDB::Plugin::Core
