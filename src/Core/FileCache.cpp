// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
#include <algorithm>
#include <boost/log/trivial.hpp>
#include <cassert>
#include <utility>
#include <vector>
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Core {
namespace {
constexpr size_t MaxUndownloadedStaleEntries = 2048;
}

FileCache::FileCache(
    std::filesystem::path cachePath, int64_t maxCacheSize, std::shared_ptr<ContainerClient> containerClient,
    std::shared_ptr<Filesystem> filesystem,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger)
    : m_cachePath(std::move(cachePath)), m_maxSize(maxCacheSize), m_containerClient(std::move(containerClient)),
      m_filesystem(std::move(filesystem)), m_logger(std::move(logger)) {
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

bool FileCache::HasFile(std::string_view filePath) const {
    std::scoped_lock lock(m_mutex);
    auto it = m_cache.find(filePath);
    // The intrusive hook answers "is it in the entry list" in O(1); a list scan per call made Debug runs with large
    // caches quadratic.
    assert((it == m_cache.end() || it->second.is_linked()) && "FileCacheEntry should also be in the entry list");
    return it != m_cache.end();
}

void FileCache::MarkFileAsStaleIfExists(const std::string& filePath) {
    std::scoped_lock lock(m_mutex);
    auto it = m_cache.find(filePath);
    if (it != m_cache.end()) {
        // A downloader that already popped this entry is still QueuedForDownload; the generation is how it learns
        // the blob changed under it.
        it->second.BumpGeneration();

        // Mark as stale if exists. This should prevent reads from
        // accessing the file while it's being downloaded.
        if (it->second.GetState() != FileCacheEntry::State::QueuedForDownload) {
            BOOST_LOG_SEV(*m_logger, debug) << "Marking file '" << filePath << "' as stale";

            // If the file still in the download queue we don't have to worry about marking as stale.
            // This is because when the file is downloaded, it will get the most current state from
            // azure.
            UpdateEntryUnsafe(it->second, [](FileCacheEntry& entry) { entry.SetState(FileCacheEntry::State::Stale); });

            // The file is about to be replaced, so its open handle must not serve any more reads.
            if (auto handle = it->second.TakeReadHandle()) {
                m_pendingReleases.push_back({filePath, std::move(handle), false});
                m_cv.notify_one();
            }
        }
    }
}

std::optional<int64_t> FileCache::ReadFile(const std::string_view filePath, int64_t offset, int64_t bytesToRead,
                                           char* buffer) {
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
        BOOST_LOG_SEV(*m_logger, debug) << "File not found in cache on first access '" << filePath << "'";

        auto [inserted, _] = m_cache.emplace(std::piecewise_construct, std::forward_as_tuple(std::string(filePath)),
                                             std::forward_as_tuple(filePath, 0));
        m_entryList.push_front(inserted->second);
        // A new entry is stale with no size yet.
        m_metadataOnlyList.push_front(inserted->second);
        ++m_undownloadedStaleCount;
        const auto pruned = PruneUndownloadedStaleEntriesUnsafe();
        lock.unlock();
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
                BOOST_LOG_SEV(*m_logger, debug) << "File is stale. Queueing for download: '" << filePath << "'";
                m_fileDownloadQueue.emplace(filePath);

                // A fresh download must not sit at its old, possibly least recent, LRU position.
                EntryAccessedUnsafe(fileEntry);
                // Mark as downloading now so we don't queue it again.
                UpdateEntryUnsafe(
                    fileEntry, [](FileCacheEntry& entry) { entry.SetState(FileCacheEntry::State::QueuedForDownload); });

                lock.unlock();
                m_cv.notify_one();
            }

            return std::nullopt;
        }

        EntryAccessedUnsafe(fileEntry);

        if (buffer == nullptr) {
            return 0;
        }

        auto cachedFilePath = m_cachePath / fileEntry.GetFilePath();
        const auto handle = fileEntry.GetOrCreateReadHandle();
        // Taken under the lock: the entry may be replaced or evicted once it is released.
        const int64_t entrySize = fileEntry.GetSize();
        const int64_t expectedBytes = std::max<int64_t>(0, std::min(bytesToRead, entrySize - offset));

        // The read is disk I/O, so it must not hold the cache-wide mutex. If an eviction closes the handle after the
        // lock is released, the read fails and the caller falls back to the blob.
        lock.unlock();
        bool shortRead = false;
        std::optional<int64_t> result;
        try {
            std::scoped_lock handleLock(handle->Mutex);
            if (handle->Closed) {
                return std::nullopt;
            }
            if (!handle->Opened) {
                handle->Opened = m_filesystem->Open(cachedFilePath);
            }
            int64_t bytesRead = 0;
            try {
                bytesRead = handle->Opened->Read(buffer, offset, bytesToRead);
            } catch (...) {
                // Do not keep a handle that just failed; the next read reopens the file.
                handle->Opened.reset();
                throw;
            }
            if (bytesRead < expectedBytes) {
                // A truncated or corrupt cached file must not be returned as if it were the blob's data.
                handle->Opened.reset();
                BOOST_LOG_SEV(*m_logger, error)
                    << "Cached read of '" << filePath << "' returned " << bytesRead << " bytes but " << expectedBytes
                    << " were expected. Dropping the cached file.";
                shortRead = true;
            } else {
                result = bytesRead;
            }
        } catch (const std::exception& e) {
            // Same recovery as a short read: an Active entry that cannot be opened would otherwise fail every read
            // and keep counting against the cache size without ever being downloaded again.
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                << "Cached read of '" << filePath
                << "' failed, dropping the cached file and falling back: " << e.what();
            shortRead = true;
        }

        // Done after the handle lock is released so the two locks are never nested.
        if (shortRead) {
            RemoveFile(filePath);
        }
        return result;
    }
}

void FileCache::RemoveFile(const std::string_view filePath) {
    std::scoped_lock lock(m_mutex);
    RemoveFileUnsafe(filePath);
}

int64_t FileCache::CacheSize() const {
    std::scoped_lock lock(m_mutex);
    ValidateCountersUnsafe();
    return GetCurrentSizeUnsafe();
}

void FileCache::SetCacheSize(int64_t size) {
    if (size < 0) {
        throw std::invalid_argument("Cache size cannot be negative");
    }

    std::scoped_lock lock(m_mutex);
    const auto currentSize = GetCurrentSizeUnsafe();
    if (currentSize > size) {
        // Evict files until we are under the new size.
        const auto bytesToEvict = currentSize - size;
        EvictAtLeast(bytesToEvict);
    }

    m_maxSize = size;
}

bool FileCache::WaitUntilIdle(const std::optional<std::chrono::milliseconds> timeout) {
    std::unique_lock lock(m_mutex);
    const auto idle = [this] {
        return m_fileDownloadQueue.empty() && m_pendingReleases.empty() && !m_downloadInFlight;
    };
    if (!timeout) {
        m_idleCv.wait(lock, idle);
        return true;
    }
    return m_idleCv.wait_for(lock, *timeout, idle);
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
        if (!TryDeleteCachedFile(key)) {
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
                bool armed = false;
                ~InFlightGuard() {
                    if (armed) {
                        std::scoped_lock lock(cache.m_mutex);
                        cache.m_downloadInFlight = false;
                        cache.m_idleCv.notify_all();
                    }
                }
            } inFlight{*this};

            std::string filePath;
            uint64_t generation = 0;
            std::vector<PendingRelease> releases;
            std::vector<std::string> retryDeletes;
            bool stopping = false;
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
                    m_downloadInFlight = true;
                    inFlight.armed = true;
                }

                // We could have been woken up because it's time to close.
                if (stopToken.stop_requested()) {
                    BOOST_LOG_SEV(*m_logger, debug) << "File cache should close. Exiting thread.";
                    stopping = true;
                } else if (!m_fileDownloadQueue.empty()) {
                    filePath = m_fileDownloadQueue.front();
                    m_fileDownloadQueue.pop();
                    m_downloadInFlight = true;
                    inFlight.armed = true;

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
                if (!TryDeleteCachedFile(key)) {
                    std::scoped_lock lock(m_mutex);
                    m_failedDeletes.push_back(key);
                }
            }

            if (stopping) {
                RetryFailedDeletesOnShutdown();
                return;
            }
            if (filePath.empty()) {
                continue;
            }

            int64_t fileSize = 0;
            try {
                fileSize = m_containerClient->GetBlobSize(filePath);
            } catch (std::exception& e) {
                BOOST_LOG_SEV(*m_logger, error)
                    << "Failed to get file size for '" << filePath << "'. Error: " << e.what();

                // Without this the entry stays QueuedForDownload and is never queued again.
                std::scoped_lock lock(m_mutex);
                if (const auto failed = m_cache.find(filePath);
                    failed != m_cache.end() && failed->second.GetState() == FileCacheEntry::State::QueuedForDownload) {
                    UpdateEntryUnsafe(failed->second,
                                      [](FileCacheEntry& entry) { entry.SetState(FileCacheEntry::State::Stale); });
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
                    if (it->second.GetGeneration() != generation) {
                        // The blob was replaced while we asked for its size, so that size and any download are stale.
                        // A later read queues the entry again.
                        BOOST_LOG_SEV(*m_logger, debug)
                            << "File '" << filePath << "' was replaced before download began. Not activating.";
                        if (it->second.GetState() == FileCacheEntry::State::QueuedForDownload) {
                            UpdateEntryUnsafe(it->second, [](FileCacheEntry& entry) {
                                entry.SetState(FileCacheEntry::State::Stale);
                            });
                        }
                        continue;
                    }
                    UpdateEntryUnsafe(it->second, [fileSize](FileCacheEntry& entry) { entry.SetSize(fileSize); });
                } else {
                    // The file was likely deleted while we were waiting for the condition variable.
                    BOOST_LOG_SEV(*m_logger, error) << "Could not find file entry '" << filePath
                                                    << "' in cache after getting file size. Skipping download.";
                    continue;
                }

                const auto currentSize = GetCurrentSizeUnsafe();
                if (currentSize + fileSize > m_maxSize) {
                    if (fileSize <= m_maxSize) {
                        BOOST_LOG_SEV(*m_logger, debug) << "Cache is full at " << currentSize << " (bytes). Max "
                                                        << m_maxSize << " (bytes). Evicting files to make room for '"
                                                        << filePath << "' of size " << fileSize << " (bytes)";

                        const auto bytesToEvict = (currentSize + fileSize) - m_maxSize;
                        if (!EvictAtLeast(bytesToEvict)) {
                            BOOST_LOG_SEV(*m_logger, error)
                                << "Couldn't evict enough space to fit new file '" << filePath << "'";
                            RemoveFileUnsafe(filePath);
                            continue;
                        }
                    } else {
                        BOOST_LOG_SEV(*m_logger, debug)
                            << "Skipping eviction from file cache because the file '" << filePath << "' of size "
                            << fileSize << " (bytes) is greater than the maximum of " << m_maxSize;

                        RemoveFileUnsafe(filePath);
                        continue;
                    }
                }

                // Mark the file as downloading now so we don't queue it again.
                UpdateEntryUnsafe(it->second,
                                  [](FileCacheEntry& entry) { entry.SetState(FileCacheEntry::State::Downloading); });
                evicted.swap(m_pendingReleases);
            }
            ReleaseOnBackgroundThread(evicted);

            try {
                const auto actualFilePath = m_cachePath / filePath;

                // Blob paths are nested (db directory/file), and the downloader will not create the parents.
                std::filesystem::create_directories(actualFilePath.parent_path());

                // No need to download the _whole_ blob. There could be lots of padding
                // at the end of the file. We can just download the actual size.
                m_containerClient->DownloadBlobTo(filePath, actualFilePath, 0, fileSize);
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
                    RemoveFileUnsafe(filePath);
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
                // predate the replacement, and a queued entry will be downloaded afresh when it is popped.
                if (it->second.GetState() != FileCacheEntry::State::Downloading) {
                    BOOST_LOG_SEV(*m_logger, debug)
                        << "File '" << filePath
                        << "' was marked as stale while we were downloading it. Will not mark as active.";
                    continue;
                }

                BOOST_LOG_SEV(*m_logger, debug) << "Marking file '" << filePath << "' as active";
                UpdateEntryUnsafe(it->second,
                                  [](FileCacheEntry& entry) { entry.SetState(FileCacheEntry::State::Active); });
                // The download took a while; the entry is the newest one, so a trim below must not pick it first.
                EntryAccessedUnsafe(it->second);

                // SetCacheSize may have lowered the maximum while this file was downloading, and it was not yet
                // counted then.
                if (const auto size = GetCurrentSizeUnsafe(); size > m_maxSize) {
                    EvictAtLeast(size - m_maxSize);
                    if (GetCurrentSizeUnsafe() > m_maxSize) {
                        RemoveFileUnsafe(filePath);
                    }
                }
            } else {
                // The file was likely deleted. We should clean up after ourselves; the delete runs on the next pass
                // of this loop, outside the mutex.
                BOOST_LOG_SEV(*m_logger, debug) << "File '" << filePath << "' was deleted. Removing file from cache";
                m_pendingReleases.push_back({filePath, nullptr, true});
            }
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_logger, error) << "Error in file cache background downloader thread: '" << e.what() << "'";
        }
    }
}

void FileCache::EntryAccessedUnsafe(FileCacheEntry& file) {
    file.unlink();
    m_entryList.push_front(file);
}

size_t FileCache::PruneUndownloadedStaleEntriesUnsafe() {
    ValidateCountersUnsafe();
    if (m_undownloadedStaleCount <= MaxUndownloadedStaleEntries) {
        return 0;
    }

    // Oldest first. Removing an entry unlinks it from m_metadataOnlyList, so the list shrinks on every iteration.
    size_t pruned = 0;
    while (m_undownloadedStaleCount > MaxUndownloadedStaleEntries && !m_metadataOnlyList.empty()) {
        const std::string staleEntry = m_metadataOnlyList.back().GetFilePath();
        RemoveFileUnsafe(staleEntry);
        ++pruned;
    }
    return pruned;
}

bool FileCache::EvictAtLeast(const int64_t bytes) {
    if (bytes > m_maxSize) {
        // No point in evicting everything from the cache if we can't fit the new file.
        BOOST_LOG_SEV(*m_logger, debug) << "Skipping eviction from file cache because " << bytes
                                        << " bytes is greater than the maximum of " << m_maxSize;
        return false;
    }

    BOOST_LOG_SEV(*m_logger, debug) << "Attempting to evict " << bytes << " (bytes) from the file cache.";
    int64_t bytesEvicted = 0;
    if (m_entryList.empty()) {
        return bytesEvicted >= bytes;
    }

    // Walk from the least recently used entry towards the front. The next candidate is captured before the current
    // one is removed, and the front entry is processed too (the loop ends after it).
    auto tail = m_entryList.s_iterator_to(m_entryList.back());
    while (bytesEvicted < bytes) {
        const auto current = tail;
        const bool atFront = current == m_entryList.begin();
        if (!atFront) {
            --tail;
        }

        // Don't evict downloading files. They could be files that we are making space for.
        const auto state = current->GetState();
        if (state == FileCacheEntry::State::Downloading || state == FileCacheEntry::State::QueuedForDownload) {
            BOOST_LOG_SEV(*m_logger, debug) << "Skipping eviction of '" << current->GetFilePath()
                                            << "'. It is currently downloading or queued for download";
        } else if (state == FileCacheEntry::State::Stale && current->GetSize() == 0) {
            // Frees nothing, and removing it would discard its access history; PruneUndownloadedStaleEntriesUnsafe
            // bounds these.
        } else {
            const std::string filePath = current->GetFilePath();
            const auto fileSize = current->GetSize();

            BOOST_LOG_SEV(*m_logger, debug)
                << "Evicting '" << filePath << "' of size " << fileSize << "'bytes' from file cache";
            bytesEvicted += fileSize;
            RemoveFileUnsafe(filePath);
        }

        if (atFront) {
            break;
        }
    }

    return bytesEvicted >= bytes;
}

void FileCache::RemoveFileUnsafe(const std::string_view filePath) {
    auto it = m_cache.find(filePath);
    if (it != m_cache.end()) {
        BOOST_LOG_SEV(*m_logger, debug) << "Removing file '" << filePath << "' from file cache.";
        auto& fileEntry = it->second;
        fileEntry.unlink();

        // The entry leaves the totals now; the file itself is deleted (and its handle closed) later on the background
        // thread, outside this mutex.
        UpdateEntryUnsafe(fileEntry, [](FileCacheEntry&) {}, true);
        m_pendingReleases.push_back({std::string(filePath), fileEntry.TakeReadHandle(), true});
        m_cache.erase(it);
        m_cv.notify_one();
    }
}

template <typename F> void FileCache::UpdateEntryUnsafe(FileCacheEntry& entry, F&& mutate, const bool removing) {
    const auto contribution = [](const FileCacheEntry& e) {
        const auto state = e.GetState();
        const bool counted =
            state != FileCacheEntry::State::Downloading && state != FileCacheEntry::State::QueuedForDownload;
        return std::pair<int64_t, size_t>{counted ? e.GetSize() : 0,
                                          state == FileCacheEntry::State::Stale && e.GetSize() == 0 ? 1U : 0U};
    };

    const auto [bytesBefore, staleBefore] = contribution(entry);
    m_activeBytes -= bytesBefore;
    m_undownloadedStaleCount -= staleBefore;
    mutate(entry);
    if (!removing) {
        const auto [bytesAfter, staleAfter] = contribution(entry);
        m_activeBytes += bytesAfter;
        m_undownloadedStaleCount += staleAfter;
        if (staleAfter != 0 && !entry.MetadataOnly.is_linked()) {
            m_metadataOnlyList.push_front(entry);
        } else if (staleAfter == 0) {
            entry.MetadataOnly.unlink();
        }
    } else {
        entry.MetadataOnly.unlink();
    }
}

// Runs on the background thread with no lock held. Closing a handle waits for any reader still using it, and only
// then is the file deleted, which matters on Windows where an open file cannot be removed.
void FileCache::ReleaseOnBackgroundThread(std::vector<PendingRelease>& releases) {
    for (auto& release : releases) {
        if (release.Handle) {
            release.Handle->Close();
        }
        if (release.RemoveFromDisk && !TryDeleteCachedFile(release.FilePath)) {
            std::scoped_lock lock(m_mutex);
            m_failedDeletes.push_back(release.FilePath);
        }
    }
    releases.clear();
}

// A file that cannot be deleted is logged and remembered for a later retry; its bytes have already left the
// cache's accounting, so the only cost of a failure is disk space until the retry succeeds.
bool FileCache::TryDeleteCachedFile(const std::string& filePath) {
    const auto cachedFilePath = m_cachePath / filePath;
    try {
        if (m_filesystem->DeleteFile(cachedFilePath)) {
            return true;
        }
        BOOST_LOG_SEV(*m_logger, warning)
            << "Could not delete cached file '" << cachedFilePath.string() << "'. It will be retried.";
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, warning) << "Deleting cached file '" << cachedFilePath.string()
                                          << "' failed: " << e.what() << ". It will be retried.";
    }
    return false;
}

int64_t FileCache::GetCurrentSizeUnsafe() const noexcept { return m_activeBytes; }

// Debug-only cross-check of the running totals against a full scan of the entries.
void FileCache::ValidateCountersUnsafe() const {
#ifndef NDEBUG
    int64_t bytes = 0;
    size_t stale = 0;
    size_t linked = 0;
    for (const auto& entry : m_entryList) {
        linked += entry.MetadataOnly.is_linked() ? 1 : 0;
        const auto state = entry.GetState();
        if (state != FileCacheEntry::State::Downloading && state != FileCacheEntry::State::QueuedForDownload) {
            bytes += entry.GetSize();
        }
        if (state == FileCacheEntry::State::Stale && entry.GetSize() == 0) {
            ++stale;
        }
    }
    assert(bytes == m_activeBytes && "Active byte total should match the entries");
    assert(stale == m_undownloadedStaleCount && "Stale entry count should match the entries");
    assert(linked == stale && "Every metadata-only stale entry should be in the prune list");
#endif
}
} // namespace AVEVA::RocksDB::Plugin::Core
