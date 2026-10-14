// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
#include <boost/log/trivial.hpp>
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
    m_backgroundDownloader = std::jthread(&FileCache::BackgroundDownload, this, m_stopSource.get_token());
}

FileCache::~FileCache() {
    {
        std::scoped_lock lock(m_mutex);
        m_stopSource.request_stop();
    }

    m_cv.notify_all();
    m_backgroundDownloader.join();
}

bool FileCache::HasFile(std::string_view filePath) {
    std::scoped_lock lock(m_mutex);
    auto it = m_cache.find(filePath);
#if _DEBUG
    auto result = std::find_if(m_entryList.begin(), m_entryList.end(),
                               [&filePath](FileCacheEntry& entry) { return entry.GetFilePath() == filePath; });
    if (it != m_cache.end()) {
        assert(result != m_entryList.end() && "FileCacheEntry should also be in the entry list");
    }
#endif
    return it != m_cache.end();
}

void FileCache::MarkFileAsStaleIfExists(const std::string& filePath) {
    std::scoped_lock lock(m_mutex);
    auto it = m_cache.find(filePath);
    if (it != m_cache.end()) {
        // Mark as stale if exists. This should prevent reads from
        // accessing the file while it's being downloaded.
        if (it->second.GetState() != FileCacheEntry::State::QueuedForDownload) {
            BOOST_LOG_SEV(*m_logger, debug) << "Marking file '" << filePath << "' as stale";

            // If the file still in the download queue we don't have to worry about marking as stale.
            // This is because when the file is downloaded, it will get the most current state from
            // azure.
            it->second.SetState(FileCacheEntry::State::Stale);
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
        PruneUndownloadedStaleEntriesUnsafe();
        return std::nullopt;
    } else {
        auto& fileEntry = it->second;
        if (fileEntry.GetState() != FileCacheEntry::State::Active) {
            // File is stale, do not read, but queue in the background for download.
            const auto state = fileEntry.GetState();
            if (state == FileCacheEntry::State::Stale) {
                BOOST_LOG_SEV(*m_logger, debug) << "File is stale. Queueing for download: '" << filePath << "'";
                m_fileDownloadQueue.emplace(filePath);

                // Mark as downloading now so we don't queue it again.
                fileEntry.SetState(FileCacheEntry::State::QueuedForDownload);

                lock.unlock();
                m_cv.notify_one();
            }

            return std::nullopt;
        }

        EntryAccessedUnsafe(fileEntry);

        auto cachedFilePath = m_cachePath / fileEntry.GetFilePath();

        // The read is disk I/O, so it must not hold the cache-wide mutex. If an eviction removes the file after the
        // lock is released, the read fails and the caller falls back to the blob.
        lock.unlock();
        if (buffer == nullptr) {
            return 0;
        }
        try {
            auto file = m_filesystem->Open(cachedFilePath);
            return file->Read(buffer, offset, bytesToRead);
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_logger, debug) << "Cached read of '" << filePath << "' failed, falling back: " << e.what();
            return std::nullopt;
        }
    }
}

void FileCache::RemoveFile(const std::string_view filePath) {
    std::scoped_lock lock(m_mutex);
    RemoveFileUnsafe(filePath);
}

int64_t FileCache::CacheSize() {
    std::scoped_lock lock(m_mutex);
    const auto size = GetCurrentSizeUnsafe();
#if _DEBUG
    // Validate that size matches what is in our map
    int64_t validatedSize = 0;
    for (const auto& [_, value] : m_cache) {
        validatedSize += value.GetSize();
    }

    assert(validatedSize == size && "Sizes should match between data structures");
#endif
    return size;
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

void FileCache::WaitUntilIdle() {
    std::unique_lock lock(m_mutex);
    m_idleCv.wait(lock, [this] { return m_fileDownloadQueue.empty() && !m_downloadInFlight; });
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
            {
                std::unique_lock lock(m_mutex);
                if (stopToken.stop_requested()) {
                    BOOST_LOG_SEV(*m_logger, debug) << "File cache should close. Exiting thread.";
                    lock.unlock();
                    return; // Exit the thread if we are closing
                }

                if (m_fileDownloadQueue.empty()) {
                    BOOST_LOG_SEV(*m_logger, debug) << "File cache queue is empty. Waiting for condition variable.";
                    m_cv.wait(lock, [this, &stopToken]() {
                        return !m_fileDownloadQueue.empty() || stopToken.stop_requested();
                    });
                }

                // We could have been woken up because it's time to close.
                if (stopToken.stop_requested()) {
                    BOOST_LOG_SEV(*m_logger, debug) << "File cache should close. Exiting thread.";
                    lock.unlock();
                    return; // Exit the thread if we are closing
                }

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
                        continue;
                    }
                } else {
                    // The file was likely deleted while we were waiting for the condition variable.
                    BOOST_LOG_SEV(*m_logger, debug)
                        << "File pending download '" << filePath << "' was deleted. Skipping download.";
                    continue;
                }
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
                    failed->second.SetState(FileCacheEntry::State::Stale);
                }
                continue;
            }

            // Set the cache entries size and evict
            {
                std::unique_lock lock(m_mutex);
                auto it = m_cache.find(filePath);
                if (it != m_cache.end()) {
                    it->second.SetSize(fileSize);
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
                it->second.SetState(FileCacheEntry::State::Downloading);
            }

            try {
                const auto actualFilePath = m_cachePath / filePath;

                // No need to download the _whole_ blob. There could be lots of padding
                // at the end of the file. We can just download the actual size.
                m_containerClient->DownloadBlobTo(filePath, actualFilePath.string(), 0, fileSize);
            } catch (std::exception& e) {
                // NOTE: We're not putting the filePath back on the download queue because
                // this operation could _also_ throw. Let the next read be a cache miss which
                // will queue up the download again.
                BOOST_LOG_SEV(*m_logger, error)
                    << "Failed to download file '" << filePath << "'. Removing entry from cache. Error: " << e.what();

                std::scoped_lock lock(m_mutex);
                RemoveFileUnsafe(filePath);
                continue;
            }

            BOOST_LOG_SEV(*m_logger, debug) << "Finished downloading file '" << filePath << "'";

            // Mark the file as active in the cache.
            std::unique_lock lock(m_mutex);
            auto it = m_cache.find(filePath);
            if (it != m_cache.end()) {
                if (it->second.GetState() == FileCacheEntry::State::Stale) {
                    // Someone has marked this file as stale while we were downloading it.
                    // We should not mark it as active.
                    BOOST_LOG_SEV(*m_logger, debug)
                        << "File '" << filePath
                        << "' was marked as stale while we were downloading it. Will not mark as active.";
                    continue;
                }

                BOOST_LOG_SEV(*m_logger, debug) << "Marking file '" << filePath << "' as active";
                it->second.SetState(FileCacheEntry::State::Active);
            } else {
                // The file was likely deleted. We should clean up after ourselves.
                BOOST_LOG_SEV(*m_logger, debug) << "File '" << filePath << "' was deleted. Removing file from cache";
                std::error_code ec;
                auto cachedFilePath = m_cachePath / filePath;
                m_filesystem->DeleteFile(cachedFilePath);
            }
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_logger, error) << "Error in file cache background downloader thread: '" << e.what() << "'";
        }
    }
}

void FileCache::EntryAccessedUnsafe(FileCacheEntry& file) {
    file.Accessed();
    file.unlink();
    m_entryList.push_front(file);
}

void FileCache::PruneUndownloadedStaleEntriesUnsafe() {
    auto staleEntries = CountUndownloadedStaleEntriesUnsafe();
    if (staleEntries <= MaxUndownloadedStaleEntries) {
        return;
    }

    std::vector<std::string> staleEntriesToRemove;
    staleEntriesToRemove.reserve(staleEntries - MaxUndownloadedStaleEntries);

    for (auto it = m_entryList.rbegin(); it != m_entryList.rend() && staleEntries > MaxUndownloadedStaleEntries; ++it) {
        if (it->GetState() == FileCacheEntry::State::Stale && it->GetSize() == 0) {
            staleEntriesToRemove.push_back(it->GetFilePath());
            staleEntries--;
        }
    }

    for (const auto& staleEntry : staleEntriesToRemove) {
        BOOST_LOG_SEV(*m_logger, debug) << "Pruning stale metadata-only entry '" << staleEntry << "'";
        RemoveFileUnsafe(staleEntry);
    }
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
        } else {
            const std::string filePath = current->GetFilePath();
            const auto fileSize = current->GetSize();

            BOOST_LOG_SEV(*m_logger, debug) << "Evicting '" << filePath << "' of size " << fileSize
                                            << "'bytes' from file cache";
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

        // Capture data needed before we erase the entry.
        const auto cachedFilePath = m_cachePath / filePath;

        m_cache.erase(it);
        m_filesystem->DeleteFile(cachedFilePath);
    }
}

int64_t FileCache::GetCurrentSizeUnsafe() const noexcept {
    int64_t size = 0;
    for (const auto& entry : m_entryList) {
        const auto state = entry.GetState();
        if (state != FileCacheEntry::State::Downloading && state != FileCacheEntry::State::QueuedForDownload) {
            size += entry.GetSize();
        }
    }

    return size;
}

size_t FileCache::CountUndownloadedStaleEntriesUnsafe() const noexcept {
    size_t count = 0;
    for (const auto& entry : m_entryList) {
        if (entry.GetState() == FileCacheEntry::State::Stale && entry.GetSize() == 0) {
            count++;
        }
    }

    return count;
}
} // namespace AVEVA::RocksDB::Plugin::Core
