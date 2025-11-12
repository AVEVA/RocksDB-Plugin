// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
#include <boost/log/trivial.hpp>
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Core
{
    FileCache::FileCache(std::filesystem::path cachePath,
        std::size_t maxCacheSize,
        std::shared_ptr<ContainerClient> containerClient,
        std::shared_ptr<Filesystem> filesystem,
        std::shared_ptr<boost::log::sources::logger_mt> logger)
        : m_cachePath(std::move(cachePath)),
        m_maxSize(maxCacheSize),
        m_containerClient(std::move(containerClient)),
        m_filesystem(std::move(filesystem)),
        m_logger(std::move(logger)),
        m_backgroundDownloader(&FileCache::BackgroundDownload, this, m_stopSource.get_token())
    {
    }

    FileCache::~FileCache()
    {
        {
            std::scoped_lock lock(m_mutex);
            m_stopSource.request_stop();
        }

        m_cv.notify_all();
        m_backgroundDownloader.join();
    }

    bool FileCache::HasFile(std::string_view filePath)
    {
        std::scoped_lock lock(m_mutex);
        auto it = m_cache.find(filePath);
#if _DEBUG
        auto result = std::find_if(m_entryList.begin(), m_entryList.end(), [&filePath](FileCacheEntry& entry)
            {
                return entry.GetFilePath() == filePath;
            });
        if (it != m_cache.end())
        {
            assert(result != m_entryList.end() && "FileCacheEntry should also be in the entry list");
        }
#endif
        return it != m_cache.end();
    }

    void FileCache::MarkFileAsStaleIfExists(const std::string& filePath)
    {
        std::scoped_lock lock(m_mutex);
        auto it = m_cache.find(filePath);
        if (it != m_cache.end())
        {
            // Mark as stale if exists. This should prevent reads from
            // accessing the file while it's being downloaded.
            if (it->second.GetState() != FileCacheEntry::State::QueuedForDownload)
            {
                BOOST_LOG_SEV(*m_logger, boost::log::trivial::info) << "Marking file '" << filePath << "' as stale";

                // If the file still in the download queue we don't have to worry about marking as stale.
                // This is because when the file is downloaded, it will get the most current state from
                // azure.
                it->second.SetState(FileCacheEntry::State::Stale);
            }
        }
    }

    std::optional<std::size_t> FileCache::ReadFile(const std::string_view filePath, uint64_t offset, std::size_t bytesToRead, char* buffer)
    {
        // If there are extensions that should be filtered on then we need to process that first.
        const auto fileType = RocksDBHelpers::GetFileType(filePath);
        if (fileType != RocksDBHelpers::FileClass::SST)
        {
            return std::nullopt;
        }

        std::unique_lock lock(m_mutex);
        auto it = m_cache.find(filePath);
        if (it == m_cache.end())
        {
            // File not found, create a new entry
            BOOST_LOG_SEV(*m_logger, debug) << "File not found in cache '" << filePath << "'";

            auto [inserted, _] = m_cache.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(std::string(filePath)),
                std::forward_as_tuple(filePath, 0));
            m_entryList.push_front(inserted->second);

            BOOST_LOG_SEV(*m_logger, info) << "Queueing for download: '" << filePath << "'";
            m_fileDownloadQueue.emplace(filePath);

            lock.unlock();
            m_cv.notify_one();
            return std::nullopt;
        }
        else
        {
            auto& fileEntry = it->second;
            if (fileEntry.GetState() != FileCacheEntry::State::Active)
            {
                // File is stale, do not read, but queue in the background for download.
                const auto state = fileEntry.GetState();
                if (state == FileCacheEntry::State::Stale)
                {
                    BOOST_LOG_SEV(*m_logger, info) << "File is stale. Queueing for redownload: '" << filePath << "'";
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
            auto file = m_filesystem->Open(cachedFilePath);
            if (buffer != nullptr)
            {
                const auto bytesRead = file->Read(buffer, offset, bytesToRead);
                return bytesRead;
            }
            else
            {
                return 0;
            }
        }
    }

    void FileCache::RemoveFile(const std::string_view filePath)
    {
        std::scoped_lock lock(m_mutex);
        RemoveFileUnsafe(filePath);
    }

    size_t FileCache::CacheSize()
    {
        std::scoped_lock lock(m_mutex);
        const auto size = GetCurrentSizeUnsafe();
#if _DEBUG
        // Validate that size matches what is in our map
        size_t validatedSize = 0;
        for (const auto& [_, value] : m_cache)
        {
            validatedSize += value.GetSize();
        }

        assert(validatedSize == size && "Sizes should match between data structures");
#endif
        return size;
    }

    void FileCache::SetCacheSize(std::size_t size)
    {
        std::scoped_lock lock(m_mutex);
        const auto currentSize = GetCurrentSizeUnsafe();
        if (currentSize > size)
        {
            // Evict files until we are under the new size.
            const auto bytesToEvict = currentSize - size;
            EvictAtLeast(bytesToEvict);
        }

        m_maxSize = size;
    }

    void FileCache::BackgroundDownload(std::stop_token stopToken)
    {
        while (true)
        {
            try
            {
                std::string filePath;
                {
                    std::unique_lock lock(m_mutex);
                    if (stopToken.stop_requested())
                    {
                        BOOST_LOG_SEV(*m_logger, info) << "File cache should close. Exiting thread.";
                        lock.unlock();
                        return; // Exit the thread if we are closing
                    }

                    if (m_fileDownloadQueue.empty())
                    {
                        BOOST_LOG_SEV(*m_logger, debug) << "File cache queue is empty. Waiting for condition variable.";
                        m_cv.wait(lock, [this, &stopToken]() { return !m_fileDownloadQueue.empty() || stopToken.stop_requested(); });
                    }

                    // We could have been woken up because it's time to close.
                    if (stopToken.stop_requested())
                    {
                        BOOST_LOG_SEV(*m_logger, info) << "File cache should close. Exiting thread.";
                        lock.unlock();
                        return; // Exit the thread if we are closing
                    }

                    filePath = std::move(m_fileDownloadQueue.front());
                    m_fileDownloadQueue.pop();

                    auto it = m_cache.find(filePath);
                    if (it != m_cache.end())
                    {
                        BOOST_LOG_SEV(*m_logger, info) << "Downloading '" << filePath << "' into cache";
                        if (it->second.GetState() != FileCacheEntry::State::QueuedForDownload)
                        {
                            // The file was marked as stale while we were waiting for the condition variable.
                            // We should not download it.
                            BOOST_LOG_SEV(*m_logger, info) << "File '" << filePath << "' is no longer marked as queued for download. Skipping download.";
                            continue;
                        }
                    }
                    else
                    {
                        // The file was likely deleted while we were waiting for the condition variable.
                        BOOST_LOG_SEV(*m_logger, info) << "File pending download '" << filePath << "' was deleted. Skipping download.";
                        continue;
                    }
                }

                size_t fileSize = 0;
                try
                {
                    auto blobClient = m_containerClient->GetBlobClient(filePath);
                    fileSize = blobClient->GetSize();
                }
                catch (std::exception& e)
                {
                    BOOST_LOG_SEV(*m_logger, error) << "Failed to get file size for '" << filePath << "'. Error: " << e.what();
                    continue;
                }

                // Set the cache entries size and evict
                {
                    std::unique_lock lock(m_mutex);
                    auto it = m_cache.find(filePath);
                    if (it != m_cache.end())
                    {
                        it->second.SetSize(fileSize);
                    }
                    else
                    {
                        // The file was likely deleted while we were waiting for the condition variable.
                        BOOST_LOG_SEV(*m_logger, error) << "Could not find file entry '" << filePath << "' in cache after getting file size. Skipping download.";
                        continue;
                    }

                    const auto currentSize = GetCurrentSizeUnsafe();
                    if (currentSize + fileSize > m_maxSize)
                    {
                        if (fileSize <= m_maxSize)
                        {
                            BOOST_LOG_SEV(*m_logger, info) << "Cache is full at "
                                << currentSize << " (bytes). Max "
                                << m_maxSize
                                << " (bytes). Evicting files to make room for '"
                                << filePath
                                << "' of size "
                                << fileSize
                                << " (bytes)";

                            const auto bytesToEvict = (currentSize + fileSize) % m_maxSize;
                            if (!EvictAtLeast(bytesToEvict))
                            {
                                BOOST_LOG_SEV(*m_logger, error) << "Couldn't evict enough space to fit new file '" << filePath << "'";
                                RemoveFileUnsafe(filePath);
                                continue;
                            }
                        }
                        else
                        {
                            BOOST_LOG_SEV(*m_logger, info) << "Skipping eviction from file cache because the file '"
                                << filePath
                                << "' of size "
                                << fileSize
                                << " (bytes) is greater than the maximum of "
                                << m_maxSize;

                            RemoveFileUnsafe(filePath);
                            continue;
                        }
                    }

                    // Mark the file as downloading now so we don't queue it again.
                    it->second.SetState(FileCacheEntry::State::Downloading);
                }

                try
                {
                    auto blobClient = m_containerClient->GetBlobClient(filePath);
                    const auto actualFilePath = m_cachePath / filePath;

                    // No need to download the _whole_ blob. There could be lots of padding
                    // at the end of the file. We can just download the actual size.
                    blobClient->DownloadTo(actualFilePath.string(), 0, fileSize);
                }
                catch (std::exception& e)
                {
                    // NOTE: We're not putting the filePath back on the download queue because
                    // this operation could _also_ throw. Let the next read be a cache miss which
                    // will queue up the download again.
                    BOOST_LOG_SEV(*m_logger, error) << "Failed to download file '" << filePath << "'. Removing entry from cache. Error: " << e.what();

                    std::scoped_lock lock(m_mutex);
                    RemoveFileUnsafe(filePath);
                    continue;
                }

                BOOST_LOG_SEV(*m_logger, info) << "Finished downloading file '" << filePath << "'";

                // Mark the file as active in the cache.
                std::unique_lock lock(m_mutex);
                auto it = m_cache.find(filePath);
                if (it != m_cache.end())
                {
                    if (it->second.GetState() == FileCacheEntry::State::Stale)
                    {
                        // Someone has marked this file as stale while we were downloading it.
                        // We should not mark it as active.
                        BOOST_LOG_SEV(*m_logger, info) << "File '" << filePath << "' was marked as stale while we were downloading it. Will not mark as active.";
                        continue;
                    }

                    BOOST_LOG_SEV(*m_logger, info) << "Marking file '" << filePath << "' as active";
                    it->second.SetState(FileCacheEntry::State::Active);
                }
                else
                {
                    // The file was likely deleted. We should clean up after ourselves.
                    BOOST_LOG_SEV(*m_logger, info) << "File '" << filePath << "' was deleted. Removing file from cache";
                    std::error_code ec;
                    auto cachedFilePath = m_cachePath / filePath;
                    m_filesystem->DeleteFile(cachedFilePath);
                }
            }
            catch (const std::exception& e)
            {
                BOOST_LOG_SEV(*m_logger, error) << "Error in file cache background downloader thread: '" << e.what() << "'";
            }
        }
    }

    void FileCache::EntryAccessedUnsafe(FileCacheEntry& file)
    {
        file.Accessed();
        file.unlink();
        m_entryList.push_front(file);
    }

    bool FileCache::EvictAtLeast(std::size_t bytes)
    {
        if (bytes > m_maxSize)
        {
            // No point in evicting everything from the cache if we can't fit the new file.
            BOOST_LOG_SEV(*m_logger, info) << "Skipping eviction from file cache because "
                << bytes
                << " bytes is greater than the maximum of "
                << m_maxSize;
            return false;
        }

        BOOST_LOG_SEV(*m_logger, info) << "Attempting to evict " << bytes << " (bytes) from the file cache.";
        size_t bytesEvicted = 0;
        auto tail = m_entryList.s_iterator_to(m_entryList.back());
        while (bytesEvicted < bytes && tail != m_entryList.begin())
        {
            // Don't evict downloading files. They could be files that we are making space for.
            const auto state = tail->GetState();
            if (state == FileCacheEntry::State::Downloading || state == FileCacheEntry::State::QueuedForDownload)
            {
                BOOST_LOG_SEV(*m_logger, info) << "Skipping eviction of '" << tail->GetFilePath() << "'. It is currently downloading or queued for download";
                tail = --tail;
                continue;
            }

            const auto& filePath = tail->GetFilePath();
            const auto fileSize = tail->GetSize();

            BOOST_LOG_SEV(*m_logger, info) << "Evicting '" << filePath << "' of size " << fileSize << "'bytes' from file cache";
            tail = --tail;
            bytesEvicted += fileSize;

            RemoveFileUnsafe(filePath);
        }

        return bytesEvicted >= bytes;
    }

    void FileCache::RemoveFileUnsafe(const std::string_view filePath)
    {
        auto it = m_cache.find(filePath);
        if (it != m_cache.end())
        {
            BOOST_LOG_SEV(*m_logger, info) << "Removing file '" << filePath << "' from file cache.";
            auto& fileEntry = it->second;
            fileEntry.unlink();

            // Capture data needed before we erase the entry.
            const auto cachedFilePath = m_cachePath / filePath;

            m_cache.erase(it);
            m_filesystem->DeleteFile(cachedFilePath);
        }
    }

    std::size_t FileCache::GetCurrentSizeUnsafe() const noexcept
    {
        size_t size = 0;
        for (const auto& entry : m_entryList)
        {
            const auto state = entry.GetState();
            if (state != FileCacheEntry::State::Downloading && state != FileCacheEntry::State::QueuedForDownload)
            {
                size += entry.GetSize();
            }
        }

        return size;
    }
}
