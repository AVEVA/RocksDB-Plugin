#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <boost/intrusive/list.hpp>
#include <boost/log/sources/logger.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <queue>
namespace AVEVA::RocksDB::Plugin::Core
{
    class FileCache
    {
        std::filesystem::path m_cachePath;
        std::size_t m_maxSize;
        std::shared_ptr<ContainerClient> m_containerClient;
        std::shared_ptr<Filesystem> m_filesystem;
        std::shared_ptr<boost::log::sources::logger_mt> m_logger;
        bool m_shouldClose;

        std::mutex m_mutex;
        std::condition_variable m_cv;
        std::unordered_map<std::string, FileCacheEntry, StringHash, StringEqual> m_cache;
        std::queue<std::string> m_fileDownloadQueue;
        boost::intrusive::list<FileCacheEntry, boost::intrusive::constant_time_size<false>> m_entryList;
        std::jthread m_backgroundDownloader;
    public:
        FileCache(std::filesystem::path cachePath,
            std::size_t maxCacheSize,
            std::shared_ptr<ContainerClient> containerClient,
            std::shared_ptr<Filesystem> filesystem,
            std::shared_ptr<boost::log::sources::logger_mt> logger);
        ~FileCache();
        FileCache(const FileCache&) = delete;
        FileCache& operator=(const FileCache&) = delete;
        FileCache(FileCache&&) = delete;
        FileCache& operator=(FileCache&&) = delete;

        void MarkFileAsStaleIfExists(const std::string& filePath);
        [[nodiscard]] std::optional<std::size_t> ReadFile(const std::string& filePath, uint64_t offset, std::size_t bytesToRead, char* buffer);
        void RemoveFile(std::string_view filePath);
        [[nodiscard]] size_t CacheSize();
        void SetCacheSize(std::size_t size);
    private:
        void BackgroundDownload();
        void EntryAccessedUnsafe(FileCacheEntry& file);
        bool EvictAtLeast(std::size_t bytes);
        void RemoveFileUnsafe(std::string_view filePath);
        std::size_t GetCurrentSizeUnsafe() const noexcept;
    };
}
