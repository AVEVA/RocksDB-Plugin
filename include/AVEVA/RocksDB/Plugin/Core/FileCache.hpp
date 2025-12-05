// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCacheEntry.hpp"
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <boost/intrusive/list.hpp>
#include <boost/log/trivial.hpp>

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
    private:
        void BackgroundDownload(std::stop_token stopToken);
        void EntryAccessedUnsafe(FileCacheEntry& file);
        bool EvictAtLeast(int64_t bytes);
        void RemoveFileUnsafe(std::string_view filePath);
        int64_t GetCurrentSizeUnsafe() const noexcept;
    };
}
