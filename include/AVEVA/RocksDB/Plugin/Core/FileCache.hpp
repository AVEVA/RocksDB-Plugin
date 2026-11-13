// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>Whether the cache holds an entry for a file (in any state).</summary>
enum class CachePresence : std::uint8_t {
    Absent,
    Present,
};

class FileCacheImpl;

/// <summary>
/// A local disk cache of SST blobs. A file is downloaded in the background on its second access and then served from
/// disk; the least recently used files are evicted to stay within the maximum size.
/// </summary>
class FileCache {
  public:
    /// <summary>
    /// The cache directory should be dedicated to this cache. Leftover `*.partial-*` files from an interrupted
    /// download are deleted at construction; other files from earlier runs are neither deleted nor counted.
    /// The destructor asks an in-flight download to stop (see ContainerClient::DownloadBlobTo) and then joins, so a
    /// host `io_context` serving the downloads must outlive the cache.
    /// </summary>
    FileCache(std::filesystem::path cachePath, ByteCount maxCacheSize, std::shared_ptr<ContainerClient> containerClient,
              std::shared_ptr<Filesystem> filesystem,
              std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
    ~FileCache();
    FileCache(const FileCache&) = delete;
    FileCache& operator=(const FileCache&) = delete;
    FileCache(FileCache&&) = delete;
    FileCache& operator=(FileCache&&) = delete;

    [[nodiscard]] CachePresence HasFile(std::string_view filePath) const;
    void MarkFileAsStaleIfExists(const std::string& filePath);
    /// <summary>
    /// Reads from the cached copy of the file into <paramref name="buffer"/> starting at <paramref name="offset"/>.
    /// Returns std::nullopt when the file is not (yet) cached, so the caller reads the blob instead. An empty buffer
    /// only reports whether the file is cached (returning zero bytes).
    /// </summary>
    [[nodiscard]] std::optional<ByteCount> ReadFile(std::string_view filePath, ByteOffset offset,
                                                    std::span<char> buffer);
    void RemoveFile(std::string_view filePath);
    [[nodiscard]] ByteCount CacheSize() const;
    void SetCacheSize(ByteCount size);

  private:
    // Waiting for the background downloader is for tests only; see FileCacheTestAccess (not installed).
    friend class FileCacheTestAccess;
    std::unique_ptr<FileCacheImpl> m_impl;
};
} // namespace AVEVA::RocksDB::Plugin::Core
