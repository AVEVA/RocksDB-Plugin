// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstdint>
#include <filesystem>
#include <stop_token>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core {
class ContainerClient {
  public:
    ContainerClient() = default;
    virtual ~ContainerClient() = default;

    /// <summary>
    /// Returns the size of the blob's representable data.
    /// </summary>
    virtual int64_t GetBlobSize(const std::string& path) = 0;

    /// <summary>
    /// Downloads [offset, offset + length) of the blob to the local file at destinationPath.
    /// </summary>
    virtual void DownloadBlobTo(const std::string& path, const std::filesystem::path& destinationPath, int64_t offset,
                                int64_t length) = 0;

    /// <summary>
    /// As above, but the download should be abandoned (throwing) soon after stopToken requests a stop, so shutdown
    /// does not wait for a large transfer. The default ignores the token and calls the overload above.
    /// </summary>
    virtual void DownloadBlobTo(const std::string& path, const std::filesystem::path& destinationPath, int64_t offset,
                                int64_t length, std::stop_token stopToken) {
        static_cast<void>(stopToken);
        DownloadBlobTo(path, destinationPath, offset, length);
    }
};
} // namespace AVEVA::RocksDB::Plugin::Core
