// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <cstdint>
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
    virtual void DownloadBlobTo(const std::string& path, const std::string& destinationPath, int64_t offset,
                                int64_t length) = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Core
