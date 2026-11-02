// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <filesystem>
#include <stop_token>
#include <string>
namespace AVEVA::RocksDB::Plugin::Core {
class ContainerClient {
  public:
    ContainerClient() = default;
    virtual ~ContainerClient() = default;

    /// <summary>
    /// Returns the size of the blob's representable data. The lookup should be abandoned (throwing) soon after
    /// stopToken requests a stop, so shutdown does not wait for a slow request.
    /// </summary>
    virtual ByteCount GetBlobSize(const std::string& path, std::stop_token stopToken) = 0;

    /// <summary>
    /// Downloads [offset, offset + length) of the blob to the local file at destinationPath. The download should be
    /// abandoned (throwing) soon after stopToken requests a stop, so shutdown does not wait for a large transfer.
    /// </summary>
    virtual void DownloadBlobTo(const std::string& path, const std::filesystem::path& destinationPath,
                                ByteOffset offset, ByteCount length, std::stop_token stopToken) = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Core
