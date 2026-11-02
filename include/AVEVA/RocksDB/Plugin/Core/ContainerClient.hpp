// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <filesystem>
#include <stop_token>
#include <string>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>The name of a blob within its container, distinct from a local path or any other string.</summary>
class BlobName {
  public:
    explicit BlobName(std::string name) : m_name(std::move(name)) {}

    // The name lives as long as this object.
    [[nodiscard]] const std::string& Value() const noexcept { return m_name; }

  private:
    std::string m_name;
};

class ContainerClient {
  public:
    ContainerClient() = default;
    virtual ~ContainerClient() = default;

    /// <summary>
    /// Returns the size of the blob's representable data. The lookup should be abandoned (throwing) soon after
    /// stopToken requests a stop, so shutdown does not wait for a slow request.
    /// </summary>
    virtual ByteCount GetBlobSize(const BlobName& name, std::stop_token stopToken) = 0;

    /// <summary>
    /// Downloads [offset, offset + length) of the blob to the local file at destinationPath. The download should be
    /// abandoned (throwing) soon after stopToken requests a stop, so shutdown does not wait for a large transfer.
    /// </summary>
    virtual void DownloadBlobTo(const BlobName& name, const std::filesystem::path& destinationPath, ByteOffset offset,
                                ByteCount length, std::stop_token stopToken) = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Core
