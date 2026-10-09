// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class AzureContainerClient final : public Core::ContainerClient {
    std::shared_ptr<ClientRuntime> m_runtime;
    std::shared_ptr<AzureClient::BlobContainerClient> m_client;

  public:
    AzureContainerClient(std::shared_ptr<ClientRuntime> runtime,
                         std::shared_ptr<AzureClient::BlobContainerClient> client);
    virtual int64_t GetBlobSize(const std::string& path) override;
    virtual void DownloadBlobTo(const std::string& path, const std::string& destinationPath, int64_t offset,
                                int64_t length) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl