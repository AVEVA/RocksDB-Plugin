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
    virtual std::unique_ptr<Core::BlobClient> GetBlobClient(const std::string& path) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl