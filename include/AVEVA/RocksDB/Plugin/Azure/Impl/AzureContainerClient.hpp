// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Adapts a blob container to Core::ContainerClient. `TContainer` is the real container client in production and a
/// test double that hands out fake page blobs in tests.
/// </summary>
template <typename TContainer> class BasicAzureContainerClient final : public Core::ContainerClient {
    std::shared_ptr<ClientRuntimeBase> m_runtime;
    std::shared_ptr<TContainer> m_client;

  public:
    BasicAzureContainerClient(std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TContainer> client)
        : m_runtime(std::move(runtime)), m_client(std::move(client)) {}

    int64_t GetBlobSize(const std::string& path) override {
        auto blob = m_client->GetPageBlobClient(path);
        return BlobOperations::GetSize(blob);
    }

    void DownloadBlobTo(const std::string& path, const std::filesystem::path& destinationPath, int64_t offset,
                        int64_t length) override {
        auto blob = m_client->GetPageBlobClient(path);
        BlobOperations::DownloadToFile(blob, destinationPath, offset, length);
    }
};

using AzureContainerClient = BasicAzureContainerClient<BlobContainerClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
