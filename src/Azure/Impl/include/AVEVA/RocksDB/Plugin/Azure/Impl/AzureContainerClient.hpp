// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <stop_token>
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

    Core::ByteCount GetBlobSize(const Core::BlobName& name, std::stop_token stopToken) override {
        auto blob = m_client->GetPageBlobClient(name.Value());
        return Core::ByteCount{static_cast<std::uint64_t>(BlobOperations::GetInfo(blob, std::move(stopToken)).Size)};
    }

    void DownloadBlobTo(const Core::BlobName& name, const std::filesystem::path& destinationPath,
                        Core::ByteOffset offset, Core::ByteCount length, std::stop_token stopToken) override {
        auto blob = m_client->GetPageBlobClient(name.Value());
        BlobOperations::DownloadToFile(blob, destinationPath, offset, length, std::move(stopToken));
    }
};

using AzureContainerClient = BasicAzureContainerClient<BlobContainerClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
