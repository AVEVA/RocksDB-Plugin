// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
AzureContainerClient::AzureContainerClient(std::shared_ptr<ClientRuntime> runtime,
                                           std::shared_ptr<AzureClient::BlobContainerClient> client)
    : m_runtime(std::move(runtime)), m_client(std::move(client)) {}

std::unique_ptr<Core::BlobClient> AzureContainerClient::GetBlobClient(const std::string& path) {
    return std::make_unique<PageBlob>(m_runtime, m_client->GetPageBlobClient(path));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
