// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
AzureContainerClient::AzureContainerClient(std::shared_ptr<ClientRuntime> runtime,
                                           std::shared_ptr<AzureClient::BlobContainerClient> client)
    : m_runtime(std::move(runtime)), m_client(std::move(client)) {}

int64_t AzureContainerClient::GetBlobSize(const std::string& path) {
    auto blob = m_client->GetPageBlobClient(path);
    return BlobOperations::GetSize(*blob);
}

void AzureContainerClient::DownloadBlobTo(const std::string& path, const std::string& destinationPath, int64_t offset,
                                          int64_t length) {
    auto blob = m_client->GetPageBlobClient(path);
    BlobOperations::DownloadToFile(*blob, destinationPath, offset, length);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
