// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    AzureContainerClient::AzureContainerClient(::Azure::Storage::Blobs::BlobContainerClient client)
        : m_client(std::move(client))
    {
    }

    std::unique_ptr<Core::BlobClient> AzureContainerClient::GetBlobClient(const std::string& path)
    {
        return std::make_unique<PageBlob>(m_client.GetPageBlobClient(path));
    }
}
