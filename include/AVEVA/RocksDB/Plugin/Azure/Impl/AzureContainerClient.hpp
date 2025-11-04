#pragma once
#include "AVEVA/RocksDB/Plugin/Core/ContainerClient.hpp"

#include <azure/storage/blobs.hpp>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class AzureContainerClient final : public Core::ContainerClient
    {
        ::Azure::Storage::Blobs::BlobContainerClient m_client;

    public:
        AzureContainerClient(::Azure::Storage::Blobs::BlobContainerClient client);
        virtual std::unique_ptr<Core::BlobClient> GetBlobClient(const std::string& path) override;
    };
}
