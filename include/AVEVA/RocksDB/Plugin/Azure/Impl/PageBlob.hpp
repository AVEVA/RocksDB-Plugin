#pragma once
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include <azure/storage/blobs/page_blob_client.hpp>
#include <memory>
#include <string>
#include <cstdint>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class PageBlob : public Core::BlobClient
    {
        ::Azure::Storage::Blobs::PageBlobClient m_client;

    public:
        explicit PageBlob(::Azure::Storage::Blobs::PageBlobClient client);

        virtual uint64_t GetSize() override;
        virtual void DownloadTo(const std::string& path, uint64_t offset, uint64_t length) override;
    };
}
