#pragma once
#include <azure/storage/blobs/blob_container_client.hpp>

#include <memory>
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class DirectoryImpl
    {
        ::Azure::Storage::Blobs::BlobContainerClient m_client;
        std::string m_name;

    public:
        DirectoryImpl(::Azure::Storage::Blobs::BlobContainerClient client, std::string_view dirname);
        void Fsync();
        size_t GetUniqueId(char* id, size_t maxSize) const noexcept;
    };
}
