#pragma once
#include <azure/storage/blobs/page_blob_client.hpp>
#include <azure/storage/blobs/blob_lease_client.hpp>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class LockFileImpl
    {
        std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> m_file;
        std::unique_ptr<::Azure::Storage::Blobs::BlobLeaseClient> m_lease = nullptr;

    public:
        LockFileImpl(std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> file);
        bool Lock();
        void Renew() const;
        void Unlock();
    };
}
