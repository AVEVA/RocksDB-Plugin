#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <azure/storage/blobs/page_blob_client.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class ReadableFileImpl
    {
        std::string m_name;
        std::shared_ptr<::Azure::Storage::Blobs::PageBlobClient> m_blobClient;
        std::shared_ptr<Core::FileCache> m_fileCache;
        uint64_t m_offset;
        uint64_t m_size;

    public:
        ReadableFileImpl(std::string_view name,
            std::shared_ptr<::Azure::Storage::Blobs::PageBlobClient> blobClient,
            std::shared_ptr<Core::FileCache> fileCache);

        // NOTE: Increments m_offset
        [[nodiscard]] uint64_t SequentialRead(size_t bytesToRead, char* buffer);

        // NOTE: Random so doesn't affect the sequential reads
        [[nodiscard]] uint64_t RandomRead(uint64_t offset, size_t bytesToRead, char* buffer) const;

        uint64_t GetOffset() const;
        void Skip(uint64_t n);
    };
}
