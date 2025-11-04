#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <azure/storage/blobs/page_blob_client.hpp>
#include <boost/log/sources/logger.hpp>

#include <cstdint>
#include <string_view>
#include <string>
#include <memory>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class ReadWriteFileImpl
    {
        std::string m_name;
        std::shared_ptr<::Azure::Storage::Blobs::PageBlobClient> m_blobClient;
        std::shared_ptr<Core::FileCache> m_fileCache;
        std::shared_ptr<boost::log::sources::logger_mt> m_logger;

        size_t m_size;
        size_t m_syncSize;
        size_t m_capacity;
        bool m_closed;

        std::vector<char> m_buffer;
        std::vector<BufferChunkInfo> m_bufferStats; // to track where page info is to be inserted
    public:
        ReadWriteFileImpl(std::string_view name,
            std::shared_ptr<::Azure::Storage::Blobs::PageBlobClient> blobClient,
            std::shared_ptr<Core::FileCache> fileCache,
            std::shared_ptr<boost::log::sources::logger_mt> logger);
        ~ReadWriteFileImpl();
        ReadWriteFileImpl(const ReadWriteFileImpl&) = delete;
        ReadWriteFileImpl& operator=(const ReadWriteFileImpl&) = delete;
        ReadWriteFileImpl(ReadWriteFileImpl&&) noexcept;
        ReadWriteFileImpl& operator=(ReadWriteFileImpl&&) noexcept;

        void Close();
        void Sync();
        void Flush();
        void Write(size_t offset, const char* data, size_t size);
        size_t Read(size_t offset, size_t bytesRequested, char* buffer) const;

    private:
        void Expand();
    };
}
