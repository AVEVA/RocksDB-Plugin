#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

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
        std::shared_ptr<Core::BlobClient> m_blobClient;
        std::shared_ptr<Core::FileCache> m_fileCache;
        std::shared_ptr<boost::log::sources::logger_mt> m_logger;

        int64_t m_size;
        int64_t m_syncSize;
        int64_t m_capacity;
        bool m_closed;

        std::vector<char> m_buffer;
        std::vector<BufferChunkInfo> m_bufferStats; // to track where page info is to be inserted
    public:
        ReadWriteFileImpl(std::string_view name,
            std::shared_ptr<Core::BlobClient> blobClient,
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
        void Write(int64_t offset, const char* data, int64_t size);
        int64_t Read(int64_t offset, int64_t bytesRequested, char* buffer) const;

    private:
        void Expand();
    };
}
