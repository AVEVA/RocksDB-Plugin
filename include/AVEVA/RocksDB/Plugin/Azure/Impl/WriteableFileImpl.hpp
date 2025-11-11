#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <azure/storage/blobs/page_blob_client.hpp>
#include <boost/log/sources/logger.hpp>

#include <cstdint>
#include <string>
#include <memory>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class WriteableFileImpl
    {
        std::string m_name;
        size_t m_bufferSize;
        std::shared_ptr<Core::BlobClient> m_blobClient;
        std::shared_ptr<Core::FileCache> m_fileCache;
        std::shared_ptr<boost::log::sources::logger_mt> m_logger;

        size_t m_lastPageOffset;
        size_t m_size;
        size_t m_capacity;
        size_t m_bufferOffset;
        bool m_closed;

        std::vector<char> m_buffer;

    public:
        WriteableFileImpl(std::string_view name,
            std::shared_ptr<Core::BlobClient> blobClient,
            std::shared_ptr<Core::FileCache> fileCache,
            std::shared_ptr<boost::log::sources::logger_mt> logger,
            size_t bufferSize = Configuration::PageBlob::DefaultBufferSize);
        ~WriteableFileImpl();
        WriteableFileImpl(const WriteableFileImpl&) = delete;
        WriteableFileImpl& operator=(const WriteableFileImpl&) = delete;
        WriteableFileImpl(WriteableFileImpl&&) noexcept;
        WriteableFileImpl& operator=(WriteableFileImpl&&) noexcept;

        void Close();
        void Append(const std::span<const char> data);
        void Flush();
        void Sync();
        void Truncate(uint64_t size);
        [[nodiscard]] uint64_t GetFileSize() const noexcept;
        [[nodiscard]] uint64_t GetUniqueId(char* id, size_t maxIdSize) const noexcept;

    private:
        void Expand();
    };
}
