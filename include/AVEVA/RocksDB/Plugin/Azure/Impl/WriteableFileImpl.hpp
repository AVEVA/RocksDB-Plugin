// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <azure/storage/blobs/page_blob_client.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <string>
#include <memory>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class WriteableFileImpl
    {
        std::string m_name;
        int64_t m_bufferSize;
        std::shared_ptr<Core::BlobClient> m_blobClient;
        std::shared_ptr<Core::FileCache> m_fileCache;
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

        int64_t m_lastPageOffset;
        int64_t m_size;
        int64_t m_capacity;
        int64_t m_bufferOffset;
        bool m_closed;
        bool m_flushed;

        std::vector<char> m_buffer;

    public:
        WriteableFileImpl(std::string_view name,
            std::shared_ptr<Core::BlobClient> blobClient,
            std::shared_ptr<Core::FileCache> fileCache,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            int64_t bufferSize = Configuration::PageBlob::DefaultBufferSize);
        ~WriteableFileImpl();
        WriteableFileImpl(const WriteableFileImpl&) = delete;
        WriteableFileImpl& operator=(const WriteableFileImpl&) = delete;
        WriteableFileImpl(WriteableFileImpl&&) noexcept;
        WriteableFileImpl& operator=(WriteableFileImpl&&) noexcept;

        void Close();
        void Append(const std::span<const char> data);
        void Flush();
        void Sync();
        void Truncate(int64_t size);
        [[nodiscard]] int64_t GetFileSize() const noexcept;
        [[nodiscard]] int64_t GetUniqueId(char* id, int64_t maxIdSize) const noexcept;

    private:
        void Expand();
    };
}
