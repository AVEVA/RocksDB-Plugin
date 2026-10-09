// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <boost/log/trivial.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class ReadWriteFileImpl {
    std::string m_name;
    // Declared before the client so that it is destroyed after it: the client references the runtime's HTTP client.
    std::shared_ptr<ClientRuntime> m_runtime;
    std::shared_ptr<AzureClient::PageBlobClient> m_blob;
    std::shared_ptr<Core::FileCache> m_fileCache;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    int64_t m_size;
    int64_t m_syncSize;
    int64_t m_capacity;
    bool m_closed;

    std::vector<char> m_buffer;
    std::vector<BufferChunkInfo> m_bufferStats; // to track where page info is to be inserted
  public:
    ReadWriteFileImpl(
        std::string_view name, std::shared_ptr<ClientRuntime> runtime,
        std::shared_ptr<AzureClient::PageBlobClient> blob, std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
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
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
