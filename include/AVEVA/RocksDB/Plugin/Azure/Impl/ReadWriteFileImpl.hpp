// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/CloseWithRetries.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <boost/log/trivial.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TBlob> class BasicReadWriteFileImpl {
    BlobFileHandles<TBlob> m_h;

    int64_t m_size;
    int64_t m_syncSize;
    int64_t m_capacity;
    bool m_closed;
    // True from the first Write after a Sync until the next Sync; cached pages must not be served in between.
    bool m_dirty = false;
    std::vector<char> m_buffer;
    std::vector<BufferChunkInfo> m_bufferStats; // to track where page info is to be inserted
  public:
    BasicReadWriteFileImpl(
        std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
    ~BasicReadWriteFileImpl();
    BasicReadWriteFileImpl(const BasicReadWriteFileImpl&) = delete;
    BasicReadWriteFileImpl& operator=(const BasicReadWriteFileImpl&) = delete;
    BasicReadWriteFileImpl(BasicReadWriteFileImpl&&) noexcept;
    BasicReadWriteFileImpl& operator=(BasicReadWriteFileImpl&&) noexcept;

    void Close();
    void Sync();
    void Flush();
    void Write(int64_t offset, const char* data, int64_t size);
    /// Reads only data made durable by <see cref="Sync"/> (bounded by the size at open or at the last Sync). Bytes
    /// passed to Write, or flushed with Flush, are not visible to Read until Sync is called. RocksDB uses
    /// random-RW files only to write and then sync, so a read-your-writes guarantee is deliberately not provided.
    int64_t Read(int64_t offset, int64_t bytesRequested, char* buffer) const;

  private:
    void Expand();
};
extern template class BasicReadWriteFileImpl<PageBlobClient>;

/// <summary>The production file, over the real page blob client. Non-template so it can be forward declared.</summary>
class ReadWriteFileImpl : public BasicReadWriteFileImpl<PageBlobClient> {
  public:
    using BasicReadWriteFileImpl<PageBlobClient>::BasicReadWriteFileImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.ipp"
