// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BufferChunkInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/CloseState.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlobWriter.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <boost/log/trivial.hpp>

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TBlob> class BasicReadWriteFileImpl {
    PageBlobWriter<TBlob> m_w;

    int64_t m_size;
    int64_t m_syncSize;
    CloseState m_close;
    // True from the first Write after a Sync until the next Sync; cached pages must not be served in between.
    bool m_dirty = false;
    std::vector<char> m_buffer;
    std::vector<BufferChunkInfo> m_bufferStats; // to track where page info is to be inserted
    // Extent buffers kept between flushes; one referenced only by this list is idle and can be reused.
    std::vector<std::shared_ptr<std::vector<char>>> m_extentPool;

    struct Extent {
        int64_t Start = 0;
        int64_t End = 0;
        std::vector<size_t> Chunks;              // indices into m_bufferStats, in write order
        std::shared_ptr<std::vector<char>> Data; // shared so each upload's request views it without a copy
    };
    struct GapRead {
        BlobOperations::PendingDownload<TBlob> Download;
        int64_t Offset = 0;
        int64_t Length = 0; // bytes the gap must receive in full
    };
    [[nodiscard]] std::vector<Extent> BuildExtents() const;
    void PrepareExtent(Extent& extent, int64_t sizeBefore, std::deque<GapRead>& reads);
    void FinishGapRead(GapRead& read);
    void OverlayChunks(const Extent& extent);
    void RunWave(std::vector<Extent>& extents, size_t begin, size_t end, int64_t sizeBefore);

  public:
    BasicReadWriteFileImpl(
        std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger);
    ~BasicReadWriteFileImpl();
    BasicReadWriteFileImpl(const BasicReadWriteFileImpl&) = delete;
    BasicReadWriteFileImpl& operator=(const BasicReadWriteFileImpl&) = delete;
    BasicReadWriteFileImpl(BasicReadWriteFileImpl&&) noexcept;
    BasicReadWriteFileImpl& operator=(BasicReadWriteFileImpl&&) = delete;

    void Close();
    void Sync();
    void Flush();
    [[nodiscard]] std::shared_ptr<std::vector<char>> AcquireExtentBuffer(size_t size);
    void TrimExtentPool();
    [[nodiscard]] size_t PooledExtentBufferCount() const noexcept { return m_extentPool.size(); }
    void Write(int64_t offset, const char* data, int64_t size);
    /// Provides read-your-writes: buffered bytes are flushed to the blob first, so data passed to Write is visible
    /// whether or not Sync has been called. Reads are bounded by the written size, not the last synced size.
    int64_t Read(int64_t offset, int64_t bytesRequested, char* buffer);
};
extern template class BasicReadWriteFileImpl<PageBlobClient>;

/// <summary>The production file, over the real page blob client. Non-template so it can be forward declared.</summary>
class ReadWriteFileImpl : public BasicReadWriteFileImpl<PageBlobClient> {
  public:
    using BasicReadWriteFileImpl<PageBlobClient>::BasicReadWriteFileImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
