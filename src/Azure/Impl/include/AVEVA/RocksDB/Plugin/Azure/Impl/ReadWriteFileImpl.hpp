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

#include <boost/container/small_vector.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>Whether Write has been called since the last Sync.</summary>
enum class WriteState : std::uint8_t { Clean, Dirty };

template <typename TBlob> class BasicReadWriteFileImpl {
    PageBlobWriter<TBlob> m_w;

    int64_t m_size;
    int64_t m_syncSize;
    CloseState m_close;
    // Dirty from the first Write after a Sync until the next Sync; cached pages must not be served in between.
    WriteState m_writeState = WriteState::Clean;
    std::vector<char> m_buffer;
    std::vector<BufferChunkInfo> m_bufferStats; // to track where page info is to be inserted
    // Extent buffers kept between flushes; one referenced only by this list is idle and can be reused.
    std::vector<std::shared_ptr<std::vector<char>>> m_extentPool;

    struct Extent {
        int64_t Start = 0;
        int64_t End = 0;
        boost::container::small_vector<size_t, 8> Chunks; // indices into m_bufferStats, in write order
        std::shared_ptr<std::vector<char>> Data;          // shared so each upload's request views it without a copy
    };
    struct GapRead {
        std::span<char> Target; // inside the extent's buffer
        Core::ByteOffset Offset;
        Core::ByteCount Length; // bytes the gap must receive in full
    };
    [[nodiscard]] std::vector<Extent> BuildExtents() const;
    [[nodiscard]] Durability Persistence() const noexcept;
    void PlanGapReads(Extent& extent, int64_t sizeBefore, std::vector<GapRead>& reads);
    BlobOperations::Task<void> ReadGapAsync(GapRead read);
    void OverlayChunks(const Extent& extent);
    // Owns its wave, so the buffers the reads and uploads use live exactly as long as the wave's frame.
    BlobOperations::Task<void> RunWaveAsync(std::vector<Extent> wave, int64_t sizeBefore);
    // Only Read awaits this, blocking until it completes, so `buffer` outlives every suspension.
    [[nodiscard]] BlobOperations::Task<Core::ByteCount> ReadAsync(Core::ByteOffset offset, std::span<char> buffer);

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

    // Awaitable operations for callers on the client's executor. They never block a thread, but a file has a single
    // user, so await each before starting the next. Failures are thrown from the awaitable.
    [[nodiscard]] BlobOperations::Task<void> FlushAsync();
    [[nodiscard]] BlobOperations::Task<void> SyncAsync();
    [[nodiscard]] BlobOperations::Task<void> CloseAsync();

    // Blocking counterparts for RocksDB's threads, which must not be running the client's executor.
    void Close();
    void Sync();
    void Flush();
    [[nodiscard]] std::shared_ptr<std::vector<char>> AcquireExtentBuffer(size_t size);
    void TrimExtentPool();
    [[nodiscard]] size_t PooledExtentBufferCount() const noexcept { return m_extentPool.size(); }
    void Write(Core::ByteOffset offset, std::span<const char> data);
    /// Provides read-your-writes: buffered bytes are flushed to the blob first, so data passed to Write is visible
    /// whether or not Sync has been called. Reads are bounded by the written size, not the last synced size; at most
    /// `buffer.size()` bytes are read.
    [[nodiscard]] Core::ByteCount Read(Core::ByteOffset offset, std::span<char> buffer);
};
extern template class BasicReadWriteFileImpl<PageBlobClient>;

/// <summary>The production file, over the real page blob client. Non-template so it can be forward declared.</summary>
class ReadWriteFileImpl : public BasicReadWriteFileImpl<PageBlobClient> {
  public:
    using BasicReadWriteFileImpl<PageBlobClient>::BasicReadWriteFileImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
