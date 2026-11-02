// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/CloseState.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlobWriter.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <boost/container/static_vector.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>Whether the buffered bytes have been handed to an upload.</summary>
enum class BufferState : std::uint8_t { Dirty, Flushed };

/// <summary>Whether a flush has reached the point of starting an upload.</summary>
enum class FlushNeed : std::uint8_t { NotYet, Due };

/// <summary>Whether a flush may also upload the trailing partial page (zero padded).</summary>
enum class PartialPage : std::uint8_t { Exclude, Include };

template <typename TBlob> class BasicWriteableFileImpl {
    PageBlobWriter<TBlob> m_w;
    int64_t m_bufferSize;

    int64_t m_lastPageOffset;
    int64_t m_size;
    int64_t m_bufferOffset;
    CloseState m_close;
    BufferState m_bufferState;
    // Unpersisted while data has been appended that the blob's size metadata does not yet reflect.
    Durability m_sizeDurability = Durability::Persisted;
    std::vector<char> m_buffer;

    // Upper bound on concurrent page uploads per file; bounds memory to this many buffer copies.
    static constexpr size_t MaxInFlightUploads = 4;

    // Shared with in-flight upload completions, which run on the host io_context and may outlive a move of this file.
    struct UploadTracker {
        // Parked coroutine waiting until at most MaxRemaining uploads are in flight.
        struct Waiter {
            size_t MaxRemaining;
            std::move_only_function<void()> Wake;
        };

        std::mutex Mutex;
        size_t InFlight = 0;
        std::exception_ptr Error;
        std::vector<Waiter> Waiters;
        // Buffers of finished uploads, reused so a flush does not allocate and copy a full buffer each time.
        // Fixed capacity, so returning a buffer from a payload destructor can never allocate or throw.
        boost::container::static_vector<std::vector<char>, MaxInFlightUploads> Free;

        void Begin();
        // Records the outcome and wakes the waiters it satisfies. noexcept: an allocation failure here would strand
        // them, so it terminates instead.
        void Complete(std::exception_ptr error) noexcept;
    };
    std::shared_ptr<UploadTracker> m_uploads = std::make_shared<UploadTracker>();

  public:
    template <typename T> using Task = BlobOperations::Task<T>;

    // Size and capacity of the blob when the caller already knows them, which saves two GetProperties round trips.
    struct BlobState {
        Core::ByteCount Size;
        Core::ByteCount Capacity;
    };

    BasicWriteableFileImpl(
        std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        Core::ByteCount bufferSize = Defaults::DataFileBufferSize);
    BasicWriteableFileImpl(
        std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        Core::ByteCount bufferSize, BlobState knownState);
    ~BasicWriteableFileImpl();
    BasicWriteableFileImpl(const BasicWriteableFileImpl&) = delete;
    BasicWriteableFileImpl& operator=(const BasicWriteableFileImpl&) = delete;
    BasicWriteableFileImpl(BasicWriteableFileImpl&&) noexcept;
    BasicWriteableFileImpl& operator=(BasicWriteableFileImpl&&) = delete;

    // Awaitable operations. They never block a thread, so they may run on the client's executor, but they are not
    // safe to overlap: a file has a single writer, so await each one before starting the next. `data` must stay
    // valid until AppendAsync completes. Failures are thrown from the awaitable.
    [[nodiscard]] Task<void> AppendAsync(std::span<const char> data);
    [[nodiscard]] Task<void> FlushAsync();
    // Starts uploading the buffered pages without waiting for them (see WriteableFile::RangeSync).
    [[nodiscard]] Task<void> RangeSyncAsync();
    [[nodiscard]] Task<void> SyncAsync();
    [[nodiscard]] Task<void> CloseAsync();

    // Blocking counterparts for RocksDB's threads, which must not be running the client's executor. Append and Flush
    // only touch the executor when they have to wait; the rest are one-line Block wrappers.
    void Close();
    void Append(const std::span<const char> data);
    void Flush();
    void RangeSync();
    void Sync();
    [[nodiscard]] int64_t GetFileSize() const noexcept;
    // Copies as much of the name as fits into `id` and returns how many bytes it wrote.
    [[nodiscard]] std::size_t GetUniqueId(std::span<char> id) const noexcept;

  private:
    // Copies as much of `data` as fits while a page of space remains and returns what is left.
    [[nodiscard]] std::span<const char> CopyIntoBuffer(std::span<const char> data);
    [[nodiscard]] FlushNeed FlushNeeded() const noexcept {
        return m_bufferOffset >= m_bufferSize / 2 ? FlushNeed::Due : FlushNeed::NotYet;
    }
    [[nodiscard]] Task<void> StartFlushAsync(PartialPage partialPage);
    void StartUpload(std::shared_ptr<const std::vector<char>> data, int64_t offset);
    [[nodiscard]] static Task<void> RunUpload(std::shared_ptr<UploadTracker> tracker,
                                              std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
                                              std::shared_ptr<const std::vector<char>> data, int64_t offset);
    [[nodiscard]] std::vector<char> RentBuffer() const;
    [[nodiscard]] Task<void> WaitForUploadsAsync(size_t maxRemaining);
    // Rethrows a recorded upload failure or a fenced writer without waiting for anything.
    void ReportUploadError();
    [[nodiscard]] size_t InFlightUploads() const noexcept;
};
extern template class BasicWriteableFileImpl<PageBlobClient>;

/// <summary>The production file, over the real page blob client. Non-template so it can be forward declared.</summary>
class WriteableFileImpl : public BasicWriteableFileImpl<PageBlobClient> {
  public:
    using BasicWriteableFileImpl<PageBlobClient>::BasicWriteableFileImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
