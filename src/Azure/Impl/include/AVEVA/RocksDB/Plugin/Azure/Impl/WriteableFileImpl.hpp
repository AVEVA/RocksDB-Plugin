// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/CloseState.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlobWriter.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SignedBytes.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
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
    // Gives up on a file whose blob was deleted while it was open (RocksDB's DestroyDB does this to a live LOG): there
    // is nothing left to persist into, so the destructor must not report the buffered tail as lost.
    void AbandonDeleted() {
        BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::info)
            << "File '" << m_w.H.Name << "' was deleted while open; its buffered data is discarded";
        m_close.MarkClosed();
    }
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

template <typename TBlob>
AVEVA::RocksDB::Plugin::Azure::Impl::BasicWriteableFileImpl<TBlob>::BasicWriteableFileImpl(
    const std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    const Core::ByteCount bufferSize)
    : BasicWriteableFileImpl<TBlob>(name, std::move(runtime), blob, std::move(fileCache), std::move(logger), bufferSize,
                                    // One GetProperties call yields both the size and the capacity.
                                    [&blob] {
                                        const auto info = BlobOperations::GetInfo(*blob);
                                        return BlobState{.Size = CountFromSigned(info.Size),
                                                         .Capacity = CountFromSigned(info.Capacity)};
                                    }()) {}

template <typename TBlob>
BasicWriteableFileImpl<TBlob>::BasicWriteableFileImpl(
    const std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    const Core::ByteCount bufferSize, const BlobState knownState)
    : m_w(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger),
          SignedBytes(knownState.Capacity)),
      m_bufferSize(SignedBytes(bufferSize)), m_lastPageOffset(0), m_size(SignedBytes(knownState.Size)),
      m_bufferOffset(0), m_bufferState(BufferState::Flushed) {
    // One page is not enough: a partial page cannot be flushed, so Append could never free space.
    if (m_bufferSize < 2 * SignedPageSize) {
        throw std::invalid_argument("Buffer size cannot be smaller than two pages");
    }

    // Asynchronous flushes upload whole pages only, so a page-aligned buffer guarantees a full page is always
    // available whenever the buffer runs out of space.
    m_bufferSize = (m_bufferSize / SignedPageSize) * SignedPageSize;

    assert(m_bufferSize > 0);
    m_buffer.resize(static_cast<size_t>(m_bufferSize));
    if (m_size > 0) // Existing file with data
    {
        int64_t lastPageBytes;
        int64_t lastPageOffset;
        std::tie(lastPageBytes, lastPageOffset) = BlobHelpers::RoundToBeginningOfNearestPage(m_size);
        m_lastPageOffset = lastPageOffset;
        if (lastPageBytes > 0) // There is a partially filled page
        {
            const auto bytesDownloaded = BlobOperations::DownloadToBuffer(
                *m_w.H.Blob, m_buffer, OffsetFromSigned(m_lastPageOffset), CountFromSigned(lastPageBytes));
            BlobHelpers::RequireFullDownload(m_w.H.Name, OffsetFromSigned(m_lastPageOffset),
                                             CountFromSigned(lastPageBytes), bytesDownloaded);
            // The tail already matches the blob, so it stays "flushed" until Append changes it; otherwise a
            // reopen and close would upload an identical page, change the ETag and invalidate readers.
            m_bufferOffset = lastPageBytes;
        }
    }
}

template <typename TBlob> BasicWriteableFileImpl<TBlob>::~BasicWriteableFileImpl() {
    // Close() is the durability boundary. A destructor that ran sleeping retries would stall RocksDB's cleanup
    // threads, so it only reports unpersisted data.
    // A moved-from file is closed and has lost its logger, which must not be dereferenced.
    if (m_close.Status() == CloseStatus::Open) {
        m_close.LogIfUnclosed(*m_w.H.Logger, m_w.H.Name, m_sizeDurability);
    }

    // Uploads already running own the runtime, blob, payload and tracker, so they finish safely after the file is
    // gone. The destructor never waits for them: blocking here could deadlock when it runs on the client's io thread.
    if (m_uploads && InFlightUploads() > 0) {
        BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::warning)
            << "Writeable file '" << m_w.H.Name
            << "' destroyed with uploads still in flight; they complete in the background and any failure is lost.";
    }
}

template <typename TBlob> size_t BasicWriteableFileImpl<TBlob>::InFlightUploads() const noexcept {
    std::scoped_lock lock(m_uploads->Mutex);
    return m_uploads->InFlight;
}

template <typename TBlob>
BasicWriteableFileImpl<TBlob>::BasicWriteableFileImpl(BasicWriteableFileImpl<TBlob>&& other) noexcept
    : m_w(std::move(other.m_w)), m_bufferSize(other.m_bufferSize), m_lastPageOffset(other.m_lastPageOffset),
      m_size(other.m_size), m_bufferOffset(other.m_bufferOffset), m_close(std::move(other.m_close)),
      m_bufferState(other.m_bufferState), m_sizeDurability(other.m_sizeDurability), m_buffer(std::move(other.m_buffer)),
      m_uploads(std::move(other.m_uploads)) {}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::UploadTracker::Begin() {
    std::scoped_lock lock(Mutex);
    ++InFlight;
}

template <typename TBlob>
void BasicWriteableFileImpl<TBlob>::UploadTracker::Complete(std::exception_ptr error) noexcept {
    std::vector<std::move_only_function<void()>> ready;
    {
        std::scoped_lock lock(Mutex);
        --InFlight;
        if (error && !Error) {
            Error = std::move(error);
        }
        for (auto it = Waiters.begin(); it != Waiters.end();) {
            if (InFlight <= it->MaxRemaining) {
                ready.push_back(std::move(it->Wake));
                it = Waiters.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Outside the lock: a woken coroutine takes it again.
    for (auto& wake : ready) {
        wake();
    }
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::Close() {
    if (m_close.Status() == CloseStatus::Open) {
        Block(*m_w.H.Blob, CloseAsync());
    }
}

template <typename TBlob> BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::CloseAsync() {
    if (m_close.Status() == CloseStatus::Open) {
        co_await SyncAsync();
        m_close.MarkClosed();
    }
}

template <typename TBlob>
std::span<const char> BasicWriteableFileImpl<TBlob>::CopyIntoBuffer(std::span<const char> data) {
    while (!data.empty()) {
        const auto spaceLeft = m_bufferSize - m_bufferOffset;
        if (spaceLeft < SignedPageSize) {
            break;
        }

        const auto bytesToCopy = std::min(spaceLeft, static_cast<int64_t>(data.size()));
        std::copy_n(data.begin(), bytesToCopy, m_buffer.begin() + m_bufferOffset);

        data = data.subspan(static_cast<size_t>(bytesToCopy));
        m_bufferOffset += bytesToCopy;
        m_size += bytesToCopy;
        m_bufferState = BufferState::Dirty; // new data was added since the last flush
        m_sizeDurability = Durability::Unpersisted;
    }
    return data;
}

// Appends to the page-aligned buffer and starts an asynchronous upload whenever less than a page of space remains,
// so RocksDB's small appends are batched into few, large page-blob writes instead of one request per Append.
template <typename TBlob>
BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::AppendAsync(std::span<const char> data) {
    while (!(data = CopyIntoBuffer(data)).empty()) {
        co_await StartFlushAsync(PartialPage::Exclude);
    }
}

// Most appends fit in the buffer, so they are served on the calling thread; only one that needs an upload started
// goes through the executor.
template <typename TBlob> void BasicWriteableFileImpl<TBlob>::Append(const std::span<const char> data) {
    if (const auto rest = CopyIntoBuffer(data); !rest.empty()) {
        Block(*m_w.H.Blob, AppendAsync(rest));
    }
}

// RocksDB calls Flush after every WAL record, so it must stay cheap: once at least half the buffer is filled it starts
// the upload of the complete pages without waiting for it, and it leaves the trailing partial page and durability to
// Sync and Close (re-uploading the same zero-padded page on every flush would multiply the traffic). Below the
// threshold nothing is sent: readers only see data after Sync publishes the size, so per-page uploads gave no
// visibility, only one request per 512 B and a stall on every fifth one. An earlier failed upload is still reported.
template <typename TBlob> BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::FlushAsync() {
    if (FlushNeeded() == FlushNeed::Due) {
        co_await StartFlushAsync(PartialPage::Exclude);
    }
    ReportUploadError();
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::Flush() {
    if (FlushNeeded() == FlushNeed::Due) {
        Block(*m_w.H.Blob, FlushAsync());
    } else {
        ReportUploadError();
    }
}

template <typename TBlob> BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::RangeSyncAsync() {
    if (FlushNeeded() == FlushNeed::Due) {
        co_await StartFlushAsync(PartialPage::Exclude);
    }
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::RangeSync() {
    if (FlushNeeded() == FlushNeed::Due) {
        Block(*m_w.H.Blob, RangeSyncAsync());
    }
}

// Uploads the buffered data without waiting for its completion. Only the bounded back-pressure may suspend, and only
// to cap in-flight uploads; Flush merely reports a recorded error, while Sync and Close wait for
// durability. Asynchronous flushes send complete
// pages only and keep the trailing partial page buffered: uploading it zero-padded and again later with more data
// would put two overlapping requests in flight, and Azure does not order them. Only the final flush, which is
// always followed by a wait for all uploads, uploads the padded tail.
template <typename TBlob>
BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::StartFlushAsync(const PartialPage partialPage) {
    if (m_bufferOffset == 0 || m_bufferState == BufferState::Flushed) {
        co_return;
    }

    const auto pageSize = SignedPageSize;
    const auto includePartialPage = partialPage == PartialPage::Include;
    const auto fullBytes = (m_bufferOffset / pageSize) * pageSize;
    const auto tail = m_bufferOffset - fullBytes;
    const auto bytesToWrite = (includePartialPage && tail != 0) ? fullBytes + pageSize : fullBytes;
    if (bytesToWrite == 0) {
        co_return;
    }

    if ((m_lastPageOffset + bytesToWrite) > m_w.Capacity()) {
        // Resizing the blob while uploads are in flight would race with them.
        co_await WaitForUploadsAsync(0);
        co_await m_w.EnsureCapacityAsync(m_lastPageOffset + bytesToWrite);
    }

    // Back-pressure: also surfaces an earlier upload failure before more data is accepted.
    co_await WaitForUploadsAsync(MaxInFlightUploads - 1);
    if (includePartialPage && tail != 0) {
        // The zero padding past the tail is part of the page being uploaded.
        std::fill(m_buffer.begin() + m_bufferOffset, m_buffer.begin() + bytesToWrite, '\0');
    }
    // The full buffer becomes the upload payload as-is and a recycled buffer takes its place; only the partial-page
    // tail (under one page) is copied. A pooled buffer returns to the pool when the last reference to the payload,
    // held by the in-flight request, is released.
    // Everything fallible happens before m_buffer is touched, so a throw here leaves the buffered data intact.
    std::vector<char> next = RentBuffer();
    std::copy(m_buffer.begin() + fullBytes, m_buffer.begin() + m_bufferOffset, next.begin());
    // Owns an upload payload and hands its storage back to the pool when the last reference is released.
    struct PooledPayload {
        std::vector<char> Data;
        std::shared_ptr<UploadTracker> Pool;

        PooledPayload(std::vector<char> data, std::shared_ptr<UploadTracker> pool)
            : Data(std::move(data)), Pool(std::move(pool)) {}
        PooledPayload(const PooledPayload&) = delete;
        PooledPayload& operator=(const PooledPayload&) = delete;
        ~PooledPayload() {
            if (!Pool) {
                return;
            }
            std::scoped_lock lock(Pool->Mutex);
            if (Pool->Free.size() < MaxInFlightUploads) {
                Pool->Free.push_back(std::move(Data));
            }
        }
    };
    // The allocation may throw; make_shared forwards by reference and moves only after allocating, so `next` and
    // m_buffer are still intact then.
    const auto owner = std::make_shared<PooledPayload>(std::move(next), m_uploads);
    std::swap(m_buffer, owner->Data);
    owner->Data.resize(static_cast<size_t>(bytesToWrite));
    try {
        StartUpload(std::shared_ptr<const std::vector<char>>(owner, &owner->Data), m_lastPageOffset);
    } catch (...) {
        // Nothing was accepted, so the data stays buffered for a retry. Truncating the payload dropped the tail from
        // it; the tail now sits at the start of the current buffer, so copy it back before the swap. The rented
        // buffer ends up in the payload and returns to the pool when the payload is destroyed.
        owner->Data.resize(static_cast<size_t>(m_bufferSize));
        std::copy(m_buffer.begin(), m_buffer.begin() + tail, owner->Data.begin() + fullBytes);
        std::swap(m_buffer, owner->Data);
        throw;
    }
    BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::debug)
        << "Flushed " << bytesToWrite << " bytes to writeable file '" << m_w.H.Name << "'.";
    m_lastPageOffset += fullBytes;
    m_bufferOffset = tail;
    // The tail only counts as flushed once it has been uploaded.
    m_bufferState = (tail == 0 || includePartialPage) ? BufferState::Flushed : BufferState::Dirty;
}

// Makes everything written so far durable and publishes the logical size. The size is a separate metadata write
// because a page blob's physical length is page-rounded.
template <typename TBlob> BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::SyncAsync() {
    // Every metadata write changes the blob's ETag and invalidates open readers, so a Sync with nothing new to
    // publish (e.g. Sync followed by Close) must not write the size again.
    if (m_sizeDurability == Durability::Unpersisted) {
        m_w.MarkStale();
    }

    co_await StartFlushAsync(PartialPage::Include);
    co_await WaitForUploadsAsync(0);
    if (m_sizeDurability == Durability::Unpersisted) {
        co_await m_w.PublishSizeAsync(m_size);
        m_sizeDurability = Durability::Persisted;
    }
    BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::debug)
        << "Synced writeable file '" << m_w.H.Name << "' to " << m_size << " bytes";
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::Sync() { Block(*m_w.H.Blob, SyncAsync()); }

// Uploads of distinct page ranges are independent, so up to MaxInFlightUploads overlap. The payload is a buffer the
// caller no longer touches, so it is sent without a copy.
template <typename TBlob>
void BasicWriteableFileImpl<TBlob>::StartUpload(std::shared_ptr<const std::vector<char>> data, const int64_t offset) {
    m_uploads->Begin();
    try {
        // Fenced writes fail the upload without a request; checking here keeps that from needing the executor.
        m_w.H.Runtime->Fence().ThrowIfFenced();
    } catch (...) {
        m_uploads->Complete(std::current_exception());
        return;
    }
    try {
        boost::asio::co_spawn(m_w.H.Blob->get_executor(),
                              RunUpload(m_uploads, m_w.H.Runtime, m_w.H.Blob, std::move(data), offset),
                              boost::asio::detached);
    } catch (...) {
        m_uploads->Complete({});
        throw;
    }
}

// One upload, run detached so several overlap. The outcome goes to the tracker rather than being thrown. The
// payload and handles are released before completing: the file may be destroyed (and a buffer reused) as soon as the
// tracker reports the upload done.
template <typename TBlob>
BlobOperations::Task<void>
BasicWriteableFileImpl<TBlob>::RunUpload(std::shared_ptr<UploadTracker> tracker,
                                         std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
                                         std::shared_ptr<const std::vector<char>> data, const int64_t offset) {
    std::exception_ptr error;
    try {
        co_await BlobOperations::UploadPagesAsync(runtime, *blob, std::move(data), OffsetFromSigned(offset));
    } catch (...) {
        error = std::current_exception();
    }
    data.reset();
    blob.reset();
    runtime.reset();
    tracker->Complete(std::move(error));
}

template <typename TBlob> std::vector<char> BasicWriteableFileImpl<TBlob>::RentBuffer() const {
    std::vector<char> buffer;
    {
        std::scoped_lock lock(m_uploads->Mutex);
        if (!m_uploads->Free.empty()) {
            buffer = std::move(m_uploads->Free.back());
            m_uploads->Free.pop_back();
        }
    }
    // A buffer that carried a short final page was trimmed; growing it zero-fills only the missing part.
    buffer.resize(static_cast<size_t>(m_bufferSize));
    return buffer;
}

// Suspends until at most `maxRemaining` uploads are in flight, then reports the outcome (see ReportUploadError). The
// first upload failure is sticky: the data of a failed upload is gone, so every later Sync/Close must keep reporting
// it rather than claim durability. Writes that were fenced (lease lost) while uploads were in flight are reported
// too, even if those uploads succeeded: the service may have accepted them after another process took over, so they
// cannot be claimed as durable.
template <typename TBlob>
BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::WaitForUploadsAsync(const size_t maxRemaining) {
    const auto tracker = m_uploads;
    const auto mustWait = [&] {
        std::scoped_lock lock(tracker->Mutex);
        return tracker->InFlight > maxRemaining;
    };
    if (mustWait()) {
        co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable), void()>(
            [tracker, maxRemaining](auto handler) {
                // Resumed through the coroutine's executor, not on the thread that completed the upload.
                auto wake = [handler = std::move(handler)]() mutable {
                    const auto executor = boost::asio::get_associated_executor(handler);
                    boost::asio::post(executor, std::move(handler));
                };
                std::unique_lock lock(tracker->Mutex);
                if (tracker->InFlight <= maxRemaining) {
                    lock.unlock();
                    wake();
                    return;
                }
                tracker->Waiters.push_back({maxRemaining, std::move(wake)});
            },
            boost::asio::use_awaitable);
    }
    ReportUploadError();
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::ReportUploadError() {
    {
        std::scoped_lock lock(m_uploads->Mutex);
        if (m_uploads->Error) {
            std::rethrow_exception(m_uploads->Error);
        }
    }
    m_w.H.Runtime->Fence().ThrowIfFenced();
}

template <typename TBlob> int64_t BasicWriteableFileImpl<TBlob>::GetFileSize() const noexcept { return m_size; }

template <typename TBlob>
std::size_t BasicWriteableFileImpl<TBlob>::GetUniqueId(const std::span<char> id) const noexcept {
    const auto length = std::min(m_w.H.Name.size(), id.size());
    std::copy_n(m_w.H.Name.begin(), length, id.begin());
    return length;
}

} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
