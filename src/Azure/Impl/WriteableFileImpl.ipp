// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Definitions of BasicWriteableFileImpl; included at the end of WriteableFileImpl.hpp.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include <algorithm>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/log/trivial.hpp>
#include <limits>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TBlob>
AVEVA::RocksDB::Plugin::Azure::Impl::BasicWriteableFileImpl<TBlob>::BasicWriteableFileImpl(
    const std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    const int64_t bufferSize)
    : BasicWriteableFileImpl<TBlob>(name, std::move(runtime), blob, std::move(fileCache), std::move(logger), bufferSize,
                                    // One GetProperties call yields both the size and the capacity.
                                    [&blob] {
                                        const auto info = BlobOperations::GetInfo(*blob);
                                        return BlobState{info.Size, info.Capacity};
                                    }()) {}

template <typename TBlob>
BasicWriteableFileImpl<TBlob>::BasicWriteableFileImpl(
    const std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    const int64_t bufferSize, const BlobState knownState)
    : m_w(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger), knownState.Capacity),
      m_bufferSize(bufferSize), m_lastPageOffset(0), m_size(knownState.Size), m_bufferOffset(0), m_flushed(true) {
    // One page is not enough: a partial page cannot be flushed, so Append could never free space.
    if (m_bufferSize < 2 * Configuration::PageBlob::PageSize) {
        throw std::invalid_argument("Buffer size cannot be smaller than two pages");
    }

    // Asynchronous flushes upload whole pages only, so a page-aligned buffer guarantees a full page is always
    // available whenever the buffer runs out of space.
    m_bufferSize = (m_bufferSize / Configuration::PageBlob::PageSize) * Configuration::PageBlob::PageSize;

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
            const auto bytesDownloaded =
                BlobOperations::DownloadToBuffer(*m_w.H.Blob, m_buffer, m_lastPageOffset, lastPageBytes);
            BlobHelpers::RequireFullDownload(m_w.H.Name, m_lastPageOffset, lastPageBytes, bytesDownloaded);
            // The tail already matches the blob, so it stays "flushed" until Append changes it; otherwise a
            // reopen and close would upload an identical page, change the ETag and invalidate readers.
            m_bufferOffset = lastPageBytes;
        }
    }
}

template <typename TBlob> BasicWriteableFileImpl<TBlob>::~BasicWriteableFileImpl() {
    // Close() is the durability boundary. A destructor that ran sleeping retries would stall RocksDB's cleanup
    // threads, so it only reports unpersisted data and waits for uploads that are already running.
    // A moved-from file is closed and has lost its logger, which must not be dereferenced.
    if (!m_close.IsClosed()) {
        m_close.LogIfUnclosed(*m_w.H.Logger, m_w.H.Name, m_unsyncedSize);
    }

    // Outstanding upload completions use the blob client and logger, which are released below. Callers must not
    // destroy the file on the host executor while uploads are pending: this noexcept path cannot throw, so it logs
    // the violation and then still has to wait.
    if (m_uploads && HasPendingUploads()) {
        try {
            ThrowIfRunningOn(m_w.H.Blob->get_executor());
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_w.H.Logger, boost::log::trivial::error)
                << "Writeable file '" << m_w.H.Name
                << "' destroyed on the client executor with uploads pending; this will block: " << e.what();
        }
    }
    DrainUploads();
}

template <typename TBlob> bool BasicWriteableFileImpl<TBlob>::HasPendingUploads() const noexcept {
    std::scoped_lock lock(m_uploads->Mutex);
    return m_uploads->InFlight > 0;
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::DrainUploads() noexcept {
    if (!m_uploads) {
        return;
    }
    std::unique_lock lock(m_uploads->Mutex);
    m_uploads->Done.wait(lock, [&] { return m_uploads->InFlight == 0; });
}

template <typename TBlob>
BasicWriteableFileImpl<TBlob>::BasicWriteableFileImpl(BasicWriteableFileImpl<TBlob>&& other) noexcept
    : m_w(std::move(other.m_w)), m_bufferSize(other.m_bufferSize), m_lastPageOffset(other.m_lastPageOffset),
      m_size(other.m_size), m_bufferOffset(other.m_bufferOffset), m_close(std::move(other.m_close)),
      m_flushed(other.m_flushed), m_unsyncedSize(other.m_unsyncedSize), m_buffer(std::move(other.m_buffer)),
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
    Done.notify_all();
    // Outside the lock: a woken coroutine takes it again.
    for (auto& wake : ready) {
        wake();
    }
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::Close() {
    if (!m_close.IsClosed()) {
        Block(*m_w.H.Blob, CloseAsync());
    }
}

template <typename TBlob> BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::CloseAsync() {
    if (!m_close.IsClosed()) {
        co_await SyncAsync();
        m_close.MarkClosed();
    }
}

template <typename TBlob>
std::span<const char> BasicWriteableFileImpl<TBlob>::CopyIntoBuffer(std::span<const char> data) {
    while (!data.empty()) {
        const auto spaceLeft = m_bufferSize - m_bufferOffset;
        if (spaceLeft < Configuration::PageBlob::PageSize) {
            break;
        }

        const auto bytesToCopy = std::min(spaceLeft, static_cast<int64_t>(data.size()));
        std::copy_n(data.begin(), bytesToCopy, m_buffer.begin() + m_bufferOffset);

        data = data.subspan(static_cast<size_t>(bytesToCopy));
        m_bufferOffset += bytesToCopy;
        m_size += bytesToCopy;
        m_flushed = false; // Mark as not flushed since we added new data
        m_unsyncedSize = true;
    }
    return data;
}

// Appends to the page-aligned buffer and starts an asynchronous upload whenever less than a page of space remains,
// so RocksDB's small appends are batched into few, large page-blob writes instead of one request per Append.
template <typename TBlob>
BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::AppendAsync(std::span<const char> data) {
    while (!(data = CopyIntoBuffer(data)).empty()) {
        co_await StartFlushAsync(false);
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
    if (ShouldStartFlush()) {
        co_await StartFlushAsync(false);
    }
    ReportUploadError();
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::Flush() {
    if (ShouldStartFlush()) {
        Block(*m_w.H.Blob, FlushAsync());
    } else {
        ReportUploadError();
    }
}

template <typename TBlob> BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::RangeSyncAsync() {
    if (ShouldStartFlush()) {
        co_await StartFlushAsync(false);
    }
}

template <typename TBlob> void BasicWriteableFileImpl<TBlob>::RangeSync() {
    if (ShouldStartFlush()) {
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
BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::StartFlushAsync(const bool includePartialPage) {
    if (m_bufferOffset == 0 || m_flushed) {
        co_return;
    }

    constexpr auto pageSize = Configuration::PageBlob::PageSize;
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
    m_flushed = tail == 0 || includePartialPage;
}

// Makes everything written so far durable and publishes the logical size. The size is a separate metadata write
// because a page blob's physical length is page-rounded.
template <typename TBlob> BlobOperations::Task<void> BasicWriteableFileImpl<TBlob>::SyncAsync() {
    // Every metadata write changes the blob's ETag and invalidates open readers, so a Sync with nothing new to
    // publish (e.g. Sync followed by Close) must not write the size again.
    if (m_unsyncedSize) {
        m_w.MarkStale();
    }

    co_await StartFlushAsync(true);
    co_await WaitForUploadsAsync(0);
    if (m_unsyncedSize) {
        co_await m_w.PublishSizeAsync(m_size);
        m_unsyncedSize = false;
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
        m_w.H.Runtime->ThrowIfWritesFenced();
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
        co_await BlobOperations::UploadPagesAsync(*runtime, *blob, std::move(data), offset);
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
    m_w.H.Runtime->ThrowIfWritesFenced();
}

template <typename TBlob> int64_t BasicWriteableFileImpl<TBlob>::GetFileSize() const noexcept { return m_size; }

template <typename TBlob>
int64_t BasicWriteableFileImpl<TBlob>::GetUniqueId(char* id, const int64_t maxIdSize) const noexcept {
    const auto length = std::min(static_cast<int64_t>(m_w.H.Name.size()), maxIdSize);
    std::copy_n(m_w.H.Name.begin(), length, id);
    return length;
}

} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
