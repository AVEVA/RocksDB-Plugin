// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/asio/io_context.hpp>
#include <rocksdb/file_system.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// A counter that async reads bump when they finish, so one thread can wait for "any of these reads" (see
/// BasicReadableFile::MultiRead) instead of waiting on them in a fixed order.
/// </summary>
class AsyncReadSignal {
  public:
    void Notify();
    [[nodiscard]] uint64_t Count();
    /// <summary>Blocks until the count exceeds `seen`.</summary>
    void WaitPast(uint64_t seen);

  private:
    std::mutex m_mutex;
    std::condition_variable m_changed;
    uint64_t m_count = 0;
};

/// <summary>
/// Shared state of one FSRandomAccessFile::ReadAsync request.
///
/// The blob download completes on a thread of the host io_context, which only copies the bytes into RocksDB's scratch
/// buffer. RocksDB's callback is invoked from Poll or AbortIO on the caller's thread (the same contract as RocksDB's
/// io_uring implementation), so no RocksDB code ever runs on the io_context. RocksDB's request object is not
/// guaranteed to outlive ReadAsync, so offset, length and scratch are copied here.
/// </summary>
class AsyncReadRequest {
  public:
    using Callback = std::function<void(rocksdb::FSReadRequest&, void*)>;

    using Executor = boost::asio::io_context::executor_type;

    // `completionExecutor`, when set, is the io_context the download completes on; Wait must not block on it.
    AsyncReadRequest(const rocksdb::FSReadRequest& request, Callback callback, void* callbackArg,
                     std::shared_ptr<AsyncReadSignal> signal = nullptr,
                     std::optional<Executor> completionExecutor = std::nullopt);

    /// <summary>
    /// Throws std::logic_error when the request is still in flight and the calling thread runs the executor that
    /// would complete it: waiting there can never finish.
    /// </summary>
    void ThrowIfWaitWouldDeadlock();

    [[nodiscard]] uint64_t Offset() const noexcept { return m_offset; }
    [[nodiscard]] size_t Length() const noexcept { return m_length; }

    /// <summary>True once the request completed or was aborted.</summary>
    [[nodiscard]] bool Finished();

    /// <summary>
    /// Records the outcome. On success `data` is copied into the scratch buffer, unless the request was aborted (the
    /// scratch buffer may already be released by then), in which case the outcome is discarded.
    /// </summary>
    void Complete(rocksdb::IOStatus status, std::string_view data);

    /// <summary>
    /// Fills the scratch buffer through `fill(char* scratch) -> std::optional<size_t>` while holding the request lock,
    /// so an aborted request never has its (possibly released) scratch touched. Returns true when the request is
    /// settled (completed here, or already finished/aborted) and false when `fill` produced nothing and the request is
    /// still in flight. Exceptions from `fill` propagate and leave the request untouched.
    /// </summary>
    template <class TFill> [[nodiscard]] bool TryCompleteInline(TFill&& fill) {
        {
            std::scoped_lock lock(m_mutex);
            if (m_state.load(std::memory_order_relaxed) != State::InFlight) {
                return true;
            }
            const std::optional<size_t> filled = fill(m_scratch);
            if (!filled) {
                return false;
            }
            m_bytesRead = std::min(*filled, m_length);
            m_status = rocksdb::IOStatus::OK();
            m_state.store(State::Completed, std::memory_order_release);
        }
        NotifyFinished();
        return true;
    }

    /// <summary>
    /// Blocks until the request completed or was aborted.
    /// </summary>
    void Wait();

    /// <summary>
    /// Describes a finished request the way RocksDB's callback would see it. Returns false while still in flight.
    /// </summary>
    [[nodiscard]] bool TryGetOutcome(rocksdb::FSReadRequest& outcome);

    /// <summary>
    /// Stops the request from touching the scratch buffer and cancels its private download. Does not block: the
    /// cancelled download finishes on its own and its result is dropped. The filesystem waits for such downloads
    /// before it is destroyed.
    /// </summary>
    void Abort();

    /// <summary>
    /// Signalled by Abort, so a private download started for this request can be cancelled instead of running on
    /// until its result is dropped.
    /// </summary>
    [[nodiscard]] std::stop_token StopToken() const noexcept { return m_stop.get_token(); }

    /// <summary>
    /// Invokes RocksDB's callback with the outcome, at most once. Does nothing until completed or aborted.
    /// </summary>
    void DeliverCallback();

  private:
    enum class State { InFlight, Completed, Aborted };

    void NotifyFinished();
    void FillOutcomeLocked(rocksdb::FSReadRequest& outcome) const;

    const uint64_t m_offset;
    const size_t m_length;
    char* const m_scratch;
    Callback m_callback;
    void* const m_callbackArg;
    std::shared_ptr<AsyncReadSignal> m_signal;
    const std::optional<Executor> m_completionExecutor;
    std::stop_source m_stop;

    std::mutex m_mutex;
    // Written under m_mutex; atomic so Finished() is lock-free and Wait() can block on the atomic itself.
    std::atomic<State> m_state{State::InFlight};
    rocksdb::IOStatus m_status;
    size_t m_bytesRead = 0;
    bool m_callbackDelivered = false;
};

/// <summary>
/// The opaque io_handle handed to RocksDB. RocksDB owns it and frees it through DeleteAsyncReadHandle; the download's
/// completion handler keeps its own reference to the request, so either side may finish first.
/// </summary>
struct AsyncReadHandle {
    std::shared_ptr<AsyncReadRequest> Request;
};

void DeleteAsyncReadHandle(void* handle);

/// <summary>
/// Implements FileSystem::Poll: waits for every handle to complete and invokes the outstanding callbacks. Polling a
/// read that is still in flight from a thread running the io_context it completes on returns InvalidArgument instead
/// of deadlocking.
/// </summary>
rocksdb::IOStatus PollAsyncReads(const std::vector<void*>& ioHandles);

/// <summary>
/// Implements FileSystem::AbortIO: aborts every handle and invokes the outstanding callbacks.
/// </summary>
rocksdb::IOStatus AbortAsyncReads(const std::vector<void*>& ioHandles);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
