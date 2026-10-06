// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <rocksdb/file_system.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure {
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

    AsyncReadRequest(const rocksdb::FSReadRequest& request, Callback callback, void* callbackArg);

    [[nodiscard]] uint64_t Offset() const noexcept { return m_offset; }
    [[nodiscard]] size_t Length() const noexcept { return m_length; }

    /// <summary>
    /// Records the outcome. On success `data` is copied into the scratch buffer, unless the request was aborted (the
    /// scratch buffer may already be released by then), in which case the outcome is discarded.
    /// </summary>
    void Complete(rocksdb::IOStatus status, std::string_view data);

    /// <summary>
    /// Blocks until the request completed or was aborted.
    /// </summary>
    void Wait();

    /// <summary>
    /// Stops the request from touching the scratch buffer. Does not block: an in-flight download is left to finish
    /// and its result is dropped. The filesystem waits for such downloads before it is destroyed.
    /// </summary>
    void Abort();

    /// <summary>
    /// Invokes RocksDB's callback with the outcome, at most once. Does nothing until completed or aborted.
    /// </summary>
    void DeliverCallback();

  private:
    enum class State { InFlight, Completed, Aborted };

    const uint64_t m_offset;
    const size_t m_length;
    char* const m_scratch;
    Callback m_callback;
    void* const m_callbackArg;

    std::mutex m_mutex;
    std::condition_variable m_finished;
    State m_state = State::InFlight;
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
/// Implements FileSystem::Poll: waits for every handle to complete and invokes the outstanding callbacks.
/// </summary>
rocksdb::IOStatus PollAsyncReads(const std::vector<void*>& ioHandles);

/// <summary>
/// Implements FileSystem::AbortIO: aborts every handle and invokes the outstanding callbacks.
/// </summary>
rocksdb::IOStatus AbortAsyncReads(const std::vector<void*>& ioHandles);
} // namespace AVEVA::RocksDB::Plugin::Azure
