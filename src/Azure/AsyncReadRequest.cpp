// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/AsyncReadRequest.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure {
AsyncReadRequest::AsyncReadRequest(const rocksdb::FSReadRequest& request, Callback callback, void* callbackArg)
    : m_offset(request.offset), m_length(request.len), m_scratch(request.scratch), m_callback(std::move(callback)),
      m_callbackArg(callbackArg) {}

void AsyncReadRequest::Complete(rocksdb::IOStatus status, const std::string_view data) {
    {
        std::scoped_lock lock(m_mutex);
        if (m_state != State::InFlight) {
            return;
        }

        if (status.ok()) {
            m_bytesRead = std::min(data.size(), m_length);
            if (m_bytesRead > 0 && data.data() != m_scratch) {
                std::memcpy(m_scratch, data.data(), m_bytesRead);
            }
        }
        m_status = std::move(status);
        m_state = State::Completed;
    }
    m_finished.notify_all();
}

void AsyncReadRequest::Wait() {
    std::unique_lock lock(m_mutex);
    m_finished.wait(lock, [this] { return m_state != State::InFlight; });
}

void AsyncReadRequest::Abort() {
    {
        std::scoped_lock lock(m_mutex);
        if (m_state != State::InFlight) {
            return;
        }

        m_state = State::Aborted;
        m_status = rocksdb::IOStatus::Aborted("Async read aborted");
        m_bytesRead = 0;
    }
    m_finished.notify_all();
}

void AsyncReadRequest::DeliverCallback() {
    rocksdb::FSReadRequest request;
    {
        std::scoped_lock lock(m_mutex);
        if (m_callbackDelivered || m_state == State::InFlight) {
            return;
        }

        m_callbackDelivered = true;
        request.offset = m_offset;
        request.len = m_length;
        request.scratch = m_scratch;
        request.status = m_status;
        request.result = rocksdb::Slice(m_scratch, m_status.ok() ? m_bytesRead : 0);
    }

    // Invoked without holding the lock: the callback may re-enter the filesystem (e.g. issue another read).
    m_callback(request, m_callbackArg);
}

void DeleteAsyncReadHandle(void* handle) {
    auto* asyncHandle = static_cast<AsyncReadHandle*>(handle);
    if (asyncHandle == nullptr) {
        return;
    }

    // RocksDB may release the scratch buffer once the handle is gone, so a still-running download must not write it.
    if (asyncHandle->Request) {
        asyncHandle->Request->Abort();
    }
    delete asyncHandle;
}

rocksdb::IOStatus PollAsyncReads(const std::vector<void*>& ioHandles) {
    for (auto* handle : ioHandles) {
        auto* asyncHandle = static_cast<AsyncReadHandle*>(handle);
        if (asyncHandle == nullptr || !asyncHandle->Request) {
            continue;
        }

        asyncHandle->Request->Wait();
        asyncHandle->Request->DeliverCallback();
    }

    return rocksdb::IOStatus::OK();
}

rocksdb::IOStatus AbortAsyncReads(const std::vector<void*>& ioHandles) {
    for (auto* handle : ioHandles) {
        auto* asyncHandle = static_cast<AsyncReadHandle*>(handle);
        if (asyncHandle == nullptr || !asyncHandle->Request) {
            continue;
        }

        asyncHandle->Request->Abort();
        asyncHandle->Request->DeliverCallback();
    }

    return rocksdb::IOStatus::OK();
}
} // namespace AVEVA::RocksDB::Plugin::Azure
