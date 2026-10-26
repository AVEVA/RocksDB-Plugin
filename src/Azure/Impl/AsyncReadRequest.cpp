// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "AVEVA/AzureClient/BlockOn.hpp"

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
void AsyncReadSignal::Notify() {
    {
        std::scoped_lock lock(m_mutex);
        ++m_count;
    }
    m_changed.notify_all();
}

uint64_t AsyncReadSignal::Count() {
    std::scoped_lock lock(m_mutex);
    return m_count;
}

void AsyncReadSignal::WaitPast(const uint64_t seen) {
    std::unique_lock lock(m_mutex);
    m_changed.wait(lock, [&] { return m_count > seen; });
}

AsyncReadRequest::AsyncReadRequest(const rocksdb::FSReadRequest& request, Callback callback, void* callbackArg,
                                   std::shared_ptr<AsyncReadSignal> signal, std::optional<Executor> completionExecutor)
    : m_offset(request.offset), m_length(request.len), m_scratch(request.scratch), m_callback(std::move(callback)),
      m_callbackArg(callbackArg), m_signal(std::move(signal)), m_completionExecutor(std::move(completionExecutor)) {}

void AsyncReadRequest::ThrowIfWaitWouldDeadlock() {
    // A finished request is never waited on, so polling it from any thread is fine.
    if (m_completionExecutor && !Finished()) {
        AzureClient::ThrowIfRunningOn(*m_completionExecutor);
    }
}

bool AsyncReadRequest::Finished() { return m_state.load(std::memory_order_acquire) != State::InFlight; }

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
    NotifyFinished();
}

void AsyncReadRequest::NotifyFinished() {
    m_state.notify_all();
    if (m_signal) {
        m_signal->Notify();
    }
}

void AsyncReadRequest::Wait() { m_state.wait(State::InFlight, std::memory_order_acquire); }

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
    // Outside the lock: the stop callback may run arbitrary code (it only posts to the client executor).
    m_stop.request_stop();
    NotifyFinished();
}

bool AsyncReadRequest::TryGetOutcome(rocksdb::FSReadRequest& outcome) {
    std::scoped_lock lock(m_mutex);
    if (m_state == State::InFlight) {
        return false;
    }
    FillOutcomeLocked(outcome);
    return true;
}

void AsyncReadRequest::FillOutcomeLocked(rocksdb::FSReadRequest& outcome) const {
    outcome.offset = m_offset;
    outcome.len = m_length;
    outcome.scratch = m_scratch;
    outcome.status = m_status;
    outcome.result = rocksdb::Slice(m_scratch, m_status.ok() ? m_bytesRead : 0);
}

void AsyncReadRequest::DeliverCallback() {
    rocksdb::FSReadRequest request;
    {
        std::scoped_lock lock(m_mutex);
        if (m_callbackDelivered || m_state == State::InFlight) {
            return;
        }

        m_callbackDelivered = true;
        FillOutcomeLocked(request);
    }

    // Invoked without holding the lock: the callback may re-enter the filesystem (e.g. issue another read).
    if (m_callback) {
        m_callback(request, m_callbackArg);
    }
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

        try {
            asyncHandle->Request->ThrowIfWaitWouldDeadlock();
        } catch (const std::logic_error& error) {
            return rocksdb::IOStatus::InvalidArgument(error.what());
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
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
