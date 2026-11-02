// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "AVEVA/RocksDB/Plugin/Azure/Impl/RequestHelpers.hpp"

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

void AsyncReadRequest::AssertWaitCannotDeadlock() {
    // A finished request is never waited on, so polling it from any thread is fine.
    if (m_completionExecutor && Finished() == Completion::Pending) {
        AssertNotRunningOn(*m_completionExecutor);
    }
}

Completion AsyncReadRequest::Finished() {
    return m_state.load(std::memory_order_acquire) != State::InFlight ? Completion::Done : Completion::Pending;
}

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

RequestDeadline::RequestDeadline(const AsyncReadRequest::Executor& executor,
                                 const std::optional<std::chrono::steady_clock::time_point>& deadline,
                                 const std::shared_ptr<AsyncReadRequest>& request,
                                 const rocksdb::IOStatus& expiredStatus)
    : m_timer(executor) {
    if (!deadline) {
        return;
    }
    m_timer.expires_at(*deadline);
    m_timer.async_wait(
        [weak = std::weak_ptr<AsyncReadRequest>(request), expiredStatus](const boost::system::error_code& cancelled) {
            if (cancelled) {
                return;
            }
            if (const auto live = weak.lock()) {
                live->Expire(expiredStatus);
            }
        });
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
    // Outside the lock: the stop callback may run arbitrary code (it only posts to the client executor).
    m_stop.request_stop();
    NotifyFinished();
}

Completion AsyncReadRequest::TryGetOutcome(rocksdb::FSReadRequest& outcome) {
    std::scoped_lock lock(m_mutex);
    if (m_state == State::InFlight) {
        return Completion::Pending;
    }
    FillOutcomeLocked(outcome);
    return Completion::Done;
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
        if (m_callbackDelivered == CallbackDelivery::Delivered || m_state == State::InFlight) {
            return;
        }

        m_callbackDelivered = CallbackDelivery::Delivered;
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

        asyncHandle->Request->AssertWaitCannotDeadlock();
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
