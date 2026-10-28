// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"

#include <boost/asio/post.hpp>

#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
struct AsyncReadTracker::Registration {
    std::shared_ptr<AsyncReadTracker> Tracker;

    explicit Registration(std::shared_ptr<AsyncReadTracker> tracker) : Tracker(std::move(tracker)) {}
    Registration(const Registration&) = delete;
    Registration& operator=(const Registration&) = delete;
    Registration(Registration&&) = delete;
    Registration& operator=(Registration&&) = delete;

    // Typically runs inside an HTTP completion on an io_context thread; deferring End() to a fresh task keeps the
    // filesystem (and with it the HTTP client) alive until that completion has returned. A token released on any
    // other thread cannot be inside such a completion, so it ends the read directly and skips the post.
    ~Registration() {
        try {
            if (!Tracker->m_executor.running_in_this_thread()) {
                Tracker->End();
                return;
            }
            boost::asio::post(Tracker->m_executor, [tracker = Tracker]() { tracker->End(); });
        } catch (...) {
            Tracker->End();
        }
    }
};

AsyncReadTracker::AsyncReadTracker(boost::asio::io_context::executor_type executor) : m_executor(std::move(executor)) {}

AsyncReadTracker::Token AsyncReadTracker::Begin() {
    m_inFlight.fetch_add(1, std::memory_order_relaxed);

    try {
        return std::make_shared<const Registration>(shared_from_this());
    } catch (...) {
        End();
        throw;
    }
}

void AsyncReadTracker::Drain() {
    std::unique_lock lock(m_mutex);
    m_idle.wait(lock, [this] { return m_inFlight.load(std::memory_order_acquire) == 0; });
}

bool AsyncReadTracker::DrainFor(std::chrono::nanoseconds timeout) {
    std::unique_lock lock(m_mutex);
    return m_idle.wait_for(lock, timeout, [this] { return m_inFlight.load(std::memory_order_acquire) == 0; });
}

size_t AsyncReadTracker::InFlight() const { return m_inFlight.load(std::memory_order_acquire); }

void AsyncReadTracker::End() noexcept {
    if (m_inFlight.fetch_sub(1, std::memory_order_acq_rel) != 1) {
        return;
    }
    // Taking the mutex orders the notify after a waiter's predicate check, so the wakeup cannot be lost.
    {
        std::scoped_lock lock(m_mutex);
    }
    m_idle.notify_all();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
