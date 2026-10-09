// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"

#include <boost/asio/io_context.hpp>
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
            if (const auto* ioExecutor = Tracker->m_executor.target<boost::asio::io_context::executor_type>();
                ioExecutor != nullptr && !ioExecutor->running_in_this_thread()) {
                Tracker->End();
                return;
            }
            boost::asio::post(Tracker->m_executor, [tracker = Tracker]() { tracker->End(); });
        } catch (...) {
            Tracker->End();
        }
    }
};

AsyncReadTracker::AsyncReadTracker(boost::asio::any_io_executor executor) : m_executor(std::move(executor)) {}

AsyncReadTracker::Token AsyncReadTracker::Begin() {
    {
        std::scoped_lock lock(m_mutex);
        ++m_inFlight;
    }

    try {
        return std::make_shared<const Registration>(shared_from_this());
    } catch (...) {
        End();
        throw;
    }
}

void AsyncReadTracker::Drain() {
    std::unique_lock lock(m_mutex);
    m_idle.wait(lock, [this] { return m_inFlight == 0; });
}

bool AsyncReadTracker::DrainFor(std::chrono::nanoseconds timeout) {
    std::unique_lock lock(m_mutex);
    return m_idle.wait_for(lock, timeout, [this] { return m_inFlight == 0; });
}

size_t AsyncReadTracker::InFlight() const {
    std::scoped_lock lock(m_mutex);
    return m_inFlight;
}

void AsyncReadTracker::End() noexcept {
    {
        std::scoped_lock lock(m_mutex);
        --m_inFlight;
    }
    m_idle.notify_all();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
