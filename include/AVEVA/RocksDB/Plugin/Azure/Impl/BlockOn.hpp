// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Throws std::logic_error when the calling thread is running `executor`: blocking there can never finish.
/// </summary>
template <typename Executor> void ThrowIfRunningOn(const Executor& executor) {
    constexpr auto message = "Blocking Azure calls must not be made from a thread running the io_context; "
                             "call them from a RocksDB thread instead.";

    // The common case needs no probe: an io_context executor can answer directly.
    if constexpr (requires { executor.template target<boost::asio::io_context::executor_type>(); }) {
        if (const auto* ioExecutor = executor.template target<boost::asio::io_context::executor_type>()) {
            if (ioExecutor->running_in_this_thread()) {
                throw std::logic_error(message);
            }
            return;
        }
    }

    // any_io_executor cannot be asked running_in_this_thread() directly, but dispatch runs the handler inline
    // exactly when the calling thread is already running the executor. Otherwise it is queued and some other host
    // thread may run it at any moment (even before the check below), so the handler records which thread ran it and
    // only a run on the calling thread proves misuse.
    auto ranOn = std::make_shared<std::atomic<std::thread::id>>(std::thread::id{});
    boost::asio::dispatch(executor, [ranOn] { ranOn->store(std::this_thread::get_id()); });
    if (ranOn->load() == std::this_thread::get_id()) {
        throw std::logic_error(message);
    }
}

/// <summary>
/// Waits for a use_future result. Blocking on a thread that is running the executor the operation completes on can
/// never finish, so that misuse fails loudly with std::logic_error instead of deadlocking.
/// </summary>
template <typename Executor, typename T> T BlockOn(const Executor& executor, std::future<T> future) {
    ThrowIfRunningOn(executor);
    return future.get();
}

/// <summary>
/// Like BlockOn, but when the operation has not finished after `timeout`, calls `onTimeout` once (to cancel it) and
/// then keeps waiting for the operation to report its outcome. The cancellation is expected to complete the future
/// promptly; the timeout bounds how long a caller waits for a result that retries would otherwise stretch.
/// </summary>
template <typename Executor, typename T, typename OnTimeout>
T BlockOnFor(const Executor& executor, std::future<T> future, std::chrono::nanoseconds timeout, OnTimeout&& onTimeout) {
    ThrowIfRunningOn(executor);
    if (future.wait_for(timeout) == std::future_status::timeout) {
        std::forward<OnTimeout>(onTimeout)();
    }
    return future.get();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl