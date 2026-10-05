// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/asio/dispatch.hpp>

#include <atomic>
#include <future>
#include <memory>
#include <stdexcept>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Waits for a use_future result. Blocking on a thread that is running the executor the operation completes on can
/// never finish, so that misuse fails loudly with std::logic_error instead of deadlocking.
/// </summary>
template <typename Executor, typename T>
T BlockOn(const Executor& executor, std::future<T> future) {
    // any_io_executor cannot be asked running_in_this_thread() directly, but dispatch runs the handler inline
    // exactly when the calling thread is already running the executor. If it was queued instead, the no-op runs
    // later and only touches the shared flag.
    auto ranInline = std::make_shared<std::atomic<bool>>(false);
    boost::asio::dispatch(executor, [ranInline] { ranInline->store(true); });
    if (ranInline->load()) {
        throw std::logic_error("Blocking Azure calls must not be made from a thread running the io_context; "
                               "call them from a RocksDB thread instead.");
    }

    return future.get();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
