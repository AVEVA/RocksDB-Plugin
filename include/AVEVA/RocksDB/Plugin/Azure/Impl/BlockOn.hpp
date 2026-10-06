// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/asio/dispatch.hpp>

#include <atomic>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Waits for a use_future result. Blocking on a thread that is running the executor the operation completes on can
/// never finish, so that misuse fails loudly with std::logic_error instead of deadlocking.
/// </summary>
template <typename Executor, typename T>
T BlockOn(const Executor& executor, std::future<T> future) {
    // any_io_executor cannot be asked running_in_this_thread() directly, but dispatch runs the handler inline
    // exactly when the calling thread is already running the executor. Otherwise it is queued and some other host
    // thread may run it at any moment (even before the check below), so the handler records which thread ran it and
    // only a run on the calling thread proves misuse.
    auto ranOn = std::make_shared<std::atomic<std::thread::id>>(std::thread::id{});
    boost::asio::dispatch(executor, [ranOn] { ranOn->store(std::this_thread::get_id()); });
    if (ranOn->load() == std::this_thread::get_id()) {
        throw std::logic_error("Blocking Azure calls must not be made from a thread running the io_context; "
                               "call them from a RocksDB thread instead.");
    }

    return future.get();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
