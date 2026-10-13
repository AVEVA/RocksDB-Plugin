// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>

#include <atomic>
#include <chrono>
#include <concepts>
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
    if constexpr (requires { executor.running_in_this_thread(); }) {
        if (executor.running_in_this_thread()) {
            throw std::logic_error(message);
        }
    } else {
        if constexpr (requires { executor.template target<boost::asio::io_context::executor_type>(); }) {
            if (const auto* ioExecutor = executor.template target<boost::asio::io_context::executor_type>()) {
                if (ioExecutor->running_in_this_thread()) {
                    throw std::logic_error(message);
                }
                return;
            }
        }

        // any_io_executor cannot be asked running_in_this_thread() directly, but dispatch runs the handler inline
        // exactly when the calling thread is already running the executor. Otherwise it is queued and some other
        // host thread may run it at any moment (even before the check below), so the handler records which thread
        // ran it and only a run on the calling thread proves misuse.
        auto ranOn = std::make_shared<std::atomic<std::thread::id>>(std::thread::id{});
        boost::asio::dispatch(executor, [ranOn] { ranOn->store(std::this_thread::get_id()); });
        if (ranOn->load() == std::this_thread::get_id()) {
            throw std::logic_error(message);
        }
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
/// Checks the calling thread *before* `start` initiates the operation, so misuse never leaves an abandoned request
/// in flight. `start` receives the use_future token, e.g. `[&](auto token) { return c.GetPropertiesAsync(token); }`.
/// </summary>
template <typename Client, typename Start>
    requires std::invocable<Start, decltype(boost::asio::use_future)>
auto BlockOn(Client& client, Start&& start) {
    ThrowIfRunningOn(client.get_executor());
    return std::forward<Start>(start)(boost::asio::use_future).get();
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

/// <summary>
/// Runs a cancellable request and waits for it, cancelling it when `timeout` elapses. The request is started on a
/// strand and its handler is bound to that strand, so the cancellation signal is emitted on the executor that also
/// installs and clears the slot, as the AzureClient requires. `start` receives a completion token and the request
/// options (carrying the cancellation slot), e.g.
/// `[&](auto token, auto options) { return c.RenewLeaseAsync(opts, std::move(token), std::move(options)); }`.
/// It is also invoked with use_future in an unevaluated context to learn the result type.
/// </summary>
template <typename Client, typename Start>
auto BlockOnWithin(Client& client, std::chrono::nanoseconds timeout, Start&& start) {
    using Options = std::remove_cvref_t<decltype(client.GetDefaultRequestOptions())>;
    using Result =
        std::remove_cvref_t<decltype(std::declval<Start&>()(boost::asio::use_future, std::declval<Options>()).get())>;

    ThrowIfRunningOn(client.get_executor());

    struct State {
        explicit State(const typename Client::executor_type& executor) : Strand(boost::asio::make_strand(executor)) {}
        boost::asio::strand<typename Client::executor_type> Strand;
        boost::asio::cancellation_signal Cancel;
        std::promise<Result> Promise;
    };
    auto state = std::make_shared<State>(client.get_executor());
    auto future = state->Promise.get_future();

    // Referenced only until the future is ready, which this function always waits for.
    boost::asio::post(state->Strand, [state, &client, &start] {
        try {
            auto options = client.GetDefaultRequestOptions();
            options.SetCancellationSlot(state->Cancel.slot());
            start(boost::asio::bind_executor(state->Strand,
                                             [state](Result result) { state->Promise.set_value(std::move(result)); }),
                  std::move(options));
        } catch (...) {
            state->Promise.set_exception(std::current_exception());
        }
    });

    if (future.wait_for(timeout) == std::future_status::timeout) {
        boost::asio::post(state->Strand, [state] { state->Cancel.emit(boost::asio::cancellation_type::terminal); });
    }
    return future.get();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl