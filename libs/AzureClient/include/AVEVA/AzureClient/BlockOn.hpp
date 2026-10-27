// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/async_result.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>

#include <atomic>
#include <concepts>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient {
// Completion token that turns any ...Async operation into a blocking call: the initiating function returns the
// operation's result (std::expected<Response<T>, BlobStorageError>) directly, e.g.
// `auto result = client.GetPropertiesAsync(AVEVA::AzureClient::block_on);`.
//
// Blocking on a thread that is running the client's executor can never finish, so that misuse throws
// std::logic_error before any request is started (nothing is left in flight).
struct BlockOnToken {};
inline constexpr BlockOnToken block_on{};

namespace Private {
template <class T> inline constexpr bool IsBlockOnToken = std::same_as<std::remove_cvref_t<T>, BlockOnToken>;
} // namespace Private

// Throws std::logic_error when the calling thread is running `executor`.
template <class Executor> void ThrowIfRunningOn(const Executor& executor) {
    constexpr auto message = "Blocking AzureClient calls must not be made from a thread running the io_context "
                             "that drives the client.";

    // The common case needs no probe: an io_context executor can answer directly.
    if constexpr (requires { executor.running_in_this_thread(); }) {
        if (executor.running_in_this_thread()) {
            throw std::logic_error(message);
        }
        // A strand only answers "inside this strand"; another handler on the same single-threaded context
        // would still deadlock, so also ask the executor it adapts.
        if constexpr (requires { executor.get_inner_executor(); }) {
            ThrowIfRunningOn(executor.get_inner_executor());
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

        // An erased executor cannot be asked directly, but dispatch runs the handler inline exactly when the
        // calling thread is already running it. Otherwise it is queued and another host thread may run it at any
        // moment, so the handler records which thread ran it and only a run on this thread proves misuse.
        auto ranOn = std::make_shared<std::atomic<std::thread::id>>(std::thread::id{});
        boost::asio::dispatch(executor, [ranOn] { ranOn->store(std::this_thread::get_id()); });
        if (ranOn->load() == std::this_thread::get_id()) {
            throw std::logic_error(message);
        }
    }
}

namespace Private {
// The rendezvous lives on the blocked caller's stack, which outlives the handler because the caller waits for Done.
template <class TResult> struct BlockOnState {
    std::optional<TResult> Value;
    std::atomic<bool> Done{false};
};

template <class TResult> struct BlockOnHandler {
    BlockOnState<TResult>* State;
    void operator()(TResult result) {
        auto* state = State;
        state->Value.emplace(std::move(result));
        state->Done.store(true, std::memory_order_release);
        // Must be the last access: the waiter may destroy the state as soon as it observes Done.
        state->Done.notify_one();
    }
};
} // namespace Private
} // namespace AVEVA::AzureClient

namespace boost::asio {
template <class TResult> struct async_result<AVEVA::AzureClient::BlockOnToken, void(TResult)> {
    using return_type = TResult;

    template <class Initiation, class... Args>
    static return_type initiate(Initiation&& initiation, AVEVA::AzureClient::BlockOnToken, Args&&... args) {
        AVEVA::AzureClient::Private::BlockOnState<TResult> state;
        std::forward<Initiation>(initiation)(AVEVA::AzureClient::Private::BlockOnHandler<TResult>{&state},
                                             std::forward<Args>(args)...);
        state.Done.wait(false, std::memory_order_acquire);
        return std::move(*state.Value);
    }
};
} // namespace boost::asio
