// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Response.hpp>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <atomic>
#include <cstdint>
#include <exception>
#include <expected>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace Detail {
// Connects a std::stop_token to the cancellation slot given to co_spawn. The registration is made after co_spawn has
// installed its handler, so a stop that was already requested fires immediately on arming. Arm and Release may race
// (the operation can finish before arming): whichever comes second wins, so no registration is left behind. The
// registration holds a reference to the state until Release, which the completion must call.
class CancelState {
    std::mutex m_mutex;
    boost::asio::cancellation_signal m_signal;
    std::optional<std::stop_callback<std::function<void()>>> m_registration;
    bool m_released = false;

  public:
    [[nodiscard]] boost::asio::cancellation_slot Slot() noexcept { return m_signal.slot(); }

    void Arm(std::stop_token token, const std::shared_ptr<CancelState>& self) {
        const std::lock_guard lock(m_mutex);
        if (!m_released) {
            m_registration.emplace(std::move(token),
                                   [self] { self->m_signal.emit(boost::asio::cancellation_type::terminal); });
        }
    }

    // Destroying the registration waits for a running emission, which takes no lock, so holding m_mutex is safe.
    void Release() noexcept {
        const std::lock_guard lock(m_mutex);
        m_released = true;
        m_registration.reset();
    }
};
} // namespace Detail

/// <summary>Builds the exception ThrowRequestFailed would throw, for error paths that hand it on as an
/// exception_ptr without throwing.</summary>
[[nodiscard]] RequestFailedException MakeRequestFailed(const AzureClient::BlobStorageError& error);

[[noreturn]] void ThrowRequestFailed(const AzureClient::BlobStorageError& error);

/// <summary>
/// Request options for a request transferring `bytes` of payload: raises the response body limit so the
/// payload fits, and extends the timeout so that large transfers on slow links are not cut short.
/// </summary>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions
RequestOptionsForTransfer(const AVEVA::AzureClient::AzureRequestOptions& defaults, uint64_t bytes);

/// Request options for moving `bytes` through `client`: the client's defaults with the timeout and response limit
/// scaled to the transfer size.
template <typename Client>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions TransferOptions(const Client& client, uint64_t bytes) {
    return RequestOptionsForTransfer(client.GetDefaultRequestOptions(), bytes);
}

/// <summary>
/// Returns the full response of a completed operation, or throws RequestFailedException on failure.
/// Use this for operations whose Response::Error() carries information (e.g. DeleteIfExists).
/// </summary>
template <class T>
AzureClient::Response<T> UnwrapResponse(std::expected<AzureClient::Response<T>, AzureClient::BlobStorageError> result) {
    if (!result.has_value()) {
        ThrowRequestFailed(result.error());
    }

    return std::move(*result);
}

/// <summary>
/// Returns the value of a completed operation, or throws RequestFailedException on failure.
/// </summary>
template <class T> T Unwrap(std::expected<AzureClient::Response<T>, AzureClient::BlobStorageError> result) {
    return std::move(UnwrapResponse(std::move(result))).Value();
}

/// <summary>Throws std::logic_error when the calling thread is running `executor`: blocking there can never finish.
/// </summary>
template <class Executor> void ThrowIfRunningOn(const Executor& executor) {
    constexpr auto message = "Blocking calls must not be made from a thread running the io_context that drives the "
                             "client.";

    if constexpr (requires { executor.running_in_this_thread(); }) {
        if (executor.running_in_this_thread()) {
            throw std::logic_error(message);
        }
        // A strand only answers "inside this strand"; ask the executor it adapts as well.
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

        // An erased executor cannot be asked directly, but dispatch runs the handler inline exactly when the calling
        // thread is already running it; a queued run on another host thread proves nothing.
        auto ranOn = std::make_shared<std::atomic<std::thread::id>>(std::thread::id{});
        boost::asio::dispatch(executor, [ranOn] { ranOn->store(std::this_thread::get_id()); });
        if (ranOn->load() == std::this_thread::get_id()) {
            throw std::logic_error(message);
        }
    }
}

/// <summary>
/// Starts `operation` on `executor`; `done(std::exception_ptr error, Result result)` runs on the executor when it
/// completes. A stop request on `stopToken` cancels the operation, which then completes with its cancellation error.
/// </summary>
template <class Executor, class Awaitable, class Done>
void StartOperation(const Executor& executor, Awaitable operation, std::stop_token stopToken, Done done) {
    using Result = typename Awaitable::value_type;
    auto state = std::make_shared<Detail::CancelState>();
    // Binding the executor makes the co_spawn cancellation handler dispatch the emission onto it, so a stop request
    // never runs the coroutine's cancellation handlers on the stopping thread.
    boost::asio::co_spawn(
        executor, std::move(operation),
        boost::asio::bind_cancellation_slot(
            state->Slot(), boost::asio::bind_executor(executor, [state, executor, done = std::move(done)](
                                                                    std::exception_ptr error, Result result) mutable {
                state->Release();
                // The coroutine machinery may still touch the signal after this handler returns,
                // so its destruction is deferred rather than done here.
                boost::asio::post(executor, [state = std::move(state)] {});
                done(std::move(error), std::move(result));
            })));
    if (stopToken.stop_possible()) {
        state->Arm(std::move(stopToken), state);
    }
}

/// <summary>
/// Runs `operation` on `executor` and returns a future for its result. Nothing blocks. A stop request on `stopToken`
/// cancels the operation through the coroutine's cancellation slot; the result is then the operation's cancellation
/// error. Unlike a std::async future, the future does not wait when destroyed, so whatever the operation reads or
/// writes must stay alive until it completes.
/// </summary>
template <class Executor, class Awaitable>
[[nodiscard]] auto Spawn(const Executor& executor, Awaitable operation, std::stop_token stopToken = {})
    -> std::future<typename Awaitable::value_type> {
    using Result = typename Awaitable::value_type;
    std::promise<Result> promise;
    auto future = promise.get_future();
    StartOperation(executor, std::move(operation), std::move(stopToken),
                   [promise = std::move(promise)](std::exception_ptr error, Result result) mutable {
                       if (error) {
                           promise.set_exception(std::move(error));
                       } else {
                           promise.set_value(std::move(result));
                       }
                   });
    return future;
}

/// <summary>
/// Runs an operation on the client's executor and waits for its raw result on the calling thread, which must not be
/// running that executor.
/// </summary>
template <class Client, class Awaitable> auto Block(const Client& client, Awaitable operation) {
    ThrowIfRunningOn(client.get_executor());
    return Spawn(client.get_executor(), std::move(operation)).get();
}

/// <summary>
/// Runs an operation on the client's executor, waits for it on the calling thread and returns its value, throwing
/// RequestFailedException on failure. `client` is anything with get_executor(); the thread must not be running that
/// executor. Example: `Call(client, client.GetPropertiesAsync())`.
/// </summary>
template <class Client, class Awaitable> auto Call(const Client& client, Awaitable operation) {
    return Unwrap(Block(client, std::move(operation)));
}

/// <summary>As Call, but returns the full response (for operations whose Response::Error() carries
/// information).</summary>
template <class Client, class Awaitable> auto CallResponse(const Client& client, Awaitable operation) {
    return UnwrapResponse(Block(client, std::move(operation)));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
