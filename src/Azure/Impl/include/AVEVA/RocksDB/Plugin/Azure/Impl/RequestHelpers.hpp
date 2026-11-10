// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Response.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>

#include <cassert>
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
// registration only refers to the state weakly; the completion handler owns it and must call Release.
class CancelState {
    std::mutex m_mutex;
    boost::asio::cancellation_signal m_signal;
    std::optional<std::stop_callback<std::function<void()>>> m_registration;
    enum class Lifecycle : std::uint8_t { Live, Released };
    Lifecycle m_lifecycle = Lifecycle::Live;

  public:
    [[nodiscard]] boost::asio::cancellation_slot Slot() noexcept { return m_signal.slot(); }

    /// <summary>
    /// A cancellation_signal is not thread-safe: emitting races with the coroutine clearing its slot when it finishes,
    /// which frees the handler the emission is calling. The stop callback runs on whatever thread requests the stop,
    /// so it only posts the emission to `strand`, the strand the operation itself runs on.
    /// </summary>
    template <class Strand>
    void Arm(std::stop_token token, const std::shared_ptr<CancelState>& self, Strand strand) {
        const std::lock_guard lock(m_mutex);
        if (m_lifecycle == Lifecycle::Live) {
            // Weak: the completion handler owns the state, so a registration that outlives it (an io_context stopped
            // before the operation completed) must not keep it alive through a cycle.
            m_registration.emplace(std::move(token), [weak = std::weak_ptr<CancelState>(self), strand] {
                boost::asio::post(strand, [weak] {
                    if (const auto state = weak.lock()) {
                        state->m_signal.emit(boost::asio::cancellation_type::terminal);
                    }
                });
            });
        }
    }

    // Destroying the registration waits for a running emission, which takes no lock, so holding m_mutex is safe.
    void Release() noexcept {
        const std::lock_guard lock(m_mutex);
        m_lifecycle = Lifecycle::Released;
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

/// <summary>
/// Awaits a client operation and returns its value, throwing RequestFailedException on failure. The coroutine
/// counterpart of Unwrap for callers that hand an operation to something that wants a plain value (Block, Spawn).
/// </summary>
template <class T>
boost::asio::awaitable<T>
Unwrapped(boost::asio::awaitable<std::expected<AzureClient::Response<T>, AzureClient::BlobStorageError>> operation) {
    co_return Unwrap(co_await std::move(operation));
}

namespace Detail {
enum class ThreadAffinity : std::uint8_t { OtherThread, ThisThread };

/// <summary>Whether the calling thread runs `executor` (or the io_context an erased/strand executor wraps).</summary>
template <class Executor> [[nodiscard]] ThreadAffinity AffinityOf(const Executor& executor) {
    if constexpr (requires { executor.running_in_this_thread(); }) {
        if (executor.running_in_this_thread()) {
            return ThreadAffinity::ThisThread;
        }
        if constexpr (requires { executor.get_inner_executor(); }) {
            return AffinityOf(executor.get_inner_executor());
        }
        return ThreadAffinity::OtherThread;
    } else if constexpr (requires { executor.template target<boost::asio::io_context::executor_type>(); }) {
        const auto* ioExecutor = executor.template target<boost::asio::io_context::executor_type>();
        return ioExecutor != nullptr && ioExecutor->running_in_this_thread() ? ThreadAffinity::ThisThread
                                                                             : ThreadAffinity::OtherThread;
    } else {
        return ThreadAffinity::OtherThread;
    }
}
} // namespace Detail

/// <summary>
/// Blocking on an io thread can never finish (or races the completion it waits for), so every blocking entry point
/// refuses it, in release builds too, with a BlockingOnIoThreadException that translates to an IOStatus. An executor
/// that cannot report whether the calling thread runs it (an erased one that does not wrap an io_context) passes.
/// </summary>
template <class Executor> void ThrowIfRunningOn(const Executor& executor) {
    if (Detail::AffinityOf(executor) == Detail::ThreadAffinity::ThisThread) {
        throw BlockingOnIoThreadException(
            "Blocking calls must not be made from a thread running the io_context that drives the Azure clients");
    }
}

/// <summary>
/// Starts `operation` on `executor`; `done(std::exception_ptr error, Result result)` runs on the executor when it
/// completes (without the `result` argument when the operation returns void). A stop request on `stopToken` cancels
/// the operation, which then completes with its cancellation error.
/// </summary>
template <class Executor, class Awaitable, class Done>
void StartOperation(const Executor& executor, Awaitable operation, std::stop_token stopToken, Done done) {
    auto state = std::make_shared<Detail::CancelState>();
    // The operation runs on its own strand so a stop request, which arrives on an arbitrary thread, can emit on the
    // coroutine's cancellation slot without racing the coroutine finishing on an io thread. The cancellation handler
    // co_spawn installs then dispatches onto the strand too, so it never runs on the stopping thread.
    const auto strand = boost::asio::make_strand(executor);
    boost::asio::co_spawn(strand, std::move(operation),
                          boost::asio::bind_cancellation_slot(
                              state->Slot(), boost::asio::bind_executor(
                                                 executor, [state, executor, done = std::move(done)](
                                                               std::exception_ptr error, auto&&... result) mutable {
                                                     state->Release();
                                                     // The coroutine machinery may still touch the signal after this
                                                     // handler returns, so its destruction is deferred rather than done
                                                     // here.
                                                     boost::asio::post(executor, [state = std::move(state)] {});
                                                     done(std::move(error), std::move(result)...);
                                                 })));
    if (stopToken.stop_possible()) {
        state->Arm(std::move(stopToken), state, strand);
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
                   [promise = std::move(promise)](std::exception_ptr error, auto&&... result) mutable {
                       if (error) {
                           promise.set_exception(std::move(error));
                       } else {
                           promise.set_value(std::move(result)...);
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
