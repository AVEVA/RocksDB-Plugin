// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/CompletionOperation.hpp>
#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>

#include <gsl/pointers>

#include <memory>
#include <system_error>
#include <utility>

namespace AVEVA {
/// <summary>
/// Asynchronous HTTP client. Non-copyable and non-movable. Requests of any completion-handler type share
/// one non-template implementation; the handler is stored once per request in an intrusive operation (see
/// <c>Detail::CompletionOperation</c>) rather than in a type-erased function object.
/// </summary>
class HttpClient {
  public:
    using executor_type = boost::asio::io_context::executor_type;

    /// <summary>
    /// Creates a client that runs its work on an io_context. Throws if the requested TLS settings cannot be
    /// applied.
    /// </summary>
    /// <param name="context">The io_context the client runs on. The caller owns it and must keep it alive,
    /// and keep running it, for as long as requests are outstanding.</param>
    /// <param name="options">The client-wide settings to apply.</param>
    explicit HttpClient(boost::asio::io_context& context, HttpClientOptions options = {});

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    HttpClient(HttpClient&&) = delete;
    HttpClient& operator=(HttpClient&&) = delete;

    /// <summary>
    /// Fails every request still waiting for a per-origin slot. Requests already running keep the state they
    /// need alive on their own.
    /// </summary>
    ~HttpClient();

    /// <summary>
    /// Creates a client held through a <c>std::unique_ptr</c>.
    /// </summary>
    /// <param name="context">The io_context the client runs on; see the constructor.</param>
    /// <param name="options">The client-wide settings to apply.</param>
    /// <returns>The new client.</returns>
    static std::unique_ptr<HttpClient> Create(boost::asio::io_context& context, HttpClientOptions options = {});

    /// <summary>
    /// The executor associated with this client (used as the default executor for completion tokens that do
    /// not carry their own associated executor).
    /// </summary>
    /// <returns>The client's executor.</returns>
    executor_type get_executor() const noexcept { return m_context->get_executor(); }

    /// <summary>
    /// Executor for blocking work that must stay off the I/O thread, such as download-to-file writes. Backed by a
    /// small thread pool that is created on first use, owned by this client and joined in its destructor, so the
    /// thread count stays bounded. Safe to call from any thread.
    /// </summary>
    /// <returns>An executor that runs tasks on the pool.</returns>
    [[nodiscard]] boost::asio::thread_pool::executor_type GetDownloadWriter();

    /// <summary>
    /// Sends a request. Propagates the token's associated executor, allocator, and cancellation slot
    /// (falling back to this client's executor, the default allocator, and <paramref name="options"/>'s
    /// existing cancellation slot respectively) so that callers using <c>use_future</c>,
    /// <c>use_awaitable</c>, <c>bind_executor</c>, etc. get the behavior they expect. The handler is never
    /// invoked from inside this call. If initiation itself fails, nothing is thrown: the handler completes exactly
    /// once, later, with <c>std::errc::not_enough_memory</c> for <c>std::bad_alloc</c> and
    /// <c>HttpClientError::InvalidRequest</c> for rejected input, and <c>std::errc::state_not_recoverable</c> for
    /// anything else. Only building the completion handler itself (the
    /// token's allocation) can still throw <c>std::bad_alloc</c> out of this call.
    /// </summary>
    /// <param name="request">The request to send.</param>
    /// <param name="options">The per-request settings to apply.</param>
    /// <param name="token">The completion token, called with <c>(std::error_code, HttpResponse)</c>.</param>
    /// <returns>Whatever <paramref name="token"/>'s async_result produces, for example a future.</returns>
    template <class CompletionToken>
    auto SendAsync(HttpRequest request, HttpRequestOptions options, CompletionToken&& token) {
        using Signature = void(std::error_code, HttpResponse);
        return boost::asio::async_initiate<CompletionToken, Signature>(
            [this](auto&& handler, HttpRequest req, HttpRequestOptions opts) {
                auto executor = boost::asio::get_associated_executor(handler, get_executor());
                auto allocator = boost::asio::get_associated_allocator(handler);
                // Held for the whole operation so the handler's executor cannot run out of work (and let
                // run() return) before the completion is dispatched.
                auto work = boost::asio::make_work_guard(executor);
                auto cancellationSlot =
                    boost::asio::get_associated_cancellation_slot(handler, boost::asio::cancellation_slot());
                const bool ownsSlot = cancellationSlot.is_connected() && !opts.GetCancellationSlot().is_connected();
                if (ownsSlot) {
                    opts.SetCancellationSlot(cancellationSlot);
                }

                Submit(std::move(req), std::move(opts),
                       Detail::MakeCompletion(
                           [executor, allocator, work = std::move(work), cancellationSlot, ownsSlot,
                            handler = std::forward<decltype(handler)>(handler)](std::error_code ec,
                                                                                HttpResponse response) mutable {
                               boost::asio::dispatch(
                                   executor,
                                   boost::asio::bind_allocator(
                                       allocator, [handler = std::move(handler), ec, response = std::move(response),
                                                   work = std::move(work), cancellationSlot, ownsSlot]() mutable {
                                           // Cleared here, on the handler's executor, so that it is serialized with
                                           // emissions from the caller; clearing on the transport thread would race.
                                           if (ownsSlot) {
                                               cancellationSlot.clear();
                                           }
                                           std::move(handler)(ec, std::move(response));
                                       }));
                           },
                           allocator));
            },
            token, std::move(request), std::move(options));
    }

  private:
    class Impl;

    /// <summary>
    /// Starts the request; the operation is completed exactly once, never from inside this call, and the
    /// request honors <c>options.GetCancellationSlot()</c>.
    /// </summary>
    /// <param name="request">The request to send.</param>
    /// <param name="options">The per-request settings to apply.</param>
    /// <param name="completion">Completed with the outcome.</param>
    void Submit(HttpRequest request, HttpRequestOptions options, Detail::CompletionPointer completion);

    gsl::not_null<boost::asio::io_context*> m_context;
    std::unique_ptr<Impl> m_impl;
};
} // namespace AVEVA
