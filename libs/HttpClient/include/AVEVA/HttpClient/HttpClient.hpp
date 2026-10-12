// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <functional>
#include <memory>
#include <system_error>
#include <utility>

namespace boost
{
    namespace asio
    {
        class io_context;
    }
} // namespace boost

namespace AVEVA
{
    /// <summary>
    /// Asynchronous HTTP client interface. Non-copyable and non-movable, so it is always held through the
    /// <c>std::unique_ptr</c> returned by <see cref="Create"/>.
    /// </summary>
    class IHttpClient
    {
      public:
        /// <summary>
        /// Invoked once per request with either an error and a default-constructed response, or a null error
        /// and the response.
        /// </summary>
        using CompletionHandler = std::move_only_function<void(std::error_code, HttpResponse)>;

        using executor_type = boost::asio::any_io_executor;

        IHttpClient() = default;
        IHttpClient(const IHttpClient&) = delete;
        IHttpClient& operator=(const IHttpClient&) = delete;
        IHttpClient(IHttpClient&&) = delete;
        IHttpClient& operator=(IHttpClient&&) = delete;
        virtual ~IHttpClient();

        /// <summary>
        /// Creates a client that runs its work on an io_context.
        /// </summary>
        /// <param name="context">The io_context the client runs on. The caller owns it and must keep it alive,
        /// and keep running it, for as long as requests are outstanding.</param>
        /// <param name="options">The client-wide settings to apply.</param>
        /// <returns>The new client.</returns>
        static std::unique_ptr<IHttpClient> Create(boost::asio::io_context& context, HttpClientOptions options = {});

        /// <summary>
        /// The executor associated with this client (used as the default executor for completion tokens that do
        /// not carry their own associated executor).
        /// </summary>
        /// <returns>The client's executor.</returns>
        virtual executor_type get_executor() const = 0;

        /// <summary>
        /// Type-erased send operation. Implementations must never invoke the completion handler
        /// synchronously/re-entrantly from within this call, and must honor
        /// <c>options.GetCancellationSlot()</c> if set.
        /// </summary>
        /// <param name="request">The request to send.</param>
        /// <param name="completion">Invoked exactly once with the outcome.</param>
        /// <param name="options">The per-request settings to apply.</param>
        virtual void SendAsyncErased(HttpRequest request, CompletionHandler completion, HttpRequestOptions options) = 0;

        /// <summary>
        /// Completion-token-aware overload. Propagates the token's associated executor, allocator, and
        /// cancellation slot (falling back to this client's executor, the default allocator, and
        /// <paramref name="options"/>'s existing cancellation slot respectively) so that callers using
        /// <c>use_future</c>, <c>use_awaitable</c>, <c>bind_executor</c>, etc. get the behavior they expect
        /// without this class needing to know about any of those token types.
        /// </summary>
        /// <param name="request">The request to send.</param>
        /// <param name="options">The per-request settings to apply.</param>
        /// <param name="token">The completion token, called with <c>(std::error_code, HttpResponse)</c>.</param>
        /// <returns>Whatever <paramref name="token"/>'s async_result produces, for example a future.</returns>
        template <class CompletionToken>
        auto SendAsync(HttpRequest request, HttpRequestOptions options, CompletionToken&& token)
        {
            using Signature = void(std::error_code, HttpResponse);
            return boost::asio::async_initiate<CompletionToken, Signature>(
                [this](auto&& handler, HttpRequest req, HttpRequestOptions opts)
                {
                    auto executor = boost::asio::get_associated_executor(handler, get_executor());
                    auto allocator = boost::asio::get_associated_allocator(handler);
                    // Held for the whole operation so the handler's executor cannot run out of work (and let
                    // run() return) before the completion is dispatched.
                    auto work = boost::asio::make_work_guard(executor);
                    auto cancellationSlot = boost::asio::get_associated_cancellation_slot(handler, boost::asio::cancellation_slot());
                    const bool ownsSlot = cancellationSlot.is_connected() && !opts.GetCancellationSlot().is_connected();
                    if (ownsSlot)
                    {
                        opts.SetCancellationSlot(cancellationSlot);
                    }

                    SendAsyncErased(std::move(req),
                        CompletionHandler(
                            [executor, allocator, work = std::move(work), cancellationSlot, ownsSlot,
                                handler = std::forward<decltype(handler)>(handler)](
                                std::error_code ec, HttpResponse response) mutable
                            {
                                boost::asio::dispatch(executor,
                                    boost::asio::bind_allocator(allocator,
                                        [handler = std::move(handler), ec, response = std::move(response),
                                            work = std::move(work), cancellationSlot, ownsSlot]() mutable
                                        {
                                            // Cleared here, on the handler's executor, so that it is serialized with
                                            // emissions from the caller; clearing on the transport thread would race.
                                            if (ownsSlot)
                                            {
                                                cancellationSlot.clear();
                                            }
                                            std::move(handler)(ec, std::move(response));
                                        }));
                            }),
                        std::move(opts));
                },
                token, std::move(request), std::move(options));
        }
    };
} // namespace AVEVA
