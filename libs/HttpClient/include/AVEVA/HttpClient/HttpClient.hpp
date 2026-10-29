// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>

#include <gsl/pointers>

#include <expected>
#include <memory>
#include <system_error>

namespace AVEVA {
/// <summary>
/// The outcome of a request: the response, or the transport-level error. An HTTP error status is a response, not
/// an error.
/// </summary>
using HttpResult = std::expected<HttpResponse, std::error_code>;

/// <summary>
/// Asynchronous HTTP client, used from C++20 coroutines. Non-copyable and non-movable. Each request is a single
/// coroutine, so cancelling the awaiting coroutine cancels the request.
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
    /// thread count stays bounded. The client may be destroyed from one of these writer threads (or from a completion
    /// bound to this executor): the destructor then hands the join and teardown to a helper thread instead of
    /// joining itself. Safe to call from any thread.
    /// </summary>
    /// <returns>An executor that runs tasks on the pool.</returns>
    [[nodiscard]] boost::asio::thread_pool::executor_type GetDownloadWriter();

    /// <summary>
    /// Sends a request. Cancelling the awaiting coroutine cancels the request, and the per-request timeout covers
    /// everything, including time spent waiting for a per-origin connection slot.
    /// Nothing is thrown for a rejected request: the result carries <c>std::errc::not_enough_memory</c> for
    /// <c>std::bad_alloc</c>, <c>HttpClientError::InvalidRequest</c> for rejected input, and
    /// <c>std::errc::state_not_recoverable</c> for anything else.
    /// </summary>
    /// <param name="request">The request to send.</param>
    /// <param name="options">The per-request settings to apply.</param>
    /// <returns>The response, or the transport-level error.</returns>
    [[nodiscard]] boost::asio::awaitable<HttpResult> SendAsync(HttpRequest request, HttpRequestOptions options);

  private:
    class Impl;

    gsl::not_null<boost::asio::io_context*> m_context;
    std::unique_ptr<Impl> m_impl;
};
} // namespace AVEVA
