// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"
#include "IdleSweeper.hpp"
#include "Route.hpp"
#include "StreamTypes.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <variant>

namespace AVEVA::Private {
template <typename Stream> class Exchange;

/// <summary>
/// The state a client shares with its requests: the TLS context and the connection pools and their idle sweeper.
/// Every in-flight request holds a reference, so the client object can be destroyed while requests are still
/// running.
/// </summary>
class HttpClientCore : public std::enable_shared_from_this<HttpClientCore> {
  public:
    /// <summary>
    /// Builds the TLS context and the pools. Throws if the requested TLS settings cannot be applied.
    /// </summary>
    /// <param name="runtime">The io_context every connection and timer runs on; must outlive the client.</param>
    /// <param name="options">The TLS and pooling settings to build from.</param>
    HttpClientCore(asio::io_context& runtime, const HttpClientOptions& options);

    HttpClientCore(const HttpClientCore&) = delete;
    HttpClientCore& operator=(const HttpClientCore&) = delete;
    ~HttpClientCore();

    /// <summary>
    /// Disposes idle connections now, while the caller's io_context is still alive. Requests already running finish
    /// on their own. Safe to repeat.
    /// </summary>
    void Shutdown() noexcept;

    [[nodiscard]] std::shared_ptr<const DiagnosticsSink> Diagnostics() const noexcept { return m_diagnostics; }

    /// <summary>
    /// Sends a request. The deadline covers everything, and cancelling the awaiting coroutine cancels the request.
    /// Nothing is thrown: a failure to even start the request is reported as a classified error code.
    /// </summary>
    asio::awaitable<HttpResult> Send(HttpRequest request, HttpRequestOptions options);

  private:
    /// <summary>The request was aborted, by the deadline or the caller, while in <c>phase</c>.</summary>
    struct Interrupted {
        HttpRequestPhase phase;
    };
    using Outcome = std::variant<HttpResult, Interrupted>;

    /// <summary>
    /// Runs the request with the deadline attached. Throws operation_aborted when the deadline passes or the
    /// awaiting coroutine is cancelled before the request has started.
    /// </summary>
    /// <param name="expired">Set to true once the deadline has passed, so a caller can tell a timeout from a cancel.</param>
    asio::awaitable<Outcome> RunWithDeadline(Route route, HttpRequest request, HttpRequestOptions options,
                                             std::chrono::steady_clock::time_point deadline,
                                             std::shared_ptr<std::atomic<bool>> expired);

    /// <summary>
    /// Runs the request, and again after a transient failure when the options carry a retry policy. The
    /// request's deadline covers every attempt and wait. The last attempt's outcome is returned when retries run out,
    /// so a persistent 503 is still reported as a response.
    /// </summary>
    asio::awaitable<Outcome> Dispatch(Route route, HttpRequest request, HttpRequestOptions options);

    /// <summary>
    /// One attempt: runs the exchange over the stream the route calls for. An abort is returned as the phase it
    /// interrupted, so the caller can report where a timeout hit.
    /// </summary>
    /// <param name="phase">Receives how far the attempt got, which tells the caller whether it can be replayed.</param>
    asio::awaitable<Outcome> Attempt(Route route, HttpRequest request, HttpRequestOptions options,
                                     HttpRequestPhase& phase);

    /// <summary>
    /// Runs an exchange, turning the operation_aborted its cancellation throws into the phase it interrupted.
    /// </summary>
    template <typename Stream>
    static asio::awaitable<Outcome> RunExchange(Exchange<Stream>& exchange, HttpRequestPhase& phase);

    asio::io_context& m_context;
    std::shared_ptr<const DiagnosticsSink> m_diagnostics; // Before everything built with it.
    std::shared_ptr<asio::ssl::context> m_tlsContext;
    std::shared_ptr<IdleSweeper> m_sweeper;
    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
};
} // namespace AVEVA::Private
