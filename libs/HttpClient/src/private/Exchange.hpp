// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"
#include "DnsCache.hpp"
#include "HeaderArena.hpp"
#include "Route.hpp"
#include "StreamTypes.hpp"

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "AVEVA/HttpClient/HttpRequest.hpp"
#include "AVEVA/HttpClient/HttpRequestOptions.hpp"
#include "AVEVA/HttpClient/HttpResponse.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_state.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>

#include <chrono>
#include <expected>
#include <memory>
#include <optional>
#include <string_view>
#include <system_error>

namespace AVEVA::Private {
using ArenaFields = boost::beast::http::basic_fields<ArenaAllocator<char>>;
using RequestMessage = boost::beast::http::message<true, boost::beast::http::buffer_body, ArenaFields>;

/// <summary>
/// Carries the outcome of a DNS lookup led by another request to the request waiting on it. The channel holds one
/// value, so the leader can publish from any thread without waiting for the joiner to be listening.
/// </summary>
using ResolveChannel =
    boost::asio::experimental::concurrent_channel<void(boost::system::error_code, Tcp::resolver::results_type)>;

/// <summary>
/// The DNS cache's waiter: forwards the leader's outcome into the joining request's channel.
/// </summary>
class ResolveWaiter {
  public:
    explicit ResolveWaiter(std::shared_ptr<ResolveChannel> channel) noexcept : m_channel(std::move(channel)) {}

    void operator()(boost::system::error_code error, ResolveResults results) const noexcept {
        try {
            m_channel->try_send(error, std::move(results));
        } catch (...) {
            // The joiner is woken with a closed-channel error instead of waiting for its deadline.
            m_channel->close();
        }
    }

  private:
    std::shared_ptr<ResolveChannel> m_channel;
};

/// <summary>
/// The process-wide DNS cache type, whose waiters are the requests of this client.
/// </summary>
using DnsCache = BasicDnsCache<std::chrono::steady_clock, ResolveWaiter>;

/// <summary>
/// One HTTP request over a <see cref="PlainStream"/> or <see cref="TlsStream"/>, written as the sequence it is:
/// build the message, take a pooled connection or resolve, connect and handshake a new one, write, read, then pool
/// or close the connection. A pooled connection the peer closed while idle is replaced once, on a fresh one.
///
/// <see cref="Run"/> is a single coroutine, so nothing here needs a strand or a completion handler. Cancellation and
/// the deadline are applied from outside by cancelling the coroutine; the pending I/O then fails, and the coroutine
/// only returns once it has finished, which also keeps a caller-owned request body valid until the write has drained.
/// Failures are returned, never thrown, except for allocation failures that the caller classifies.
/// </summary>
template <typename Stream> class Exchange {
  public:
    /// <summary>
    /// What an exchange shares with the client that created it.
    /// </summary>
    struct Environment {
        boost::asio::io_context& context;
        std::shared_ptr<boost::asio::ssl::context> tlsContext;
        std::shared_ptr<ConnectionPool<Stream>> pool;
        std::shared_ptr<const DiagnosticsSink> diagnostics;
    };

    /// <param name="environment">The client's shared state.</param>
    /// <param name="route">The request's validated destination; must outlive the exchange.</param>
    /// <param name="request">The request to send; its owned body is moved out when the message is built.</param>
    /// <param name="options">The response limits and TLS truncation policy for this request.</param>
    /// <param name="phase">Updated as the exchange progresses, so the caller can say where a timeout hit.</param>
    Exchange(Environment environment, const Route& route, HttpRequest request, HttpRequestOptions options,
             HttpRequestPhase& phase);

    Exchange(const Exchange&) = delete;
    Exchange& operator=(const Exchange&) = delete;

    /// <summary>
    /// Performs the request.
    /// </summary>
    /// <returns>The response, or the reason it could not be obtained.</returns>
    boost::asio::awaitable<HttpResult> Run();

  private:
    struct Failure {
        std::error_code error;
        // Whether sending the request again on a fresh connection is safe: nothing of a response has been seen,
        // and either no request bytes could have reached the server or the method is idempotent.
        bool replayable = false;
    };
    struct Reply {
        HttpResponse response;
        bool keepAlive = false;
    };

    void Report(HttpDiagnosticKind kind, std::error_code error, std::string_view message) const noexcept {
        DiagnosticsSink::Report(m_env.diagnostics, kind, m_phase, m_key.host, m_key.service, error, message);
    }
    static std::error_code CanceledError() { return std::make_error_code(std::errc::operation_canceled); }
    [[nodiscard]] bool Cancelled() const noexcept {
        return m_cancellation.cancelled() != boost::asio::cancellation_type::none;
    }

    /// <summary>
    /// Builds the wire message. Throws on input Beast cannot represent or on allocation failure.
    /// </summary>
    void BuildRequest();
    void ResetParser();
    void NewStream();
    /// <summary>
    /// Offers a resumable session and sets up host verification and SNI for the origin.
    /// </summary>
    std::error_code ConfigureTlsForHost();

    /// <summary>
    /// Resolves the origin, sharing the lookup and its cached answer with concurrent requests.
    /// </summary>
    boost::asio::awaitable<std::expected<ResolveResults, std::error_code>> Resolve();
    /// <summary>
    /// Resolves, connects and, for TLS, handshakes a new stream.
    /// </summary>
    boost::asio::awaitable<std::error_code> Connect();
    /// <summary>
    /// Writes the request and reads the final response, skipping interim 1xx ones.
    /// </summary>
    boost::asio::awaitable<std::expected<Reply, Failure>> Transfer();

    boost::system::error_code AcceptTruncatedTlsBody(boost::system::error_code error);
    static HttpClientError ClassifyReadError(const boost::system::error_code& error);
    static bool IsIdempotentMethod(http::verb method) noexcept;
    bool Replayable(bool requestMayHaveBeenSent) const;
    /// <summary>
    /// Whether a failure on a reused connection should be retried once on a fresh one.
    /// </summary>
    bool ShouldRetry(const Failure& failure) const noexcept {
        return failure.replayable && m_reused && !m_retried && !Cancelled();
    }
    std::expected<HttpResponse, std::error_code> BuildResponse();

    /// <summary>
    /// Pools the connection when the response allows it, and otherwise closes it.
    /// </summary>
    void ReleaseOrClose(bool keepAlive);
    void Close() noexcept;

    Environment m_env;
    const Route& m_route;
    ConnectionKey<Stream> m_key;
    HttpRequest m_stagedRequest;
    HttpRequestOptions m_options;
    HttpRequestPhase& m_phase;
    boost::asio::cancellation_state m_cancellation;
    std::unique_ptr<Stream> m_stream;
    beast::flat_buffer m_buffer;
    bool m_reused = false;
    bool m_retried = false;
    bool m_resolvedFromCache = false;
    bool m_receivedAnyResponse = false; // Set once a 1xx proves the server has the request.
    // Header storage for the request and the response. Declared before the messages that allocate from them
    // so they are destroyed last; each is reset only while no message using it is alive.
    HeaderArena m_requestArena;
    HeaderArena m_responseArena;
    std::optional<RequestMessage> m_request;
    // Held as a member so async_write uses the non-stable write_op instead of heap-allocating a serializer.
    std::optional<http::request_serializer<http::buffer_body, ArenaFields>> m_serializer;
    // Backing storage for an owned (SetBody()) request body; for a SetBodyView() body the caller owns it.
    std::string m_ownedBodyStorage;
    std::shared_ptr<const void> m_bodyKeepAlive;
    std::optional<http::response_parser<http::string_body, ArenaAllocator<char>>> m_parser;
};

extern template class Exchange<PlainStream>;
extern template class Exchange<TlsStream>;
} // namespace AVEVA::Private
