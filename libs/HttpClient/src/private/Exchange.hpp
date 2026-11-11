// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"
#include "HeaderArena.hpp"
#include "Route.hpp"
#include "StreamTypes.hpp"

#include "AVEVA/HttpClient/Detail/Mailbox.hpp"
#include "AVEVA/HttpClient/HttpClient.hpp"
#include "AVEVA/HttpClient/HttpRequest.hpp"
#include "AVEVA/HttpClient/HttpRequestOptions.hpp"
#include "AVEVA/HttpClient/HttpResponse.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/beast/http/span_body.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <variant>

namespace AVEVA::Private {
using ArenaFields = boost::beast::http::basic_fields<ArenaAllocator<char>>;
using RequestBody = boost::beast::http::span_body<const std::byte>;
using RequestMessage = boost::beast::http::message<true, RequestBody, ArenaFields>;

using ResolveResults = Tcp::resolver::results_type;

/// <summary>
/// What a DNS lookup produced: its own error code (distinct from the mailbox's receive status) and the endpoints.
/// </summary>
struct ResolveOutcome {
    boost::system::error_code Error;
    ResolveResults Results;
};

/// <summary>
/// Carries the outcome of a request's DNS lookup back to the request. The mailbox keeps the value, so the lookup can
/// publish from any thread without waiting for the request to be listening.
/// </summary>
using ResolveChannel = Detail::Mailbox<ResolveOutcome>;

/// <summary>
/// Forwards a lookup's outcome into the waiting request's channel.
/// </summary>
class ResolveWaiter {
  public:
    ResolveWaiter(std::shared_ptr<ResolveChannel> channel, std::shared_ptr<const DiagnosticsSink> diagnostics) noexcept
        : m_channel(std::move(channel)), m_diagnostics(std::move(diagnostics)) {}

    /// <summary>
    /// Delivers the outcome. If that fails (out of memory), the failure is reported and the channel is closed, which
    /// wakes the request with an error instead of leaving it to its deadline. If even closing fails, the request is
    /// left to its deadline.
    /// </summary>
    void operator()(boost::system::error_code error, const ResolveResults& results) const noexcept {
        try {
            m_channel->Send(ResolveOutcome{error, results});
            return;
        } catch (const std::exception& failure) {
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Resolve,
                                    {}, {}, {}, failure.what());
        }
        try {
            m_channel->Close();
        } catch (const std::exception& failure) {
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Resolve,
                                    {}, {}, {}, failure.what());
        }
    }

  private:
    std::shared_ptr<ResolveChannel> m_channel;
    std::shared_ptr<const DiagnosticsSink> m_diagnostics;
};

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
        // Only a TLS exchange carries a context; a plain one leaves this empty.
        std::conditional_t<std::is_same_v<Stream, TlsStream>, std::shared_ptr<boost::asio::ssl::context>, std::monostate>
            tlsContext;
        std::shared_ptr<ConnectionPool<Stream>> pool;
        std::shared_ptr<const DiagnosticsSink> diagnostics;
    };

    /// <param name="environment">The client's shared state.</param>
    /// <param name="route">The request's validated destination.</param>
    /// <param name="request">The request to send.</param>
    /// <param name="options">The response limits and TLS truncation policy for this request.</param>
    Exchange(Environment environment, Route route, HttpRequest request, HttpRequestOptions options);

    Exchange(const Exchange&) = delete;
    Exchange& operator=(const Exchange&) = delete;

    /// <summary>
    /// Performs the request.
    /// </summary>
    /// <returns>The response, or the reason it could not be obtained.</returns>
    boost::asio::awaitable<HttpResult> Run();

    /// <summary>
    /// How far the exchange got, so a caller whose deadline cut <see cref="Run"/> short can say where it hit.
    /// </summary>
    [[nodiscard]] HttpRequestPhase Phase() const noexcept { return m_phase; }

  private:
    // Whether sending the request again on a fresh connection is safe.
    enum class Replay : std::uint8_t { Unsafe, Safe };
    enum class RequestDelivery : std::uint8_t { NotSent, MaybeSent };
    enum class KeepAlive : std::uint8_t { Close, Reuse };
    enum class Connection : std::uint8_t { Fresh, Reused };
    enum class RetryState : std::uint8_t { NotRetried, Retried };
    enum class RetryDecision : std::uint8_t { GiveUp, Retry };
    enum class InterimResponse : std::uint8_t { None, Seen };

    struct Failure {
        HttpFailure failure;
        // Nothing of a response has been seen, and either no request bytes could have reached the server or the
        // method is idempotent.
        Replay replay = Replay::Unsafe;
    };
    struct Reply {
        HttpResponse response;
        KeepAlive keepAlive = KeepAlive::Close;
    };

    void Report(HttpDiagnosticKind kind, std::error_code error, std::string_view message) const noexcept {
        DiagnosticsSink::Report(m_env.diagnostics, kind, m_phase, m_connectionKey.host, m_connectionKey.service, error, message);
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
    /// Resolves the origin without blocking the request past its deadline.
    /// </summary>
    boost::asio::awaitable<std::expected<ResolveResults, HttpFailure>> Resolve();
    /// <summary>
    /// Resolves, connects and, for TLS, handshakes a new stream.
    /// </summary>
    boost::asio::awaitable<std::optional<HttpFailure>> Connect();
    /// <summary>
    /// Writes the request and reads the final response, skipping interim 1xx ones.
    /// </summary>
    boost::asio::awaitable<std::expected<Reply, Failure>> Transfer();

    boost::system::error_code AcceptTruncatedTlsBody(boost::system::error_code error);
    static HttpClientError ClassifyReadError(const boost::system::error_code& error);
    static Replay MethodReplay(http::verb method) noexcept;
    Replay ReplayAfter(RequestDelivery delivery) const;
    /// <summary>
    /// Whether a failure on a reused connection should be retried once on a fresh one.
    /// </summary>
    [[nodiscard]] RetryDecision ShouldRetry(const Failure& failure) const noexcept {
        return failure.replay == Replay::Safe && m_connection == Connection::Reused && m_retry == RetryState::NotRetried
                   ? RetryDecision::Retry
                   : RetryDecision::GiveUp;
    }
    std::expected<HttpResponse, std::error_code> BuildResponse();

    /// <summary>
    /// Pools the connection when the response allows it, and otherwise closes it.
    /// </summary>
    void ReleaseOrClose(KeepAlive keepAlive);
    void Close() noexcept;

    Environment m_env;
    Route m_route;
    ConnectionKey<Stream> m_connectionKey;
    HttpRequest m_stagedRequest;
    HttpRequestOptions m_options;
    HttpRequestPhase m_phase = HttpRequestPhase::Queued;
    std::unique_ptr<Stream> m_stream;
    beast::flat_buffer m_buffer;
    Connection m_connection = Connection::Fresh;
    RetryState m_retry = RetryState::NotRetried;
    InterimResponse m_interim = InterimResponse::None; // Seen once a 1xx proves the server has the request.
    // Header storage for the request and the response. Declared before the messages that allocate from them
    // so they are destroyed last; each is reset only while no message using it is alive.
    HeaderArena m_requestArena;
    HeaderArena m_responseArena;
    std::optional<RequestMessage> m_request;
    // Held as a member so async_write uses the non-stable write_op instead of heap-allocating a serializer.
    std::optional<http::request_serializer<RequestBody, ArenaFields>> m_serializer;
    // Keeps the bytes the message's span refers to alive until the exchange ends.
    HttpBody m_body;
    std::optional<http::response_parser<http::string_body, ArenaAllocator<char>>> m_parser;
};

extern template class Exchange<PlainStream>;
extern template class Exchange<TlsStream>;
} // namespace AVEVA::Private
