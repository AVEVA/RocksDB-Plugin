// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "Exchange.hpp"

#include "HttpVerb.hpp"
#include "RequestValidation.hpp"
#include "Timeouts.hpp"
#include "TlsContextConfigurator.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/channel_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gsl/util>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include <algorithm>
#include <cassert>
#include <iterator>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace AVEVA::Private {
namespace {
constexpr auto Await = asio::as_tuple(asio::use_awaitable);

/// <summary>
/// Turns an aborted operation into the exception a cancelled await throws, so cancellation unwinds to the boundary in
/// HttpClientCore::Send instead of being classified as an I/O failure here.
/// </summary>
void ThrowIfAborted(const boost::system::error_code& error) {
    if (error == asio::error::operation_aborted) {
        throw boost::system::system_error(error);
    }
}

/// <summary>
/// Sends close_notify so the peer can tell a finished exchange from a truncation, bounded by a grace period.
/// Runs detached so the caller's result does not wait for it.
/// </summary>
asio::awaitable<void> ShutdownTls(std::unique_ptr<TlsStream> stream) {
    beast::get_lowest_layer(*stream).expires_after(TlsShutdownGrace);
    co_await stream->async_shutdown(Await);
    boost::system::error_code ignored;
    auto& socket = beast::get_lowest_layer(*stream).socket();
    socket.shutdown(Tcp::socket::shutdown_both, ignored);
    socket.close(ignored);
}
} // namespace

template <typename Stream>
Exchange<Stream>::Exchange(Environment environment, Route route, HttpRequest request, HttpRequestOptions options)
    : m_env(std::move(environment)), m_route(std::move(route)), m_key{m_route.Host, m_route.Port},
      m_stagedRequest(std::move(request)), m_options(std::move(options)) {}

template <typename Stream> asio::awaitable<HttpResult> Exchange<Stream>::Run() {
    BuildRequest();
    if (auto pooled = m_env.pool->Acquire(m_key)) {
        m_stream = std::move(pooled->stream);
        m_buffer = std::move(pooled->buffer);
        m_connection = Connection::Reused;
    } else {
        NewStream();
        if (const auto error = ConfigureTlsForHost()) {
            co_return std::unexpected(HttpFailure{error});
        }
    }

    for (;;) {
        if (m_connection == Connection::Fresh) {
            if (auto failure = co_await Connect()) {
                Close();
                co_return std::unexpected(*failure);
            }
        }
        auto reply = co_await Transfer();
        if (reply) {
            ReleaseOrClose(reply->keepAlive);
            co_return std::move(reply->response);
        }
        if (ShouldRetry(reply.error()) == RetryDecision::GiveUp) {
            Close();
            co_return std::unexpected(reply.error().failure);
        }

        // The pooled connection was closed by the peer while idle; the request never reached it in a way that
        // forbids sending it again, so it gets one more attempt on a fresh connection.
        m_retry = RetryState::Retried;
        m_connection = Connection::Fresh;
        Report(HttpDiagnosticKind::StaleConnectionRetry, {},
               "A reused connection was closed by the peer; retrying on a new connection");
        Close();
        NewStream();
        m_buffer.consume(m_buffer.size());
        ResetParser();
        if (const auto error = ConfigureTlsForHost()) {
            co_return std::unexpected(HttpFailure{error});
        }
    }
}

template <typename Stream> void Exchange<Stream>::NewStream() {
    const auto executor = m_env.context.get_executor();
    if constexpr (std::is_same_v<Stream, TlsStream>) {
        m_stream = std::make_unique<TlsStream>(executor, *m_env.tlsContext);
    } else {
        m_stream = std::make_unique<PlainStream>(executor);
    }
}

template <typename Stream> void Exchange<Stream>::BuildRequest() {
    // HttpClient fails unparseable or invalid requests before an exchange exists.
    assert(m_route.Sendable() == Sendability::Sendable);

    const auto verb = Private::ToBeastVerb(m_stagedRequest.GetMethod());

    // The serializer and the message are gone before the arena they allocated from is reset.
    m_serializer.reset();
    m_request.reset();
    m_requestArena.Reset();
    auto& req = m_request.emplace(std::piecewise_construct, std::forward_as_tuple(),
                                  std::forward_as_tuple(ArenaAllocator<char>(m_requestArena)));
    req.version(11);
    req.method(verb);
    req.target(m_route.Target);
    for (const auto& header : m_stagedRequest.GetHeaders()) {
        const auto& name = header.GetName();
        const auto& value = header.GetValue();
        const auto field = http::string_to_field(name);
        switch (field) {
        case http::field::host:
        case http::field::content_length:
        case http::field::transfer_encoding:
        case http::field::connection:
            continue;
        case http::field::unknown:
            req.insert(name, value);
            break;
        default:
            req.insert(field, value);
            break;
        }
    }
    req.set(http::field::host, m_route.Authority);
    req.keep_alive(true);
    m_body = m_stagedRequest.GetBody();
    const auto bytes = m_body.Bytes();
    req.body() = RequestBody::value_type(bytes.data(), bytes.size());
    // Set by hand because message::prepare_payload() omits a zero length for PATCH: a bodyless GET, HEAD or DELETE
    // carries none, but a bodyless POST, PUT or PATCH must state zero.
    const auto method = req.method();
    if (!bytes.empty() || method == http::verb::post || method == http::verb::put || method == http::verb::patch) {
        req.content_length(bytes.size());
    }
    ResetParser();
}

template <typename Stream> void Exchange<Stream>::ResetParser() {
    m_parser.reset();
    m_responseArena.Reset();
    m_parser.emplace(std::piecewise_construct, std::forward_as_tuple(),
                     std::forward_as_tuple(ArenaAllocator<char>(m_responseArena)));
    m_parser->body_limit(m_options.GetResponseBodyLimit().Value());
    m_parser->header_limit(gsl::narrow_cast<std::uint32_t>(std::min<std::uint64_t>(
        m_options.GetResponseHeaderLimit().Value(), std::numeric_limits<std::uint32_t>::max())));
    m_parser->skip(m_request->method() == http::verb::head);
}

template <typename Stream> std::error_code Exchange<Stream>::ConfigureTlsForHost() {
    if constexpr (std::is_same_v<Stream, TlsStream>) {
        SSL* const ssl = m_stream->native_handle();
        PrepareTlsSessionResumption(ssl, m_key);
        if (SSL_get_verify_mode(ssl) != SSL_VERIFY_NONE) {
            // Native OpenSSL host/IP checks run inside the built-in verifier, avoiding a type-erased callback.
            int verifyHostResult = 0;
            if (m_route.Kind == HostKind::Name) {
                SSL_set_hostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                verifyHostResult = SSL_set1_host(ssl, m_key.host.c_str());
            } else {
                verifyHostResult = X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), m_key.host.c_str());
            }
            if (verifyHostResult != 1) {
                return HttpClientError::TlsFailed;
            }
        }
        if (m_route.Kind == HostKind::Name) {
            // SSL_set_tlsext_host_name is a macro with an old-style cast, so call SSL_ctrl directly. It takes the name
            // as a mutable void* (it only copies it), so hand it a copy rather than casting away const.
            std::string hostName = m_key.host;
            if (SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name, hostName.data()) == 0) {
                return HttpClientError::TlsFailed;
            }
        }
    }
    return {};
}

/// <summary>
/// The lookup runs detached and the request waits on a channel. getaddrinfo cannot be cancelled, so awaiting the
/// resolver directly would hold the request past its deadline; waiting on the channel lets the deadline or a
/// cancellation end the wait at once while the lookup finishes in the background.
/// </summary>
template <typename Stream> asio::awaitable<std::expected<ResolveResults, HttpFailure>> Exchange<Stream>::Resolve() {
    m_phase = HttpRequestPhase::Resolve;
    const auto executor = m_env.context.get_executor();

    const auto outcome = std::make_shared<ResolveChannel>(executor, 1);
    auto resolver = std::make_shared<Tcp::resolver>(executor);
    auto& lookup = *resolver;
    lookup.async_resolve(m_key.host, m_key.service,
                         [resolver = std::move(resolver), waiter = ResolveWaiter(outcome, m_env.diagnostics)](
                             const boost::system::error_code& error, const ResolveResults& results) {
                             waiter(error, results);
                         });

    const auto [error, results] = co_await outcome->async_receive(Await);
    if (error == asio::experimental::error::channel_cancelled) {
        // This request's own deadline or cancellation fired while it waited for the lookup, not a failure of the
        // lookup itself; let it unwind as cancellation instead of being reported as ResolveFailed.
        ThrowIfAborted(asio::error::operation_aborted);
    }
    if (error == asio::experimental::error::channel_closed) {
        co_return std::unexpected(HttpFailure{std::make_error_code(std::errc::not_enough_memory)});
    }
    if (error) {
        co_return std::unexpected(HttpFailure{HttpClientError::ResolveFailed, TransportCause{error}});
    }
    co_return results;
}
template <typename Stream> asio::awaitable<std::optional<HttpFailure>> Exchange<Stream>::Connect() {
    const auto resolved = co_await Resolve();
    if (!resolved) {
        co_return resolved.error();
    }
    m_phase = HttpRequestPhase::Connect;
    const auto [connectError, endpoint] = co_await beast::get_lowest_layer(*m_stream).async_connect(*resolved, Await);
    if (connectError) {
        ThrowIfAborted(connectError);
        co_return HttpFailure{HttpClientError::ConnectFailed, TransportCause{connectError}};
    }

    if constexpr (std::is_same_v<Stream, TlsStream>) {
        m_phase = HttpRequestPhase::Handshake;
        const auto [handshakeError] = co_await m_stream->async_handshake(asio::ssl::stream_base::client, Await);
        if (handshakeError) {
            ThrowIfAborted(handshakeError);
            co_return HttpFailure{HttpClientError::TlsFailed, TransportCause{handshakeError}};
        }
    }
    co_return std::nullopt;
}

template <typename Stream>
typename Exchange<Stream>::Replay Exchange<Stream>::ReplayAfter(RequestDelivery delivery) const {
    return m_interim == InterimResponse::None && !m_parser->got_some() &&
                   (delivery == RequestDelivery::NotSent || MethodReplay(m_request->method()) == Replay::Safe)
               ? Replay::Safe
               : Replay::Unsafe;
}

template <typename Stream>
asio::awaitable<std::expected<typename Exchange<Stream>::Reply, typename Exchange<Stream>::Failure>>
Exchange<Stream>::Transfer() {
    m_phase = HttpRequestPhase::Write;
    m_serializer.emplace(*m_request);
    const auto [writeError, bytesWritten] = co_await http::async_write(*m_stream, *m_serializer, Await);
    if (writeError) {
        ThrowIfAborted(writeError);
        co_return std::unexpected(
            Failure{HttpFailure{HttpClientError::WriteFailed, TransportCause{writeError}},
                    ReplayAfter(bytesWritten != 0 ? RequestDelivery::MaybeSent : RequestDelivery::NotSent)});
    }

    for (;;) {
        m_phase = HttpRequestPhase::Read;
        // The parser consumes body bytes eagerly, so without a reserve beast reads only ~0.5-2 KiB per round trip.
        if (m_buffer.capacity() < ReadChunk) {
            m_buffer.reserve(ReadChunk);
        }
        auto [readError, bytesRead] = co_await http::async_read(*m_stream, m_buffer, *m_parser, Await);
        ThrowIfAborted(readError);
        if (readError) {
            readError = AcceptTruncatedTlsBody(readError);
        }
        if (readError) {
            const auto classified = ClassifyReadError(readError);
            // Limit errors are the response's fault, not the connection's, so they are never retried. For the
            // rest, the request was fully written before the read started.
            co_return std::unexpected(Failure{HttpFailure{classified, TransportCause{readError}},
                                              classified == HttpClientError::ResponseTooLarge
                                                  ? Replay::Unsafe
                                                  : ReplayAfter(RequestDelivery::MaybeSent)});
        }

        const auto status = m_parser->get().result_int();
        if (status == 101) {
            co_return std::unexpected(Failure{HttpFailure{HttpClientError::ProtocolError}, Replay::Unsafe});
        }
        if (status < 200) {
            m_interim = InterimResponse::Seen; // The server has the request; ResetParser() would hide that.
            ResetParser();
            continue;
        }
        break;
    }

    // The parser-level check also rejects EOF-delimited bodies; leftover bytes would be mistaken for the next
    // response on a pooled connection.
    const auto keepAlive = m_parser->keep_alive() && m_buffer.size() == 0 ? KeepAlive::Reuse : KeepAlive::Close;
    auto response = BuildResponse();
    if (!response) {
        co_return std::unexpected(Failure{HttpFailure{response.error()}, Replay::Unsafe});
    }
    co_return Reply{std::move(*response), keepAlive};
}

template <typename Stream>
boost::system::error_code Exchange<Stream>::AcceptTruncatedTlsBody(boost::system::error_code error) {
    if constexpr (std::is_same_v<Stream, TlsStream>) {
        // A TLS server may close TCP without close_notify, and an attacker can truncate an EOF-delimited body that
        // way, so it is only accepted when the request opted in.
        if (error == asio::ssl::error::stream_truncated && m_parser->got_some() &&
            m_options.GetTruncatedTlsBodyPolicy() == TruncatedTlsBodyPolicy::Accept) {
            boost::system::error_code eofError;
            m_parser->put_eof(eofError);
            if (!eofError) {
                return {};
            }
        }
    }
    return error;
}

template <typename Stream> HttpClientError Exchange<Stream>::ClassifyReadError(const boost::system::error_code& error) {
    if (error == http::error::body_limit || error == http::error::header_limit) {
        return HttpClientError::ResponseTooLarge;
    }
    // A connection that ends mid-message is a transport failure, not a malformed response.
    const auto truncated = error == http::error::end_of_stream || error == http::error::partial_message;
    return truncated || SourceOf(error) == ErrorSource::Other ? HttpClientError::ReadFailed
                                                              : HttpClientError::ProtocolError;
}

// Only side-effect-free methods may be replayed once the request might have reached the server; PUT and DELETE
// are idempotent in HTTP but Azure mutations are not, so the Azure retry layer decides about those.
template <typename Stream>
typename Exchange<Stream>::Replay Exchange<Stream>::MethodReplay(http::verb method) noexcept {
    switch (method) {
    case http::verb::get:
    case http::verb::head:
    case http::verb::options:
    case http::verb::trace:
        return Replay::Safe;
    default:
        return Replay::Unsafe;
    }
}

template <typename Stream> std::expected<HttpResponse, std::error_code> Exchange<Stream>::BuildResponse() {
    try {
        auto message = m_parser->release();
        std::vector<HttpHeader> headers;
        // Walking Beast's linked list twice is cheaper than the reallocations of a growing vector.
        headers.reserve(gsl::narrow_cast<std::size_t>(std::distance(message.base().begin(), message.base().end())));
        for (const auto& header : message.base()) {
            headers.emplace_back(HeaderName{std::string(header.name_string())},
                                 HeaderValue{std::string(header.value())});
        }
        return HttpResponse(HttpStatusCode{message.result_int()}, std::move(headers), std::move(message.body()));
    } catch (...) {
        return std::unexpected(ClassifyBuildFailure(std::current_exception()));
    }
}

template <typename Stream> void Exchange<Stream>::ReleaseOrClose(KeepAlive keepAlive) {
    try {
        auto admission = PoolAdmission::Declined;
        if (keepAlive == KeepAlive::Reuse) {
            // A pooled connection is idle for a long time; don't let it pin the last response's buffer.
            TrimIdleBuffer(m_buffer);
            PooledConnection<Stream> connection{std::move(m_stream), std::move(m_buffer)};
            admission = m_env.pool->Release(m_key, std::move(connection));
            if (admission == PoolAdmission::Declined) {
                // Release only takes the connection when it returns Retained.
                m_stream = std::move(connection.stream);
                // Only a pool that is enabled but declined the connection is worth reporting; with pooling off this
                // is the normal path and would fire on every request.
                if (m_env.pool->Pooling() == PoolingMode::Enabled) {
                    Report(HttpDiagnosticKind::ConnectionNotPooled, {},
                           "The connection was reusable but the pool did not take it; closing it");
                }
            }
        }
        if (admission == PoolAdmission::Declined && m_stream) {
            // Only clean completions reach here, so the peer is in a known state and a close_notify lets it tell
            // this from a truncation.
            if constexpr (std::is_same_v<Stream, TlsStream>) {
                asio::co_spawn(m_env.context.get_executor(), ShutdownTls(std::move(m_stream)), asio::detached);
            } else {
                Close();
            }
        }
    } catch (...) {
        Report(HttpDiagnosticKind::SwallowedException, {}, "Releasing the connection failed; closing it");
        Close();
    }
}

template <typename Stream> void Exchange<Stream>::Close() noexcept {
    if (!m_stream) {
        return;
    }
    boost::system::error_code ignored;
    auto& socket = beast::get_lowest_layer(*m_stream).socket();
    socket.shutdown(Tcp::socket::shutdown_both, ignored);
    socket.close(ignored);
}

template class Exchange<PlainStream>;
template class Exchange<TlsStream>;
} // namespace AVEVA::Private
