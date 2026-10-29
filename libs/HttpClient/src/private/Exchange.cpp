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
#include <boost/asio/use_awaitable.hpp>

#include <gsl/util>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>

#include <algorithm>
#include <cassert>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>

namespace AVEVA::Private {
namespace {
constexpr auto Await = asio::as_tuple(asio::use_awaitable);

/// <summary>
/// Releases a DNS lookup the leading request abandons, so requests joined to it are not left waiting. Also
/// runs if the coroutine is destroyed without resuming (the io_context was torn down).
/// </summary>
class LeaderClaim {
  public:
    LeaderClaim(std::string_view host, std::string_view service, const void* scope)
        : m_host(host), m_service(service), m_scope(scope) {}
    LeaderClaim(const LeaderClaim&) = delete;
    LeaderClaim& operator=(const LeaderClaim&) = delete;

    ~LeaderClaim() {
        if (m_armed) {
            DnsCache::Shared().Finish(m_host, m_service, asio::error::operation_aborted, {}, m_scope);
        }
    }

    /// <summary>
    /// Publishes the lookup's real outcome to the cache and its waiters.
    /// </summary>
    void Publish(boost::system::error_code error, const ResolveResults& results) {
        m_armed = false;
        DnsCache::Shared().Finish(m_host, m_service, error, results, m_scope);
    }

  private:
    std::string_view m_host;
    std::string_view m_service;
    const void* m_scope;
    bool m_armed = true;
};

/// <summary>
/// Sends close_notify so the peer can tell a finished exchange from a truncation, bounded by a grace period.
/// Runs detached so the caller's result does not wait for it.
/// </summary>
asio::awaitable<void> ShutdownTls(std::unique_ptr<TlsStream> stream) {
    co_await asio::this_coro::throw_if_cancelled(false);
    beast::get_lowest_layer(*stream).expires_after(TlsShutdownGrace);
    co_await stream->async_shutdown(Await);
    boost::system::error_code ignored;
    auto& socket = beast::get_lowest_layer(*stream).socket();
    socket.shutdown(Tcp::socket::shutdown_both, ignored);
    socket.close(ignored);
}
} // namespace

template <typename Stream>
Exchange<Stream>::Exchange(Environment environment, const Route& route, HttpRequest request, HttpRequestOptions options,
                           HttpRequestPhase& phase)
    : m_env(std::move(environment)), m_route(route), m_key{route.Host, route.Port}, m_stagedRequest(std::move(request)),
      m_options(std::move(options)), m_phase(phase) {}

template <typename Stream> asio::awaitable<HttpResult> Exchange<Stream>::Run() {
    co_await asio::this_coro::throw_if_cancelled(false);
    m_cancellation = co_await asio::this_coro::cancellation_state;

    // A cancellation that arrived while this request was still being set up has no operation to interrupt, so each
    // step below checks for it before starting I/O.
    if (Cancelled()) {
        co_return std::unexpected(CanceledError());
    }
    BuildRequest();
    if (auto pooled = m_env.pool->Acquire(m_key)) {
        m_stream = std::move(pooled->stream);
        m_buffer = std::move(pooled->buffer);
        m_reused = true;
    } else {
        NewStream();
        if (const auto error = ConfigureTlsForHost()) {
            co_return std::unexpected(error);
        }
    }

    for (;;) {
        if (!m_reused) {
            if (const auto error = co_await Connect()) {
                Close();
                co_return std::unexpected(error);
            }
        }
        auto reply = co_await Transfer();
        if (reply) {
            ReleaseOrClose(reply->keepAlive);
            co_return std::move(reply->response);
        }
        if (!ShouldRetry(reply.error())) {
            Close();
            co_return std::unexpected(reply.error().error);
        }

        // The pooled connection was closed by the peer while idle; the request never reached it in a way that
        // forbids sending it again, so it gets one more attempt on a fresh connection.
        m_retried = true;
        m_reused = false;
        Report(HttpDiagnosticKind::StaleConnectionRetry, {},
               "A reused connection was closed by the peer; retrying on a new connection");
        Close();
        NewStream();
        m_buffer.consume(m_buffer.size());
        ResetParser();
        if (const auto error = ConfigureTlsForHost()) {
            co_return std::unexpected(error);
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
    assert(m_route.Sendable());

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
        const auto field = http::string_to_field(header.GetName());
        switch (field) {
        case http::field::host:
        case http::field::content_length:
        case http::field::transfer_encoding:
        case http::field::connection:
            continue;
        case http::field::unknown:
            req.insert(header.GetName(), header.GetValue());
            break;
        default:
            req.insert(field, header.GetValue());
            break;
        }
    }
    req.set(http::field::host, m_route.Authority);
    req.keep_alive(true);
    if (m_stagedRequest.HasBodyView()) {
        const auto view = m_stagedRequest.GetBodyView();
        m_ownedBodyStorage.clear();
        m_bodyKeepAlive = m_stagedRequest.GetBodyKeepAlive();
        // Beast's buffer_body wants a mutable pointer but only reads it when sending.
        GSL_SUPPRESS(type.3)
        req.body().data = const_cast<void*>(static_cast<const void*>(view.data()));
        req.body().size = view.size();
    } else {
        m_bodyKeepAlive.reset();
        m_ownedBodyStorage = m_stagedRequest.ReleaseBody();
        GSL_SUPPRESS(type.3)
        req.body().data = m_ownedBodyStorage.empty()
                              ? nullptr
                              : const_cast<void*>(static_cast<const void*>(m_ownedBodyStorage.data()));
        req.body().size = m_ownedBodyStorage.size();
    }
    req.body().more = false;
    // buffer_body has no BodyWriter::size(), so message::prepare_payload() cannot be used; set Content-Length
    // manually. A bodyless GET, HEAD or DELETE carries none, but a bodyless POST, PUT or PATCH must state zero.
    const auto method = req.method();
    if (req.body().size != 0 || method == http::verb::post || method == http::verb::put ||
        method == http::verb::patch) {
        req.content_length(req.body().size);
    }
    ResetParser();
}

template <typename Stream> void Exchange<Stream>::ResetParser() {
    m_parser.reset();
    m_responseArena.Reset();
    m_parser.emplace(std::piecewise_construct, std::forward_as_tuple(),
                     std::forward_as_tuple(ArenaAllocator<char>(m_responseArena)));
    m_parser->body_limit(m_options.GetResponseBodyLimit());
    m_parser->header_limit(m_options.GetResponseHeaderLimit());
    m_parser->skip(m_request->method() == http::verb::head);
}

template <typename Stream> std::error_code Exchange<Stream>::ConfigureTlsForHost() {
    if constexpr (std::is_same_v<Stream, TlsStream>) {
        SSL* const ssl = m_stream->native_handle();
        PrepareTlsSessionResumption(ssl, m_key.host, m_key.service);
        if (SSL_get_verify_mode(ssl) != SSL_VERIFY_NONE) {
            // Native OpenSSL host/IP checks run inside the built-in verifier, avoiding a type-erased callback.
            int verifyHostResult = 0;
            if (m_route.HostIsName) {
                SSL_set_hostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                verifyHostResult = SSL_set1_host(ssl, m_key.host.c_str());
            } else {
                verifyHostResult = X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), m_key.host.c_str());
            }
            if (verifyHostResult != 1) {
                return HttpClientError::TlsFailed;
            }
        }
        // SSL_set_tlsext_host_name is a macro with an old-style cast, so call SSL_ctrl directly.
        // SSL_ctrl takes the name as void* but only reads it.
        GSL_SUPPRESS(type.3)
        char* const hostName = const_cast<char*>(m_key.host.c_str());
        if (m_route.HostIsName && !SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name, hostName)) {
            return HttpClientError::TlsFailed;
        }
    }
    return {};
}

template <typename Stream> asio::awaitable<std::expected<ResolveResults, std::error_code>> Exchange<Stream>::Resolve() {
    m_phase = HttpRequestPhase::Resolve;
    m_resolvedFromCache = false;
    auto& cache = DnsCache::Shared();
    const void* const scope = &m_env.context;
    const auto executor = m_env.context.get_executor();

    for (;;) {
        std::shared_ptr<ResolveChannel> joined;
        const auto admission = cache.Acquire(
            m_key.host, m_key.service,
            [&] {
                joined = std::make_shared<ResolveChannel>(executor, 1);
                return ResolveWaiter(joined);
            },
            scope);

        if (admission.Kind == DnsCache::Admission::Cached) {
            m_resolvedFromCache = true;
            co_return admission.Results;
        }

        if (admission.Kind == DnsCache::Admission::Leader) {
            LeaderClaim claim(m_key.host, m_key.service, scope);
            Tcp::resolver resolver(executor);
            const auto [error, results] = co_await resolver.async_resolve(m_key.host, m_key.service, Await);
            claim.Publish(error, results);
            if (error) {
                co_return std::unexpected(Cancelled() ? std::make_error_code(std::errc::operation_canceled)
                                                      : std::error_code(HttpClientError::ResolveFailed));
            }
            co_return results;
        }

        const auto [error, results] = co_await joined->async_receive(Await);
        if (Cancelled()) {
            co_return std::unexpected(std::make_error_code(std::errc::operation_canceled));
        }
        if (error == asio::error::operation_aborted) {
            continue; // The leader was cancelled or timed out, which says nothing about this request's lookup.
        }
        if (error == asio::experimental::error::channel_closed) {
            co_return std::unexpected(std::make_error_code(std::errc::not_enough_memory));
        }
        if (error) {
            co_return std::unexpected(std::error_code(HttpClientError::ResolveFailed));
        }
        co_return results;
    }
}

template <typename Stream> asio::awaitable<std::error_code> Exchange<Stream>::Connect() {
    const auto resolved = co_await Resolve();
    if (!resolved) {
        co_return resolved.error();
    }
    if (Cancelled()) {
        co_return CanceledError();
    }

    m_phase = HttpRequestPhase::Connect;
    const auto [connectError, endpoint] = co_await beast::get_lowest_layer(*m_stream).async_connect(*resolved, Await);
    if (connectError) {
        if (m_resolvedFromCache) {
            // The cached address may be stale; make the next request resolve again.
            DnsCache::Shared().Invalidate(m_key.host, m_key.service);
            Report(HttpDiagnosticKind::DnsInvalidated, connectError,
                   "Connecting to a cached address failed; dropped the cached DNS answer");
        }
        co_return Cancelled() ? std::make_error_code(std::errc::operation_canceled)
                              : std::error_code(HttpClientError::ConnectFailed);
    }

    if constexpr (std::is_same_v<Stream, TlsStream>) {
        if (Cancelled()) {
            co_return CanceledError();
        }
        m_phase = HttpRequestPhase::Handshake;
        const auto [handshakeError] = co_await m_stream->async_handshake(asio::ssl::stream_base::client, Await);
        if (handshakeError) {
            co_return std::error_code(HttpClientError::TlsFailed);
        }
    }
    co_return std::error_code{};
}

template <typename Stream> bool Exchange<Stream>::Replayable(bool requestMayHaveBeenSent) const {
    return !m_receivedAnyResponse && !m_parser->got_some() &&
           (!requestMayHaveBeenSent || IsIdempotentMethod(m_request->method()));
}

template <typename Stream>
asio::awaitable<std::expected<typename Exchange<Stream>::Reply, typename Exchange<Stream>::Failure>>
Exchange<Stream>::Transfer() {
    if (Cancelled()) {
        co_return std::unexpected(Failure{CanceledError(), false});
    }
    m_phase = HttpRequestPhase::Write;
    m_serializer.emplace(*m_request);
    const auto [writeError, bytesWritten] = co_await http::async_write(*m_stream, *m_serializer, Await);
    if (writeError) {
        co_return std::unexpected(Failure{HttpClientError::WriteFailed, Replayable(bytesWritten != 0)});
    }

    for (;;) {
        if (Cancelled()) {
            co_return std::unexpected(Failure{CanceledError(), false});
        }
        m_phase = HttpRequestPhase::Read;
        // The parser consumes body bytes eagerly, so without a reserve beast reads only ~0.5-2 KiB per round trip.
        if (m_buffer.capacity() < ReadChunk) {
            m_buffer.reserve(ReadChunk);
        }
        auto [readError, bytesRead] = co_await http::async_read(*m_stream, m_buffer, *m_parser, Await);
        if (readError) {
            readError = AcceptTruncatedTlsBody(readError);
        }
        if (readError) {
            const auto classified = ClassifyReadError(readError);
            // Limit errors are the response's fault, not the connection's, so they are never retried. For the
            // rest, the request was fully written before the read started.
            co_return std::unexpected(
                Failure{classified, classified != HttpClientError::ResponseTooLarge && Replayable(true)});
        }

        const auto status = m_parser->get().result_int();
        if (status == 101) {
            co_return std::unexpected(Failure{HttpClientError::ProtocolError, false});
        }
        if (status < 200) {
            m_receivedAnyResponse = true; // The server has the request; ResetParser() would hide that.
            ResetParser();
            continue;
        }
        break;
    }

    // The parser-level check also rejects EOF-delimited bodies; leftover bytes would be mistaken for the next
    // response on a pooled connection.
    const bool keepAlive = m_parser->keep_alive() && m_buffer.size() == 0;
    auto response = BuildResponse();
    if (!response) {
        co_return std::unexpected(Failure{response.error(), false});
    }
    co_return Reply{std::move(*response), keepAlive};
}

template <typename Stream>
boost::system::error_code Exchange<Stream>::AcceptTruncatedTlsBody(boost::system::error_code error) {
    if constexpr (std::is_same_v<Stream, TlsStream>) {
        // A TLS server may close TCP without close_notify, and an attacker can truncate an EOF-delimited body that
        // way, so it is only accepted when the request opted in.
        if (error == asio::ssl::error::stream_truncated && m_parser->got_some() &&
            m_options.GetAcceptTruncatedTlsBody()) {
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
    const bool truncated = error == http::error::end_of_stream || error == http::error::partial_message;
    return !truncated && IsBeastHttpError(error) ? HttpClientError::ProtocolError : HttpClientError::ReadFailed;
}

// Only side-effect-free methods may be replayed once the request might have reached the server; PUT and DELETE
// are idempotent in HTTP but Azure mutations are not, so the Azure retry layer decides about those.
template <typename Stream> bool Exchange<Stream>::IsIdempotentMethod(http::verb method) noexcept {
    switch (method) {
    case http::verb::get:
    case http::verb::head:
    case http::verb::options:
    case http::verb::trace:
        return true;
    default:
        return false;
    }
}

template <typename Stream> std::expected<HttpResponse, std::error_code> Exchange<Stream>::BuildResponse() {
    try {
        auto message = m_parser->release();
        std::vector<HttpHeader> headers;
        // Walking Beast's linked list twice is cheaper than the reallocations of a growing vector.
        headers.reserve(gsl::narrow_cast<std::size_t>(std::distance(message.base().begin(), message.base().end())));
        for (const auto& header : message.base()) {
            headers.emplace_back(std::string(header.name_string()), std::string(header.value()));
        }
        return HttpResponse(message.result_int(), std::move(headers), std::move(message.body()));
    } catch (...) {
        return std::unexpected(ClassifyBuildFailure(std::current_exception()));
    }
}

template <typename Stream> void Exchange<Stream>::ReleaseOrClose(bool keepAlive) {
    try {
        bool pooled = false;
        if (keepAlive) {
            // A pooled connection is idle for a long time; don't let it pin the last response's buffer.
            TrimIdleBuffer(m_buffer);
            PooledConnection<Stream> connection{std::move(m_stream), std::move(m_buffer)};
            pooled = m_env.pool->Release(m_key, std::move(connection));
            if (!pooled) {
                // Release only takes the connection when it returns true.
                m_stream = std::move(connection.stream);
                // Only a pool that is enabled but declined the connection is worth reporting; with pooling off this
                // is the normal path and would fire on every request.
                if (m_env.pool->Enabled()) {
                    Report(HttpDiagnosticKind::ConnectionNotPooled, {},
                           "The connection was reusable but the pool did not take it; closing it");
                }
            }
        }
        if (!pooled && m_stream) {
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
