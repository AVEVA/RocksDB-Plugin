// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "RequestOperation.hpp"

#include "HttpVerb.hpp"
#include "RequestValidation.hpp"
#include "TlsContextConfigurator.hpp"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/execution/context.hpp>
#include <boost/asio/post.hpp>

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

template <typename Stream> template <typename Stage> void RequestOperation<Stream>::Contain(Stage&& stage) {
    std::error_code failure;
    try {
        stage();
        return;
    } catch (...) {
        if (m_finished) {
            throw;
        }
        failure = ClassifyBuildFailure(std::current_exception());
    }
    Finish(failure, {});
}

template <typename Stream> void RequestOperation<Stream>::Start() {
    Contain([&] {
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (!BuildRequest(m_stagedRequest, m_stagedRoute)) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (DeadlinePassed()) {
            return Fail(HttpClientError::TimedOut);
        }
        m_reused = false;
        if constexpr (std::is_same_v<Stream, TlsStream>) {
            if (!ConfigureTlsForHost()) {
                return;
            }
        }
        ArmTimer();
        Resolve();
    });
}

template <typename Stream> void RequestOperation<Stream>::StartReused() {
    Contain([&] {
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (!BuildRequest(m_stagedRequest, m_stagedRoute)) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (DeadlinePassed()) {
            return Fail(HttpClientError::TimedOut);
        }
        m_reused = true;
        ArmTimer();
        Write();
    });
}

template <typename Stream> bool RequestOperation<Stream>::BuildRequest(HttpRequest& request, const Route& route) {
    try {
        BuildRequestUnchecked(request, route);
        return true;
    } catch (...) {
        Finish(ClassifyBuildFailure(std::current_exception()), {});
        return false;
    }
}

template <typename Stream>
void RequestOperation<Stream>::BuildRequestUnchecked(HttpRequest& request, const Route& route) {
    // HttpClient::Submit fails unparseable or invalid requests before any operation exists.
    assert(route.Sendable());

    const auto verb = Private::ToBeastVerb(request.GetMethod());

    // Each build starts from an empty message, so no field of an earlier attempt can survive. The serializer and
    // the message are gone before the arena they allocated from is reset.
    m_serializer.reset();
    m_request.reset();
    m_requestArena.Reset();
    auto& req = m_request.emplace(std::piecewise_construct, std::forward_as_tuple(),
                                  std::forward_as_tuple(ArenaAllocator<char>(m_requestArena)));
    req.version(11);
    req.method(verb);
    req.target(route.Target);
    for (const auto& header : request.GetHeaders()) {
        if (beast::iequals(header.GetName(), "Host") || beast::iequals(header.GetName(), "Content-Length") ||
            beast::iequals(header.GetName(), "Transfer-Encoding") || beast::iequals(header.GetName(), "Connection")) {
            continue;
        }
        req.insert(header.GetName(), header.GetValue());
    }
    req.set(http::field::host, route.Authority);
    req.keep_alive(true);
    if (request.HasBodyView()) {
        const auto view = request.GetBodyView();
        m_ownedBodyStorage.clear();
        m_bodyKeepAlive = request.GetBodyKeepAlive();
        // Beast's buffer_body wants a mutable pointer but only reads it when sending.
        GSL_SUPPRESS(type.3)
        req.body().data = const_cast<void*>(static_cast<const void*>(view.data()));
        req.body().size = view.size();
    } else {
        m_bodyKeepAlive.reset();
        m_ownedBodyStorage = request.ReleaseBody();
        GSL_SUPPRESS(type.3)
        req.body().data = m_ownedBodyStorage.empty()
                              ? nullptr
                              : const_cast<void*>(static_cast<const void*>(m_ownedBodyStorage.data()));
        req.body().size = m_ownedBodyStorage.size();
    }
    req.body().more = false;
    // buffer_body has no BodyWriter::size(), so message::prepare_payload() cannot be used;
    // set Content-Length manually instead (the same value it would have computed). A bodyless GET, HEAD or
    // DELETE carries none, but a bodyless POST, PUT or PATCH must state an explicit zero.
    const auto method = req.method();
    if (req.body().size != 0 || method == http::verb::post || method == http::verb::put ||
        method == http::verb::patch) {
        req.set(http::field::content_length, std::to_string(req.body().size));
    }
    ResetParser();
    m_hostIsName = route.HostIsName;
}

template <typename Stream> void RequestOperation<Stream>::ArmTimer() {
    m_timer.expires_at(m_deadline);
    auto self = this->shared_from_this();
    m_timer.async_wait([self](boost::system::error_code error) {
        if (!error) {
            if (self->m_resolveDeliveryFailed.load()) {
                self->FailLostResolve();
            } else {
                self->Fail(HttpClientError::TimedOut);
            }
        }
    });
}

// The shared lookup finished but its outcome could not be posted to this strand (an allocation failure), so
// the request cannot continue; reported the way any other allocation failure inside the operation is.
template <typename Stream> void RequestOperation<Stream>::FailLostResolve() {
    Finish(std::make_error_code(std::errc::not_enough_memory), {});
}

template <typename Stream> bool RequestOperation<Stream>::ConfigureTlsForHost() {
    if constexpr (std::is_same_v<Stream, TlsStream>) {
        PrepareTlsSessionResumption(m_stream->native_handle(), m_key.host, m_key.service);
        if (SSL_get_verify_mode(m_stream->native_handle()) != SSL_VERIFY_NONE) {
            // Native OpenSSL host/IP checks run inside the built-in verifier, avoiding a type-erased callback.
            SSL* const ssl = m_stream->native_handle();
            int verifyHostResult = 0;
            if (m_hostIsName) {
                SSL_set_hostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
                verifyHostResult = SSL_set1_host(ssl, m_key.host.c_str());
            } else {
                verifyHostResult = X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), m_key.host.c_str());
            }
            if (verifyHostResult != 1) {
                Fail(HttpClientError::TlsFailed);
                return false;
            }
        }
        // SSL_set_tlsext_host_name is a macro with an old-style cast, so call SSL_ctrl directly.
        // SSL_ctrl takes the name as void* but only reads it.
        GSL_SUPPRESS(type.3)
        char* const hostName = const_cast<char*>(m_key.host.c_str());
        if (m_hostIsName &&
            !SSL_ctrl(m_stream->native_handle(), SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name, hostName)) {
            Fail(HttpClientError::TlsFailed);
            return false;
        }
    }
    return true;
}

template <typename Stream> bool RequestOperation<Stream>::MaybeRetryAfterReuseFailure(bool requestMayHaveBeenSent) {
    if (IsCancellationRequested() || !m_reused || m_retried || m_receivedAnyResponse || m_parser->got_some() ||
        (requestMayHaveBeenSent && !IsIdempotentMethod(m_request->method()))) {
        return false;
    }
    m_retried = true;
    m_reused = false;
    Report(HttpDiagnosticKind::StaleConnectionRetry, {},
           "A reused connection was closed by the peer; retrying on a new connection");

    boost::system::error_code ignored;
    beast::get_lowest_layer(*m_stream).socket().close(ignored);
    if constexpr (std::is_same_v<Stream, TlsStream>) {
        m_stream = std::make_unique<TlsStream>(m_executor, *m_tlsContext);
        if (!ConfigureTlsForHost()) {
            return true;
        }
    } else {
        m_stream = std::make_unique<PlainStream>(m_executor);
    }
    m_buffer.consume(m_buffer.size());
    ResetParser();

    Resolve();
    return true;
}

// Only side-effect-free methods may be replayed once the request might have reached the server; PUT and DELETE
// are idempotent in HTTP but Azure mutations are not, so the Azure retry layer decides about those.
template <typename Stream> bool RequestOperation<Stream>::IsIdempotentMethod(http::verb method) noexcept {
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

template <typename Stream> void RequestOperation<Stream>::ResetParser() {
    m_parser.reset();
    m_responseArena.Reset();
    m_parser.emplace(std::piecewise_construct, std::forward_as_tuple(),
                     std::forward_as_tuple(ArenaAllocator<char>(m_responseArena)));
    m_parser->body_limit(m_options.GetResponseBodyLimit());
    m_parser->header_limit(m_options.GetResponseHeaderLimit());
    m_parser->skip(m_request->method() == http::verb::head);
}

template <typename Stream>
void RequestOperation<Stream>::OnSharedResolve(boost::system::error_code error, ResolveResults results) {
    auto self = this->shared_from_this();
    try {
        asio::post(m_executor, [self, error, results]() mutable {
            self->Contain([&] {
                // The leader was cancelled or timed out, which says nothing about this request's lookup.
                if (error == asio::error::operation_aborted && !self->m_finished && !self->IsCancellationRequested()) {
                    return self->Resolve();
                }
                self->OnResolve(error, results);
            });
        });
    } catch (...) {
        // The outcome cannot reach the request's executor. This runs on the leader's thread, where touching the
        // request's timer, socket or completion would race its own strand, so only an atomic mark is left; the
        // strand acts on it at its next callback, and at the latest when the request's deadline timer fires.
        self->m_resolveDeliveryFailed.store(true);
    }
}

template <typename Stream> void RequestOperation<Stream>::Resolve() {
    m_phase = HttpRequestPhase::Resolve;
    m_resolvedFromCache = false;
    auto& cache = DnsCache::Shared();
    const void* const scope = m_lookupScope;
    auto admission =
        cache.Acquire(m_key.host, m_key.service, [this] { return ResolveWaiter(this->shared_from_this()); }, scope);
    if (admission.Kind == DnsCache::Admission::Cached) {
        m_resolvedFromCache = true;
        return OnResolve({}, admission.Results);
    }
    if (admission.Kind == DnsCache::Admission::Joined) {
        return;
    }
    // If the handler is destroyed unrun (io_context torn down), the guard still releases the waiters.
    struct LeaderGuard {
        std::shared_ptr<RequestOperation> owner;
        const void* scope;

        LeaderGuard(std::shared_ptr<RequestOperation> operation, const void* lookupScope)
            : owner(std::move(operation)), scope(lookupScope) {}
        // A move transfers the leader claim; a copy would leave a second owner of it.
        LeaderGuard(LeaderGuard&&) noexcept = default;
        LeaderGuard& operator=(LeaderGuard&&) = delete;
        LeaderGuard(const LeaderGuard&) = delete;
        LeaderGuard& operator=(const LeaderGuard&) = delete;

        ~LeaderGuard() {
            if (owner) {
                DnsCache::Shared().Finish(owner->m_key.host, owner->m_key.service, asio::error::operation_aborted, {},
                                          scope);
            }
        }
    };
    m_resolver.async_resolve(m_key.host, m_key.service,
                             [guard = LeaderGuard(this->shared_from_this(), scope)](
                                 boost::system::error_code error, Tcp::resolver::results_type results) mutable {
                                 auto self = std::move(guard.owner);
                                 DnsCache::Shared().Finish(self->m_key.host, self->m_key.service, error, results,
                                                           guard.scope);
                                 self->OnResolve(error, results);
                             });
}

// Every asynchronous callback starts here: a finished request has nothing left to do, and a requested
// cancellation is carried out on the request's own executor.
template <typename Stream> bool RequestOperation<Stream>::AbortIfNeeded() {
    if (m_finished) {
        return true;
    }
    if (IsCancellationRequested()) {
        Cancel();
        return true;
    }
    if (m_resolveDeliveryFailed.load()) {
        FailLostResolve();
        return true;
    }
    return false;
}

template <typename Stream>
void RequestOperation<Stream>::OnResolve(boost::system::error_code error, const Tcp::resolver::results_type& results) {
    Contain([&] {
        if (AbortIfNeeded()) {
            return;
        }
        if (error) {
            return Fail(HttpClientError::ResolveFailed, error);
        }
        m_phase = HttpRequestPhase::Connect;
        beast::get_lowest_layer(*m_stream).async_connect(
            results, [self = this->shared_from_this()](boost::system::error_code connectError, const Tcp::endpoint&) {
                self->OnConnect(connectError);
            });
    });
}

template <typename Stream> void RequestOperation<Stream>::OnConnect(boost::system::error_code error) {
    Contain([&] {
        if (AbortIfNeeded()) {
            return;
        }
        if (error) {
            if (m_resolvedFromCache) {
                // The cached address may be stale; make the next request resolve again.
                DnsCache::Shared().Invalidate(m_key.host, m_key.service);
                Report(HttpDiagnosticKind::DnsInvalidated, error,
                       "Connecting to a cached address failed; dropped the cached DNS answer");
            }
            return Fail(HttpClientError::ConnectFailed, error);
        }
        if constexpr (std::is_same_v<Stream, TlsStream>) {
            m_phase = HttpRequestPhase::Handshake;
            m_stream->async_handshake(asio::ssl::stream_base::client,
                                      [self = this->shared_from_this()](boost::system::error_code handshakeError) {
                                          self->Contain([&] {
                                              if (self->AbortIfNeeded()) {
                                                  return;
                                              }
                                              if (handshakeError) {
                                                  return self->Fail(HttpClientError::TlsFailed, handshakeError);
                                              }
                                              self->Write();
                                          });
                                      });
        } else {
            Write();
        }
    });
}

template <typename Stream> void RequestOperation<Stream>::Write() {
    if (AbortIfNeeded()) {
        return;
    }
    m_phase = HttpRequestPhase::Write;
    m_writeInFlight = true;
    try {
        m_serializer.emplace(*m_request);
        http::async_write(*m_stream, *m_serializer,
                          [self = this->shared_from_this()](boost::system::error_code error, std::size_t bytesWritten) {
                              self->m_writeInFlight = false;
                              if (self->DeliverDeferredCompletion()) {
                                  return;
                              }
                              self->Contain([&] {
                                  if (self->AbortIfNeeded()) {
                                      return;
                                  }
                                  if (error) {
                                      if (self->MaybeRetryAfterReuseFailure(bytesWritten != 0)) {
                                          return;
                                      }
                                      return self->Fail(HttpClientError::WriteFailed, error);
                                  }
                                  self->Read();
                              });
                          });
    } catch (...) {
        // Nothing was started, so no write handler will ever clear the flag that defers completion.
        m_writeInFlight = false;
        throw;
    }
}

template <typename Stream> void RequestOperation<Stream>::Read() {
    if (AbortIfNeeded()) {
        return;
    }
    m_phase = HttpRequestPhase::Read;
    // The parser consumes body bytes eagerly, so without a reserve beast reads only ~0.5-2 KiB per round trip.
    if (m_buffer.capacity() < ReadChunk) {
        m_buffer.reserve(ReadChunk);
    }
    http::async_read(
        *m_stream, m_buffer, *m_parser,
        [self = this->shared_from_this()](boost::system::error_code error, std::size_t) { self->OnRead(error); });
}

template <typename Stream> void RequestOperation<Stream>::OnRead(boost::system::error_code error) {
    Contain([&] { OnReadStage(error); });
}

template <typename Stream> void RequestOperation<Stream>::OnReadStage(boost::system::error_code error) {
    if (AbortIfNeeded()) {
        return;
    }
    if (error) {
        error = AcceptTruncatedTlsBody(error);
    }
    if (error) {
        return FailRead(error);
    }
    const auto status = m_parser->get().result_int();
    if (status == 101) {
        return Fail(HttpClientError::ProtocolError);
    }
    if (status < 200) {
        m_receivedAnyResponse = true; // The server has the request; ResetParser() would hide that.
        ResetParser();
        return Read();
    }

    // The parser-level check also rejects EOF-delimited bodies; leftover bytes would be mistaken for the
    // next response on a pooled connection.
    const bool keepAlive = m_parser->keep_alive() && m_buffer.size() == 0;
    auto response = BuildResponse();
    if (!response) {
        // Without this the handler would never be called (e.g. bad_alloc while copying headers).
        return Finish(response.error(), {});
    }
    CompleteSuccess(std::move(*response), keepAlive);
}

template <typename Stream>
boost::system::error_code RequestOperation<Stream>::AcceptTruncatedTlsBody(boost::system::error_code error) {
    if constexpr (std::is_same_v<Stream, TlsStream>) {
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

template <typename Stream>
HttpClientError RequestOperation<Stream>::ClassifyReadError(const boost::system::error_code& error) {
    if (error == http::error::body_limit || error == http::error::header_limit) {
        return HttpClientError::ResponseTooLarge;
    }
    // A connection that ends mid-message is a transport failure, not a malformed response.
    const bool truncated = error == http::error::end_of_stream || error == http::error::partial_message;
    return !truncated && IsBeastHttpError(error) ? HttpClientError::ProtocolError : HttpClientError::ReadFailed;
}

template <typename Stream> void RequestOperation<Stream>::FailRead(const boost::system::error_code& error) {
    const auto classified = ClassifyReadError(error);
    // Limit errors are the response's fault, not the connection's, so they are never retried. For the rest,
    // the request was fully written before the read started.
    if (classified != HttpClientError::ResponseTooLarge && MaybeRetryAfterReuseFailure(true)) {
        return;
    }
    Fail(classified, error);
}

template <typename Stream> std::expected<HttpResponse, std::error_code> RequestOperation<Stream>::BuildResponse() {
    try {
        auto message = m_parser->release();
        std::vector<HttpHeader> headers;
        // Walking Beast's linked list twice is cheaper than the reallocations of a growing vector. Each name and
        // value is built once, straight from the parser's view, and moved into the header.
        headers.reserve(gsl::narrow_cast<std::size_t>(std::distance(message.base().begin(), message.base().end())));
        for (const auto& header : message.base()) {
            headers.emplace_back(std::string(header.name_string()), std::string(header.value()));
        }
        return HttpResponse(message.result_int(), std::move(headers), std::move(message.body()));
    } catch (...) {
        return std::unexpected(ClassifyBuildFailure(std::current_exception()));
    }
}

template <typename Stream> void RequestOperation<Stream>::ReleaseOrClose(bool keepAlive) {
    bool pooled = false;
    if (keepAlive) {
        // Declared outside the try so a throw from Release leaves the stream here, to be closed gracefully below.
        std::optional<PooledConnection<Stream>> connection;
        try {
            // A pooled connection is idle for a long time; don't let it pin the last response's buffer.
            TrimIdleBuffer(m_buffer);
            connection.emplace(PooledConnection<Stream>{std::move(m_stream), std::move(m_buffer)});
            pooled = m_pool->Release(m_key, std::move(*connection));
        } catch (...) {
            // The completion must still run; the connection is closed below if it could not be pooled.
            Report(HttpDiagnosticKind::SwallowedException, {}, "Returning the connection to the pool failed");
        }
        // Pooling is disabled, or Release threw: the connection was not taken, so it is closed below.
        if (!pooled && connection && connection->stream) {
            m_stream = std::move(connection->stream);
        }
    }
    // Only a pool that is enabled but declined the connection (or threw) is worth reporting; with pooling off this
    // is the normal path and would fire on every request.
    if (keepAlive && !pooled && m_stream && m_pool->Enabled()) {
        Report(HttpDiagnosticKind::ConnectionNotPooled, {},
               "The connection was reusable but the pool did not take it; closing it");
    }
    if (!pooled && m_stream) {
        // Only clean completions reach here (error paths close immediately in Finish), so the peer is in a known
        // state and a close_notify lets it distinguish this from a truncation. The bounded grace period keeps a
        // silent peer from holding the stream open. The stream moves into the handler so the operation (request,
        // body owner, buffers) ends with the completion instead of lingering until the shutdown finishes.
        if constexpr (std::is_same_v<Stream, TlsStream>) {
            TlsStream& tls = *m_stream;
            beast::get_lowest_layer(tls).expires_after(TlsShutdownGrace);
            tls.async_shutdown([stream = std::move(m_stream)](boost::system::error_code) {
                boost::system::error_code ignored;
                auto& socket = beast::get_lowest_layer(*stream).socket();
                socket.shutdown(Tcp::socket::shutdown_both, ignored);
                socket.close(ignored);
            });
        } else {
            Close();
        }
    }
}

template <typename Stream> void RequestOperation<Stream>::CompleteSuccess(HttpResponse response, bool keepAlive) {
    m_timer.cancel();
    m_resolver.cancel();
    m_finished = true;
    auto completion = std::move(m_completion);
    // The completion was already moved out, so nothing thrown while releasing the connection (for example an
    // allocation failure in the TLS shutdown setup) may stop it from running.
    try {
        ReleaseOrClose(keepAlive);
    } catch (...) {
        Report(HttpDiagnosticKind::SwallowedException, {}, "Releasing the connection failed; closing it");
        Close();
    }
    Detail::Complete(std::move(completion), {}, std::move(response));
}

template <typename Stream> void RequestOperation<Stream>::Close() {
    if (!m_stream) {
        return;
    }
    boost::system::error_code ignored;
    auto& socket = beast::get_lowest_layer(*m_stream).socket();
    socket.shutdown(Tcp::socket::shutdown_both, ignored);
    socket.close(ignored);
}

template <typename Stream> void RequestOperation<Stream>::Fail(HttpClientError error, boost::system::error_code cause) {
    if (error == HttpClientError::TimedOut && !m_finished) {
        Report(HttpDiagnosticKind::Timeout, static_cast<std::error_code>(cause), "The request timed out");
    }
    HttpResponse response;
    if (cause) {
        response.SetCause(static_cast<std::error_code>(cause));
    }
    Finish(error, std::move(response));
}

template <typename Stream> void RequestOperation<Stream>::Finish(std::error_code error, HttpResponse response) {
    if (m_finished) {
        return;
    }
    m_finished = true;
    m_timer.cancel();
    m_resolver.cancel();
    Close();
    if (m_writeInFlight) {
        // Closing the socket only starts cancelling the write, and the request body (possibly a caller-owned raw
        // view) must stay valid until the transport reports the write finished, so completion waits for it.
        m_deferredCompletion.emplace(error, std::move(response));
        return;
    }
    auto completion = std::move(m_completion);
    Detail::Complete(std::move(completion), error, std::move(response));
}

template <typename Stream> bool RequestOperation<Stream>::DeliverDeferredCompletion() {
    if (!m_deferredCompletion) {
        return false;
    }
    auto outcome = std::move(*m_deferredCompletion);
    m_deferredCompletion.reset();
    auto completion = std::move(m_completion);
    Detail::Complete(std::move(completion), outcome.first, std::move(outcome.second));
    return true;
}

template <typename Stream> void RequestOperation<Stream>::RequestCancellation() {
    if (m_cancellationRequested.exchange(true)) {
        return;
    }
    asio::post(m_executor, [self = this->shared_from_this()]() { self->Cancel(); });
}

void ResolveWaiter::operator()(boost::system::error_code error, ResolveResults results) {
    std::visit(
        [&](auto& operation) {
            if (auto strong = operation.lock()) {
                strong->OnSharedResolve(error, std::move(results));
            }
        },
        m_operation);
}

template class RequestOperation<PlainStream>;
template class RequestOperation<TlsStream>;
} // namespace AVEVA::Private
