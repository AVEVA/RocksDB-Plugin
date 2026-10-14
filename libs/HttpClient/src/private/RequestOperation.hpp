// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "CancelState.hpp"
#include "ConnectionPool.hpp"
#include "DnsCache.hpp"
#include "HttpVerb.hpp"
#include "RequestValidation.hpp"
#include "Route.hpp"
#include "StreamTypes.hpp"
#include "TlsContextConfigurator.hpp"

#include "AVEVA/HttpClient/Detail/CompletionOperation.hpp"
#include "AVEVA/HttpClient/HttpRequest.hpp"
#include "AVEVA/HttpClient/HttpRequestOptions.hpp"
#include "AVEVA/HttpClient/HttpResponse.hpp"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>

#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/steady_timer.hpp>

#include <openssl/ssl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>

namespace AVEVA::Private {
/// <summary>
/// Any error raised by Beast's HTTP parser (bad status line, bad header, bad chunk, ...) means the peer sent
/// something that is not valid HTTP, as opposed to a transport failure.
/// </summary>
/// <param name="error">The error to classify.</param>
/// <returns>True if it came from the HTTP parser.</returns>
inline bool IsBeastHttpError(const boost::system::error_code& error) noexcept {
    return error.category() == boost::beast::http::make_error_code(boost::beast::http::error{}).category();
}

template <typename Stream> class RequestOperation;

/// <summary>
/// Hands the outcome of a shared DNS lookup to the request that was waiting on it. A request is either
/// plain or TLS, so a variant of the two operation types covers every waiter without a type-erased
/// function.
/// </summary>
class ResolveWaiter {
  public:
    /// <summary>
    /// Constructs a waiter for one request.
    /// </summary>
    /// <param name="operation">The request waiting for the lookup.</param>
    template <typename Stream>
    explicit ResolveWaiter(std::shared_ptr<RequestOperation<Stream>> operation) : m_operation(std::move(operation)) {}

    /// <summary>
    /// Delivers the lookup's outcome to the request.
    /// </summary>
    /// <param name="error">The leader's outcome.</param>
    /// <param name="results">The endpoints the leader found, if any.</param>
    void operator()(boost::system::error_code error, Tcp::resolver::results_type results);

  private:
    std::variant<std::shared_ptr<RequestOperation<PlainStream>>, std::shared_ptr<RequestOperation<TlsStream>>>
        m_operation;
};

/// <summary>
/// The process-wide DNS cache type, whose waiters are the requests of this client.
/// </summary>
using DnsCache = BasicDnsCache<std::chrono::steady_clock, ResolveWaiter>;

/// <summary>
/// One HTTP request from start to completion over a <see cref="PlainStream"/> or <see cref="TlsStream"/>:
/// building the message, resolving, connecting, handshaking, writing, reading the response, and then either
/// returning the connection to its pool or closing it.
///
/// The operation keeps itself alive through <c>shared_from_this()</c> for the duration, and runs entirely on
/// one strand; only cancellation may arrive from another thread, and it posts to that strand. The completion
/// handler is invoked exactly once, on every path including a thrown exception.
/// </summary>
template <typename Stream>
class RequestOperation final : public std::enable_shared_from_this<RequestOperation<Stream>> {
  public:
    /// <summary>
    /// Constructs an operation over an already-created stream.
    /// </summary>
    /// <param name="tlsContext">The TLS context, held so a retry can build a replacement stream; unused for
    /// a plain stream.</param>
    /// <param name="stream">The stream to send over; its executor becomes the operation's strand.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    /// <param name="pool">The pool the connection is returned to if the response allows reuse.</param>
    /// <param name="key">The origin the connection belongs to.</param>
    /// <param name="buffer">The read buffer carried over from a reused connection; empty otherwise.</param>
    RequestOperation(std::shared_ptr<asio::ssl::context> tlsContext, std::unique_ptr<Stream> stream,
                     Detail::CompletionPointer completion, HttpRequestOptions options,
                     std::shared_ptr<ConnectionPool<Stream>> pool, ConnectionKey<Stream> key,
                     beast::flat_buffer buffer = {})
        : m_tlsContext(std::move(tlsContext)), m_executor(stream->get_executor()), m_stream(std::move(stream)),
          m_resolver(m_executor), m_timer(m_executor), m_completion(std::move(completion)), m_options(options),
          m_pool(std::move(pool)), m_key(std::move(key)), m_buffer(std::move(buffer)) {}

    /// <summary>
    /// Makes this operation the target of the request's cancellation state, so a cancel (including one that
    /// already happened while the request was queued) reaches it. Never touches the caller's slot, which only
    /// the submitting thread may assign. Must be called before the operation is started.
    /// </summary>
    /// <param name="cancellation">The state shared with the caller's slot; null when the request has no
    /// cancellation slot.</param>
    void BindCancellation(const std::shared_ptr<CancelState>& cancellation) {
        if (!cancellation) {
            return;
        }
        // The target runs on whichever thread emits the signal, so it only posts to the operation's strand,
        // where Cancel() may safely use the stream.
        cancellation->SetTarget([weak = this->weak_from_this()] {
            if (auto self = weak.lock()) {
                self->RequestCancellation();
            }
        });
    }

    /// <summary>
    /// Fresh connection: resolve, connect, (TLS handshake), then send. Must run on the operation's strand.
    /// </summary>
    /// <param name="request">The request to send; its owned body is moved out.</param>
    void Start(HttpRequest request, const Route& route) {
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (!BuildRequest(request, route)) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        m_reused = false;
        if constexpr (std::is_same_v<Stream, TlsStream>) {
            if (!ConfigureTlsForHost()) {
                return;
            }
        }
        ArmTimer();
        Resolve();
    }

    /// <summary>
    /// Reused pooled connection: already connected (and, for TLS, already handshaked), so this writes
    /// straight away. Must run on the operation's strand.
    /// </summary>
    /// <param name="request">The request to send; its owned body is moved out.</param>
    void StartReused(HttpRequest request, const Route& route) {
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (!BuildRequest(request, route)) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        m_reused = true;
        ArmTimer();
        Write();
    }

  private:
    /// <summary>
    /// Translates the request into the Beast message to be sent, failing the operation if it cannot be.
    /// Anything that still throws (for example an oversized Host authority) must complete the request
    /// exactly once instead of escaping onto the host's io_context thread and dropping the completion
    /// handler.
    /// </summary>
    /// <param name="request">The request to translate; its owned body is moved out.</param>
    /// <returns>True if the operation may continue.</returns>
    bool BuildRequest(HttpRequest& request, const Route& route) {
        try {
            return BuildRequestUnchecked(request, route);
        } catch (const std::exception&) {
            Fail(HttpClientError::InvalidRequest);
            return false;
        }
    }

    /// <summary>
    /// Validates the URL, method, headers and options, and fills in the Beast request: target, Host,
    /// keep-alive, body and Content-Length. Fails the operation on anything invalid. May throw, which is
    /// why callers go through <see cref="BuildRequest"/>.
    /// </summary>
    /// <param name="request">The request to translate; its owned body is moved out.</param>
    /// <returns>True if the operation may continue.</returns>
    bool BuildRequestUnchecked(HttpRequest& request, const Route& route) {
        if (!route.Parsed) {
            Fail(HttpClientError::InvalidUrl);
            return false;
        }
        if (route.Error) {
            // ValidateRequest only ever reports HttpClientError values.
            Fail(static_cast<HttpClientError>(route.Error.value()));
            return false;
        }

        const auto verb = Private::ToBeastVerb(request.GetMethod());

        m_request.version(11);
        m_request.method(verb);
        m_request.target(route.Target);
        for (const auto& header : request.GetHeaders()) {
            if (beast::iequals(header.GetName(), "Host") || beast::iequals(header.GetName(), "Content-Length") ||
                beast::iequals(header.GetName(), "Transfer-Encoding") ||
                beast::iequals(header.GetName(), "Connection")) {
                continue;
            }
            m_request.insert(header.GetName(), header.GetValue());
        }
        m_request.set(http::field::host, route.Authority);
        m_request.keep_alive(true);
        if (request.HasBodyView()) {
            const auto view = request.GetBodyView();
            m_ownedBodyStorage.clear();
            m_bodyKeepAlive = request.GetBodyKeepAlive();
            m_request.body().data = const_cast<void*>(static_cast<const void*>(view.data()));
            m_request.body().size = view.size();
        } else {
            m_bodyKeepAlive.reset();
            m_ownedBodyStorage = request.ReleaseBody();
            m_request.body().data = m_ownedBodyStorage.empty()
                                        ? nullptr
                                        : const_cast<void*>(static_cast<const void*>(m_ownedBodyStorage.data()));
            m_request.body().size = m_ownedBodyStorage.size();
        }
        m_request.body().more = false;
        // buffer_body has no BodyWriter::size(), so message::prepare_payload() cannot be used;
        // set Content-Length manually instead (the same value it would have computed).
        m_request.set(http::field::content_length, std::to_string(m_request.body().size));
        ResetParser();
        m_hostIsName = route.HostIsName;
        return true;
    }

    /// <summary>
    /// Starts the deadline covering the whole operation, failing it with TimedOut when it elapses.
    /// </summary>
    void ArmTimer() {
        // Clamp so huge timeouts (e.g. milliseconds::max()) cannot overflow the clock's duration or time_point.
        constexpr auto MaxTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(EffectivelyInfinite);
        m_timer.expires_after(std::min(m_options.GetTimeout(), MaxTimeout));
        auto self = this->shared_from_this();
        m_timer.async_wait([self](boost::system::error_code error) {
            if (!error) {
                self->Fail(HttpClientError::TimedOut);
            }
        });
    }

    /// <summary>
    /// Prepares the TLS stream for this origin: offers a resumable session, installs host name verification
    /// when verification is on, and sets SNI for a named host. Fails the operation if SNI could not be set.
    /// </summary>
    /// <returns>True if the operation may continue.</returns>
    bool ConfigureTlsForHost() {
        PrepareTlsSessionResumption(m_stream->native_handle(), m_key.host, m_key.service);
        if (SSL_get_verify_mode(m_stream->native_handle()) != SSL_VERIFY_NONE) {
            m_stream->set_verify_callback(asio::ssl::host_name_verification(m_key.host));
        }
        if (m_hostIsName && !SSL_set_tlsext_host_name(m_stream->native_handle(), m_key.host.c_str())) {
            Fail(HttpClientError::TlsFailed);
            return false;
        }
        return true;
    }

    /// <summary>
    /// A pooled connection may have been closed by the peer while idle. If nothing has been
    /// parsed yet, it is safe to silently retry once on a brand-new connection.
    /// Because the request may already have been processed by the time the failure is seen, a request that
    /// could have reached the server is only re-sent when its method is idempotent.
    /// </summary>
    /// <param name="requestMayHaveBeenSent">Whether the bytes of the request could already have reached the
    /// server.</param>
    /// <returns>True if a retry was started, in which case the caller must not also fail the
    /// operation.</returns>
    bool MaybeRetryAfterReuseFailure(bool requestMayHaveBeenSent) {
        if (IsCancellationRequested() || !m_reused || m_retried || m_receivedAnyResponse || m_parser->got_some() ||
            (requestMayHaveBeenSent && !IsIdempotentMethod(m_request.method()))) {
            return false;
        }
        m_retried = true;
        m_reused = false;

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

    /// <summary>
    /// Whether re-sending a method cannot have an effect beyond that of sending it once, and so may be
    /// retried after a failure that might have reached the server.
    /// </summary>
    /// <param name="method">The method to classify.</param>
    /// <returns>True if the method is idempotent.</returns>
    static bool IsIdempotentMethod(http::verb method) noexcept {
        switch (method) {
        case http::verb::get:
        case http::verb::head:
        case http::verb::options:
        case http::verb::trace:
        case http::verb::put:
        case http::verb::delete_:
            return true;
        default:
            return false;
        }
    }

    /// <summary>
    /// Installs a fresh response parser carrying the request's limits, and skips the body for a HEAD
    /// response, which has none despite its Content-Length.
    /// </summary>
    void ResetParser() {
        m_parser.emplace();
        m_parser->body_limit(m_options.GetResponseBodyLimit());
        m_parser->header_limit(m_options.GetResponseHeaderLimit());
        m_parser->skip(m_request.method() == http::verb::head);
    }

  public:
    /// <summary>
    /// Receives the outcome of a lookup that another request led. Callable from any thread; the result is
    /// handled on this operation's strand.
    /// </summary>
    /// <param name="error">The leader's outcome.</param>
    /// <param name="results">The endpoints the leader found, if any.</param>
    void OnSharedResolve(boost::system::error_code error, Tcp::resolver::results_type results) {
        asio::post(m_executor, [self = this->shared_from_this(), error, results = std::move(results)]() mutable {
            // The leader was cancelled or timed out, which says nothing about this request's lookup.
            if (error == asio::error::operation_aborted && !self->finished_ && !self->IsCancellationRequested()) {
                return self->Resolve();
            }
            self->OnResolve(error, std::move(results));
        });
    }

  private:
    /// <summary>
    /// Serves a recent result from the shared cache and collapses concurrent lookups for one origin into a
    /// single resolve. Every caller already runs on this operation's strand, so a hit is dispatched (run inline)
    /// instead of posted; completions are always posted, so the caller's handler is still never re-entered.
    /// </summary>
    void Resolve() {
        auto& cache = DnsCache::Shared();
        if (auto cached = cache.Find(m_key.host, m_key.service)) {
            asio::dispatch(m_executor, [self = this->shared_from_this(), results = std::move(*cached)]() mutable {
                self->OnResolve({}, std::move(results));
            });
            return;
        }
        ResolveWaiter waiter(this->shared_from_this());
        if (!cache.JoinOrLead(m_key.host, m_key.service, waiter)) {
            return;
        }
        // If the handler is destroyed unrun (io_context torn down), the guard still releases the waiters.
        struct LeaderGuard {
            std::string host;
            std::string service;
            bool finished = false;

            ~LeaderGuard() {
                if (!finished) {
                    DnsCache::Shared().Finish(host, service, asio::error::operation_aborted, {});
                }
            }
        };
        auto guard = std::make_shared<LeaderGuard>(LeaderGuard{m_key.host, m_key.service});
        m_resolver.async_resolve(m_key.host, m_key.service,
                                 [self = this->shared_from_this(), guard](boost::system::error_code error,
                                                                          Tcp::resolver::results_type results) {
                                     guard->finished = true;
                                     DnsCache::Shared().Finish(self->m_key.host, self->m_key.service, error, results);
                                     self->OnResolve(error, std::move(results));
                                 });
    }

    /// <summary>
    /// Connects to the resolved endpoints, or fails the operation if the lookup did not succeed.
    /// </summary>
    /// <param name="error">The outcome of the lookup.</param>
    /// <param name="results">The endpoints to connect to.</param>
    void OnResolve(boost::system::error_code error, Tcp::resolver::results_type results) {
        if (finished_) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (error) {
            return Fail(HttpClientError::ResolveFailed, error);
        }
        beast::get_lowest_layer(*m_stream).async_connect(
            results, [self = this->shared_from_this()](boost::system::error_code connectError, const Tcp::endpoint&) {
                self->OnConnect(connectError);
            });
    }

    /// <summary>
    /// Performs the TLS handshake on a secure stream and then writes, or writes straight away on a plain
    /// one. Fails the operation if the connect or the handshake did not succeed.
    /// </summary>
    /// <param name="error">The outcome of the connect.</param>
    void OnConnect(boost::system::error_code error) {
        if (finished_) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        if (error) {
            return Fail(HttpClientError::ConnectFailed, error);
        }
        if constexpr (std::is_same_v<Stream, TlsStream>) {
            m_stream->async_handshake(asio::ssl::stream_base::client,
                                      [self = this->shared_from_this()](boost::system::error_code handshakeError) {
                                          if (self->finished_) {
                                              return;
                                          }
                                          if (self->IsCancellationRequested()) {
                                              return self->Cancel();
                                          }
                                          if (handshakeError) {
                                              return self->Fail(HttpClientError::TlsFailed, handshakeError);
                                          }
                                          self->Write();
                                      });
        } else {
            Write();
        }
    }

    /// <summary>
    /// Writes the request, then starts reading. A failure on a reused connection may be retried instead of
    /// reported.
    /// </summary>
    void Write() {
        if (finished_) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        http::async_write(*m_stream, m_request,
                          [self = this->shared_from_this()](boost::system::error_code error, std::size_t bytesWritten) {
                              if (self->finished_) {
                                  return;
                              }
                              if (self->IsCancellationRequested()) {
                                  return self->Cancel();
                              }
                              if (error) {
                                  if (self->MaybeRetryAfterReuseFailure(bytesWritten != 0)) {
                                      return;
                                  }
                                  return self->Fail(HttpClientError::WriteFailed, error);
                              }
                              self->Read();
                          });
    }

    /// <summary>
    /// Reads one complete response message into the parser.
    /// </summary>
    void Read() {
        if (finished_) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
        }
        http::async_read(
            *m_stream, m_buffer, *m_parser,
            [self = this->shared_from_this()](boost::system::error_code error, std::size_t) { self->OnRead(error); });
    }

    /// <summary>
    /// Completes the operation with the response just read: skips informational (1xx) responses, classifies
    /// a read failure, and then either returns the connection to the pool or shuts it down before handing
    /// the response to the completion handler.
    /// </summary>
    /// <param name="error">The outcome of the read.</param>
    void OnRead(boost::system::error_code error) {
        if (finished_) {
            return;
        }
        if (IsCancellationRequested()) {
            return Cancel();
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
            return Fail(HttpClientError::ReadFailed);
        }
        CompleteSuccess(std::move(*response), keepAlive);
    }

    /// <summary>
    /// A TLS server may close TCP without close_notify; that still ends an EOF-delimited body, so such a
    /// truncation is not an error once the parser accepts the EOF.
    /// </summary>
    /// <param name="error">The failed read's error.</param>
    /// <returns>An empty error code if the message turned out to be complete; otherwise the original error.</returns>
    boost::system::error_code AcceptTruncatedTlsBody(boost::system::error_code error) {
        if constexpr (std::is_same_v<Stream, TlsStream>) {
            if (error == asio::ssl::error::stream_truncated && m_parser->got_some()) {
                boost::system::error_code eofError;
                m_parser->put_eof(eofError);
                if (!eofError) {
                    return {};
                }
            }
        }
        return error;
    }

    /// <summary>
    /// Maps a failed read to the error reported to the caller.
    /// </summary>
    /// <param name="error">The failed read's (non-empty) error.</param>
    /// <returns>ResponseTooLarge for limit errors, ProtocolError for a malformed response, otherwise
    /// ReadFailed.</returns>
    static HttpClientError ClassifyReadError(const boost::system::error_code& error) {
        if (error == http::error::body_limit || error == http::error::header_limit) {
            return HttpClientError::ResponseTooLarge;
        }
        // A connection that ends mid-message is a transport failure, not a malformed response.
        const bool truncated = error == http::error::end_of_stream || error == http::error::partial_message;
        return !truncated && IsBeastHttpError(error) ? HttpClientError::ProtocolError : HttpClientError::ReadFailed;
    }

    /// <summary>
    /// Completes a failed read, first retrying on a fresh connection when a reused one turned out to be dead.
    /// </summary>
    /// <param name="error">The failed read's (non-empty) error.</param>
    void FailRead(const boost::system::error_code& error) {
        const auto classified = ClassifyReadError(error);
        // Limit errors are the response's fault, not the connection's, so they are never retried. For the rest,
        // the request was fully written before the read started.
        if (classified != HttpClientError::ResponseTooLarge && MaybeRetryAfterReuseFailure(true)) {
            return;
        }
        Fail(classified, error);
    }

    /// <summary>
    /// Moves the parsed message into an HttpResponse.
    /// </summary>
    /// <returns>The response, or nothing if building it threw (for example bad_alloc).</returns>
    std::optional<HttpResponse> BuildResponse() {
        try {
            auto message = m_parser->release();
            std::vector<HttpHeader> headers;
            headers.reserve(static_cast<std::size_t>(std::distance(message.base().begin(), message.base().end())));
            for (const auto& header : message.base()) {
                headers.push_back({std::string(header.name_string()), std::string(header.value())});
            }
            return HttpResponse(message.result_int(), std::move(headers), std::move(message.body()));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    }

    /// <summary>
    /// Returns the connection to the pool when the response allows reuse, and otherwise shuts it down.
    /// </summary>
    /// <param name="keepAlive">Whether the connection may be reused.</param>
    void ReleaseOrClose(bool keepAlive) {
        bool pooled = false;
        if (keepAlive) {
            try {
                // A pooled connection is idle for a long time; don't let it pin the last response's buffer.
                TrimIdleBuffer(m_buffer);
                m_pool->Release(m_key, PooledConnection<Stream>{std::move(m_stream), std::move(m_buffer)});
                pooled = true;
            } catch (const std::exception&) {
                // The completion must still run; drop the connection if it could not be pooled.
            }
        }
        if (!pooled && m_stream) {
            // Only clean completions reach here (error paths close immediately in Finish), so the peer is in a known
            // state and a close_notify lets it distinguish this from a truncation. The bounded grace period keeps a
            // silent peer from holding the operation alive.
            if constexpr (std::is_same_v<Stream, TlsStream>) {
                beast::get_lowest_layer(*m_stream).expires_after(TlsShutdownGrace);
                m_stream->async_shutdown(
                    [self = this->shared_from_this()](boost::system::error_code) { self->Close(); });
            } else {
                Close();
            }
        }
    }

    /// <summary>
    /// Finishes the operation successfully: stops the timer and resolver, disposes of the connection, and hands
    /// the response to the completion handler.
    /// </summary>
    /// <param name="response">The response to report.</param>
    /// <param name="keepAlive">Whether the connection may be reused.</param>
    void CompleteSuccess(HttpResponse response, bool keepAlive) {
        m_timer.cancel();
        m_resolver.cancel();
        finished_ = true;
        auto completion = std::move(m_completion);
        ReleaseOrClose(keepAlive);
        Detail::Complete(std::move(completion), {}, std::move(response));
    }

    /// <summary>
    /// Shuts down and closes the underlying socket, ignoring errors; a no-op once the stream has been
    /// released to the pool.
    /// </summary>
    void Close() {
        if (!m_stream) {
            return;
        }
        boost::system::error_code ignored;
        auto& socket = beast::get_lowest_layer(*m_stream).socket();
        socket.shutdown(Tcp::socket::shutdown_both, ignored);
        socket.close(ignored);
    }

    /// <summary>
    /// Completes the operation with an error and an empty response.
    /// </summary>
    /// <param name="error">The error to complete with.</param>
    /// <param name="cause">The underlying transport error, kept on the response for diagnostics.</param>
    void Fail(HttpClientError error, boost::system::error_code cause = {}) {
        HttpResponse response;
        if (cause) {
            response.SetCause(static_cast<std::error_code>(cause));
        }
        Finish(error, std::move(response));
    }

    /// <summary>
    /// Completes the operation with <c>operation_canceled</c> and an empty response.
    /// </summary>
    void Cancel() { Finish(std::make_error_code(std::errc::operation_canceled), {}); }

    /// <summary>
    /// Stops the timer and the resolver, closes the connection, and invokes the completion handler. Only the
    /// first call has an effect, so a failure that races the deadline is reported once.
    /// </summary>
    /// <param name="error">The outcome to report; empty on success.</param>
    /// <param name="response">The response to report; empty on failure.</param>
    void Finish(std::error_code error, HttpResponse response) {
        if (finished_) {
            return;
        }
        finished_ = true;
        m_timer.cancel();
        m_resolver.cancel();
        Close();
        auto completion = std::move(m_completion);
        Detail::Complete(std::move(completion), error, std::move(response));
    }

    /// <summary>
    /// Records that cancellation was asked for and posts the cancellation to the strand. Safe to call from
    /// any thread, and only the first call has an effect.
    /// </summary>
    void RequestCancellation() {
        if (m_cancellationRequested.exchange(true)) {
            return;
        }
        asio::post(m_executor, [self = this->shared_from_this()]() { self->Cancel(); });
    }

    /// <summary>
    /// Whether cancellation has been asked for. Checked at every step so a cancelled operation stops as
    /// soon as the strand next runs.
    /// </summary>
    /// <returns>True once cancellation has been requested.</returns>
    [[nodiscard]] bool IsCancellationRequested() const noexcept { return m_cancellationRequested.load(); }

    std::shared_ptr<asio::ssl::context> m_tlsContext;
    // The operation's strand, fixed at construction so that threads other than the strand (the
    // cancellation handler) can post to it without touching m_stream, which is replaced and moved on it.
    typename Stream::executor_type m_executor;
    std::unique_ptr<Stream> m_stream;
    Tcp::resolver m_resolver;
    asio::steady_timer m_timer;
    Detail::CompletionPointer m_completion;
    HttpRequestOptions m_options;
    std::shared_ptr<ConnectionPool<Stream>> m_pool;
    ConnectionKey<Stream> m_key;
    beast::flat_buffer m_buffer;
    http::request<http::buffer_body> m_request;
    // Backing storage for an owned (SetBody()) request body. buffer_body's writer only
    // stores a pointer/size pair, so something must own the bytes for the lifetime of the
    // write; for a SetBodyView() body the caller owns that storage instead (see the
    // lifetime contract on HttpRequest::SetBodyView()).
    std::string m_ownedBodyStorage;
    // Keeps a shared body view alive for as long as this operation can still read it.
    std::shared_ptr<const void> m_bodyKeepAlive;
    std::optional<http::response_parser<http::string_body>> m_parser;
    bool finished_ = false;
    bool m_reused = false;
    bool m_retried = false;
    bool m_receivedAnyResponse = false;
    bool m_hostIsName = false;
    std::atomic_bool m_cancellationRequested = false;
};

inline void ResolveWaiter::operator()(boost::system::error_code error, Tcp::resolver::results_type results) {
    std::visit([&](auto& operation) { operation->OnSharedResolve(error, std::move(results)); }, m_operation);
}
} // namespace AVEVA::Private
