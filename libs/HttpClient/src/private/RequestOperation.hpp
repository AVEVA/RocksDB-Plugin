// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "CancelState.hpp"
#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"
#include "DnsCache.hpp"
#include "Route.hpp"
#include "StreamTypes.hpp"

#include "AVEVA/HttpClient/Detail/CompletionOperation.hpp"
#include "AVEVA/HttpClient/HttpRequest.hpp"
#include "AVEVA/HttpClient/HttpRequestOptions.hpp"
#include "AVEVA/HttpClient/HttpResponse.hpp"

#include <boost/asio/execution/context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <expected>
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
    /// Constructs a waiter for one request. The waiter only observes the request, so a request that has already
    /// finished (for example by timing out while the lookup was still in flight) is not kept alive until the
    /// leader's lookup ends.
    /// </summary>
    /// <param name="operation">The request waiting for the lookup.</param>
    template <typename Stream>
    explicit ResolveWaiter(const std::shared_ptr<RequestOperation<Stream>>& operation) : m_operation(operation) {}

    /// <summary>
    /// Delivers the lookup's outcome to the request, if it still exists.
    /// </summary>
    /// <param name="error">The leader's outcome.</param>
    /// <param name="results">The endpoints the leader found, if any.</param>
    void operator()(boost::system::error_code error, ResolveResults results);

  private:
    std::variant<std::weak_ptr<RequestOperation<PlainStream>>, std::weak_ptr<RequestOperation<TlsStream>>> m_operation;
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
    /// <param name="deadline">When the request times out, measured from submission so time spent queued behind the
    /// per-origin cap counts against its timeout.</param>
    /// <param name="buffer">The read buffer carried over from a reused connection; empty otherwise.</param>
    RequestOperation(std::shared_ptr<asio::ssl::context> tlsContext, std::unique_ptr<Stream> stream,
                     Detail::CompletionPointer completion, HttpRequestOptions options,
                     std::shared_ptr<ConnectionPool<Stream>> pool, ConnectionKey<Stream> key,
                     std::chrono::steady_clock::time_point deadline, beast::flat_buffer buffer = {})
        : m_tlsContext(std::move(tlsContext)), m_executor(stream->get_executor()),
          m_lookupScope(static_cast<const void*>(&asio::query(m_executor, asio::execution::context))),
          m_stream(std::move(stream)), m_resolver(m_executor), m_timer(m_executor), m_completion(std::move(completion)),
          m_options(std::move(options)), m_pool(std::move(pool)), m_key(std::move(key)), m_buffer(std::move(buffer)),
          m_deadline(deadline) {}

    /// <summary>
    /// Makes this operation the target of the request's cancellation state, so a cancel (including one that
    /// already happened while the request was queued) reaches it. Never touches the caller's slot, which only
    /// the submitting thread may assign. Must be called before the operation is started.
    /// </summary>
    /// <param name="cancellation">The state shared with the caller's slot; null when the request has no
    /// cancellation slot.</param>
    void BindCancellation(const std::shared_ptr<CancelState>& cancellation);

    /// <summary>
    /// Sets where this operation reports events the caller would otherwise never see. Must be called before the
    /// operation is started; null (the default) reports nothing.
    /// </summary>
    void SetDiagnostics(std::shared_ptr<const DiagnosticsSink> diagnostics) { m_diagnostics = std::move(diagnostics); }

    /// <summary>
    /// Fresh connection: resolve, connect, (TLS handshake), then send. Must run on the operation's strand.
    /// </summary>
    /// <param name="request">The request to send; its owned body is moved out.</param>
    void Start(HttpRequest request, const Route& route);

    /// <summary>
    /// Reused pooled connection: already connected (and, for TLS, already handshaked), so this writes
    /// straight away. Must run on the operation's strand.
    /// </summary>
    /// <param name="request">The request to send; its owned body is moved out.</param>
    void StartReused(HttpRequest request, const Route& route);

  private:
    void Report(HttpDiagnosticKind kind, std::error_code error, std::string message) const noexcept {
        if (m_diagnostics) {
            DiagnosticsSink::Report(m_diagnostics, kind, m_phase, m_key.host + ':' + m_key.service, error,
                                    std::move(message));
        }
    }

    /// <summary>
    /// Translates the request into the Beast message to be sent, failing the operation if it cannot be.
    /// Anything that still throws (for example an oversized Host authority) must complete the request
    /// exactly once instead of escaping onto the host's io_context thread and dropping the completion
    /// handler.
    /// </summary>
    /// <param name="request">The request to translate; its owned body is moved out.</param>
    /// <returns>True if the operation may continue.</returns>
    bool BuildRequest(HttpRequest& request, const Route& route);

    /// <summary>
    /// Validates the URL, method, headers and options, and fills in the Beast request: target, Host,
    /// keep-alive, body and Content-Length. Fails the operation on anything invalid. May throw, which is
    /// why callers go through <see cref="BuildRequest"/>.
    /// </summary>
    /// <param name="request">The request to translate; its owned body is moved out.</param>
    /// <returns>True if the operation may continue.</returns>
    bool BuildRequestUnchecked(HttpRequest& request, const Route& route);

    /// <summary>
    /// Starts the deadline covering the whole operation, failing it with TimedOut when it elapses.
    /// </summary>
    void ArmTimer();

    /// <summary>
    /// Prepares the TLS stream for this origin: offers a resumable session, installs host name verification
    /// when verification is on, and sets SNI for a named host. Fails the operation if SNI could not be set.
    /// </summary>
    /// <returns>True if the operation may continue.</returns>
    bool ConfigureTlsForHost();

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
    bool MaybeRetryAfterReuseFailure(bool requestMayHaveBeenSent);

    /// <summary>
    /// Whether re-sending a method cannot have an effect beyond that of sending it once, and so may be
    /// retried after a failure that might have reached the server.
    /// </summary>
    /// <param name="method">The method to classify.</param>
    /// <returns>True if the method is idempotent.</returns>
    static bool IsIdempotentMethod(http::verb method) noexcept;

    /// <summary>
    /// Installs a fresh response parser carrying the request's limits, and skips the body for a HEAD
    /// response, which has none despite its Content-Length.
    /// </summary>
    void ResetParser();

  public:
    /// <summary>
    /// Receives the outcome of a lookup that another request led. Callable from any thread; the result is
    /// handled on this operation's strand.
    /// </summary>
    /// <param name="error">The leader's outcome.</param>
    /// <param name="results">The endpoints the leader found, if any.</param>
    void OnSharedResolve(boost::system::error_code error, ResolveResults results);

  private:
    /// <summary>
    /// Serves a recent result from the shared cache and collapses concurrent lookups for one origin into a
    /// single resolve. Every caller already runs on this operation's strand, so a hit is handled inline.
    /// Completions are always posted, so the caller's handler is still never re-entered.
    /// </summary>
    void Resolve();

    /// <summary>
    /// Identifies the io_context this request runs on. Lookups are only shared between requests on the same
    /// context: a leader whose context is stopped never finishes, and requests on other contexts must not wait
    /// for it.
    /// </summary>
    const void* LookupScope() const;

    /// <summary>
    /// Connects to the resolved endpoints, or fails the operation if the lookup did not succeed.
    /// </summary>
    /// <param name="error">The outcome of the lookup.</param>
    /// <param name="results">The endpoints to connect to.</param>
    void OnResolve(boost::system::error_code error, const Tcp::resolver::results_type& results);

    /// <summary>
    /// Performs the TLS handshake on a secure stream and then writes, or writes straight away on a plain
    /// one. Fails the operation if the connect or the handshake did not succeed.
    /// </summary>
    /// <param name="error">The outcome of the connect.</param>
    void OnConnect(boost::system::error_code error);

    /// <summary>
    /// Writes the request, then starts reading. A failure on a reused connection may be retried instead of
    /// reported.
    /// </summary>
    void Write();

    /// <summary>
    /// Reads one complete response message into the parser.
    /// </summary>
    void Read();

    /// <summary>
    /// Completes the operation with the response just read: skips informational (1xx) responses, classifies
    /// a read failure, and then either returns the connection to the pool or shuts it down before handing
    /// the response to the completion handler.
    /// </summary>
    /// <param name="error">The outcome of the read.</param>
    void OnRead(boost::system::error_code error);

    /// <summary>
    /// A TLS server may close TCP without close_notify. An attacker can also truncate an EOF-delimited body
    /// that way, so it is only accepted for bodyless statuses (204/304) or when the request opted in.
    /// </summary>
    /// <param name="error">The failed read's error.</param>
    /// <returns>An empty error code if the message turned out to be complete; otherwise the original error.</returns>
    boost::system::error_code AcceptTruncatedTlsBody(boost::system::error_code error);

    /// <summary>
    /// Maps a failed read to the error reported to the caller.
    /// </summary>
    /// <param name="error">The failed read's (non-empty) error.</param>
    /// <returns>ResponseTooLarge for limit errors, ProtocolError for a malformed response, otherwise
    /// ReadFailed.</returns>
    static HttpClientError ClassifyReadError(const boost::system::error_code& error);

    /// <summary>
    /// Completes a failed read, first retrying on a fresh connection when a reused one turned out to be dead.
    /// </summary>
    /// <param name="error">The failed read's (non-empty) error.</param>
    void FailRead(const boost::system::error_code& error);

    /// <summary>
    /// Moves the parsed message into an HttpResponse.
    /// </summary>
    /// <returns>The response, or the classified error if building it threw (not_enough_memory for bad_alloc).</returns>
    std::expected<HttpResponse, std::error_code> BuildResponse();

    /// <summary>
    /// Returns the connection to the pool when the response allows reuse, and otherwise shuts it down.
    /// </summary>
    /// <param name="keepAlive">Whether the connection may be reused.</param>
    void ReleaseOrClose(bool keepAlive);

    /// <summary>
    /// Finishes the operation successfully: stops the timer and resolver, disposes of the connection, and hands
    /// the response to the completion handler.
    /// </summary>
    /// <param name="response">The response to report.</param>
    /// <param name="keepAlive">Whether the connection may be reused.</param>
    void CompleteSuccess(HttpResponse response, bool keepAlive);

    /// <summary>
    /// Shuts down and closes the underlying socket, ignoring errors; a no-op once the stream has been
    /// released to the pool.
    /// </summary>
    void Close();

    /// <summary>
    /// Completes the operation with an error and an empty response.
    /// </summary>
    /// <param name="error">The error to complete with.</param>
    /// <param name="cause">The underlying transport error, kept on the response for diagnostics.</param>
    void Fail(HttpClientError error, boost::system::error_code cause = {});

    /// <summary>
    /// Completes the operation with <c>operation_canceled</c> and an empty response.
    /// </summary>
    void Cancel() { Finish(std::make_error_code(std::errc::operation_canceled), {}); }

    /// <summary>
    /// Called first in every asynchronous callback.
    /// </summary>
    /// <returns>True when the request is already finished or has just been cancelled, so the callback must
    /// do nothing further.</returns>
    bool AbortIfNeeded();

    /// <summary>
    /// Stops the timer and the resolver, closes the connection, and invokes the completion handler. Only the
    /// first call has an effect, so a failure that races the deadline is reported once.
    /// </summary>
    /// <param name="error">The outcome to report; empty on success.</param>
    /// <param name="response">The response to report; empty on failure.</param>
    void Finish(std::error_code error, HttpResponse response);

    /// <summary>
    /// Records that cancellation was asked for and posts the cancellation to the strand. Safe to call from
    /// any thread, and only the first call has an effect.
    /// </summary>
    void RequestCancellation();

    /// <summary>
    /// Whether cancellation has been asked for. Checked at every step so a cancelled operation stops as
    /// soon as the strand next runs.
    /// </summary>
    /// <returns>True once cancellation has been requested.</returns>
    [[nodiscard]] bool IsCancellationRequested() const noexcept { return m_cancellationRequested.load(); }

    /// <summary>
    /// Whether the submission-time deadline, if one was set, has already passed.
    /// </summary>
    [[nodiscard]] bool DeadlinePassed() const { return std::chrono::steady_clock::now() >= m_deadline; }

    std::shared_ptr<asio::ssl::context> m_tlsContext;
    // The operation's strand, fixed at construction so that threads other than the strand (the
    // cancellation handler) can post to it without touching m_stream, which is replaced and moved on it.
    typename Stream::executor_type m_executor;
    const void* m_lookupScope; // Identity of m_executor's execution context, fixed for the operation's lifetime.
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
    bool m_finished = false;
    bool m_reused = false;
    bool m_retried = false;
    bool m_receivedAnyResponse = false;
    bool m_hostIsName = false;
    bool m_resolvedFromCache = false;
    HttpRequestPhase m_phase = HttpRequestPhase::Queued; // Where a timeout is reported to have happened.
    std::shared_ptr<const DiagnosticsSink> m_diagnostics;
    std::atomic_bool m_cancellationRequested = false;
    std::chrono::steady_clock::time_point m_deadline; // Taken at submission, so queueing counts against the timeout.
};

} // namespace AVEVA::Private
