// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/ConnectionPool.hpp"
#include "private/IdleSweeper.hpp"
#include "private/OriginLimiter.hpp"
#include "private/RequestOperation.hpp"
#include "private/RequestValidation.hpp"
#include "private/StreamTypes.hpp"
#include "private/TlsContextConfigurator.hpp"

#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/url.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace AVEVA {
namespace {
using Private::BasicConnectionKey;
using Private::ConnectionKey;
using Private::ConnectionPool;
using Private::PlainStream;
using Private::PooledConnection;
using Private::RequestOperation;
using Private::TlsConnectionKey;
using Private::TlsStream;
namespace asio = Private::asio;
namespace urls = Private::urls;

using Private::IdleSweeper;
using Private::PlainConnectionPool;
using Private::SweeperNotifier;
using Private::TlsConnectionPool;

/// <summary>
/// A request waiting in the per-origin limiter for a slot.
/// </summary>
struct PendingRequest {
    std::string origin;
    HttpRequest request;
    HttpRequestOptions options;
    Detail::CompletionPointer completion;
    std::shared_ptr<std::atomic<bool>> cancelled;
};
} // namespace

/// <summary>
/// The concrete client: owns the TLS context, the plain and TLS connection pools, the idle sweeper, and
/// (when configured) the per-origin limiter, and turns each send into a RequestOperation on its own strand.
/// </summary>
class HttpClient::Impl {
  public:
    /// <summary>
    /// Starts or aborts requests that passed through the limiter. Refers back to the owning Impl, which
    /// closes the limiter before it is destroyed, so no request can reach a dead Impl.
    /// </summary>
    class LimiterSink {
      public:
        explicit LimiterSink(Impl* impl) noexcept : m_impl(impl) {}

        /// <summary>
        /// Sends a request that was given a slot, releasing the slot when it completes.
        /// </summary>
        /// <param name="pending">The request to send.</param>
        void Start(PendingRequest&& pending) const { m_impl->StartLimited(std::move(pending)); }

        /// <summary>
        /// Fails a request that never got a slot.
        /// </summary>
        /// <param name="pending">The request to fail.</param>
        /// <param name="error">Why it was dropped.</param>
        void Abort(PendingRequest&& pending, std::error_code error) const {
            m_impl->AbortLimited(std::move(pending), error);
        }

      private:
        Impl* m_impl;
    };

    using Limiter = Private::OriginLimiter<PendingRequest, LimiterSink>;

    /// <summary>
    /// Builds the TLS context and the pools and wires the pools to the sweeper. Throws if the requested
    /// TLS settings cannot be applied.
    /// </summary>
    /// <param name="runtime">The io_context every connection and timer runs on; must outlive the
    /// client.</param>
    /// <param name="options">The TLS, pooling, and concurrency settings to build from.</param>
    Impl(asio::io_context& runtime, const HttpClientOptions& options)
        : context_(runtime), m_tlsContext(std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client)),
          m_plainPool(std::make_shared<PlainConnectionPool>(options.GetMaxIdleConnectionsPerHost(),
                                                            options.GetIdleConnectionTimeout())),
          m_tlsPool(std::make_shared<TlsConnectionPool>(options.GetMaxIdleConnectionsPerHost(),
                                                        options.GetIdleConnectionTimeout())),
          m_sweeper(std::make_shared<IdleSweeper>(m_plainPool, m_tlsPool, options.GetIdleConnectionTimeout())) {
        if (options.GetMaxConnectionsPerHost() > 0) {
            m_limiter = std::make_shared<Limiter>(options.GetMaxConnectionsPerHost(), LimiterSink(this));
        }
        m_sweeper->BindSelf(m_sweeper);
        m_plainPool->SetNotifier(SweeperNotifier(m_sweeper));
        m_tlsPool->SetNotifier(SweeperNotifier(m_sweeper));
        m_tlsContext->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                  asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                  asio::ssl::context::no_tlsv1_1);
        Private::ConfigureTlsContext(*m_tlsContext, options);
    }

    /// <summary>
    /// Fails every request still waiting for a slot. Requests already running keep the state they need
    /// alive on their own.
    /// </summary>
    ~Impl() {
        if (m_limiter) {
            m_limiter->Close();
        }
    }

    /// <summary>
    /// Sends a request, routing it through the per-origin limiter when one is configured.
    /// </summary>
    /// <param name="request">The request to send.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    /// <param name="completion">Completed exactly once with the outcome.</param>
    void Submit(HttpRequest request, HttpRequestOptions options, Detail::CompletionPointer completion) {
        if (const auto origin = m_limiter ? OriginOf(request.GetUrl()) : std::nullopt) {
            return SendLimited(*origin, std::move(request), std::move(completion), std::move(options));
        }
        SendUnlimited(std::move(request), std::move(completion), std::move(options));
    }

  private:
    /// <summary>
    /// Derives the "scheme://host:port" the limiter counts against, with the default port filled in.
    /// Unparseable URLs bypass the cap; they fail fast and never open a connection.
    /// </summary>
    /// <param name="url">The request URL to take the origin from.</param>
    /// <returns>The origin, or nullopt when the URL cannot be parsed.</returns>
    static std::optional<std::string> OriginOf(std::string_view url) {
        const auto parsed = urls::parse_uri(url);
        if (!parsed) {
            return std::nullopt;
        }
        const bool isTls = parsed->scheme_id() == urls::scheme::https;
        return std::string(parsed->scheme()) + "://" + std::string(parsed->host_address()) + ':' +
               (parsed->has_port() ? std::string(parsed->port()) : (isTls ? "443" : "80"));
    }

    /// <summary>
    /// Queues the request behind the origin's cap and sends it once a slot is free, releasing that slot
    /// when it completes.
    ///
    /// Cancellation-slot ownership: a caller's slot holds exactly one handler at a time, so ownership is
    /// handed over rather than shared. While a request waits in the limiter, the handler installed here
    /// removes it from the queue. When the request starts, RequestOperation::BindCancellationSlot replaces
    /// that handler with one that cancels the running operation. After that the limiter can no longer be
    /// reached through the slot, which is correct: a started request holds its slot until it completes.
    /// </summary>
    /// <param name="origin">The origin whose cap the request counts against.</param>
    /// <param name="request">The request to send.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    void SendLimited(std::string origin, HttpRequest request, Detail::CompletionPointer completion,
                     HttpRequestOptions options) {
        const auto ticket = m_limiter->NextTicket();
        auto cancelled = std::make_shared<std::atomic<bool>>(false);
        // Set before queueing so a cancellation cannot slip in between; a started request replaces it with
        // its own handler.
        if (auto slot = options.GetCancellationSlot(); slot.is_connected()) {
            slot.assign([weak = std::weak_ptr{m_limiter}, origin, ticket, cancelled](asio::cancellation_type type) {
                if (type != asio::cancellation_type::none) {
                    // Recorded first: a request dequeued but not yet bound to its own handler is invisible to
                    // Cancel(), so StartLimited replays this flag once the operation owns the slot.
                    cancelled->store(true);
                    if (auto limiter = weak.lock()) {
                        limiter->Cancel(origin, ticket);
                    }
                }
            });
        }
        m_limiter->Run(
            origin, ticket,
            PendingRequest{origin, std::move(request), std::move(options), std::move(completion), std::move(cancelled)});
    }

    /// <summary>
    /// Sends a request the limiter has given a slot, and hands the slot back when it completes.
    /// </summary>
    /// <param name="pending">The request to send.</param>
    void StartLimited(PendingRequest&& pending) {
        auto wrapped = Detail::MakeCompletion(
            [limiter = m_limiter, origin = std::move(pending.origin),
             inner = std::move(pending.completion)](std::error_code error, HttpResponse response) mutable {
                limiter->Release(origin);
                Detail::Complete(std::move(inner), error, std::move(response));
            });
        auto slot = pending.options.GetCancellationSlot();
        SendUnlimited(std::move(pending.request), std::move(wrapped), std::move(pending.options),
                      std::move(pending.cancelled));
    }

    /// <summary>
    /// Fails a request that never got a slot. Posted because a completion must never run inside the call
    /// that triggered it.
    /// </summary>
    /// <param name="pending">The request to fail.</param>
    /// <param name="error">Why it was dropped.</param>
    void AbortLimited(PendingRequest&& pending, std::error_code error) {
        asio::post(context_, [completion = std::move(pending.completion), error]() mutable {
            Detail::Complete(std::move(completion), error, HttpResponse{});
        });
    }

    /// <summary>
    /// Sends the request immediately, reusing a pooled connection to its origin when one is available
    /// and the request may be sent on one, and otherwise opening a fresh connection on a new strand.
    /// </summary>
    /// <param name="request">The request to send.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    /// <param name="cancelled">Set by the limiter's handler if the caller cancelled while the request was
    /// queued or being dequeued; the operation checks it once it owns the slot.</param>
    void SendUnlimited(HttpRequest request, Detail::CompletionPointer completion, HttpRequestOptions options,
                       std::shared_ptr<std::atomic<bool>> cancelled = nullptr) {
        // Invalid requests take the fresh-connection path, which fails them without closing a healthy
        // pooled connection.
        auto parsed = urls::parse_uri(request.GetUrl());
        if (parsed && IsPoolableRequest(request, options) && Private::IsValidRequestUrl(*parsed)) {
            const bool isTls = parsed->scheme_id() == urls::scheme::https;
            if (isTls) {
                TlsConnectionKey key{parsed->host_address(), parsed->has_port() ? std::string(parsed->port()) : "443"};
                if (auto pooled = m_tlsPool->Acquire(key)) {
                    return ResumeWithPooledConnection<TlsStream>(std::move(*pooled), std::move(key), std::move(request),
                                                                 std::move(completion), std::move(options), m_tlsPool,
                                                                 std::move(cancelled));
                }
            } else {
                BasicConnectionKey key{parsed->host_address(), parsed->has_port() ? std::string(parsed->port()) : "80"};
                if (auto pooled = m_plainPool->Acquire(key)) {
                    return ResumeWithPooledConnection<PlainStream>(std::move(*pooled), std::move(key),
                                                                   std::move(request), std::move(completion),
                                                                   std::move(options), m_plainPool,
                                                                   std::move(cancelled));
                }
            }
        }

        auto executor = asio::make_strand(context_);
        const auto& innerParsed = parsed;
        if (innerParsed && innerParsed->scheme_id() == urls::scheme::https) {
            TlsConnectionKey key{innerParsed->host_address(),
                                 innerParsed->has_port() ? std::string(innerParsed->port()) : "443"};
            auto stream = std::make_unique<TlsStream>(executor, *m_tlsContext);
            auto operation = std::make_shared<RequestOperation<TlsStream>>(
                m_tlsContext, std::move(stream), std::move(completion), std::move(options), m_tlsPool, std::move(key));
            operation->BindCancellationSlot(cancelled.get());
            asio::post(executor,
                       [operation, request = std::move(request)]() mutable { operation->Start(std::move(request)); });
            return;
        }

        BasicConnectionKey key{innerParsed ? innerParsed->host_address() : std::string{},
                               innerParsed && innerParsed->has_port() ? std::string(innerParsed->port()) : "80"};
        auto stream = std::make_unique<PlainStream>(executor);
        auto operation = std::make_shared<RequestOperation<PlainStream>>(
            m_tlsContext, std::move(stream), std::move(completion), std::move(options), m_plainPool, std::move(key));
        operation->BindCancellationSlot(cancelled.get());
        asio::post(executor,
                   [operation, request = std::move(request)]() mutable { operation->Start(std::move(request)); });
    }

  private:
    /// <summary>
    /// Whether the request is one that may be sent on a pooled connection. An invalid request is not,
    /// so that failing it cannot cost a healthy pooled connection.
    /// </summary>
    /// <param name="request">The request to classify.</param>
    /// <param name="options">The options the request would be sent with.</param>
    /// <returns>True if the request may use a pooled connection.</returns>
    static bool IsPoolableRequest(const HttpRequest& request, const HttpRequestOptions& options) {
        const HttpMethod method = request.GetMethod();
        if (Private::ToBeastVerb(method) == boost::beast::http::verb::unknown || method == HttpMethod::Connect ||
            (method == HttpMethod::Trace && request.GetBodySize() != 0) || options.GetTimeout().count() <= 0) {
            return false;
        }
        return std::all_of(request.GetHeaders().begin(), request.GetHeaders().end(), [](const HttpHeader& header) {
            return Private::IsValidHeader(header.GetName(), header.GetValue());
        });
    }

    /// <summary>
    /// Starts the request on an already-connected pooled stream, on the executor that stream was
    /// created with.
    /// </summary>
    /// <param name="pooled">The connection and read buffer taken from the pool.</param>
    /// <param name="key">The origin the connection belongs to.</param>
    /// <param name="request">The request to send.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    /// <param name="pool">The pool the connection is returned to if the response allows reuse.</param>
    template <typename Stream>
    void ResumeWithPooledConnection(PooledConnection<Stream> pooled, ConnectionKey<Stream> key, HttpRequest request,
                                    Detail::CompletionPointer completion, HttpRequestOptions options,
                                    std::shared_ptr<ConnectionPool<Stream>> pool,
                                    std::shared_ptr<std::atomic<bool>> cancelled) {
        auto executor = pooled.stream->get_executor();
        auto operation = std::make_shared<RequestOperation<Stream>>(
            m_tlsContext, std::move(pooled.stream), std::move(completion), std::move(options), std::move(pool),
            std::move(key), std::move(pooled.buffer));
        operation->BindCancellationSlot(cancelled.get());
        asio::post(executor,
                   [operation, request = std::move(request)]() mutable { operation->StartReused(std::move(request)); });
    }

    asio::io_context& context_;
    std::shared_ptr<asio::ssl::context> m_tlsContext;
    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::shared_ptr<Limiter> m_limiter;     // Null when no per-origin cap is configured.
    std::shared_ptr<IdleSweeper> m_sweeper; // Last: stops scheduling sweeps before the pools it sweeps go away.
};

HttpClient::HttpClient(boost::asio::io_context& context, HttpClientOptions options)
    : m_context(&context), m_impl(std::make_unique<Impl>(context, options)) {}

HttpClient::~HttpClient() = default;

std::unique_ptr<HttpClient> HttpClient::Create(boost::asio::io_context& context, HttpClientOptions options) {
    return std::make_unique<HttpClient>(context, std::move(options));
}

void HttpClient::Submit(HttpRequest request, HttpRequestOptions options, Detail::CompletionPointer completion) {
    m_impl->Submit(std::move(request), std::move(options), std::move(completion));
}
} // namespace AVEVA
