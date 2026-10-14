// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "ConnectionPool.hpp"
#include "IdleSweeper.hpp"
#include "OriginLimiter.hpp"
#include "RequestOperation.hpp"
#include "RequestValidation.hpp"
#include "Route.hpp"
#include "StreamTypes.hpp"
#include "TlsContextConfigurator.hpp"

#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/url.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>

namespace AVEVA::Private {
/// <summary>
/// The production fault policy: injects nothing, and compiles away.
/// </summary>
struct NoFault {
    static void OnSubmit() noexcept {}
};

/// <summary>
/// Holds a completion until it is delivered, so initiation can still fail the request if it throws after
/// something took ownership of the handler. Whoever takes it first completes it; nobody completes it twice.
/// </summary>
class CompletionCell {
  public:
    explicit CompletionCell(Detail::CompletionPointer completion) : m_completion(std::move(completion)) {}

    Detail::CompletionPointer Take() {
        std::lock_guard<std::mutex> lock(m_mutex);
        return std::move(m_completion);
    }

  private:
    std::mutex m_mutex;
    Detail::CompletionPointer m_completion;
};

/// <summary>
/// A request waiting in the per-origin limiter for a slot.
/// </summary>
struct PendingRequest {
    std::string origin;
    Route route;
    HttpRequest request;
    HttpRequestOptions options;
    Detail::CompletionPointer completion;
    std::shared_ptr<std::atomic<bool>> cancelled;
};

/// <summary>
/// The concrete client: owns the TLS context, the plain and TLS connection pools, the idle sweeper, and
/// (when configured) the per-origin limiter, and turns each send into a RequestOperation on its own strand.
/// </summary>
/// <typeparam name="FaultPolicy">Provides <c>static void OnSubmit()</c>, called first thing in
/// <see cref="Submit"/>; tests use it to make initiation throw. Production uses <see cref="NoFault"/>.</typeparam>
template <typename FaultPolicy = NoFault>
class BasicHttpClientCore : public std::enable_shared_from_this<BasicHttpClientCore<FaultPolicy>> {
  public:
    /// <summary>
    /// Starts or aborts requests that passed through the limiter. Refers back to the core weakly, so a
    /// slot released after the client is destroyed fails the request instead of touching freed state.
    /// </summary>
    class LimiterSink {
      public:
        explicit LimiterSink(std::weak_ptr<BasicHttpClientCore> core) noexcept : m_core(std::move(core)) {}

        /// <summary>
        /// Sends a request that was given a slot, releasing the slot when it completes.
        /// </summary>
        /// <param name="pending">The request to send.</param>
        void Start(PendingRequest&& pending) const {
            if (auto core = m_core.lock()) {
                core->StartLimited(std::move(pending));
                return;
            }
            // The client is gone; there is nothing left to post to, so fail the request directly.
            Detail::Complete(std::move(pending.completion), std::make_error_code(std::errc::operation_canceled),
                             HttpResponse{});
        }

        /// <summary>
        /// Fails a request that never got a slot.
        /// </summary>
        /// <param name="pending">The request to fail.</param>
        /// <param name="error">Why it was dropped.</param>
        void Abort(PendingRequest&& pending, std::error_code error) const {
            if (auto core = m_core.lock()) {
                core->AbortLimited(std::move(pending), error);
                return;
            }
            Detail::Complete(std::move(pending.completion), error, HttpResponse{});
        }

      private:
        std::weak_ptr<BasicHttpClientCore> m_core;
    };

    using Limiter = OriginLimiter<PendingRequest, LimiterSink>;

    /// <summary>
    /// Builds the TLS context and the pools and wires the pools to the sweeper. Throws if the requested
    /// TLS settings cannot be applied.
    /// </summary>
    /// <param name="runtime">The io_context every connection and timer runs on; must outlive the
    /// client.</param>
    /// <param name="options">The TLS, pooling, and concurrency settings to build from.</param>
    BasicHttpClientCore(asio::io_context& runtime, const HttpClientOptions& options)
        : context_(runtime), m_tlsContext(std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client)),
          m_plainPool(std::make_shared<PlainConnectionPool>(options.GetMaxIdleConnectionsPerHost(),
                                                            options.GetIdleConnectionTimeout())),
          m_tlsPool(std::make_shared<TlsConnectionPool>(options.GetMaxIdleConnectionsPerHost(),
                                                        options.GetIdleConnectionTimeout())),
          m_sweeper(std::make_shared<IdleSweeper>(m_plainPool, m_tlsPool, options.GetIdleConnectionTimeout())) {
        m_sweeper->BindSelf(m_sweeper);
        m_plainPool->SetNotifier(SweeperNotifier(m_sweeper));
        m_tlsPool->SetNotifier(SweeperNotifier(m_sweeper));
        m_tlsContext->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                  asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                  asio::ssl::context::no_tlsv1_1);
        ConfigureTlsContext(*m_tlsContext, options);
        m_maxPerOrigin = options.GetMaxConnectionsPerHost();
    }

    /// <summary>
    /// Creates the limiter, which needs a weak reference to this core and so cannot be built in the
    /// constructor. Call once, right after make_shared.
    /// </summary>
    void Initialize() {
        if (m_maxPerOrigin > 0) {
            m_limiter = std::make_shared<Limiter>(m_maxPerOrigin, LimiterSink(this->weak_from_this()));
        }
    }

    /// <summary>
    /// Fails every request still waiting for a slot. Requests already running keep the state they need
    /// alive on their own.
    /// </summary>
    void Close() {
        if (m_limiter) {
            m_limiter->Close();
        }
    }

    ~BasicHttpClientCore() { Close(); }

    /// <summary>
    /// Sends a request, routing it through the per-origin limiter when one is configured.
    /// </summary>
    /// <param name="request">The request to send.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    /// <param name="completion">Completed exactly once with the outcome.</param>
    void Submit(HttpRequest request, HttpRequestOptions options, Detail::CompletionPointer completion) {
        // Initiation must never throw into the caller with the handler lost: the handler is parked in a cell
        // so that, if anything throws (in practice std::bad_alloc), it is failed exactly once and, as always,
        // never from inside this call.
        auto cell = std::make_shared<CompletionCell>(std::move(completion));
        try {
            FaultPolicy::OnSubmit();
            auto guarded = Detail::MakeCompletion([cell](std::error_code error, HttpResponse response) {
                if (auto pending = cell->Take()) {
                    Detail::Complete(std::move(pending), error, std::move(response));
                }
            });
            auto route = Route::Of(request, options);
            // Unparseable URLs bypass the cap; they fail fast and never open a connection.
            if (m_limiter && route.Parsed) {
                auto origin = route.Origin();
                return SendLimited(std::move(origin), std::move(route), std::move(request), std::move(guarded),
                                   std::move(options));
            }
            SendUnlimited(route, std::move(request), std::move(guarded), std::move(options));
        } catch (...) {
            if (auto pending = cell->Take()) {
                asio::post(context_, [pending = std::move(pending)]() mutable {
                    Detail::Complete(std::move(pending), make_error_code(HttpClientError::InvalidRequest),
                                     HttpResponse{});
                });
            }
        }
    }

  private:
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
    /// <param name="route">The request's parsed destination.</param>
    /// <param name="request">The request to send.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    void SendLimited(std::string origin, Route route, HttpRequest request, Detail::CompletionPointer completion,
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
        m_limiter->Run(origin, ticket,
                       PendingRequest{origin, std::move(route), std::move(request), std::move(options),
                                      std::move(completion), std::move(cancelled)});
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
        SendUnlimited(pending.route, std::move(pending.request), std::move(wrapped), std::move(pending.options),
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
    /// <param name="route">The request's parsed destination.</param>
    /// <param name="request">The request to send.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    /// <param name="cancelled">Set by the limiter's handler if the caller cancelled while the request was
    /// queued or being dequeued; the operation checks it once it owns the slot.</param>
    void SendUnlimited(const Route& route, HttpRequest request, Detail::CompletionPointer completion,
                       HttpRequestOptions options, std::shared_ptr<std::atomic<bool>> cancelled = nullptr) {
        // Only requests that pass the same validation the operation applies may take a pooled connection, so that
        // failing an invalid one cannot cost a healthy connection; invalid ones take the fresh-connection path.
        if (route.Sendable()) {
            if (route.Tls) {
                TlsConnectionKey key{route.Host, route.Port};
                if (auto pooled = m_tlsPool->Acquire(key)) {
                    return ResumeWithPooledConnection<TlsStream>(route, std::move(*pooled), std::move(key),
                                                                 std::move(request), std::move(completion),
                                                                 std::move(options), m_tlsPool, std::move(cancelled));
                }
            } else {
                BasicConnectionKey key{route.Host, route.Port};
                if (auto pooled = m_plainPool->Acquire(key)) {
                    return ResumeWithPooledConnection<PlainStream>(
                        route, std::move(*pooled), std::move(key), std::move(request), std::move(completion),
                        std::move(options), m_plainPool, std::move(cancelled));
                }
            }
        }

        auto executor = asio::make_strand(context_);
        if (route.Parsed && route.Tls) {
            TlsConnectionKey key{route.Host, route.Port};
            auto stream = std::make_unique<TlsStream>(executor, *m_tlsContext);
            auto operation = std::make_shared<RequestOperation<TlsStream>>(
                m_tlsContext, std::move(stream), std::move(completion), std::move(options), m_tlsPool, std::move(key));
            operation->BindCancellationSlot(cancelled.get());
            asio::post(executor, [operation, request = std::move(request), route]() mutable {
                operation->Start(std::move(request), route);
            });
            return;
        }

        // An unparseable URL has an empty route; the operation then fails it with InvalidUrl.
        BasicConnectionKey key{route.Host, route.Parsed ? route.Port : "80"};
        auto stream = std::make_unique<PlainStream>(executor);
        auto operation = std::make_shared<RequestOperation<PlainStream>>(
            m_tlsContext, std::move(stream), std::move(completion), std::move(options), m_plainPool, std::move(key));
        operation->BindCancellationSlot(cancelled.get());
        asio::post(executor, [operation, request = std::move(request), route]() mutable {
            operation->Start(std::move(request), route);
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
    /// <param name="cancelled">See <see cref="SendUnlimited"/>.</param>
    template <typename Stream>
    void ResumeWithPooledConnection(const Route& route, PooledConnection<Stream> pooled, ConnectionKey<Stream> key,
                                    HttpRequest request, Detail::CompletionPointer completion,
                                    HttpRequestOptions options, std::shared_ptr<ConnectionPool<Stream>> pool,
                                    std::shared_ptr<std::atomic<bool>> cancelled) {
        auto executor = pooled.stream->get_executor();
        auto operation = std::make_shared<RequestOperation<Stream>>(
            m_tlsContext, std::move(pooled.stream), std::move(completion), std::move(options), std::move(pool),
            std::move(key), std::move(pooled.buffer));
        operation->BindCancellationSlot(cancelled.get());
        asio::post(executor, [operation, request = std::move(request), route]() mutable {
            operation->StartReused(std::move(request), route);
        });
    }

    asio::io_context& context_;
    std::shared_ptr<asio::ssl::context> m_tlsContext;
    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::size_t m_maxPerOrigin = 0;
    std::shared_ptr<Limiter> m_limiter;     // Null when no per-origin cap is configured.
    std::shared_ptr<IdleSweeper> m_sweeper; // Last: stops scheduling sweeps before the pools it sweeps go away.
};
/// <summary>
/// The client's owner-facing handle. The real state lives in a shared Core so that limiter callbacks racing
/// with destruction find it expired (weak_ptr) rather than dangling.
/// </summary>
template <typename FaultPolicy = NoFault> class BasicHttpClientImpl {
  public:
    /// <param name="runtime">The io_context every connection and timer runs on; must outlive the
    /// client.</param>
    /// <param name="options">The TLS, pooling, and concurrency settings to build from.</param>
    BasicHttpClientImpl(asio::io_context& runtime, const HttpClientOptions& options)
        : m_core(std::make_shared<BasicHttpClientCore<FaultPolicy>>(runtime, options)) {
        m_core->Initialize();
    }

    BasicHttpClientImpl(const BasicHttpClientImpl&) = delete;
    BasicHttpClientImpl& operator=(const BasicHttpClientImpl&) = delete;

    /// <summary>
    /// Fails every request still waiting for a slot; requests already running keep the state they need
    /// alive on their own.
    /// </summary>
    ~BasicHttpClientImpl() { m_core->Close(); }

    /// <summary>
    /// Sends a request, routing it through the per-origin limiter when one is configured.
    /// </summary>
    void Submit(HttpRequest request, HttpRequestOptions options, Detail::CompletionPointer completion) {
        m_core->Submit(std::move(request), std::move(options), std::move(completion));
    }

  private:
    std::shared_ptr<BasicHttpClientCore<FaultPolicy>> m_core;
};
} // namespace AVEVA::Private
