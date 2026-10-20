// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "CancelState.hpp"
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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
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
    static void OnStartLimited() noexcept {}
};

/// <summary>
/// Holds a completion until it is delivered, so initiation can still fail the request if it throws after
/// something took ownership of the handler. Whoever takes it first completes it; nobody completes it twice.
/// A single atomic exchange decides the winner, so no lock is needed.
/// </summary>
class CompletionCell {
  public:
    explicit CompletionCell(Detail::CompletionPointer completion) noexcept : m_completion(completion.release()) {}

    CompletionCell(const CompletionCell&) = delete;
    CompletionCell& operator=(const CompletionCell&) = delete;

    ~CompletionCell() { Take(); }

    Detail::CompletionPointer Take() noexcept { return Detail::CompletionPointer(m_completion.exchange(nullptr)); }

  private:
    std::atomic<Detail::CompletionOperation*> m_completion;
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
    std::shared_ptr<CancelState> cancellation;
    std::chrono::steady_clock::time_point deadline;
    std::shared_ptr<void> state; // The request's RequestState, kept so a failed start can return its slot.
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
    /// A request's handler and, once it has started under a per-origin cap, its slot. Whichever of the normal
    /// completion and a failure path runs first completes the handler; the slot is returned exactly once. One
    /// allocation per request serves both the unlimited and the limited path.
    /// </summary>
    struct RequestState {
        explicit RequestState(Detail::CompletionPointer completion) noexcept : cell(std::move(completion)) {}

        CompletionCell cell;
        std::shared_ptr<Limiter> limiter;
        std::string origin;
        std::atomic<bool> holdsSlot{false};

        void HoldSlot(std::shared_ptr<Limiter> limiterIn, std::string originIn) noexcept {
            limiter = std::move(limiterIn);
            origin = std::move(originIn);
            holdsSlot.store(true);
        }

        void ReleaseSlot() {
            if (holdsSlot.exchange(false)) {
                limiter->Release(origin);
            }
        }

        void Finish(std::error_code error, HttpResponse response) {
            // The slot goes back before the handler runs, so a slow or blocked handler cannot hold up queued
            // requests, and a throwing handler cannot hang the origin.
            auto inner = cell.Take();
            ReleaseSlot();
            if (inner) {
                Detail::Complete(std::move(inner), error, std::move(response));
            }
        }
    };

    /// <summary>
    /// Builds the TLS context and the pools and wires the pools to the sweeper. Throws if the requested
    /// TLS settings cannot be applied.
    /// </summary>
    /// <param name="runtime">The io_context every connection and timer runs on; must outlive the
    /// client.</param>
    /// <param name="options">The TLS, pooling, and concurrency settings to build from.</param>
    BasicHttpClientCore(asio::io_context& runtime, const HttpClientOptions& options)
        : m_context(runtime), m_tlsContext(std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client)),
          m_sweeper(IdleSweeper::Create(options.GetMaxIdleConnectionsPerHost(), options.GetIdleConnectionTimeout(),
                                        options.GetIdleProbeThreshold())),
          m_plainPool(m_sweeper->PlainPool()), m_tlsPool(m_sweeper->TlsPool()) {
        m_tlsContext->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                  asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                  asio::ssl::context::no_tlsv1_1);
        ConfigureTlsContext(*m_tlsContext, options);
        m_diagnostics = DiagnosticsSink::For(options);
        m_sweeper->SetDiagnostics(m_diagnostics);
        m_maxPerOrigin = options.GetMaxConnectionsPerHost();
    }

    /// <summary>
    /// Creates the limiter, which needs a weak reference to this core and so cannot be built in the
    /// constructor. Call once, right after make_shared.
    /// </summary>
    void Initialize() {
        if (m_maxPerOrigin > 0) {
            m_limiter = std::make_shared<Limiter>(m_maxPerOrigin, LimiterSink(this->weak_from_this()));
            m_limiter->EnableDeadlines(m_context, make_error_code(HttpClientError::TimedOut));
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

    ~BasicHttpClientCore() {
        Close();
        m_sweeper->Stop();
    }

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
        auto cell = std::make_shared<RequestState>(std::move(completion));
        try {
            FaultPolicy::OnSubmit();
            auto route = Route::Of(request, options);
            // A request that can never be sent fails without building a strand, stream or operation.
            if (!route.Sendable()) {
                return FailLater(cell, route.Parsed ? route.Error : make_error_code(HttpClientError::InvalidUrl));
            }
            auto guarded = Detail::MakeCompletion(
                [cell](std::error_code error, HttpResponse response) { cell->Finish(error, std::move(response)); });
            auto cancellation = BindCallerSlot(options);
            // The deadline covers time spent queued behind the per-origin cap, not just the network exchange.
            const auto deadline = DeadlineFor(options);
            if (m_limiter) {
                auto origin = route.Origin();
                return SendLimited(std::move(origin), std::move(route), std::move(request), std::move(guarded),
                                   std::move(options), std::move(cancellation), deadline, std::move(cell));
            }
            SendUnlimited(std::move(route), std::move(request), std::move(guarded), std::move(options),
                          std::move(cancellation), deadline);
        } catch (...) {
            FailLater(cell, ClassifyBuildFailure(std::current_exception()));
        }
    }

  private:
    void ReportHandlerThrew() const noexcept {
        DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {}, {},
                                "A completion handler threw while the client was failing a request");
    }

    /// <summary>
    /// Fails the request still parked in the cell, posted so the handler never runs inside the submitting
    /// call. The cell stays populated until the posted function runs, so if the post itself throws the handler
    /// is completed inline as a last resort rather than lost or thrown into the caller.
    /// </summary>
    /// <param name="cell">The cell holding the request's completion.</param>
    /// <param name="error">Why the request failed.</param>
    void FailLater(const std::shared_ptr<RequestState>& cell, std::error_code error) noexcept {
        try {
            asio::post(m_context, [cell, error] { cell->Finish(error, HttpResponse{}); });
            return;
        } catch (...) {
        }
        try {
            cell->Finish(error, HttpResponse{});
        } catch (...) {
            // The handler or response construction threw; only the diagnostics receiver remains to report it to.
            ReportHandlerThrew();
        }
    }

    static std::chrono::steady_clock::time_point DeadlineFor(const HttpRequestOptions& options) {
        // Clamp so huge timeouts (e.g. milliseconds::max()) cannot overflow the clock's time_point.
        constexpr auto MaxTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(EffectivelyInfinite);
        return std::chrono::steady_clock::now() + std::min(options.GetTimeout(), MaxTimeout);
    }

    /// <summary>
    /// Connects the caller's cancellation slot, once and on the submitting thread, to a shared state that
    /// later stages retarget. Asio slots are single-owner and not thread-safe to reassign, so no later stage
    /// may call <c>assign</c> on it.
    /// </summary>
    /// <param name="options">The request options carrying the caller's slot.</param>
    /// <returns>The state, or null if the caller has no connected slot.</returns>
    static std::shared_ptr<CancelState> BindCallerSlot(const HttpRequestOptions& options) {
        auto slot = options.GetCancellationSlot();
        if (!slot.is_connected()) {
            return nullptr;
        }
        auto state = std::make_shared<CancelState>();
        slot.assign([state](asio::cancellation_type type) {
            if (type != asio::cancellation_type::none) {
                state->Cancel();
            }
        });
        return state;
    }

    /// <summary>
    /// Queues the request behind the origin's cap and sends it once a slot is free, releasing that slot
    /// when it completes.
    ///
    /// Cancellation: the caller's slot was wired once in Submit to a CancelState. While the request waits in
    /// the limiter, the state's target removes it from the queue; when it starts, the operation retargets the
    /// state to cancel itself. Nothing here ever reassigns the slot.
    ///
    /// Timeout: the deadline is fixed at submission, so queue time counts. The limiter wakes a queued request
    /// when its deadline passes and fails it with TimedOut without connecting.
    /// </summary>
    /// <param name="origin">The origin whose cap the request counts against.</param>
    /// <param name="route">The request's parsed destination.</param>
    /// <param name="request">The request to send.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    void SendLimited(std::string origin, Route route, HttpRequest request, Detail::CompletionPointer completion,
                     HttpRequestOptions options, std::shared_ptr<CancelState> cancellation,
                     std::chrono::steady_clock::time_point deadline, std::shared_ptr<RequestState> state) {
        const auto ticket = m_limiter->NextTicket();
        // Set before queueing so a cancellation cannot slip in between; a started request retargets it.
        if (cancellation) {
            cancellation->SetTarget([weak = std::weak_ptr{m_limiter}, origin, ticket] {
                if (auto limiter = weak.lock()) {
                    limiter->Cancel(origin, ticket);
                }
            });
        }
        PendingRequest pending{std::move(origin),     std::move(route), std::move(request), std::move(options),
                               std::move(completion), cancellation,     deadline,           std::move(state)};
        // Run reads the origin from the request before it consumes it.
        m_limiter->Run(pending.origin, ticket, std::move(pending), deadline);
        // A cancel that fired before the request was queued found nothing to remove; deliver it again to
        // whichever target is current (the queue entry, or the operation if it has already started).
        if (cancellation && cancellation->IsCancelled()) {
            cancellation->Cancel();
        }
    }
    /// <summary>
    /// Sends a request the limiter has given a slot, and hands the slot back when it completes.
    /// </summary>
    /// <param name="pending">The request to send.</param>
    void StartLimited(PendingRequest&& pending) noexcept {
        // The state was allocated at submission, so starting cannot fail for lack of tracking memory: the slot
        // is handed to it first, and from here its Finish returns the slot whichever path completes the request.
        auto state = std::static_pointer_cast<RequestState>(std::move(pending.state));
        state->HoldSlot(m_limiter, std::move(pending.origin));
        try {
            FaultPolicy::OnStartLimited();
            SendUnlimited(std::move(pending.route), std::move(pending.request), std::move(pending.completion),
                          std::move(pending.options), std::move(pending.cancellation), pending.deadline);
        } catch (...) {
            // Whatever threw, the slot must come back and the handler must be completed exactly once. `state`
            // decides which of this path and the (possibly already queued) normal completion gets there first.
            FailLater(state, ClassifyBuildFailure(std::current_exception()));
        }
    }

    /// <summary>
    /// Fails a request that never got a slot. Posted because a completion must never run inside the call
    /// that triggered it.
    /// </summary>
    /// <param name="pending">The request to fail.</param>
    /// <param name="error">Why it was dropped.</param>
    void AbortLimited(PendingRequest&& pending, std::error_code error) {
        asio::post(m_context, [completion = std::move(pending.completion), error]() mutable {
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
    /// <param name="cancellation">The state the caller's cancellation slot is wired to; the operation makes itself
    /// its target. Null when the caller has no slot.</param>
    /// <param name="deadline">When the request times out, measured from submission.</param>
    void SendUnlimited(Route route, HttpRequest request, Detail::CompletionPointer completion,
                       HttpRequestOptions options, std::shared_ptr<CancelState> cancellation,
                       std::chrono::steady_clock::time_point deadline) {
        if (route.Tls) {
            SendOver<TlsStream>(std::move(route), m_tlsPool, std::move(request), std::move(completion),
                                std::move(options), std::move(cancellation), deadline);
        } else {
            SendOver<PlainStream>(std::move(route), m_plainPool, std::move(request), std::move(completion),
                                  std::move(options), std::move(cancellation), deadline);
        }
    }

    /// <summary>
    /// Sends over one kind of stream: a pooled connection when the request may take one, and otherwise a fresh
    /// connection on a new strand. The route is consumed; its host and port become the connection key.
    /// </summary>
    template <typename Stream>
    void SendOver(Route route, const std::shared_ptr<ConnectionPool<Stream>>& pool, HttpRequest request,
                  Detail::CompletionPointer completion, HttpRequestOptions options,
                  std::shared_ptr<CancelState> cancellation, std::chrono::steady_clock::time_point deadline) {
        // Submit has already failed every route that cannot be sent, so a pooled connection is never risked on a
        // request that validation would reject.
        assert(route.Sendable());
        ConnectionKey<Stream> key{std::move(route.Host), std::move(route.Port)};

        if (auto pooled = pool->Acquire(key)) {
            return ResumeWithPooledConnection<Stream>(std::move(route), std::move(*pooled), std::move(key),
                                                      std::move(request), std::move(completion), std::move(options),
                                                      pool, std::move(cancellation), deadline);
        }

        auto executor = asio::make_strand(m_context);
        auto stream = [&] {
            if constexpr (std::is_same_v<Stream, TlsStream>) {
                return std::make_unique<TlsStream>(executor, *m_tlsContext);
            } else {
                return std::make_unique<PlainStream>(executor);
            }
        }();
        auto operation = std::make_shared<RequestOperation<Stream>>(
            m_tlsContext, std::move(stream), std::move(completion), std::move(options), pool, std::move(key), deadline);
        operation->SetDiagnostics(m_diagnostics);
        Launch(operation, executor, cancellation, std::move(request), std::move(route), false);
    }

    /// <summary>
    /// Starts the request on an already-connected pooled stream, on the executor that stream was
    /// created with.
    /// </summary>
    /// <param name="route">The request's parsed destination; consumed.</param>
    /// <param name="pooled">The connection and read buffer taken from the pool.</param>
    /// <param name="key">The origin the connection belongs to.</param>
    /// <param name="request">The request to send.</param>
    /// <param name="completion">Invoked exactly once with the outcome.</param>
    /// <param name="options">The timeout, response limits, and cancellation slot for this request.</param>
    /// <param name="pool">The pool the connection is returned to if the response allows reuse.</param>
    /// <param name="cancellation">See <see cref="SendUnlimited"/>.</param>
    template <typename Stream>
    void ResumeWithPooledConnection(Route route, PooledConnection<Stream> pooled, ConnectionKey<Stream> key,
                                    HttpRequest request, Detail::CompletionPointer completion,
                                    HttpRequestOptions options, std::shared_ptr<ConnectionPool<Stream>> pool,
                                    std::shared_ptr<CancelState> cancellation,
                                    std::chrono::steady_clock::time_point deadline) {
        auto executor = pooled.stream->get_executor();
        auto operation = std::make_shared<RequestOperation<Stream>>(
            m_tlsContext, std::move(pooled.stream), std::move(completion), std::move(options), std::move(pool),
            std::move(key), deadline, std::move(pooled.buffer));
        operation->SetDiagnostics(m_diagnostics);
        Launch(operation, executor, cancellation, std::move(request), std::move(route), true);
    }

    /// <summary>
    /// Wires the operation to the caller's cancellation and starts it on its executor, as a fresh connection or on
    /// an already-connected pooled one.
    /// </summary>
    template <typename Stream, typename Executor>
    static void Launch(const std::shared_ptr<RequestOperation<Stream>>& operation, const Executor& executor,
                       const std::shared_ptr<CancelState>& cancellation, HttpRequest request, Route route,
                       bool reused) {
        operation->BindCancellation(cancellation);
        asio::post(executor, [operation, request = std::move(request), route = std::move(route), reused]() mutable {
            if (reused) {
                operation->StartReused(std::move(request), route);
            } else {
                operation->Start(std::move(request), route);
            }
        });
    }

    asio::io_context& m_context;
    std::shared_ptr<asio::ssl::context> m_tlsContext;
    std::shared_ptr<IdleSweeper> m_sweeper;
    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::size_t m_maxPerOrigin = 0;
    std::shared_ptr<const DiagnosticsSink> m_diagnostics;
    std::shared_ptr<Limiter> m_limiter; // Null when no per-origin cap is configured.
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
