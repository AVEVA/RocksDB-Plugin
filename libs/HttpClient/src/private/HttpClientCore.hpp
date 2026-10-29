// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"
#include "Exchange.hpp"
#include "IdleSweeper.hpp"
#include "OriginGate.hpp"
#include "RequestValidation.hpp"
#include "Route.hpp"
#include "StreamTypes.hpp"
#include "Timeouts.hpp"
#include "TlsContextConfigurator.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <variant>

namespace AVEVA::Private {
/// <summary>
/// The state a client shares with its requests: the TLS context, the connection pools and their idle sweeper, and
/// the optional per-origin cap. Every in-flight request holds a reference, so the client object can be destroyed
/// while requests are still running.
/// </summary>
class HttpClientCore : public std::enable_shared_from_this<HttpClientCore> {
  public:
    /// <summary>
    /// Builds the TLS context and the pools. Throws if the requested TLS settings cannot be applied.
    /// </summary>
    /// <param name="runtime">The io_context every connection and timer runs on; must outlive the client.</param>
    /// <param name="options">The TLS, pooling, and concurrency settings to build from.</param>
    HttpClientCore(asio::io_context& runtime, const HttpClientOptions& options)
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
        if (options.GetMaxConnectionsPerHost() > 0) {
            m_gate = std::make_shared<OriginGate>(runtime.get_executor(), options.GetMaxConnectionsPerHost());
        }
    }

    HttpClientCore(const HttpClientCore&) = delete;
    HttpClientCore& operator=(const HttpClientCore&) = delete;
    ~HttpClientCore() { Shutdown(); }

    /// <summary>
    /// Fails requests still waiting for a slot and disposes idle connections now, while the caller's io_context is
    /// still alive. Requests already running finish on their own. Safe to repeat.
    /// </summary>
    void Shutdown() noexcept {
        if (m_gate) {
            m_gate->Close();
        }
        m_sweeper->Stop();
    }

    const std::shared_ptr<const DiagnosticsSink>& Diagnostics() const noexcept { return m_diagnostics; }

    /// <summary>
    /// Sends a request. The deadline covers everything, including the wait for a per-origin slot, and cancelling the
    /// awaiting coroutine cancels the request. Nothing is thrown: a failure to even start the request is reported as
    /// a classified error code.
    /// </summary>
    asio::awaitable<HttpResult> Send(HttpRequest request, HttpRequestOptions options) {
        using namespace asio::experimental::awaitable_operators;
        co_await asio::this_coro::throw_if_cancelled(false);
        const auto cancellation = co_await asio::this_coro::cancellation_state;

        std::error_code failure;
        try {
            const Route route = Route::Of(request, options);
            if (!route.Sendable()) {
                co_return std::unexpected(route.Parsed ? route.Error : make_error_code(HttpClientError::InvalidUrl));
            }

            if (cancellation.cancelled() != asio::cancellation_type::none) {
                co_return std::unexpected(std::make_error_code(std::errc::operation_canceled));
            }
            HttpRequestPhase phase = HttpRequestPhase::Queued;
            const auto deadline = DeadlineFor(options);
            auto outcome =
                co_await (Dispatch(route, std::move(request), std::move(options), phase) || Expire(deadline));
            const bool cancelled = cancellation.cancelled() != asio::cancellation_type::none;
            if (outcome.index() == 0) {
                auto& result = std::get<0>(outcome);
                if (result || !cancelled) {
                    co_return std::move(result);
                }
            }
            if (cancelled) {
                co_return std::unexpected(std::make_error_code(std::errc::operation_canceled));
            }
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::Timeout, phase, route.Host, route.Port, {},
                                    "The request timed out");
            co_return std::unexpected(make_error_code(HttpClientError::TimedOut));
        } catch (...) {
            failure = ClassifyBuildFailure(std::current_exception());
        }
        co_return std::unexpected(failure);
    }

  private:
    static std::chrono::steady_clock::time_point DeadlineFor(const HttpRequestOptions& options) {
        // Clamp so huge timeouts (e.g. milliseconds::max()) cannot overflow the clock's time_point.
        constexpr auto MaxTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(EffectivelyInfinite);
        return std::chrono::steady_clock::now() + std::min(options.GetTimeout(), MaxTimeout);
    }

    /// <summary>
    /// Completes when the deadline passes, or is cancelled.
    /// </summary>
    static asio::awaitable<void> Expire(std::chrono::steady_clock::time_point deadline) {
        co_await asio::this_coro::throw_if_cancelled(false);
        asio::steady_timer timer(co_await asio::this_coro::executor, deadline);
        co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
    }

    /// <summary>
    /// Waits for a per-origin slot when a cap is configured, then runs the exchange over the stream the route calls
    /// for. The slot is returned as the coroutine ends, before the caller sees the result.
    /// </summary>
    asio::awaitable<HttpResult> Dispatch(const Route& route, HttpRequest request, HttpRequestOptions options,
                                         HttpRequestPhase& phase) {
        co_await asio::this_coro::throw_if_cancelled(false);
        OriginGate::Slot slot;
        if (m_gate) {
            auto acquired = co_await m_gate->Acquire(route.Origin());
            if (!acquired) {
                co_return std::unexpected(acquired.error());
            }
            slot = std::move(*acquired);
        }
        if (route.Tls) {
            Exchange<TlsStream> exchange({m_context, m_tlsContext, m_tlsPool, m_diagnostics}, route, std::move(request),
                                         std::move(options), phase);
            co_return co_await exchange.Run();
        }
        Exchange<PlainStream> exchange({m_context, m_tlsContext, m_plainPool, m_diagnostics}, route, std::move(request),
                                       std::move(options), phase);
        co_return co_await exchange.Run();
    }

    asio::io_context& m_context;
    std::shared_ptr<asio::ssl::context> m_tlsContext;
    std::shared_ptr<IdleSweeper> m_sweeper;
    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::shared_ptr<const DiagnosticsSink> m_diagnostics;
    std::shared_ptr<OriginGate> m_gate; // Null when no per-origin cap is configured.
};
} // namespace AVEVA::Private
