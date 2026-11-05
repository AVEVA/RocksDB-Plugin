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

#include "TlsContextConfigurator.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_at.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <optional>
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
        : m_context(runtime), m_diagnostics(DiagnosticsSink::For(options)),
          m_tlsContext(MakeTlsContext(options, m_diagnostics)),
          m_sweeper(IdleSweeper::Create(options.GetMaxIdleConnectionsPerHost(), options.GetIdleConnectionTimeout(),
                                        options.GetIdleProbeThreshold(), m_diagnostics)),
          m_plainPool(m_sweeper->PlainPool()), m_tlsPool(m_sweeper->TlsPool()) {
        if (options.GetMaxConnectionsPerHost() != ConnectionCount{}) {
            m_gate =
                std::make_shared<OriginGate>(runtime.get_executor(), options.GetMaxConnectionsPerHost(), m_diagnostics);
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

    [[nodiscard]] std::shared_ptr<const DiagnosticsSink> Diagnostics() const noexcept { return m_diagnostics; }

    /// <summary>
    /// Sends a request. The deadline covers everything, including the wait for a per-origin slot, and cancelling the
    /// awaiting coroutine cancels the request. Nothing is thrown: a failure to even start the request is reported as
    /// a classified error code.
    /// </summary>
    asio::awaitable<HttpResult> Send(HttpRequest request, HttpRequestOptions options) {
        std::error_code failure;
        try {
            Route route = Route::Of(request);
            if (route.Sendable() == Sendability::Unsendable) {
                co_return std::unexpected(HttpFailure{
                    route.Parsed == UrlParse::Parsed ? route.Error : make_error_code(HttpClientError::InvalidUrl)});
            }

            const auto deadline = DeadlineFor(options);
            Outcome outcome = Interrupted{HttpRequestPhase::Queued};
            try {
                // The strand serializes the deadline timer with the request's completion, which cancel_at requires.
                outcome = co_await asio::co_spawn(
                    asio::make_strand(co_await asio::this_coro::executor),
                    RunWithDeadline(route, std::move(request), std::move(options), deadline), asio::use_awaitable);
            } catch (const boost::system::system_error& aborted) {
                // Cancelled before the request got anywhere: it is still queued.
                if (aborted.code() != asio::error::operation_aborted) {
                    throw;
                }
            }
            if (auto* const result = std::get_if<HttpResult>(&outcome)) {
                co_return std::move(*result);
            }
            // Either the caller cancelled or the deadline fired; a deadline abort can only happen once it has passed.
            if (std::chrono::steady_clock::now() < deadline) {
                co_return std::unexpected(HttpFailure{std::make_error_code(std::errc::operation_canceled)});
            }
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::Timeout, std::get<Interrupted>(outcome).phase,
                                    route.Host, route.Port, {}, "The request timed out");
            co_return std::unexpected(HttpFailure{make_error_code(HttpClientError::TimedOut)});
        } catch (...) {
            failure = ClassifyBuildFailure(std::current_exception());
        }
        co_return std::unexpected(HttpFailure{failure});
    }

  private:
    /// <summary>The request was aborted, by the deadline or the caller, while in <c>phase</c>.</summary>
    struct Interrupted {
        HttpRequestPhase phase;
    };
    using Outcome = std::variant<HttpResult, Interrupted>;

    static std::chrono::steady_clock::time_point DeadlineFor(const HttpRequestOptions& options) {
        // Timeout already caps itself well below what could overflow the clock's time_point.
        return std::chrono::steady_clock::now() + options.GetTimeout().Value();
    }

    /// <summary>
    /// Runs the request with the deadline attached. Throws operation_aborted when the deadline passes or the
    /// awaiting coroutine is cancelled before the request has started.
    /// </summary>
    asio::awaitable<Outcome> RunWithDeadline(Route route, HttpRequest request, HttpRequestOptions options,
                                             std::chrono::steady_clock::time_point deadline) {
        co_return co_await asio::co_spawn(co_await asio::this_coro::executor,
                                          Dispatch(std::move(route), std::move(request), std::move(options)),
                                          asio::cancel_at(deadline, asio::use_awaitable));
    }

    /// <summary>
    /// Waits for a per-origin slot when a cap is configured, then runs the exchange over the stream the route calls
    /// for. The slot is returned as the coroutine ends, before the caller sees the result. An abort is returned as
    /// the phase it interrupted, so the caller can report where a timeout hit.
    /// </summary>
    asio::awaitable<Outcome> Dispatch(Route route, HttpRequest request, HttpRequestOptions options) {
        OriginGate::Slot slot;
        if (m_gate) {
            auto acquired = co_await AcquireSlot(route.Origin());
            if (!acquired) {
                co_return std::move(acquired.error());
            }
            slot = std::move(*acquired);
        }
        if (route.Tls == Transport::Tls) {
            Exchange<TlsStream> exchange({m_context, m_tlsContext, m_tlsPool, m_diagnostics}, std::move(route),
                                         std::move(request), std::move(options));
            co_return co_await RunExchange(exchange);
        }
        Exchange<PlainStream> exchange({m_context, m_tlsContext, m_plainPool, m_diagnostics}, std::move(route),
                                       std::move(request), std::move(options));
        co_return co_await RunExchange(exchange);
    }

    /// <summary>
    /// Waits for a per-origin slot. A closed gate fails the request; an abort while queued is returned as such.
    /// </summary>
    asio::awaitable<std::expected<OriginGate::Slot, Outcome>> AcquireSlot(std::string origin) {
        std::optional<Outcome> failure;
        try {
            auto acquired = co_await m_gate->Acquire(std::move(origin));
            if (acquired) {
                co_return std::move(*acquired);
            }
            failure.emplace(HttpResult{std::unexpect, HttpFailure{acquired.error()}});
        } catch (const boost::system::system_error& aborted) {
            if (aborted.code() != asio::error::operation_aborted) {
                throw;
            }
            failure.emplace(Interrupted{HttpRequestPhase::Queued});
        }
        co_return std::unexpected(std::move(*failure));
    }

    /// <summary>
    /// Runs an exchange, turning the operation_aborted its cancellation throws into the phase it interrupted.
    /// </summary>
    template <typename Stream> static asio::awaitable<Outcome> RunExchange(Exchange<Stream>& exchange) {
        std::optional<Interrupted> interrupted;
        try {
            co_return co_await exchange.Run();
        } catch (const boost::system::system_error& aborted) {
            if (aborted.code() != asio::error::operation_aborted) {
                throw;
            }
            interrupted = Interrupted{exchange.Phase()};
        }
        co_return *interrupted;
    }

    asio::io_context& m_context;
    std::shared_ptr<const DiagnosticsSink> m_diagnostics; // Before everything built with it.
    std::shared_ptr<asio::ssl::context> m_tlsContext;
    std::shared_ptr<IdleSweeper> m_sweeper;
    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::shared_ptr<OriginGate> m_gate; // Null when no per-origin cap is configured.
};
} // namespace AVEVA::Private
