// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "HttpClientCore.hpp"

#include "Exchange.hpp"
#include "RequestValidation.hpp"
#include "RetryDecision.hpp"
#include "TlsContextConfigurator.hpp"

#include <boost/asio/cancel_at.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <cstdint>
#include <gsl/util>
#include <optional>
#include <system_error>
#include <utility>

namespace AVEVA::Private {
namespace {
std::chrono::steady_clock::time_point DeadlineFor(const HttpRequestOptions& options) {
    // Timeout already caps itself well below what could overflow the clock's time_point.
    return std::chrono::steady_clock::now() + options.GetTimeout().Value();
}
} // namespace

HttpClientCore::HttpClientCore(asio::io_context& runtime, const HttpClientOptions& options)
    : m_context(runtime), m_diagnostics(DiagnosticsSink::For(options)),
      m_tlsContext(MakeTlsContext(options, m_diagnostics)),
      m_sweeper(IdleSweeper::Create(options.GetMaxIdleConnectionsPerHost(), options.GetIdleConnectionTimeout(),
                                    options.GetIdleProbeThreshold(), m_diagnostics)),
      m_plainPool(m_sweeper->PlainPool()), m_tlsPool(m_sweeper->TlsPool()) {}

HttpClientCore::~HttpClientCore() { Shutdown(); }

void HttpClientCore::Shutdown() noexcept { m_sweeper->Stop(); }

asio::awaitable<HttpResult> HttpClientCore::Send(HttpRequest request, HttpRequestOptions options) {
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
            outcome = co_await asio::co_spawn(asio::make_strand(co_await asio::this_coro::executor),
                                              RunWithDeadline(route, std::move(request), std::move(options), deadline),
                                              asio::use_awaitable);
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

asio::awaitable<HttpClientCore::Outcome>
HttpClientCore::RunWithDeadline(Route route, HttpRequest request, HttpRequestOptions options,
                                std::chrono::steady_clock::time_point deadline) {
    co_return co_await asio::co_spawn(co_await asio::this_coro::executor,
                                      Dispatch(std::move(route), std::move(request), std::move(options)),
                                      asio::cancel_at(deadline, asio::use_awaitable));
}

asio::awaitable<HttpClientCore::Outcome> HttpClientCore::Dispatch(Route route, HttpRequest request,
                                                                  HttpRequestOptions options) {
    HttpRequestPhase phase = HttpRequestPhase::Queued;
    const auto retry = options.GetRetryOptions();
    if (!retry.has_value()) {
        co_return co_await Attempt(std::move(route), std::move(request), std::move(options), phase);
    }

    const HttpMethod method = request.GetMethod();
    for (std::uint32_t retries = 0;; ++retries) {
        Outcome outcome = co_await Attempt(route, request, options, phase);
        const auto* const result = std::get_if<HttpResult>(&outcome);
        if (result == nullptr || retries >= retry->MaxRetries.Value()) {
            co_return outcome;
        }
        const HttpResponse* const response = *result ? &**result : nullptr;
        const bool again = response != nullptr ? ShouldRetryStatus(response->GetStatus(), method)
                                               : ShouldRetryFailure(result->error().Error(), phase, method);
        if (!again) {
            co_return outcome;
        }

        const auto delay = RetryDelay(*retry, gsl::narrow_cast<int>(retries + 1U), response);
        asio::steady_timer timer(co_await asio::this_coro::executor, delay);
        try {
            co_await timer.async_wait(asio::use_awaitable);
        } catch (const boost::system::system_error& aborted) {
            if (aborted.code() != asio::error::operation_aborted) {
                throw;
            }
            co_return Interrupted{HttpRequestPhase::Queued};
        }
    }
}

asio::awaitable<HttpClientCore::Outcome> HttpClientCore::Attempt(Route route, HttpRequest request,
                                                                 HttpRequestOptions options, HttpRequestPhase& phase) {
    phase = HttpRequestPhase::Queued;
    if (route.Tls == Transport::Tls) {
        Exchange<TlsStream> exchange({m_context, m_tlsContext, m_tlsPool, m_diagnostics}, std::move(route),
                                     std::move(request), std::move(options));
        co_return co_await RunExchange(exchange, phase);
    }
    Exchange<PlainStream> exchange({m_context, m_tlsContext, m_plainPool, m_diagnostics}, std::move(route),
                                   std::move(request), std::move(options));
    co_return co_await RunExchange(exchange, phase);
}

template <typename Stream>
asio::awaitable<HttpClientCore::Outcome> HttpClientCore::RunExchange(Exchange<Stream>& exchange,
                                                                     HttpRequestPhase& phase) {
    std::optional<Interrupted> interrupted;
    try {
        auto outcome = co_await exchange.Run();
        phase = exchange.Phase();
        co_return outcome;
    } catch (const boost::system::system_error& aborted) {
        if (aborted.code() != asio::error::operation_aborted) {
            throw;
        }
        interrupted = Interrupted{exchange.Phase()};
    }
    co_return *interrupted;
}
} // namespace AVEVA::Private
