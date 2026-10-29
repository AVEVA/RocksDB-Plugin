// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "AVEVA/AzureClient/HttpClientLike.hpp"
#include "AVEVA/AzureClient/TokenCredential.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/ProtocolConstants.hpp>

#include <AVEVA/HttpClient/HttpClientError.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <boost/system/error_code.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <chrono>
#include <cmath>
#include <cstdint>
#include <exception>
#include <expected>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient::Private {
// Only terminal and total cancellation abort an operation; a partial request has nothing safe to hand back
// half-done, so every other cancellation type is ignored.
[[nodiscard]] inline bool IsAbortingCancellation(boost::asio::cancellation_type_t type) noexcept {
    return (type & (boost::asio::cancellation_type::terminal | boost::asio::cancellation_type::total)) !=
           boost::asio::cancellation_type::none;
}

namespace RetryDetail {
// Backoff jitter and exponent bounds live in RetryPolicyConstants.
inline constexpr double MinBackoffJitter = RetryPolicyConstants::MinBackoffJitter;
inline constexpr double MaxBackoffJitter = RetryPolicyConstants::MaxBackoffJitter;
inline constexpr int MaxBackoffExponent = RetryPolicyConstants::MaxBackoffExponent;

// Exponential backoff (InitialDelay * 2^(retry-1)) with +/- jitter in [0.8, 1.3), capped at MaxDelay. The generator is
// a parameter so tests can make the jitter deterministic.
template <typename Generator>
[[nodiscard]] std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber,
                                                       Generator& generator) {
    std::uniform_real_distribution<double> jitter(MinBackoffJitter, MaxBackoffJitter);

    // Bound the delays in integers first: int64 max rounds up to 2^63 as a double, which llround cannot convert.
    constexpr auto maxBound = AzureRequestOptions::MaxTotalDeadline;
    const double maxDelay =
        static_cast<double>(std::clamp(retry.MaxDelay, std::chrono::milliseconds{0}, maxBound).count());
    const double initial =
        static_cast<double>(std::clamp(retry.InitialDelay, std::chrono::milliseconds{0}, maxBound).count());
    const double exponential = initial * std::ldexp(1.0, std::clamp(retryNumber - 1, 0, MaxBackoffExponent));
    const double delay = std::min(exponential * jitter(generator), maxDelay);
    return std::chrono::milliseconds{static_cast<std::int64_t>(std::llround(delay))};
}

[[nodiscard]] std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber);

// Resolves the x-ms-copy-source-authorization placeholder (see CopySourceClientCredentialMarker): an empty
// `bearerToken` means the client has no bearer token to forward, so the header is dropped before the request is
// signed or sent.
void ResolveCopySourceAuthorization(HttpRequest& request, std::string_view bearerToken);

[[nodiscard]] std::error_code CanceledError() noexcept;

// Replaces any Authorization header with the bearer token, so exactly one is ever sent.
void SetBearerAuthorization(HttpRequest& request, std::string_view token);

// A request that cannot be replayed safely after an ambiguous transport failure: a conditional create
// (If-None-Match: *) would answer 409 BlobAlreadyExists, a delete 404, and an ETag/unmodified-since conditional
// mutation 412 if the first attempt had in fact succeeded.
[[nodiscard]] bool IsReplaySafe(const HttpRequest& request);

// The timer that waits out a retry delay. A client type may name its own `retry_timer_type` so tests can observe
// the requested delay without really waiting; every real client uses a steady_timer. It must offer
// `expires_after` and an `async_wait` that accepts `as_tuple(use_awaitable)`.
template <class THttp> struct RetryTimerFor {
    using type = boost::asio::steady_timer;
};
template <class THttp>
    requires requires { typename THttp::retry_timer_type; }
struct RetryTimerFor<THttp> {
    using type = typename THttp::retry_timer_type;
};

// The error a failed preparation (request rebuild, signing) reports: an allocation failure, an error the failing
// code attached, or an invalid request otherwise.
[[nodiscard]] inline std::error_code ErrorFromCurrentException() noexcept {
    try {
        throw;
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    } catch (const std::system_error& e) {
        return e.code();
    } catch (...) {
        return std::make_error_code(std::errc::invalid_argument);
    }
}

// Builds the request of each attempt from the caller's request. The last possible attempt takes the request itself;
// earlier ones copy only the headers and share the body.
class AttemptFactory {
  public:
    explicit AttemptFactory(HttpRequest request) : m_request(std::move(request)) {}

    [[nodiscard]] HttpRequest Build(bool canMoveRequest) {
        HttpRequest attempt = canMoveRequest && !m_sharedBody ? std::move(m_request) : Copy();
        // Stamped right before signing, so a request that waited for a token or a backoff never carries a stale
        // date that the service would reject as clock skew.
        StampDate(attempt, BuildDateHeaderValue(std::chrono::system_clock::now()));
        return attempt;
    }

    [[nodiscard]] const HttpRequest& Original() const noexcept { return m_request; }

  private:
    [[nodiscard]] HttpRequest Copy() {
        HttpRequest attempt;
        attempt.SetMethod(m_request.GetMethod());
        attempt.SetUrl(m_request.GetUrl());
        attempt.SetHeaders(m_request.GetHeaders());
        if (m_request.HasBodyView()) {
            attempt.SetBodyView(m_request.GetBodyView(), m_request.GetBodyKeepAlive());
        } else {
            // An owned body is moved into shared storage once; every attempt then views it instead of
            // copying it, and the HTTP operation of a timed-out attempt keeps it alive until it is done.
            if (!m_sharedBody && !m_request.GetBody().empty()) {
                m_sharedBody = std::make_shared<const std::string>(m_request.ReleaseBody());
            }
            if (m_sharedBody) {
                attempt.SetBodyView(std::as_bytes(std::span{m_sharedBody->data(), m_sharedBody->size()}), m_sharedBody);
            }
        }
        return attempt;
    }

    // Every attempt copies the same header list, so the date header's position (or its absence) is learned once
    // and later attempts skip the rescan. A hit is re-validated, so a stale index only costs the scan.
    void StampDate(HttpRequest& request, const std::string& now) {
        auto& headers = request.GetHeaders();
        if (m_dateHeaderIndex < headers.size() && IEquals(headers[m_dateHeaderIndex].GetName(), XMsDateHeaderName)) {
            headers[m_dateHeaderIndex].SetValue(now);
            return;
        }
        if (!m_dateHeaderScanned) {
            m_dateHeaderScanned = true;
            const auto found = std::ranges::find_if(
                headers, [](const HttpHeader& header) { return IEquals(header.GetName(), XMsDateHeaderName); });
            if (found != headers.end()) {
                m_dateHeaderIndex = static_cast<std::size_t>(found - headers.begin());
                found->SetValue(now);
                return;
            }
        }
        request.AddHeader(HttpHeader{std::string{XMsDateHeaderName}, now});
    }

    HttpRequest m_request;
    std::shared_ptr<const std::string> m_sharedBody;
    std::size_t m_dateHeaderIndex = std::numeric_limits<std::size_t>::max();
    bool m_dateHeaderScanned = false;
};

[[nodiscard]] constexpr int ComputeMaxAttempts(int maxRetries) noexcept {
    return maxRetries >= std::numeric_limits<int>::max() - 1 ? std::numeric_limits<int>::max()
                                                             : std::max(0, maxRetries) + 1;
}

// One request's attempts, from the first send to the final result. Each step of the retry policy is a small member
// (authorize, decide, back off) so `Run` reads as the policy itself. Lives in the awaiting coroutine's frame.
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild> class RetryingSender {
  public:
    RetryingSender(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
                   HttpRequest request, AzureRequestOptions requestOptions, bool retryNotFoundAndGone, TRebuild rebuild,
                   bool replaySafe)
        : m_http(httpClient), m_auth(std::move(auth)), m_credential(std::move(credential)), m_retry(retry),
          m_requestOptions(std::move(requestOptions)), m_retryNotFoundAndGone(retryNotFoundAndGone),
          m_rebuild(std::move(rebuild)), m_totalDeadline(m_requestOptions.GetTotalDeadline()),
          m_start(std::chrono::steady_clock::now()),
          m_maxAttempts(ComputeMaxAttempts(m_requestOptions.GetMaxRetries().value_or(retry.MaxRetries))),
          m_replaySafe(replaySafe || IsReplaySafe(request)), m_attempts(std::move(request)) {}

    boost::asio::awaitable<HttpResult> Run() {
        // Cancellation is checked at every step below instead of surfacing as an exception from the next co_await.
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        m_cancellation = co_await boost::asio::this_coro::cancellation_state;

        for (;;) {
            if (m_countsAsAttempt) {
                ++m_attempt;
            }
            m_countsAsAttempt = true;
            if (Cancelled()) {
                co_return std::unexpected(CanceledError());
            }

            auto request = co_await PrepareAttempt();
            if (!request) {
                co_return std::unexpected(request.error());
            }
            auto options = AttemptOptions();
            if (!options) {
                co_return std::unexpected(options.error());
            }

            auto sent = co_await m_http.SendAsync(std::move(*request), std::move(*options));
            const std::error_code error = sent ? std::error_code{} : sent.error();
            HttpResponse response = sent ? std::move(*sent) : HttpResponse{};

            if (Cancelled()) {
                co_return std::unexpected(CanceledError());
            }
            if (error && DeadlinePassed()) {
                co_return std::unexpected(std::make_error_code(std::errc::timed_out));
            }

            switch (RefreshTokenAfter401(error, response)) {
            case Refresh::Retry:
                continue;
            case Refresh::DeadlinePassed:
                co_return std::unexpected(std::make_error_code(std::errc::timed_out));
            case Refresh::NotApplicable:
                break;
            }

            const auto delay = BackoffBeforeRetry(error, response);
            if (!delay) {
                if (error) {
                    co_return std::unexpected(error);
                }
                co_return std::move(response);
            }
            if (m_totalDeadline && Remaining() <= *delay) {
                // The backoff alone would outlast the deadline, so another attempt can never be made.
                co_return std::unexpected(std::make_error_code(std::errc::timed_out));
            }

            typename RetryTimerFor<THttp>::type timer(m_http.get_executor());
            timer.expires_after(*delay);
            const auto [waitError] = co_await timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
            if (waitError || Cancelled()) {
                co_return std::unexpected(CanceledError());
            }
        }
    }

  private:
    enum class Refresh { NotApplicable, Retry, DeadlinePassed };

    static constexpr bool CanInvalidateTokens =
        requires(TCred& credential, const RequestAuth& auth) { credential.Invalidate(auth.TokenScopes); };

    bool Cancelled() const { return IsAbortingCancellation(m_cancellation.cancelled()); }

    std::chrono::milliseconds Remaining() const {
        return *m_totalDeadline -
               std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - m_start);
    }

    bool DeadlinePassed() const { return m_totalDeadline && Remaining() <= std::chrono::milliseconds{0}; }

    bool CanRefreshToken() const {
        if constexpr (CanInvalidateTokens) {
            return m_auth.Type == RequestAuth::Kind::Token && m_credential != nullptr && !m_tokenRefreshed;
        } else {
            return false;
        }
    }

    // Builds the attempt's request and authorizes it. A failure to build or authorize ends the request rather than
    // escaping.
    boost::asio::awaitable<std::expected<HttpRequest, std::error_code>> PrepareAttempt() {
        HttpRequest request;
        try {
            request = m_attempts.Build(m_attempt >= m_maxAttempts && !CanRefreshToken());
        } catch (...) {
            co_return std::unexpected(ErrorFromCurrentException());
        }

        std::string_view bearerToken;
        if (m_auth.Type == RequestAuth::Kind::Token) {
            auto token = co_await GetTokenForScopes(*m_credential, m_auth.TokenScopes);
            if (Cancelled()) {
                co_return std::unexpected(CanceledError());
            }
            if (!token) {
                co_return std::unexpected(token.error());
            }
            // m_attemptToken is what the 401 path invalidates.
            m_attemptToken = std::move(token->Token);
            bearerToken = m_attemptToken;
        }

        try {
            ResolveCopySourceAuthorization(request, bearerToken);
            if constexpr (!std::is_same_v<TRebuild, NoRebuild>) {
                if constexpr (std::invocable<TRebuild&, HttpRequest&, std::string_view, bool>) {
                    m_rebuild(request, bearerToken, m_replayAfterAmbiguous);
                } else {
                    m_rebuild(request, bearerToken);
                }
            }
            if (m_auth.Type == RequestAuth::Kind::SharedKey) {
                AuthorizeRequest(*m_auth.Signer, request);
            } else if (m_auth.Type == RequestAuth::Kind::Token) {
                SetBearerAuthorization(request, bearerToken);
            }
        } catch (...) {
            co_return std::unexpected(ErrorFromCurrentException());
        }
        co_return request;
    }

    // The options for the next send: the caller's, with the timeout clamped to what is left of the total deadline.
    std::expected<AzureRequestOptions, std::error_code> AttemptOptions() const {
        AzureRequestOptions options = m_requestOptions;
        if (m_totalDeadline) {
            const auto left = Remaining();
            if (left <= std::chrono::milliseconds{0}) {
                // The deadline has passed: never start a fresh attempt.
                return std::unexpected(std::make_error_code(std::errc::timed_out));
            }
            options.SetTimeout(std::min(options.GetTimeout(), left));
        }
        return options;
    }

    // A 401 on a token-authorized request usually means the cached token was revoked or rotated, so one extra attempt
    // with a freshly requested token is allowed regardless of the retry budget.
    Refresh RefreshTokenAfter401(const std::error_code& error, const HttpResponse& response) {
        constexpr unsigned int Unauthorized = 401;
        if (error || response.GetStatus() != Unauthorized || !CanRefreshToken()) {
            return Refresh::NotApplicable;
        }
        if constexpr (CanInvalidateTokens) {
            // Don't invalidate the cached token and fetch a new one for an attempt that can never be sent.
            if (DeadlinePassed()) {
                return Refresh::DeadlinePassed;
            }
            m_tokenRefreshed = true;
            if constexpr (requires { m_credential->Invalidate(m_auth.TokenScopes, std::string_view{}); }) {
                m_credential->Invalidate(m_auth.TokenScopes, m_attemptToken);
            } else {
                m_credential->Invalidate(m_auth.TokenScopes);
            }
            m_countsAsAttempt = false;
            return Refresh::Retry;
        } else {
            return Refresh::NotApplicable;
        }
    }

    // How long to wait before the next attempt, or empty when this outcome is final (success, a failure that is not
    // worth retrying, or the budget is spent).
    std::optional<std::chrono::milliseconds> BackoffBeforeRetry(const std::error_code& error,
                                                                const HttpResponse& response) {
        constexpr unsigned int NotFound = 404;
        constexpr unsigned int Gone = 410;
        const bool retriableNotFoundOrGone =
            m_retryNotFoundAndGone && !error && (response.GetStatus() == NotFound || response.GetStatus() == Gone);
        if (m_attempt >= m_maxAttempts || !(IsRetriableFailure(error, response) || retriableNotFoundOrGone)) {
            return std::nullopt;
        }

        // After an ambiguous transport failure the request may already have been applied, so replaying one that
        // cannot be repeated safely would misreport the outcome (e.g. 409 for a create that succeeded). Surface the
        // transport error as "outcome unknown" and let the caller decide.
        if (error && !m_replaySafe && IsAmbiguousTransportError(error)) {
            return std::nullopt;
        }

        constexpr unsigned int RequestTimeout = 408;
        constexpr unsigned int ServerErrorFloor = 500;
        // Remembered for the rebuild hook: only a replay that follows an unknown outcome can see its own earlier
        // effect (a token-refresh repeat after a 401 cannot).
        if (error ? IsAmbiguousTransportError(error)
                  : (response.GetStatus() >= ServerErrorFloor || response.GetStatus() == RequestTimeout)) {
            m_replayAfterAmbiguous = true;
        }

        // A server hint may exceed MaxDelay (the service knows when it will recover) but is capped so a
        // bogus value cannot stall a request for hours.
        constexpr std::chrono::milliseconds maxHint{std::chrono::minutes{1}};
        if (const auto hint = ParseRetryAfter(response); hint.has_value()) {
            return std::min(*hint, std::max(m_retry.MaxDelay, maxHint));
        }
        return std::clamp(ComputeBackoff(m_retry, m_attempt), std::chrono::milliseconds{0},
                          std::max(m_retry.MaxDelay, std::chrono::milliseconds{0}));
    }

    THttp& m_http;
    RequestAuth m_auth;
    std::shared_ptr<TCred> m_credential;
    RetryOptions m_retry;
    AzureRequestOptions m_requestOptions;
    bool m_retryNotFoundAndGone;
    TRebuild m_rebuild;
    std::optional<std::chrono::milliseconds> m_totalDeadline;
    std::chrono::steady_clock::time_point m_start;
    int m_maxAttempts;
    bool m_replaySafe;
    AttemptFactory m_attempts;
    boost::asio::cancellation_state m_cancellation;
    int m_attempt = 0;
    bool m_countsAsAttempt = true;
    bool m_tokenRefreshed = false;
    bool m_replayAfterAmbiguous = false;
    std::string m_attemptToken;
};
} // namespace RetryDetail

// Sends `request` with per-attempt authorization and retry/backoff according to `retry`, and returns the final
// HTTP response, or the transport/pipeline error. All state is owned by the coroutine; only `httpClient` must
// outlive it. `rebuild` is called with (HttpRequest&, std::string_view bearerToken[, bool replayAfterAmbiguous])
// before each attempt is authorized and sent, so a retry never replays a stale body, signature or token; throwing
// fails the request. Cancelling the awaiting coroutine cancels the in-flight attempt or the pending backoff and
// returns std::errc::operation_canceled; an expired total deadline returns std::errc::timed_out.
// `retryNotFoundAndGone` additionally retries HTTP 404/410 (managed identity endpoints answer these while
// starting). `replaySafe` marks a request whose caller absorbs the outcome of a replay (the *IfExists/*IfNotExists
// operations), so it may be retried even after an ambiguous transport error.
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild>
boost::asio::awaitable<HttpResult> SendWithRetry(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential,
                                                 RetryOptions retry, HttpRequest request,
                                                 AzureRequestOptions requestOptions, bool retryNotFoundAndGone,
                                                 TRebuild rebuild, bool replaySafe) {
    RetryDetail::RetryingSender<THttp, TCred, TRebuild> sender(httpClient, std::move(auth), std::move(credential),
                                                               retry, std::move(request), std::move(requestOptions),
                                                               retryNotFoundAndGone, std::move(rebuild), replaySafe);
    co_return co_await sender.Run();
}

// Validates the shape of the caller's headers, then sends the request with retries. The result carries
// std::errc::invalid_argument for a header with control characters and BlobStorageErrorCode::InvalidMetadata for
// bad metadata, before anything reaches the transport.
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild>
boost::asio::awaitable<HttpResult>
SendAuthorizedRequest(THttp& httpClient, const ConnectionState& connection, std::shared_ptr<TCred> credential,
                      HttpRequest request, AzureRequestOptions requestOptions, TRebuild rebuild, bool replaySafe) {
    // Checked here rather than per operation so every request carrying metadata, including the
    // internal commits issued by multi-part uploads, is rejected before reaching the service.
    for (const HttpHeader& header : request.GetHeaders()) {
        const std::string_view name = header.GetName();
        if (HasHeaderControlCharacters(name) || HasHeaderControlCharacters(header.GetValue())) {
            co_return std::unexpected(std::make_error_code(std::errc::invalid_argument));
        }
        const bool isMetadata = IStartsWith(name, XMsMetaHeaderPrefix);
        const auto isNonAscii = [](char c) noexcept { return static_cast<unsigned char>(c) >= 0x80U; };
        if (isMetadata && (!IsValidMetadataName(name.substr(XMsMetaHeaderPrefix.size())) ||
                           std::ranges::any_of(header.GetValue(), isNonAscii))) {
            co_return std::unexpected(std::error_code{BlobStorageErrorCode::InvalidMetadata});
        }
    }
    const bool hasCredential = credential != nullptr;
    co_return co_await SendWithRetry(httpClient, MakeRequestAuth(connection, hasCredential), std::move(credential),
                                     connection.Retry, std::move(request), std::move(requestOptions), false,
                                     std::move(rebuild), replaySafe);
}
} // namespace AVEVA::AzureClient::Private
