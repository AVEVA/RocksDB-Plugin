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
    using namespace RetryDetail;
    // Cancellation is checked at every step below instead of surfacing as an exception from the next co_await.
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    const auto cancellation = co_await boost::asio::this_coro::cancellation_state;
    const auto cancelled = [&cancellation] { return IsAbortingCancellation(cancellation.cancelled()); };

    const auto totalDeadline = requestOptions.GetTotalDeadline();
    const auto start = std::chrono::steady_clock::now();
    const auto remaining = [&] {
        return *totalDeadline -
               std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    };
    const int maxAttempts = ComputeMaxAttempts(requestOptions.GetMaxRetries().value_or(retry.MaxRetries));
    const bool requestReplaySafe = replaySafe || IsReplaySafe(request);

    // The explicit slot of a caller's options is replaced by the coroutine's own cancellation.
    requestOptions.SetCancellationSlot({});

    AttemptFactory attempts(std::move(request));
    int attempt = 0;
    bool tokenRefreshed = false;
    bool replayAfterAmbiguous = false;
    bool countsAsAttempt = true;
    std::string attemptToken;

    const auto canRefreshToken = [&] {
        if constexpr (requires { credential->Invalidate(auth.TokenScopes); }) {
            return auth.Type == RequestAuth::Kind::Token && credential != nullptr && !tokenRefreshed;
        } else {
            return false;
        }
    };
    // Runs the caller's rebuild hook and the authorization of the attempt; a failure ends the request rather than
    // escaping.
    const auto prepare = [&](HttpRequest& attemptRequest, std::string_view bearerToken) -> std::error_code {
        try {
            ResolveCopySourceAuthorization(attemptRequest, bearerToken);
            if constexpr (!std::is_same_v<TRebuild, NoRebuild>) {
                if constexpr (std::invocable<TRebuild&, HttpRequest&, std::string_view, bool>) {
                    rebuild(attemptRequest, bearerToken, replayAfterAmbiguous);
                } else {
                    rebuild(attemptRequest, bearerToken);
                }
            }
            if (auth.Type == RequestAuth::Kind::SharedKey) {
                AuthorizeRequest(*auth.Signer, attemptRequest);
            } else if (auth.Type == RequestAuth::Kind::Token) {
                SetBearerAuthorization(attemptRequest, bearerToken);
            }
        } catch (...) {
            return ErrorFromCurrentException();
        }
        return {};
    };

    for (;;) {
        if (countsAsAttempt) {
            ++attempt;
        }
        countsAsAttempt = true;
        if (cancelled()) {
            co_return std::unexpected(CanceledError());
        }

        HttpRequest attemptRequest;
        try {
            attemptRequest = attempts.Build(attempt >= maxAttempts && !canRefreshToken());
        } catch (...) {
            co_return std::unexpected(ErrorFromCurrentException());
        }

        std::string_view bearerToken;
        if (auth.Type == RequestAuth::Kind::Token) {
            auto token = co_await GetTokenForScopes(*credential, auth.TokenScopes);
            if (cancelled()) {
                co_return std::unexpected(CanceledError());
            }
            if (!token) {
                co_return std::unexpected(token.error());
            }
            // m_attemptToken is what the 401 path invalidates.
            attemptToken = std::move(token->Token);
            bearerToken = attemptToken;
        }
        if (const auto prepared = prepare(attemptRequest, bearerToken)) {
            co_return std::unexpected(prepared);
        }

        AzureRequestOptions attemptOptions = requestOptions;
        if (totalDeadline) {
            const auto left = remaining();
            if (left <= std::chrono::milliseconds{0}) {
                // The deadline has passed: never start a fresh attempt.
                co_return std::unexpected(std::make_error_code(std::errc::timed_out));
            }
            attemptOptions.SetTimeout(std::min(attemptOptions.GetTimeout(), left));
        }

        auto sent = co_await httpClient.SendAsync(std::move(attemptRequest), std::move(attemptOptions));
        const std::error_code error = sent ? std::error_code{} : sent.error();
        HttpResponse response = sent ? std::move(*sent) : HttpResponse{};

        if (cancelled()) {
            co_return std::unexpected(CanceledError());
        }
        if (error && totalDeadline && remaining() <= std::chrono::milliseconds{0}) {
            co_return std::unexpected(std::make_error_code(std::errc::timed_out));
        }

        // A 401 on a token-authorized request usually means the cached token was revoked or rotated, so one extra
        // attempt with a freshly requested token is allowed regardless of the retry budget.
        constexpr unsigned int Unauthorized = 401;
        if (!error && response.GetStatus() == Unauthorized && canRefreshToken()) {
            if constexpr (requires { credential->Invalidate(auth.TokenScopes); }) {
                // Don't invalidate the cached token and fetch a new one for an attempt that can never be sent.
                if (totalDeadline && remaining() <= std::chrono::milliseconds{0}) {
                    co_return std::unexpected(std::make_error_code(std::errc::timed_out));
                }
                tokenRefreshed = true;
                if constexpr (requires { credential->Invalidate(auth.TokenScopes, std::string_view{}); }) {
                    credential->Invalidate(auth.TokenScopes, attemptToken);
                } else {
                    credential->Invalidate(auth.TokenScopes);
                }
                countsAsAttempt = false;
                continue;
            }
        }

        constexpr unsigned int NotFound = 404;
        constexpr unsigned int Gone = 410;
        const bool retriableNotFoundOrGone =
            retryNotFoundAndGone && !error && (response.GetStatus() == NotFound || response.GetStatus() == Gone);
        const auto finish = [&]() -> HttpResult {
            if (error) {
                return std::unexpected(error);
            }
            return std::move(response);
        };
        if (attempt >= maxAttempts || !(IsRetriableFailure(error, response) || retriableNotFoundOrGone)) {
            co_return finish();
        }

        // After an ambiguous transport failure the request may already have been applied, so replaying one that
        // cannot be repeated safely would misreport the outcome (e.g. 409 for a create that succeeded). Surface the
        // transport error as "outcome unknown" and let the caller decide.
        if (error && !requestReplaySafe && IsAmbiguousTransportError(error)) {
            co_return finish();
        }

        constexpr unsigned int RequestTimeout = 408;
        constexpr unsigned int ServerErrorFloor = 500;
        // Remembered for the rebuild hook: only a replay that follows an unknown outcome can see its own earlier
        // effect (a token-refresh repeat after a 401 cannot).
        if (error ? IsAmbiguousTransportError(error)
                  : (response.GetStatus() >= ServerErrorFloor || response.GetStatus() == RequestTimeout)) {
            replayAfterAmbiguous = true;
        }

        // A server hint may exceed MaxDelay (the service knows when it will recover) but is capped so a
        // bogus value cannot stall a request for hours.
        constexpr std::chrono::milliseconds maxHint{std::chrono::minutes{1}};
        std::chrono::milliseconds delay;
        if (const auto hint = ParseRetryAfter(response); hint.has_value()) {
            delay = std::min(*hint, std::max(retry.MaxDelay, maxHint));
        } else {
            delay = std::clamp(ComputeBackoff(retry, attempt), std::chrono::milliseconds{0},
                               std::max(retry.MaxDelay, std::chrono::milliseconds{0}));
        }
        if (totalDeadline && remaining() <= delay) {
            // The backoff alone would outlast the deadline, so another attempt can never be made.
            co_return std::unexpected(std::make_error_code(std::errc::timed_out));
        }

        typename RetryTimerFor<THttp>::type timer(httpClient.get_executor());
        timer.expires_after(delay);
        const auto [waitError] = co_await timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
        if (waitError || cancelled()) {
            co_return std::unexpected(CanceledError());
        }
    }
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
