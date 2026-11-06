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
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <boost/system/error_code.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <boost/system/system_error.hpp>
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
#include <variant>

namespace AVEVA::AzureClient::Private {
namespace RetryDetail {
/// <summary>
/// Resolves the x-ms-copy-source-authorization placeholder (see CopySourceClientCredentialMarker): an empty
/// <paramref name="bearerToken"/> means the client has no bearer token to forward, so the header is dropped before
/// the request is signed or sent.
/// </summary>
/// <param name="request">The request whose placeholder header is resolved; unchanged when it has none.</param>
/// <param name="bearerToken">The token to forward, or empty when there is none.</param>
void ResolveCopySourceAuthorization(HttpRequest& request, std::string_view bearerToken);

/// <summary>
/// The result of a request that was cancelled before it finished.
/// </summary>
/// <returns>An error of <c>std::errc::operation_canceled</c>.</returns>
[[nodiscard]] std::error_code CanceledError() noexcept;

/// <summary>
/// Replaces any Authorization header with the bearer token, so exactly one is ever sent.
/// </summary>
/// <param name="request">The request to authorize.</param>
/// <param name="token">The bearer token.</param>
void SetBearerAuthorization(HttpRequest& request, std::string_view token);

/// <summary>
/// Classifies whether a request can be replayed safely after an ambiguous transport failure: a conditional create
/// (If-None-Match: *) would answer 409 BlobAlreadyExists, a delete 404, and an ETag/unmodified-since conditional
/// mutation 412 if the first attempt had in fact succeeded.
/// </summary>
/// <param name="request">The request to classify.</param>
/// <returns>Whether replaying the request is safe.</returns>
[[nodiscard]] ReplaySafety ReplaySafetyOf(const HttpRequest& request);

/// <summary>
/// The timer that waits out a retry delay. A client type may name its own <c>retry_timer_type</c> so tests can observe
/// the requested delay without really waiting; every real client uses a steady_timer. It must offer
/// <c>expires_after</c> and an <c>async_wait</c> that accepts <c>as_tuple(use_awaitable)</c>.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
template <class THttp> struct RetryTimerFor {
    /// <summary>The timer type.</summary>
    using type = boost::asio::steady_timer;
};

/// <summary>
/// The timer a client names through <c>retry_timer_type</c>.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
template <class THttp>
    requires requires { typename THttp::retry_timer_type; }
struct RetryTimerFor<THttp> {
    /// <summary>The timer type the client names.</summary>
    using type = typename THttp::retry_timer_type;
};

/// <summary>
/// The error a failed preparation (request rebuild, signing) reports: an allocation failure, an error the failing
/// code attached, or an invalid request otherwise. Must be called from a catch handler, because it rethrows the
/// current exception to classify it.
/// </summary>
/// <returns>The error code for the exception being handled.</returns>
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

/// <summary>
/// Builds the request of each attempt from the caller's request. The last possible attempt takes the request itself;
/// earlier ones copy only the headers and share the body.
/// </summary>
class AttemptFactory {
  public:
    /// <summary>
    /// Takes the caller's request as the source of every attempt.
    /// </summary>
    /// <param name="request">The caller's request.</param>
    explicit AttemptFactory(HttpRequest request) : m_request(std::move(request)) {}

    /// <summary>
    /// Whether an attempt must copy the request (<c>Copy</c>, leaving it for later attempts) or, as the last possible
    /// attempt, may take the request itself instead of a copy (<c>MayMove</c>).
    /// </summary>
    enum class Reuse : std::uint8_t { Copy, MayMove };

    /// <summary>
    /// Builds one attempt's request, stamped with the current date.
    /// </summary>
    /// <param name="reuse">Whether the request may be moved out instead of copied.</param>
    /// <returns>The attempt's request.</returns>
    [[nodiscard]] HttpRequest Build(Reuse reuse) {
        // Copying shares the body, so only the headers are duplicated.
        HttpRequest attempt = reuse == Reuse::MayMove ? std::move(m_request) : m_request;
        // Stamped right before signing, so a request that waited for a token or a backoff never carries a stale
        // date that the service would reject as clock skew.
        attempt.RemoveHeaders(HeaderName{std::string{XMsDateHeaderName}});
        attempt.AddHeader(HttpHeader(HeaderName{std::string{XMsDateHeaderName}},
                                     HeaderValue{BuildDateHeaderValue(std::chrono::system_clock::now())}));
        return attempt;
    }

    /// <summary>
    /// The caller's request, as far as it has not been moved into an attempt.
    /// </summary>
    /// <returns>The request the factory was built from.</returns>
    [[nodiscard]] const HttpRequest& Original() const noexcept { return m_request; }

  private:
    HttpRequest m_request;
};
/// <summary>
/// Turns a retry count into an attempt count, without overflowing.
/// </summary>
/// <param name="maxRetries">The retries after the first attempt.</param>
/// <returns>The number of attempts, saturated at the largest int.</returns>
[[nodiscard]] constexpr int ComputeMaxAttempts(RetryCount maxRetries) noexcept {
    constexpr auto Largest = static_cast<RetryCount::ValueType>(std::numeric_limits<int>::max());
    return maxRetries.Value() >= Largest ? std::numeric_limits<int>::max() : static_cast<int>(maxRetries.Value()) + 1;
}

/// <summary>
/// One request's attempts, from the first send to the final result. Each step of the retry policy is a small member
/// (authorize, decide, back off) so <c>Run</c> reads as the policy itself. Lives in the awaiting coroutine's frame.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The token credential type.</typeparam>
/// <typeparam name="TRebuild">The type of the hook that adjusts each attempt's request before it is
/// authorized.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild> class RetryingSender {
  public:
    /// <summary>
    /// Prepares the sender; nothing is sent until <c>Run</c>.
    /// </summary>
    /// <param name="httpClient">The client to send with; must outlive the sender.</param>
    /// <param name="auth">How each attempt is authorized.</param>
    /// <param name="credential">The token credential for token authorization; may be null.</param>
    /// <param name="retry">The client's retry policy.</param>
    /// <param name="request">The caller's request.</param>
    /// <param name="requestOptions">The per-request options;     they may override the retry policy or count and set a
    /// total deadline.</param>
    /// <param name="notFoundRetry">Whether HTTP 404 and 410 are retried as well.</param>
    /// <param name="rebuild">The hook called before each attempt is authorized.</param>
    /// <param name="replaySafety">Whether the caller absorbs the outcome of a replay; <c>Unsafe</c> defers to
    /// <c>ReplaySafetyOf</c> on the request.</param>
    RetryingSender(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
                   HttpRequest request, AzureRequestOptions requestOptions, NotFoundRetry notFoundRetry,
                   TRebuild rebuild, ReplaySafety replaySafety)
        : m_http(httpClient), m_auth(std::move(auth)), m_credential(std::move(credential)),
          m_retry(requestOptions.GetRetryOptions().value_or(retry)), m_requestOptions(std::move(requestOptions)),
          m_notFoundRetry(notFoundRetry), m_rebuild(std::move(rebuild)),
          m_totalDeadline(m_requestOptions.GetTotalDeadline()), m_start(std::chrono::steady_clock::now()),
          m_maxAttempts(ComputeMaxAttempts(m_requestOptions.GetMaxRetries().value_or(m_retry.MaxRetries))),
          m_replaySafety(replaySafety == ReplaySafety::Safe ? ReplaySafety::Safe : ReplaySafetyOf(request)),
          m_attempts(std::move(request)) {}

    /// <summary>
    /// Sends the request, retrying according to the policy.
    /// </summary>
    /// <returns>The final HTTP response, or the transport or pipeline error; <c>std::errc::operation_canceled</c>
    /// when the awaiting coroutine is cancelled.</returns>
    boost::asio::awaitable<HttpResult> Run() {
        // Cancelling the awaiting coroutine makes whichever await is pending throw operation_aborted; this is the
        // one place it becomes a result.
        try {
            co_return co_await Attempts();
        } catch (const boost::system::system_error& aborted) {
            if (aborted.code() != boost::asio::error::operation_aborted) {
                throw;
            }
        }
        co_return std::unexpected(CanceledError());
    }

  private:
    /// <summary>
    /// The attempt loop: prepare, send, then refresh the token, finish or back off.
    /// </summary>
    /// <returns>The final HTTP response, or the error that ended the attempts.</returns>
    boost::asio::awaitable<HttpResult> Attempts() {
        for (;;) {
            if (m_attemptAccounting == AttemptAccounting::Counted) {
                ++m_attempt;
            }
            m_attemptAccounting = AttemptAccounting::Counted;

            auto request = co_await PrepareAttempt();
            if (!request) {
                co_return std::unexpected(HttpFailure{request.error()});
            }
            auto options = AttemptOptions();
            if (!options) {
                co_return std::unexpected(HttpFailure{options.error()});
            }

            auto sent = co_await m_http.SendAsync(std::move(*request), std::move(*options));
            const std::error_code error = sent ? std::error_code{} : sent.error().Error();
            HttpResponse response = sent ? std::move(*sent) : HttpResponse{};

            if (error && DeadlineStatus() == DeadlineState::Expired) {
                co_return std::unexpected(HttpFailure{std::make_error_code(std::errc::timed_out)});
            }

            switch (RefreshTokenAfter401(error, response)) {
            case Refresh::Retry:
                continue;
            case Refresh::DeadlinePassed:
                co_return std::unexpected(HttpFailure{std::make_error_code(std::errc::timed_out)});
            case Refresh::NotApplicable:
                break;
            }

            const auto delay = BackoffBeforeRetry(error, response);
            if (!delay) {
                if (!sent) {
                    // Keeps the transport cause, which tells a certificate rejection from a refused connection.
                    co_return std::unexpected(sent.error());
                }
                co_return std::move(response);
            }
            if (m_totalDeadline && Remaining() <= *delay) {
                // The backoff alone would outlast the deadline, so another attempt can never be made.
                co_return std::unexpected(HttpFailure{std::make_error_code(std::errc::timed_out)});
            }

            typename RetryTimerFor<THttp>::type timer(m_http.get_executor());
            timer.expires_after(*delay);
            const auto [waitError] = co_await timer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
            if (waitError) {
                throw boost::system::system_error(boost::asio::error::operation_aborted);
            }
        }
    }

    /// <summary>
    /// What a 401 means for the next step: nothing (<c>NotApplicable</c>), one more attempt with a fresh token
    /// (<c>Retry</c>), or no further attempt because the total deadline has passed (<c>DeadlinePassed</c>).
    /// </summary>
    enum class Refresh { NotApplicable, Retry, DeadlinePassed };

    /// <summary>
    /// Whether the credential can drop a cached token, which a 401 refresh needs.
    /// </summary>
    /// <typeparam name="TCredential">The credential type to test.</typeparam>
    template <typename TCredential>
    static constexpr auto CanInvalidate =
        requires(TCredential& credential, const RequestAuth& auth) { credential.Invalidate(auth.TokenScopes); };

    /// <summary>
    /// The time left of the total deadline. Only meaningful when a total deadline is set.
    /// </summary>
    /// <returns>The remaining time; zero or negative once it has passed.</returns>
    std::chrono::milliseconds Remaining() const {
        return *m_totalDeadline -
               std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - m_start);
    }

    /// <summary>
    /// Whether the total deadline, if any, has passed.
    /// </summary>
    /// <returns>Expired once the deadline has passed; Open otherwise, and always when none is set.</returns>
    DeadlineState DeadlineStatus() const {
        return m_totalDeadline && Remaining() <= std::chrono::milliseconds{0} ? DeadlineState::Expired
                                                                              : DeadlineState::Open;
    }

    /// <summary>
    /// Whether a 401 may still trigger a token refresh: the request must be token-authorized, the credential able to
    /// invalidate its cache, and no refresh already made.
    /// </summary>
    /// <returns>Whether a refresh is allowed.</returns>
    RefreshEligibility TokenRefreshEligibility() const {
        if constexpr (CanInvalidate<TCred>) {
            return m_auth.Type == RequestAuth::Kind::Token && m_credential != nullptr &&
                           m_tokenRefresh == TokenRefreshState::NotRefreshed
                       ? RefreshEligibility::Eligible
                       : RefreshEligibility::Ineligible;
        } else {
            return RefreshEligibility::Ineligible;
        }
    }

    /// <summary>
    /// Waits for <paramref name="limit"/>, then reports it; abandoned (cancelled, so it throws operation_aborted) when
    /// the token arrives first.
    /// </summary>
    /// <param name="limit">How long to wait.</param>
    static boost::asio::awaitable<void> Expire(std::chrono::milliseconds limit) {
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor, limit);
        co_await timer.async_wait(boost::asio::use_awaitable);
    }

    /// <summary>Races the token request against <paramref name="limit"/>; see <see cref="AcquireToken"/>.</summary>
    boost::asio::awaitable<std::variant<TokenResult, std::monostate>>
    RaceTokenAgainstLimit(std::chrono::milliseconds limit) {
        using namespace boost::asio::experimental::awaitable_operators;
        co_return co_await (GetTokenForScopes(*m_credential, m_auth.TokenScopes) || Expire(limit));
    }

    /// <summary>
    /// A hung identity endpoint must not stall an attempt past its timeout or the total deadline, so the token
    /// request is raced against the smaller of the two. Losing cancels the credential, which cancellation-aware
    /// credentials report as operation_canceled and the token cache does not remember.
    /// </summary>
    /// <returns>The credential's result; <c>std::errc::timed_out</c> when the limit won the race.</returns>
    boost::asio::awaitable<TokenResult> AcquireToken() {
        auto limit = m_requestOptions.GetTimeout().Value();
        if (m_totalDeadline) {
            limit = std::min(limit, std::max(Remaining(), std::chrono::milliseconds{0}));
        }
        // `||` installs its cancellation handler on this coroutine's slot after starting both branches. On a
        // multithreaded io_context a branch can finish, and resume this coroutine, while that is still happening,
        // corrupting the slot (found with ThreadSanitizer; it later crashes in cancellation_slot::clear). Running the
        // race on a strand serializes its start with its completions.
        const auto strand = boost::asio::make_strand(co_await boost::asio::this_coro::executor);
        auto outcome = co_await boost::asio::co_spawn(strand, RaceTokenAgainstLimit(limit), boost::asio::use_awaitable);
        if (outcome.index() == 0) {
            co_return std::move(std::get<0>(outcome));
        }
        co_return std::unexpected(std::make_error_code(std::errc::timed_out));
    }

    /// <summary>
    /// Builds the attempt's request and authorizes it. A failure to build or authorize ends the request rather than
    /// escaping.
    /// </summary>
    /// <returns>The request ready to send, or the error that ended the request.</returns>
    boost::asio::awaitable<std::expected<HttpRequest, std::error_code>> PrepareAttempt() {
        HttpRequest request;
        try {
            request = m_attempts.Build(m_attempt >= m_maxAttempts &&
                                               TokenRefreshEligibility() == RefreshEligibility::Ineligible
                                           ? AttemptFactory::Reuse::MayMove
                                           : AttemptFactory::Reuse::Copy);
        } catch (...) {
            co_return std::unexpected(ErrorFromCurrentException());
        }

        std::string_view bearerToken;
        if (m_auth.Type == RequestAuth::Kind::Token) {
            auto token = co_await AcquireToken();
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
                if constexpr (std::invocable<TRebuild&, HttpRequest&, std::string_view, AttemptKind>) {
                    m_rebuild(request, bearerToken, m_attemptKind);
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

    /// <summary>
    /// The options for the next send: the caller's, with the timeout clamped to what is left of the total deadline.
    /// </summary>
    /// <returns>The options; <c>std::errc::timed_out</c> when the deadline has already passed, since a fresh attempt
    /// must never start then.</returns>
    std::expected<AzureRequestOptions, std::error_code> AttemptOptions() const {
        AzureRequestOptions options = m_requestOptions;
        // This pipeline owns the retrying; the client retrying again inside each attempt would multiply the attempts.
        options.ClearRetryOptions();
        if (m_totalDeadline) {
            const auto left = Remaining();
            if (left <= std::chrono::milliseconds{0}) {
                // The deadline has passed: never start a fresh attempt.
                return std::unexpected(std::make_error_code(std::errc::timed_out));
            }
            options.SetTimeout(std::min(options.GetTimeout(), Timeout{left}));
        }
        return options;
    }

    /// <summary>
    /// A 401 on a token-authorized request usually means the cached token was revoked or rotated, so one extra attempt
    /// with a freshly requested token is allowed regardless of the retry budget.
    /// </summary>
    /// <param name="error">The transport error of the attempt, if any.</param>
    /// <param name="response">The response of the attempt.</param>
    /// <returns>Retry after invalidating the cached token; DeadlinePassed when no further attempt can be made;
    /// NotApplicable when the outcome is not a refreshable 401.</returns>
    Refresh RefreshTokenAfter401(const std::error_code& error, const HttpResponse& response) {
        if (error || response.GetStatus() != HttpStatus::Unauthorized ||
            TokenRefreshEligibility() == RefreshEligibility::Ineligible) {
            return Refresh::NotApplicable;
        }
        if constexpr (CanInvalidate<TCred>) {
            // Don't invalidate the cached token and fetch a new one for an attempt that can never be sent.
            if (DeadlineStatus() == DeadlineState::Expired) {
                return Refresh::DeadlinePassed;
            }
            m_tokenRefresh = TokenRefreshState::Refreshed;
            if constexpr (requires { m_credential->Invalidate(m_auth.TokenScopes, std::string_view{}); }) {
                m_credential->Invalidate(m_auth.TokenScopes, m_attemptToken);
            } else {
                m_credential->Invalidate(m_auth.TokenScopes);
            }
            m_attemptAccounting = AttemptAccounting::Free;
            return Refresh::Retry;
        } else {
            return Refresh::NotApplicable;
        }
    }

    /// <summary>
    /// How long to wait before the next attempt, or empty when this outcome is final (success, a failure that is not
    /// worth retrying, or the budget is spent).
    /// </summary>
    /// <param name="error">The transport error of the attempt, if any.</param>
    /// <param name="response">The response of the attempt.</param>
    /// <returns>The delay; empty when the outcome is final.</returns>
    std::optional<std::chrono::milliseconds> BackoffBeforeRetry(const std::error_code& error,
                                                                const HttpResponse& response) {
        const auto notFoundRetried =
            m_notFoundRetry == NotFoundRetry::Retry && !error &&
                    (response.GetStatus() == HttpStatus::NotFound || response.GetStatus() == HttpStatus::Gone)
                ? Retriability::Retriable
                : Retriability::NotRetriable;
        if (m_attempt >= m_maxAttempts || (RetriabilityOfFailure(error, response) == Retriability::NotRetriable &&
                                           notFoundRetried == Retriability::NotRetriable)) {
            return std::nullopt;
        }

        // After an ambiguous transport failure the request may already have been applied, so replaying one that
        // cannot be repeated safely would misreport the outcome (e.g. 409 for a create that succeeded). Surface the
        // transport error as "outcome unknown" and let the caller decide.
        if (error && m_replaySafety == ReplaySafety::Unsafe &&
            DeliveryCertaintyOfTransportError(error) == DeliveryCertainty::Ambiguous) {
            return std::nullopt;
        }

        // Remembered for the rebuild hook: only a replay that follows an unknown outcome can see its own earlier
        // effect (a token-refresh repeat after a 401 cannot).
        if (error ? DeliveryCertaintyOfTransportError(error) == DeliveryCertainty::Ambiguous
                  : (response.GetStatus().Class() == HttpStatusClass::ServerError ||
                     response.GetStatus() == HttpStatus::RequestTimeout)) {
            m_attemptKind = AttemptKind::AfterAmbiguousFailure;
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
    NotFoundRetry m_notFoundRetry;
    TRebuild m_rebuild;
    std::optional<std::chrono::milliseconds> m_totalDeadline;
    std::chrono::steady_clock::time_point m_start;
    int m_maxAttempts;
    ReplaySafety m_replaySafety;
    AttemptFactory m_attempts;
    int m_attempt = 0;
    AttemptAccounting m_attemptAccounting = AttemptAccounting::Counted;
    TokenRefreshState m_tokenRefresh = TokenRefreshState::NotRefreshed;
    AttemptKind m_attemptKind = AttemptKind::Initial;
    std::string m_attemptToken;
};
} // namespace RetryDetail

/// <summary>
/// Sends <paramref name="request"/> with per-attempt authorization and retry/backoff according to
/// <paramref name="retry"/>, and returns the final HTTP response, or the transport/pipeline error. All state is owned
/// by the coroutine; only <paramref name="httpClient"/> must outlive it. <paramref name="rebuild"/> is called with
/// (HttpRequest&amp;, std::string_view bearerToken[, AttemptKind attemptKind]) before each attempt is authorized and
/// sent, so a retry never replays a stale body, signature or token; throwing fails the request. Cancelling the
/// awaiting coroutine cancels the in-flight attempt or the pending backoff and returns
/// <c>std::errc::operation_canceled</c>; an expired total deadline returns <c>std::errc::timed_out</c>.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The token credential type.</typeparam>
/// <typeparam name="TRebuild">The type of the hook that adjusts each attempt's request.</typeparam>
/// <param name="httpClient">The client to send with.</param>
/// <param name="auth">How each attempt is authorized.</param>
/// <param name="credential">The token credential for token authorization; may be null.</param>
/// <param name="retry">The client's retry policy.</param>
/// <param name="request">The request to send.</param>
/// <param name="requestOptions">The per-request options.</param>
/// <param name="notFoundRetry">Set to Retry to additionally retry HTTP 404/410 (managed identity endpoints answer
/// these while starting).</param>
/// <param name="rebuild">The hook called before each attempt is authorized.</param>
/// <param name="replaySafety">Safe marks a request whose caller absorbs the outcome of a replay (the
/// *IfExists/*IfNotExists operations), so it may be retried even after an ambiguous transport error.</param>
/// <returns>The final HTTP response, or the transport or pipeline error.</returns>
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild>
boost::asio::awaitable<HttpResult> SendWithRetry(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential,
                                                 RetryOptions retry, HttpRequest request,
                                                 AzureRequestOptions requestOptions, NotFoundRetry notFoundRetry,
                                                 TRebuild rebuild, ReplaySafety replaySafety) {
    RetryDetail::RetryingSender<THttp, TCred, TRebuild> sender(httpClient, std::move(auth), std::move(credential),
                                                               retry, std::move(request), std::move(requestOptions),
                                                               notFoundRetry, std::move(rebuild), replaySafety);
    co_return co_await sender.Run();
}

/// <summary>
/// Validates the shape of the caller's headers, then sends the request with retries. The result carries
/// <c>std::errc::invalid_argument</c> for a header with control characters and
/// <c>BlobStorageErrorCode::InvalidMetadata</c> for bad metadata, before anything reaches the transport.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <typeparam name="TCred">The token credential type.</typeparam>
/// <typeparam name="TRebuild">The type of the hook that adjusts each attempt's request.</typeparam>
/// <param name="httpClient">The client to send with.</param>
/// <param name="connection">The connection's authorization and retry configuration.</param>
/// <param name="credential">The token credential; may be null.</param>
/// <param name="request">The request to send.</param>
/// <param name="requestOptions">The per-request options.</param>
/// <param name="rebuild">The hook called before each attempt is authorized.</param>
/// <param name="replaySafety">Whether the caller absorbs the outcome of a replay.</param>
/// <returns>The final HTTP response, or the validation, transport or pipeline error.</returns>
template <HttpClientLike THttp, TokenCredentialLike TCred, class TRebuild>
boost::asio::awaitable<HttpResult> SendAuthorizedRequest(THttp& httpClient, const ConnectionState& connection,
                                                         std::shared_ptr<TCred> credential, HttpRequest request,
                                                         AzureRequestOptions requestOptions, TRebuild rebuild,
                                                         ReplaySafety replaySafety) {
    // Checked here rather than per operation so every request carrying metadata, including the
    // internal commits issued by multi-part uploads, is rejected before reaching the service.
    for (const HttpHeader& header : request.GetHeaders()) {
        const std::string_view name = header.GetName();
        if (ControlCharactersIn(name) == ControlCharacters::Present ||
            ControlCharactersIn(header.GetValue()) == ControlCharacters::Present) {
            co_return std::unexpected(std::make_error_code(std::errc::invalid_argument));
        }
        const auto metadata = IStartsWith(name, XMsMetaHeaderPrefix);
        const auto isNonAscii = [](char c) noexcept { return static_cast<unsigned char>(c) >= 0x80U; };
        if (metadata == StringMatch::Match &&
            (ValidityOfMetadataName(name.substr(XMsMetaHeaderPrefix.size())) == Validity::Invalid ||
             std::ranges::any_of(header.GetValue(), isNonAscii))) {
            co_return std::unexpected(std::error_code{BlobStorageErrorCode::InvalidMetadata});
        }
    }
    const CredentialPresence presence = PresenceOf(credential);
    co_return co_await SendWithRetry(httpClient, MakeRequestAuth(connection, presence), std::move(credential),
                                     connection.Retry, std::move(request), std::move(requestOptions),
                                     NotFoundRetry::Fail, std::move(rebuild), replaySafety);
}
} // namespace AVEVA::AzureClient::Private
