// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "AVEVA/AzureClient/HttpClientLike.hpp"
#include "AVEVA/AzureClient/TokenCredential.hpp"
#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/ProtocolConstants.hpp>

#include <AVEVA/HttpClient/HttpClientError.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <boost/system/error_code.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>

namespace AVEVA::AzureClient::Private {
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
// the requested delay without really waiting; every real client uses a steady_timer.
template <class THttp> struct RetryTimerFor {
    using type = boost::asio::steady_timer;
};
template <class THttp>
    requires requires { typename THttp::retry_timer_type; }
struct RetryTimerFor<THttp> {
    using type = typename THttp::retry_timer_type;
};

template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler, class TRebuild>
class RetryOperation final : public std::enable_shared_from_this<RetryOperation<THttp, TCred, THandler, TRebuild>> {
  public:
    RetryOperation(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
                   HttpRequest request, THandler completion, AzureRequestOptions requestOptions,
                   bool retryNotFoundAndGone, TRebuild rebuild, bool callerAbsorbsReplay = false)
        : m_httpClient(httpClient), m_auth(std::move(auth)), m_credential(std::move(credential)), m_retry(retry),
          m_request(std::move(request)), m_completion(std::move(completion)), m_requestOptions(requestOptions),
          m_retryNotFoundAndGone(retryNotFoundAndGone), m_rebuild(std::move(rebuild)),
          m_replaySafe(callerAbsorbsReplay || IsReplaySafe(m_request)),
          m_parentSlot(m_requestOptions.GetCancellationSlot()), m_totalDeadline(m_requestOptions.GetTotalDeadline()),
          m_start(std::chrono::steady_clock::now()),
          m_maxAttempts(ComputeMaxAttempts(m_requestOptions.GetMaxRetries().value_or(retry.MaxRetries))) {}

    void Start() {
        if (m_parentSlot.is_connected()) {
            std::weak_ptr<RetryOperation> const weak = this->weak_from_this();
            m_parentSlot.assign([weak](boost::asio::cancellation_type_t type) {
                if (!IsAbortingCancellation(type)) {
                    return;
                }
                if (auto self = weak.lock()) {
                    self->OnCancel(type);
                }
            });
        }

        if (m_totalDeadline) {
            // Bounds retries and backoff together; the per-attempt timeout only bounds one send.
            // Created only when a deadline is set: construction takes the executor's service-registry mutex.
            m_deadlineTimer.emplace(m_httpClient.get_executor());
            m_deadlineTimer->expires_after(*m_totalDeadline);
            m_deadlineTimer->async_wait([weak = this->weak_from_this()](const boost::system::error_code& waitError) {
                if (auto self = weak.lock(); self && !waitError) {
                    self->Run(DeadlineEvent{});
                }
            });
        }

        m_initiating.store(true, std::memory_order_release);
        Run(StartAttemptEvent{});
        m_initiating.store(false, std::memory_order_release);
    }

  private:
    // The units of work serialized by Run. They are plain data rather than closures so the queue needs
    // no type erasure.
    struct StartAttemptEvent {};
    struct CancelEvent {
        boost::asio::cancellation_type_t Type;
    };
    struct TokenEvent {
        std::error_code Error;
        AccessToken Token;
        HttpRequest Attempt;
    };
    struct AttemptCompleteEvent {
        std::error_code Error;
        HttpResponse Response;
    };
    struct BackoffElapsedEvent {
        bool Aborted;
    };
    struct DeadlineEvent {};
    struct FinishEvent {
        std::error_code Error;
    };
    using Event = std::variant<StartAttemptEvent, CancelEvent, TokenEvent, AttemptCompleteEvent, BackoffElapsedEvent,
                               DeadlineEvent, FinishEvent>;

    void Dispatch(Event& event) {
        std::visit(
            [this](auto& e) {
                using TEvent = std::decay_t<decltype(e)>;
                if constexpr (std::is_same_v<TEvent, StartAttemptEvent>) {
                    StartAttempt();
                } else if constexpr (std::is_same_v<TEvent, CancelEvent>) {
                    OnCancelOnStrand(e.Type);
                } else if constexpr (std::is_same_v<TEvent, TokenEvent>) {
                    OnTokenOnStrand(e.Error, e.Token, std::move(e.Attempt));
                } else if constexpr (std::is_same_v<TEvent, AttemptCompleteEvent>) {
                    OnAttemptCompleteOnStrand(e.Error, std::move(e.Response));
                } else if constexpr (std::is_same_v<TEvent, DeadlineEvent>) {
                    OnDeadlineOnStrand();
                } else if constexpr (std::is_same_v<TEvent, BackoffElapsedEvent>) {
                    if (m_cancelled || e.Aborted) {
                        Finish(CancelError(), HttpResponse{});
                    } else {
                        StartAttempt();
                    }
                } else {
                    Finish(e.Error, HttpResponse{});
                }
            },
            event);
    }

    // Serializes all state mutation: transport, timer, token and cancellation callbacks may
    // arrive from different threads. Runs inline when idle (so initiation stays synchronous)
    // and queues work that arrives while another thread, or an outer call, is running.
    // Queueing allocates. When it fails the event cannot be kept, so the failure is recorded in a flag that needs no
    // allocation and the active runner fails the request with it; otherwise an arrived completion would vanish
    // on a transport thread and leave the request waiting forever.
    void Run(Event event) {
        {
            const std::scoped_lock lock(m_queueMutex);
            if (m_draining) {
                try {
                    m_queue.push_back(std::move(event));
                } catch (...) {
                    m_queueOverflowed = true;
                }
                return;
            }
            m_draining = true;
        }

        const auto keepAlive = this->shared_from_this();
        try {
            DispatchGuarded(event);
            for (;;) {
                bool overflowed = false;
                {
                    const std::scoped_lock lock(m_queueMutex);
                    if (m_queueOverflowed) {
                        m_queueOverflowed = false;
                        overflowed = true;
                    } else if (m_queue.empty()) {
                        m_draining = false;
                        return;
                    } else {
                        event = std::move(m_queue.front());
                        m_queue.pop_front();
                    }
                }
                if (overflowed) {
                    FailOverflowedQueue();
                } else {
                    DispatchGuarded(event);
                }
            }
        } catch (...) {
            // Only a throwing user completion reaches here; never leave the runner marked active.
            {
                const std::scoped_lock lock(m_queueMutex);
                m_draining = false;
            }
            throw;
        }
    }

    // Ends the request inline, on the runner: posting the failure would allocate again. Whatever event was lost,
    // stopping the accepted attempt or backoff and reporting the allocation failure is the only outcome that
    // cannot leave the request waiting for it.
    void FailOverflowedQueue() {
        if (m_finished.load(std::memory_order_acquire)) {
            return;
        }
        m_internalError = std::make_error_code(std::errc::not_enough_memory);
        m_cancelled.store(true, std::memory_order_release);
        CancelBackoff();
        m_attemptSignal.emit(boost::asio::cancellation_type::terminal);
        if (m_initiating.load(std::memory_order_acquire)) {
            // A completion never runs inside the initiating call; only if even the hop cannot be posted does it.
            try {
                PostFinish(m_internalError);
                return;
            } catch (...) {
            }
        }
        Finish(m_internalError, HttpResponse{});
    }

    // An internal throw (allocation, request building, transport initiation) after initiation must not escape onto
    // the host event loop or strand the operation: it becomes the request's error. The very first attempt is still
    // inside the caller's initiating function, where nothing has been accepted yet, so its throw propagates and
    // the caller can retry. Once the request has finished, a throw can only come from the caller's completion,
    // which is not a storage failure and is left to propagate.
    void DispatchGuarded(Event& event) {
        try {
            Dispatch(event);
        } catch (...) {
            if (m_finished.load(std::memory_order_acquire) || std::holds_alternative<StartAttemptEvent>(event)) {
                throw;
            }
            FailInternal(std::current_exception());
        }
    }

    void FailInternal(const std::exception_ptr& failure) {
        std::error_code error = std::make_error_code(std::errc::invalid_argument);
        try {
            std::rethrow_exception(failure);
        } catch (const std::bad_alloc&) {
            error = std::make_error_code(std::errc::not_enough_memory);
        } catch (const std::system_error& e) {
            error = e.code();
        } catch (...) {
        }
        m_internalError = error;
        m_cancelled.store(true, std::memory_order_release);
        // Stops any attempt or backoff already accepted; their completions report this error, not a cancellation.
        CancelBackoff();
        m_attemptSignal.emit(boost::asio::cancellation_type::terminal);
        PostFinish(error);
    }

    void OnCancel(boost::asio::cancellation_type_t type) {
        if (m_finished.load(std::memory_order_acquire)) {
            return;
        }
        m_cancelled.store(true, std::memory_order_release);
        Run(CancelEvent{type});
    }

    void OnCancelOnStrand(boost::asio::cancellation_type_t type) {
        if (m_finished.load(std::memory_order_acquire)) {
            return;
        }

        CancelBackoff();
        m_attemptSignal.emit(type);

        // Token credentials have no cancellation hook, so don't wait for a slow credential:
        // complete now and ignore the token when it eventually arrives.
        if (m_awaitingToken.load(std::memory_order_acquire)) {
            PostFinish(CancelError());
        }
    }

    void OnDeadlineOnStrand() {
        if (m_finished.load(std::memory_order_acquire)) {
            return;
        }
        if (!m_cancelled.load(std::memory_order_acquire)) {
            m_timedOut = true;
            m_cancelled.store(true, std::memory_order_release);
        }
        OnCancelOnStrand(boost::asio::cancellation_type::terminal);
    }

    // A caller cancellation reports operation_canceled; an expired total deadline reports timed_out.
    [[nodiscard]] std::error_code CancelError() const noexcept {
        if (m_internalError) {
            return m_internalError;
        }
        return m_timedOut ? std::make_error_code(std::errc::timed_out) : CanceledError();
    }

    [[nodiscard]] static int ComputeMaxAttempts(int maxRetries) noexcept {
        return maxRetries >= std::numeric_limits<int>::max() - 1 ? std::numeric_limits<int>::max()
                                                                 : std::max(0, maxRetries) + 1;
    }

    [[nodiscard]] HttpRequest BuildAttemptRequest() {
        HttpRequest attempt = CopyOrMoveRequest();
        // Stamped here, right before signing, so a request that waited for a token or a backoff never carries a
        // stale date that the service would reject as clock skew.
        StampDate(attempt, BuildDateHeaderValue(std::chrono::system_clock::now()));
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

    // The last possible attempt takes the request itself; earlier ones copy only the headers and share the body.
    [[nodiscard]] HttpRequest CopyOrMoveRequest() {
        if (m_attempt >= m_maxAttempts && !CanRefreshToken() && !m_sharedBody) {
            return std::move(m_request);
        }

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

    // `countsAsAttempt` is false for the one token-refresh repeat, which must not consume the retry budget.
    void StartAttempt(bool countsAsAttempt = true) {
        if (countsAsAttempt) {
            ++m_attempt;
        }
        HttpRequest attempt = BuildAttemptRequest();

        switch (m_auth.Type) {
        case RequestAuth::Kind::SharedKey:
            ResolveCopySourceAuthorization(attempt, {});
            if (!RebuildAttempt(attempt, {})) {
                return;
            }
            try {
                AuthorizeRequest(*m_auth.Signer, attempt);
            } catch (const std::bad_alloc&) {
                PostFinish(std::make_error_code(std::errc::not_enough_memory));
                return;
            } catch (const std::system_error& e) {
                PostFinish(e.code());
                return;
            } catch (const std::exception&) {
                PostFinish(std::make_error_code(std::errc::invalid_argument));
                return;
            }
            Send(std::move(attempt));
            return;

        case RequestAuth::Kind::Token:
            RequestToken(std::move(attempt));
            return;

        case RequestAuth::Kind::None:
            ResolveCopySourceAuthorization(attempt, {});
            if (!RebuildAttempt(attempt, {})) {
                return;
            }
            break;
        }

        Send(std::move(attempt));
    }

    void RequestToken(HttpRequest attempt) {
        auto self = this->shared_from_this();
        m_awaitingToken.store(true, std::memory_order_release);
        GetTokenForScopes(*m_credential, m_auth.TokenScopes,
                          [self, attempt = std::move(attempt)](std::error_code error, AccessToken token) mutable {
                              auto proceed = [self, error, token = std::move(token),
                                              attempt = std::move(attempt)]() mutable {
                                  self->OnToken(error, std::move(token), std::move(attempt));
                              };

                              // Never complete inside the caller's initiating function, even if the
                              // credential answered synchronously.
                              if (self->m_initiating.load(std::memory_order_acquire)) {
                                  boost::asio::post(self->m_httpClient.get_executor(), std::move(proceed));
                              } else {
                                  proceed();
                              }
                          });
    }

    void OnToken(std::error_code error, AccessToken token, HttpRequest attempt) {
        Run(TokenEvent{error, std::move(token), std::move(attempt)});
    }

    void OnTokenOnStrand(std::error_code error, const AccessToken& token, HttpRequest attempt) {
        m_awaitingToken.store(false, std::memory_order_release);
        if (m_finished.load(std::memory_order_acquire)) {
            return;
        }
        if (m_cancelled.load(std::memory_order_acquire)) {
            Finish(CancelError(), HttpResponse{});
            return;
        }
        if (error) {
            Finish(error, HttpResponse{});
            return;
        }

        m_attemptToken = token.Token;
        if (!RebuildAttempt(attempt, token.Token)) {
            return;
        }
        ResolveCopySourceAuthorization(attempt, token.Token);
        SetBearerAuthorization(attempt, token.Token);
        Send(std::move(attempt));
    }

    // Runs the caller's per-attempt rebuild, if any. A failure ends the request rather than escaping onto a
    // transport thread.
    [[nodiscard]] bool RebuildAttempt(HttpRequest& attempt, std::string_view bearerToken) {
        if constexpr (std::is_same_v<TRebuild, NoRebuild>) {
            return true;
        } else {
            try {
                m_rebuild(attempt, bearerToken);
            } catch (const std::bad_alloc&) {
                PostFinish(std::make_error_code(std::errc::not_enough_memory));
                return false;
            } catch (const std::system_error& e) {
                PostFinish(e.code());
                return false;
            } catch (const std::exception&) {
                PostFinish(std::make_error_code(std::errc::invalid_argument));
                return false;
            }
            return true;
        }
    }

    void Send(HttpRequest attempt) {
        AzureRequestOptions options = m_requestOptions;
        m_attemptSignal.slot().clear();
        if (m_totalDeadline) {
            const auto remaining = *m_totalDeadline - std::chrono::duration_cast<std::chrono::milliseconds>(
                                                          std::chrono::steady_clock::now() - m_start);
            if (remaining <= std::chrono::milliseconds{0}) {
                // The deadline has passed (its timer may not have run yet): never start a fresh attempt.
                m_timedOut = true;
                PostFinish(std::make_error_code(std::errc::timed_out));
                return;
            }
            options.SetTimeout(std::min(options.GetTimeout(), remaining));
        }
        options.SetCancellationSlot(m_parentSlot.is_connected() || m_totalDeadline ? m_attemptSignal.slot()
                                                                                   : boost::asio::cancellation_slot{});

        auto self = this->shared_from_this();
        m_httpClient.SendAsync(std::move(attempt), std::move(options),
                               [self](std::error_code error, HttpResponse response) {
                                   // A transport that completes inside SendAsync must not complete the caller's
                                   // operation inside its initiating function.
                                   if (self->m_initiating.load(std::memory_order_acquire)) {
                                       boost::asio::post(self->m_httpClient.get_executor(),
                                                         [self, error, response = std::move(response)]() mutable {
                                                             self->OnAttemptComplete(error, std::move(response));
                                                         });
                                       return;
                                   }
                                   self->OnAttemptComplete(error, std::move(response));
                               });
    }

    void OnAttemptComplete(std::error_code error, HttpResponse response) {
        Run(AttemptCompleteEvent{error, std::move(response)});
    }

    [[nodiscard]] bool IsRetriableNotFoundOrGone(std::error_code error, const HttpResponse& response) const noexcept {
        constexpr unsigned int NotFound = 404;
        constexpr unsigned int Gone = 410;
        return m_retryNotFoundAndGone && !error && (response.GetStatus() == NotFound || response.GetStatus() == Gone);
    }

    // A 401 on a token-authorized request usually means the cached token was revoked or rotated, so one extra
    // attempt with a freshly requested token is allowed regardless of the retry budget.
    [[nodiscard]] bool CanRefreshToken() const {
        if constexpr (requires { m_credential->Invalidate(m_auth.TokenScopes); }) {
            return m_auth.Type == RequestAuth::Kind::Token && m_credential != nullptr && !m_tokenRefreshed;
        } else {
            return false;
        }
    }

    [[nodiscard]] bool DeadlineExpired() const {
        return m_totalDeadline.has_value() && std::chrono::steady_clock::now() - m_start >= *m_totalDeadline;
    }

    void OnAttemptCompleteOnStrand(std::error_code error, HttpResponse response) {
        if (m_cancelled) {
            Finish(CancelError(), std::move(response));
            return;
        }

        constexpr unsigned int Unauthorized = 401;
        if (!error && response.GetStatus() == Unauthorized && CanRefreshToken()) {
            if constexpr (requires { m_credential->Invalidate(m_auth.TokenScopes); }) {
                // Don't invalidate the cached token and fetch a new one for an attempt that can never be sent.
                if (DeadlineExpired()) {
                    m_timedOut = true;
                    Finish(std::make_error_code(std::errc::timed_out), std::move(response));
                    return;
                }
                m_tokenRefreshed = true;
                if constexpr (requires { m_credential->Invalidate(m_auth.TokenScopes, std::string_view{}); }) {
                    m_credential->Invalidate(m_auth.TokenScopes, m_attemptToken);
                } else {
                    m_credential->Invalidate(m_auth.TokenScopes);
                }
                StartAttempt(false);
                return;
            }
        }

        if (m_attempt >= m_maxAttempts ||
            !(IsRetriableFailure(error, response) || IsRetriableNotFoundOrGone(error, response))) {
            Finish(error, std::move(response));
            return;
        }

        // After an ambiguous transport failure the request may already have been applied, so replaying one that
        // cannot be repeated safely would misreport the outcome (e.g. 409 for a create that succeeded). Surface the
        // transport error as "outcome unknown" and let the caller decide.
        if (error && !m_replaySafe && IsAmbiguousTransportError(error)) {
            Finish(error, std::move(response));
            return;
        }

        // A server hint may exceed MaxDelay (the service knows when it will recover) but is capped so a
        // bogus value cannot stall a request for hours.
        constexpr std::chrono::milliseconds maxHint{std::chrono::minutes{1}};
        std::chrono::milliseconds delay;
        if (const auto hint = ParseRetryAfter(response); hint.has_value()) {
            delay = std::min(*hint, std::max(m_retry.MaxDelay, maxHint));
        } else {
            delay = std::clamp(ComputeBackoff(m_retry, m_attempt), std::chrono::milliseconds{0},
                               std::max(m_retry.MaxDelay, std::chrono::milliseconds{0}));
        }

        if (m_totalDeadline) {
            const auto remaining = *m_totalDeadline - std::chrono::duration_cast<std::chrono::milliseconds>(
                                                          std::chrono::steady_clock::now() - m_start);
            if (remaining <= delay) {
                // The backoff alone would outlast the deadline, so another attempt can never be made.
                m_timedOut = true;
                Finish(std::make_error_code(std::errc::timed_out), std::move(response));
                return;
            }
        }
        // Built on first use: most requests never back off, and construction takes the executor's service-registry
        // mutex.
        if (!m_timer) {
            m_timer.emplace(m_httpClient.get_executor());
        }
        m_timer->expires_after(delay);
        auto self = this->shared_from_this();
        m_timer->async_wait([self](const boost::system::error_code& waitError) {
            self->Run(BackoffElapsedEvent{static_cast<bool>(waitError)});
        });
    }

    void CancelBackoff() {
        if (m_timer) {
            m_timer->cancel();
        }
    }

    void PostFinish(std::error_code error) {
        auto self = this->shared_from_this();
        boost::asio::post(m_httpClient.get_executor(), [self, error]() { self->Run(FinishEvent{error}); });
    }

    void Finish(std::error_code error, HttpResponse response) {
        if (m_finished.exchange(true, std::memory_order_acq_rel)) {
            return;
        }

        // The parent slot is deliberately not cleared here: clearing on the transport thread would race with
        // the caller emitting on its own executor. Its handler only holds a weak_ptr and is inert once
        // finished; the caller's slot is cleared on its executor when the completion is dispatched.
        if (m_deadlineTimer) {
            m_deadlineTimer->cancel();
        }
        THandler completion = std::move(m_completion);
        completion(error, std::move(response));
    }

    THttp& m_httpClient;
    RequestAuth m_auth;
    std::shared_ptr<TCred> m_credential;
    RetryOptions m_retry;
    HttpRequest m_request;
    std::shared_ptr<const std::string> m_sharedBody;
    THandler m_completion;
    AzureRequestOptions m_requestOptions;
    bool m_retryNotFoundAndGone;
    [[no_unique_address]] TRebuild m_rebuild;
    bool m_replaySafe;
    boost::asio::cancellation_slot m_parentSlot;
    boost::asio::cancellation_signal m_attemptSignal;
    std::optional<typename RetryTimerFor<THttp>::type> m_timer;
    std::optional<boost::asio::steady_timer> m_deadlineTimer;
    std::optional<std::chrono::milliseconds> m_totalDeadline;
    std::chrono::steady_clock::time_point m_start;
    std::mutex m_queueMutex;
    std::deque<Event> m_queue;
    bool m_draining = false;
    bool m_queueOverflowed = false;
    int m_maxAttempts = 1;
    int m_attempt = 0;
    std::atomic<bool> m_initiating{false};
    std::atomic<bool> m_awaitingToken{false};
    std::atomic<bool> m_cancelled{false};
    std::atomic<bool> m_finished{false};
    bool m_timedOut = false;
    std::error_code m_internalError;
    bool m_tokenRefreshed = false;
    // Token the latest attempt was sent with; a 401 invalidates the cache only if it still holds this one.
    std::string m_attemptToken;
    std::size_t m_dateHeaderIndex = std::numeric_limits<std::size_t>::max();
    bool m_dateHeaderScanned = false;
};
} // namespace RetryDetail

template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler, class TRebuild>
void SendWithRetryAsync(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
                        HttpRequest request, THandler completion, AzureRequestOptions requestOptions,
                        bool retryNotFoundAndGone, TRebuild rebuild, bool replaySafe) {
    auto operation = std::make_shared<RetryDetail::RetryOperation<THttp, TCred, THandler, TRebuild>>(
        httpClient, std::move(auth), std::move(credential), retry, std::move(request), std::move(completion),
        requestOptions, retryNotFoundAndGone, std::move(rebuild), replaySafe);
    operation->Start();
}

template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler, class TRebuild>
void SendAuthorizedRequestAsync(THttp& httpClient, const ConnectionState& connection, std::shared_ptr<TCred> credential,
                                HttpRequest request, THandler completion, AzureRequestOptions requestOptions,
                                TRebuild rebuild, bool replaySafe) {
    // Checked here rather than per operation so every request carrying metadata, including the
    // internal commits issued by multi-part uploads, is rejected before reaching the service.
    for (const HttpHeader& header : request.GetHeaders()) {
        const std::string_view name = header.GetName();
        if (HasHeaderControlCharacters(name) || HasHeaderControlCharacters(header.GetValue())) {
            PostCompletion(httpClient, std::move(completion), std::make_error_code(std::errc::invalid_argument),
                           HttpResponse{});
            return;
        }
        const bool isMetadata = IStartsWith(name, XMsMetaHeaderPrefix);
        const auto isNonAscii = [](char c) noexcept { return static_cast<unsigned char>(c) >= 0x80U; };
        if (isMetadata && (!IsValidMetadataName(name.substr(XMsMetaHeaderPrefix.size())) ||
                           std::ranges::any_of(header.GetValue(), isNonAscii))) {
            PostCompletion(httpClient, std::move(completion), std::error_code{BlobStorageErrorCode::InvalidMetadata},
                           HttpResponse{});
            return;
        }
    }
    const bool hasCredential = credential != nullptr;
    SendWithRetryAsync(httpClient, MakeRequestAuth(connection, hasCredential), std::move(credential), connection.Retry,
                       std::move(request), std::move(completion), requestOptions, false, std::move(rebuild),
                       replaySafe);
}
} // namespace AVEVA::AzureClient::Private
