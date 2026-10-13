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

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
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

// Exponential backoff (InitialDelay * 2^(retry-1)) with +/- jitter in [0.8, 1.3), capped at MaxDelay.
[[nodiscard]] inline std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber) {
    thread_local std::mt19937_64 generator{std::random_device{}()};
    std::uniform_real_distribution<double> jitter(MinBackoffJitter, MaxBackoffJitter);

    const double maxDelay = static_cast<double>(std::max<std::int64_t>(0, retry.MaxDelay.count()));
    const double initial = static_cast<double>(std::max<std::int64_t>(0, retry.InitialDelay.count()));
    const double exponential = initial * std::ldexp(1.0, std::clamp(retryNumber - 1, 0, MaxBackoffExponent));
    const double delay = std::min(exponential * jitter(generator), maxDelay);
    return std::chrono::milliseconds{static_cast<std::int64_t>(std::llround(delay))};
}

// Resolves the x-ms-copy-source-authorization placeholder (see CopySourceClientCredentialMarker): an empty
// `bearerToken` means the client has no bearer token to forward, so the header is dropped before the request is
// signed or sent.
inline void ResolveCopySourceAuthorization(HttpRequest& request, std::string_view bearerToken) {
    auto& headers = request.GetHeaders();
    for (auto it = headers.begin(); it != headers.end(); ++it) {
        if (!IEquals(it->GetName(), XMsCopySourceAuthorizationHeaderName) ||
            it->GetValue() != CopySourceClientCredentialMarker) {
            continue;
        }
        if (bearerToken.empty()) {
            headers.erase(it);
        } else {
            it->SetValue("Bearer " + std::string{bearerToken});
        }
        return;
    }
}

[[nodiscard]] inline std::error_code CanceledError() noexcept {
    return std::make_error_code(std::errc::operation_canceled);
}

template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler>
class RetryOperation final : public std::enable_shared_from_this<RetryOperation<THttp, TCred, THandler>> {
  public:
    RetryOperation(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
                   HttpRequest request, THandler completion, HttpRequestOptions requestOptions,
                   bool retryNotFoundAndGone)
        : m_httpClient(httpClient), m_auth(std::move(auth)), m_credential(std::move(credential)), m_retry(retry),
          m_request(std::move(request)), m_completion(std::move(completion)), m_requestOptions(requestOptions),
          m_retryNotFoundAndGone(retryNotFoundAndGone), m_parentSlot(m_requestOptions.GetCancellationSlot()),
          m_timer(httpClient.get_executor()), m_deadlineTimer(httpClient.get_executor()),
          m_totalDeadline(m_requestOptions.GetTotalDeadline()), m_start(std::chrono::steady_clock::now()),
          m_maxAttempts(ComputeMaxAttempts(m_requestOptions.GetMaxRetries().value_or(retry.MaxRetries))) {}

    void Start() {
        if (m_parentSlot.is_connected()) {
            std::weak_ptr<RetryOperation> const weak = this->weak_from_this();
            m_parentSlot.assign([weak](boost::asio::cancellation_type_t type) {
                if (auto self = weak.lock()) {
                    self->OnCancel(type);
                }
            });
        }

        if (m_totalDeadline) {
            // Bounds retries and backoff together; the per-attempt timeout only bounds one send.
            m_deadlineTimer.expires_after(*m_totalDeadline);
            m_deadlineTimer.async_wait([weak = this->weak_from_this()](const boost::system::error_code& waitError) {
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
    void Run(Event event) {
        {
            const std::scoped_lock lock(m_queueMutex);
            if (m_draining) {
                m_queue.push_back(std::move(event));
                return;
            }
            m_draining = true;
        }

        const auto keepAlive = this->shared_from_this();
        Dispatch(event);
        for (;;) {
            {
                const std::scoped_lock lock(m_queueMutex);
                if (m_queue.empty()) {
                    m_draining = false;
                    return;
                }
                event = std::move(m_queue.front());
                m_queue.pop_front();
            }
            Dispatch(event);
        }
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

        m_timer.cancel();
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
        return m_timedOut ? std::make_error_code(std::errc::timed_out) : CanceledError();
    }

    [[nodiscard]] static int ComputeMaxAttempts(int maxRetries) noexcept {
        return maxRetries >= std::numeric_limits<int>::max() - 1 ? std::numeric_limits<int>::max()
                                                                 : std::max(0, maxRetries) + 1;
    }

    [[nodiscard]] HttpRequest BuildAttemptRequest() {
        if (m_maxAttempts == 1) {
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

        if (m_attempt > 1) {
            const std::string now = BuildDateHeaderValue(std::chrono::system_clock::now());
            for (HttpHeader& header : attempt.GetHeaders()) {
                if (IEquals(header.GetName(), XMsDateHeaderName)) {
                    header.SetValue(now);
                }
            }
        }
        return attempt;
    }

    void StartAttempt() {
        ++m_attempt;
        HttpRequest attempt = BuildAttemptRequest();

        switch (m_auth.Type) {
        case RequestAuth::Kind::SharedKey:
            ResolveCopySourceAuthorization(attempt, {});
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

        case RequestAuth::Kind::Bearer:
            ResolveCopySourceAuthorization(attempt, m_auth.BearerToken);
            attempt.AddHeader(HttpHeader{std::string{AuthorizationHeaderName}, "Bearer " + m_auth.BearerToken});
            Send(std::move(attempt));
            return;

        case RequestAuth::Kind::None:
            ResolveCopySourceAuthorization(attempt, {});
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

        std::erase_if(attempt.GetHeaders(),
                      [](const HttpHeader& header) { return IEquals(header.GetName(), AuthorizationHeaderName); });
        ResolveCopySourceAuthorization(attempt, token.Token);
        attempt.AddHeader(HttpHeader{std::string{AuthorizationHeaderName}, "Bearer " + token.Token});
        Send(std::move(attempt));
    }

    void Send(HttpRequest attempt) {
        HttpRequestOptions options = m_requestOptions;
        m_attemptSignal.slot().clear();
        if (m_totalDeadline) {
            const auto remaining = *m_totalDeadline - std::chrono::duration_cast<std::chrono::milliseconds>(
                                                          std::chrono::steady_clock::now() - m_start);
            options.SetTimeout(std::min(options.GetTimeout(), std::max(remaining, std::chrono::milliseconds{1})));
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

    void OnAttemptCompleteOnStrand(std::error_code error, HttpResponse response) {
        if (m_cancelled) {
            Finish(CancelError(), std::move(response));
            return;
        }

        if (m_attempt >= m_maxAttempts ||
            !(IsRetriableFailure(error, response) || IsRetriableNotFoundOrGone(error, response))) {
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

        m_timer.expires_after(delay);
        auto self = this->shared_from_this();
        m_timer.async_wait([self](const boost::system::error_code& waitError) {
            self->Run(BackoffElapsedEvent{static_cast<bool>(waitError)});
        });
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
        m_deadlineTimer.cancel();
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
    HttpRequestOptions m_requestOptions;
    bool m_retryNotFoundAndGone;
    boost::asio::cancellation_slot m_parentSlot;
    boost::asio::cancellation_signal m_attemptSignal;
    boost::asio::steady_timer m_timer;
    boost::asio::steady_timer m_deadlineTimer;
    std::optional<std::chrono::milliseconds> m_totalDeadline;
    std::chrono::steady_clock::time_point m_start;
    std::mutex m_queueMutex;
    std::deque<Event> m_queue;
    bool m_draining = false;
    int m_maxAttempts = 1;
    int m_attempt = 0;
    std::atomic<bool> m_initiating{false};
    std::atomic<bool> m_awaitingToken{false};
    std::atomic<bool> m_cancelled{false};
    std::atomic<bool> m_finished{false};
    bool m_timedOut = false;
};
} // namespace RetryDetail

template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler>
void SendWithRetryAsync(THttp& httpClient, RequestAuth auth, std::shared_ptr<TCred> credential, RetryOptions retry,
                        HttpRequest request, THandler completion, HttpRequestOptions requestOptions,
                        bool retryNotFoundAndGone) {
    auto operation = std::make_shared<RetryDetail::RetryOperation<THttp, TCred, THandler>>(
        httpClient, std::move(auth), std::move(credential), retry, std::move(request), std::move(completion),
        requestOptions, retryNotFoundAndGone);
    operation->Start();
}

template <HttpClientLike THttp, TokenCredentialLike TCred, class THandler>
void SendAuthorizedRequestAsync(THttp& httpClient, const ConnectionState& connection, std::shared_ptr<TCred> credential,
                                HttpRequest request, THandler completion, HttpRequestOptions requestOptions) {
    // Checked here rather than per operation so every request carrying metadata, including the
    // internal commits issued by multi-part uploads, is rejected before reaching the service.
    for (const HttpHeader& header : request.GetHeaders()) {
        const std::string_view name = header.GetName();
        if (HasHeaderControlCharacters(name) || HasHeaderControlCharacters(header.GetValue())) {
            PostCompletion(httpClient, std::move(completion), std::make_error_code(std::errc::invalid_argument),
                           HttpResponse{});
            return;
        }
        if (IStartsWith(name, XMsMetaHeaderPrefix) && !IsValidMetadataName(name.substr(XMsMetaHeaderPrefix.size()))) {
            PostCompletion(httpClient, std::move(completion), std::error_code{BlobStorageErrorCode::InvalidMetadata},
                           HttpResponse{});
            return;
        }
    }
    const bool hasCredential = credential != nullptr;
    SendWithRetryAsync(httpClient, MakeRequestAuth(connection, hasCredential), std::move(credential), connection.Retry,
                       std::move(request), std::move(completion), requestOptions, false);
}
} // namespace AVEVA::AzureClient::Private
