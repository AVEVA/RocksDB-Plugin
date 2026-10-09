// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "AVEVA/AzureClient/ITokenCredential.hpp"
#include "BlobRequestHelpers.hpp"
#include "ProtocolConstants.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpClientError.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <limits>
#include <atomic>
#include <boost/system/error_code.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string_view>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient::Private
{
    namespace
    {
        [[nodiscard]] std::optional<std::uint64_t> ParseHeaderUnsigned(std::string_view value) noexcept
        {
            while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
            {
                value.remove_prefix(1);
            }
            while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
            {
                value.remove_suffix(1);
            }
            std::uint64_t parsed = 0;
            const auto [ptr, ec] =
                std::from_chars(std::to_address(value.begin()), std::to_address(value.end()), parsed);
            if (value.empty() || ec != std::errc{} || ptr != std::to_address(value.end()))
            {
                return std::nullopt;
            }
            return parsed;
        }

        // Backoff jitter and exponent bounds live in RetryPolicyConstants.
        constexpr double MinBackoffJitter = RetryPolicyConstants::MinBackoffJitter;
        constexpr double MaxBackoffJitter = RetryPolicyConstants::MaxBackoffJitter;
        constexpr int MaxBackoffExponent = RetryPolicyConstants::MaxBackoffExponent;

        // Exponential backoff (InitialDelay * 2^(retry-1)) with +/- jitter in [0.8, 1.3), capped at MaxDelay.
        [[nodiscard]] std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber)
        {
            thread_local std::mt19937_64 generator{std::random_device{}()};
            std::uniform_real_distribution<double> jitter(MinBackoffJitter, MaxBackoffJitter);

            const double maxDelay = static_cast<double>(std::max<std::int64_t>(0, retry.MaxDelay.count()));
            const double initial = static_cast<double>(std::max<std::int64_t>(0, retry.InitialDelay.count()));
            const double exponential = initial * std::ldexp(1.0, std::clamp(retryNumber - 1, 0, MaxBackoffExponent));
            const double delay = std::min(exponential * jitter(generator), maxDelay);
            return std::chrono::milliseconds{static_cast<std::int64_t>(std::llround(delay))};
        }

        [[nodiscard]] std::error_code CanceledError() noexcept
        {
            return std::make_error_code(std::errc::operation_canceled);
        }

        class RetryOperation final : public std::enable_shared_from_this<RetryOperation>
        {
          public:
            RetryOperation(IHttpClient& httpClient,
                RequestAuth auth,
                RetryOptions retry,
                HttpRequest request,
                IHttpClient::CompletionHandler completion,
                HttpRequestOptions requestOptions,
                bool retryNotFoundAndGone)
                : m_httpClient(httpClient), m_auth(std::move(auth)), m_retry(retry), m_request(std::move(request)),
                  m_completion(std::move(completion)), m_requestOptions(requestOptions),
                  m_retryNotFoundAndGone(retryNotFoundAndGone), m_parentSlot(m_requestOptions.GetCancellationSlot()),
                  m_timer(httpClient.get_executor()),
                  m_maxAttempts(retry.MaxRetries >= std::numeric_limits<int>::max() - 1
                                    ? std::numeric_limits<int>::max()
                                    : std::max(0, retry.MaxRetries) + 1)
            {
            }

            void Start()
            {
                if (m_parentSlot.is_connected())
                {
                    std::weak_ptr<RetryOperation> const weak = weak_from_this();
                    m_parentSlot.assign([weak](boost::asio::cancellation_type_t type)
                    {
                        if (auto self = weak.lock())
                        {
                            self->OnCancel(type);
                        }
                    });
                }

                m_initiating.store(true, std::memory_order_release);
                Run([this]
                {
                    StartAttempt();
                });
                m_initiating.store(false, std::memory_order_release);
            }

          private:
            // Serializes all state mutation: transport, timer, token and cancellation callbacks may
            // arrive from different threads. Runs inline when idle (so initiation stays synchronous,
            // and nothing is allocated or queued) and queues work that arrives while another thread,
            // or an outer call, is running.
            template <typename F> void Run(F&& work)
            {
                {
                    const std::scoped_lock lock(m_queueMutex);
                    if (m_draining)
                    {
                        m_queue.emplace_back(std::forward<F>(work));
                        return;
                    }
                    m_draining = true;
                }

                const auto keepAlive = shared_from_this();
                std::forward<F>(work)();
                for (;;)
                {
                    std::move_only_function<void()> next;
                    {
                        const std::scoped_lock lock(m_queueMutex);
                        if (m_queue.empty())
                        {
                            m_draining = false;
                            return;
                        }
                        next = std::move(m_queue.front());
                        m_queue.pop_front();
                    }
                    next();
                }
            }

            void OnCancel(boost::asio::cancellation_type_t type)
            {
                if (m_finished.load(std::memory_order_acquire))
                {
                    return;
                }
                m_cancelled.store(true, std::memory_order_release);
                Run([this, type]
                {
                    OnCancelOnStrand(type);
                });
            }

            void OnCancelOnStrand(boost::asio::cancellation_type_t type)
            {
                if (m_finished.load(std::memory_order_acquire))
                {
                    return;
                }

                m_timer.cancel();
                m_attemptSignal.emit(type);

                // ITokenCredential has no cancellation hook, so don't wait for a slow credential:
                // complete now and ignore the token when it eventually arrives.
                if (m_awaitingToken.load(std::memory_order_acquire))
                {
                    PostFinish(CanceledError());
                }
            }

            [[nodiscard]] HttpRequest BuildAttemptRequest()
            {
                if (m_maxAttempts == 1)
                {
                    return std::move(m_request);
                }

                HttpRequest attempt;
                attempt.SetMethod(m_request.GetMethod());
                attempt.SetUrl(m_request.GetUrl());
                attempt.SetHeaders(m_request.GetHeaders());
                if (m_request.HasBodyView())
                {
                    attempt.SetBodyView(m_request.GetBodyView(), m_request.GetBodyKeepAlive());
                }
                else
                {
                    // An owned body is moved into shared storage once; every attempt then views it instead of
                    // copying it, and the HTTP operation of a timed-out attempt keeps it alive until it is done.
                    if (!m_sharedBody && !m_request.GetBody().empty())
                    {
                        m_sharedBody = std::make_shared<const std::string>(m_request.ReleaseBody());
                    }
                    if (m_sharedBody)
                    {
                        attempt.SetBodyView(std::as_bytes(std::span{m_sharedBody->data(), m_sharedBody->size()}),
                            m_sharedBody);
                    }
                }

                if (m_attempt > 1)
                {
                    const std::string now = BuildDateHeaderValue(std::chrono::system_clock::now());
                    for (HttpHeader& header : attempt.GetHeaders())
                    {
                        if (IEquals(header.GetName(), XMsDateHeaderName))
                        {
                            header.SetValue(now);
                        }
                    }
                }
                return attempt;
            }

            void StartAttempt()
            {
                ++m_attempt;
                HttpRequest attempt = BuildAttemptRequest();

                switch (m_auth.Type)
                {
                case RequestAuth::Kind::SharedKey:
                    try
                    {
                        AuthorizeRequest(*m_auth.Signer, attempt);
                    }
                    catch (const std::bad_alloc&)
                    {
                        PostFinish(std::make_error_code(std::errc::not_enough_memory));
                        return;
                    }
                    catch (const std::system_error& e)
                    {
                        PostFinish(e.code());
                        return;
                    }
                    catch (const std::exception&)
                    {
                        PostFinish(std::make_error_code(std::errc::invalid_argument));
                        return;
                    }
                    Send(std::move(attempt));
                    return;

                case RequestAuth::Kind::Token:
                    RequestToken(std::move(attempt));
                    return;

                case RequestAuth::Kind::None:
                    break;
                }

                Send(std::move(attempt));
            }

            void RequestToken(HttpRequest attempt)
            {
                auto self = shared_from_this();
                m_awaitingToken.store(true, std::memory_order_release);
                m_auth.TokenCredential->GetTokenForScopesAsync(m_auth.TokenScopes,
                    [self, attempt = std::move(attempt)](std::error_code error, AccessToken token) mutable
                {
                    auto proceed = [self, error, token = std::move(token), attempt = std::move(attempt)]() mutable
                    {
                        self->OnToken(error, std::move(token), std::move(attempt));
                    };

                    // Never complete inside the caller's initiating function, even if the
                    // credential answered synchronously.
                    if (self->m_initiating.load(std::memory_order_acquire))
                    {
                        boost::asio::post(self->m_httpClient.get_executor(), std::move(proceed));
                    }
                    else
                    {
                        proceed();
                    }
                });
            }

            void OnToken(std::error_code error, AccessToken token, HttpRequest attempt)
            {
                Run([this, error, token = std::move(token), attempt = std::move(attempt)]() mutable
                {
                    OnTokenOnStrand(error, token, std::move(attempt));
                });
            }

            void OnTokenOnStrand(std::error_code error, const AccessToken& token, HttpRequest attempt)
            {
                m_awaitingToken.store(false, std::memory_order_release);
                if (m_finished.load(std::memory_order_acquire))
                {
                    return;
                }
                if (m_cancelled.load(std::memory_order_acquire))
                {
                    Finish(CanceledError(), HttpResponse{});
                    return;
                }
                if (error)
                {
                    Finish(error, HttpResponse{});
                    return;
                }

                std::erase_if(attempt.GetHeaders(),
                    [](const HttpHeader& header)
                {
                    return IEquals(header.GetName(), AuthorizationHeaderName);
                });
                attempt.AddHeader(HttpHeader{std::string{AuthorizationHeaderName}, "Bearer " + token.Token});
                Send(std::move(attempt));
            }

            void Send(HttpRequest attempt)
            {
                HttpRequestOptions options = m_requestOptions;
                m_attemptSignal.slot().clear();
                options.SetCancellationSlot(
                    m_parentSlot.is_connected() ? m_attemptSignal.slot() : boost::asio::cancellation_slot{});

                auto self = shared_from_this();
                m_httpClient.SendAsync(std::move(attempt),
                    [self](std::error_code error, HttpResponse response)
                {
                    // A transport that completes inside SendAsync must not complete the caller's
                    // operation inside its initiating function.
                    if (self->m_initiating.load(std::memory_order_acquire))
                    {
                        boost::asio::post(self->m_httpClient.get_executor(),
                            [self, error, response = std::move(response)]() mutable
                        {
                            self->OnAttemptComplete(error, std::move(response));
                        });
                        return;
                    }
                    self->OnAttemptComplete(error, std::move(response));
                },
                    options);
            }

            void OnAttemptComplete(std::error_code error, HttpResponse response)
            {
                Run([this, error, response = std::move(response)]() mutable
                {
                    OnAttemptCompleteOnStrand(error, std::move(response));
                });
            }

            [[nodiscard]] bool IsRetriableNotFoundOrGone(std::error_code error,
                const HttpResponse& response) const noexcept
            {
                constexpr unsigned int NotFound = 404;
                constexpr unsigned int Gone = 410;
                return m_retryNotFoundAndGone && !error &&
                       (response.GetStatus() == NotFound || response.GetStatus() == Gone);
            }

            void OnAttemptCompleteOnStrand(std::error_code error, HttpResponse response)
            {
                if (m_cancelled)
                {
                    Finish(CanceledError(), std::move(response));
                    return;
                }

                if (m_attempt >= m_maxAttempts ||
                    !(IsRetriableFailure(error, response) || IsRetriableNotFoundOrGone(error, response)))
                {
                    Finish(error, std::move(response));
                    return;
                }

                // A server hint may exceed MaxDelay (the service knows when it will recover) but is capped so a
                // bogus value cannot stall a request for hours.
                constexpr std::chrono::milliseconds maxHint{std::chrono::minutes{1}};
                std::chrono::milliseconds delay;
                if (const auto hint = ParseRetryAfter(response); hint.has_value())
                {
                    delay = std::min(*hint, std::max(m_retry.MaxDelay, maxHint));
                }
                else
                {
                    delay = std::clamp(ComputeBackoff(m_retry, m_attempt),
                        std::chrono::milliseconds{0},
                        std::max(m_retry.MaxDelay, std::chrono::milliseconds{0}));
                }

                m_timer.expires_after(delay);
                auto self = shared_from_this();
                m_timer.async_wait([self](const boost::system::error_code& waitError)
                {
                    self->Run([self, waitError]
                    {
                        if (self->m_cancelled || waitError)
                        {
                            self->Finish(CanceledError(), HttpResponse{});
                            return;
                        }
                        self->StartAttempt();
                    });
                });
            }

            void PostFinish(std::error_code error)
            {
                auto self = shared_from_this();
                boost::asio::post(m_httpClient.get_executor(),
                    [self, error]()
                {
                    self->Run([self, error]
                    {
                        self->Finish(error, HttpResponse{});
                    });
                });
            }

            void Finish(std::error_code error, HttpResponse response)
            {
                if (m_finished.exchange(true, std::memory_order_acq_rel))
                {
                    return;
                }

                // The parent slot is deliberately not cleared here: clearing on the transport thread would race with
                // the caller emitting on its own executor. Its handler only holds a weak_ptr and is inert once
                // finished; the caller's slot is cleared on its executor when the completion is dispatched.
                IHttpClient::CompletionHandler completion = std::move(m_completion);
                completion(error, std::move(response));
            }

            IHttpClient& m_httpClient;
            RequestAuth m_auth;
            RetryOptions m_retry;
            HttpRequest m_request;
            std::shared_ptr<const std::string> m_sharedBody;
            IHttpClient::CompletionHandler m_completion;
            HttpRequestOptions m_requestOptions;
            bool m_retryNotFoundAndGone;
            boost::asio::cancellation_slot m_parentSlot;
            boost::asio::cancellation_signal m_attemptSignal;
            boost::asio::steady_timer m_timer;
            std::mutex m_queueMutex;
            std::deque<std::move_only_function<void()>> m_queue;
            bool m_draining = false;
            int m_maxAttempts = 1;
            int m_attempt = 0;
            std::atomic<bool> m_initiating{false};
            std::atomic<bool> m_awaitingToken{false};
            std::atomic<bool> m_cancelled{false};
            std::atomic<bool> m_finished{false};
        };
    } // namespace

    bool IsRetriableTransportError(std::error_code error) noexcept
    {
        return error == HttpClientError::ResolveFailed || error == HttpClientError::ConnectFailed ||
               error == HttpClientError::WriteFailed || error == HttpClientError::ReadFailed ||
               error == HttpClientError::TimedOut || error == std::errc::timed_out ||
               error == std::errc::connection_reset || error == std::errc::connection_aborted ||
               error == std::errc::connection_refused;
    }

    bool IsRetriableStatus(unsigned int status) noexcept
    {
        switch (status)
        {
        case HttpStatusRequestTimeout:
        case HttpStatusTooManyRequests:
        case HttpStatusInternalServerError:
        case HttpStatusBadGateway:
        case HttpStatusServiceUnavailable:
        case HttpStatusGatewayTimeout:
            return true;
        default:
            return false;
        }
    }

    bool IsRetriableFailure(std::error_code error, const HttpResponse& response) noexcept
    {
        return error ? IsRetriableTransportError(error) : IsRetriableStatus(response.GetStatus());
    }

    std::optional<std::chrono::milliseconds> ParseRetryAfter(const HttpResponse& response) noexcept
    {
        constexpr std::uint64_t MillisecondsPerSecond = 1000ULL;
        constexpr std::uint64_t MaxMilliseconds = 24ULL * 60ULL * 60ULL * 1000ULL;
        if (const auto ms = ParseHeaderUnsigned(FindHeaderValue(response, "x-ms-retry-after-ms")); ms.has_value())
        {
            return std::chrono::milliseconds{
                static_cast<std::chrono::milliseconds::rep>(std::min<std::uint64_t>(*ms, MaxMilliseconds))};
        }
        if (const auto seconds = ParseHeaderUnsigned(FindHeaderValue(response, "Retry-After")); seconds.has_value())
        {
            return std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(
                std::min<std::uint64_t>(*seconds, MaxMilliseconds / MillisecondsPerSecond) * MillisecondsPerSecond)};
        }
        // The HTTP-date form; a date in the past means "retry now".
        if (const auto date = ParseHttpDateHeader(FindHeaderValue(response, "Retry-After")); date.has_value())
        {
            const auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(*date - std::chrono::system_clock::now());
            return std::clamp(wait,
                std::chrono::milliseconds{0},
                std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(MaxMilliseconds)});
        }
        return std::nullopt;
    }

    void SendWithRetryAsync(IHttpClient& httpClient,
        RequestAuth auth,
        RetryOptions retry,
        HttpRequest request,
        IHttpClient::CompletionHandler completion,
        HttpRequestOptions requestOptions,
        bool retryNotFoundAndGone)
    {
        auto operation = std::make_shared<RetryOperation>(httpClient,
            std::move(auth),
            retry,
            std::move(request),
            std::move(completion),
            requestOptions,
            retryNotFoundAndGone);
        operation->Start();
    }
} // namespace AVEVA::AzureClient::Private

namespace AVEVA::AzureClient
{
    bool IsTransient(const BlobStorageError& error) noexcept
    {
        if (error.StatusCode != 0U)
        {
            return Private::IsRetriableStatus(error.StatusCode);
        }
        return error.Code == BlobStorageErrorCode::ServerBusy ||
               error.Code == BlobStorageErrorCode::OperationTimedOut ||
               error.Code == BlobStorageErrorCode::InternalError || Private::IsRetriableTransportError(error.Code);
    }
} // namespace AVEVA::AzureClient
