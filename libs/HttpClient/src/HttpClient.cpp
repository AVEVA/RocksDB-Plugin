// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/ConnectionPool.hpp"
#include "private/RequestOperation.hpp"
#include "private/RequestValidation.hpp"
#include "private/StreamTypes.hpp"
#include "private/TlsContextConfigurator.hpp"

#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/url.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace AVEVA
{
    IHttpClient::~IHttpClient() = default;

    namespace
    {
        using Private::BasicConnectionKey;
        using Private::ConnectionKey;
        using Private::ConnectionPool;
        using Private::PlainStream;
        using Private::PooledConnection;
        using Private::RequestOperation;
        using Private::TlsConnectionKey;
        using Private::TlsStream;
        namespace asio = Private::asio;
        namespace urls = Private::urls;

        // Closes expired idle pooled sockets even when no request arrives. It sleeps until a pool gains an idle
        // connection, and goes back to sleep once both pools are empty. A dedicated thread is used instead of an
        // io_context timer so a pending timer cannot keep the caller's io_context::run() from returning.
        class IdleSweeper
        {
          public:
            using SweepFunction = std::function<std::size_t()>;

            IdleSweeper(SweepFunction plainSweep, SweepFunction tlsSweep, std::chrono::seconds idleTimeout)
                : m_plainSweep(std::move(plainSweep)), m_tlsSweep(std::move(tlsSweep)),
                  m_interval(std::clamp(idleTimeout, std::chrono::seconds{1}, std::chrono::seconds{std::chrono::hours{24 * 365}}))
            {
            }

            IdleSweeper(const IdleSweeper&) = delete;
            IdleSweeper& operator=(const IdleSweeper&) = delete;

            // Called when a pool gains an idle connection; schedules a sweep unless one is already pending.
            void Notify()
            {
                if (!m_scheduled.exchange(true))
                {
                    SweepScheduler::Instance().Schedule(m_self, m_interval);
                }
            }

            // Runs on the scheduler thread. The flag is cleared before sweeping so a release that lands during the
            // sweep schedules itself instead of being lost.
            void RunSweep()
            {
                m_scheduled = false;
                if (m_plainSweep() + m_tlsSweep() > 0 && !m_scheduled.exchange(true))
                {
                    SweepScheduler::Instance().Schedule(m_self, m_interval);
                }
            }

            // Must be called once the sweeper is owned by a shared_ptr.
            void BindSelf(const std::shared_ptr<IdleSweeper>& self)
            {
                m_self = self;
            }

          private:
            // One thread for the whole process, started on first use, that runs every client's sweeps in deadline
            // order. A per-client thread would cost a stack for each of many short-lived clients.
            class SweepScheduler
            {
              public:
                static SweepScheduler& Instance()
                {
                    static SweepScheduler scheduler;
                    return scheduler;
                }

                void Schedule(const std::weak_ptr<IdleSweeper>& sweeper, std::chrono::seconds delay)
                {
                    {
                        std::lock_guard<std::mutex> lock(m_mutex);
                        m_queue.emplace(Clock::now() + delay, sweeper);
                        if (!m_thread.joinable())
                        {
                            m_thread = std::jthread([this](std::stop_token stop)
                            {
                                Run(stop);
                            });
                        }
                    }
                    m_wake.notify_all();
                }

                ~SweepScheduler()
                {
                    m_thread.request_stop();
                    m_wake.notify_all();
                }

              private:
                using Clock = std::chrono::steady_clock;

                void Run(std::stop_token stop)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    while (!stop.stop_requested())
                    {
                        if (m_queue.empty())
                        {
                            m_wake.wait(lock, stop, [this] { return !m_queue.empty(); });
                            continue;
                        }
                        const auto due = m_queue.begin()->first;
                        if (Clock::now() < due)
                        {
                            m_wake.wait_until(lock, stop, due, [] { return false; });
                            continue;
                        }
                        auto sweeper = m_queue.begin()->second.lock();
                        m_queue.erase(m_queue.begin());
                        if (!sweeper)
                        {
                            continue;
                        }
                        lock.unlock();
                        try
                        {
                            sweeper->RunSweep();
                        }
                        catch (...)
                        {
                            // A failing sweep must not end the thread that serves every other client.
                        }
                        sweeper.reset();
                        lock.lock();
                    }
                }

                std::mutex m_mutex;
                std::condition_variable_any m_wake;
                std::multimap<Clock::time_point, std::weak_ptr<IdleSweeper>> m_queue;
                std::jthread m_thread; // Declared last so every other member exists before the thread starts.
            };

            SweepFunction m_plainSweep;
            SweepFunction m_tlsSweep;
            std::chrono::seconds m_interval;
            std::atomic<bool> m_scheduled{false};
            std::weak_ptr<IdleSweeper> m_self;
        };

        // Caps in-flight requests per origin. Requests over the cap wait in FIFO order and start as earlier ones
        // finish. A waiting request can still be cancelled, and Close() fails whatever is left when the client dies.
        class OriginLimiter : public std::enable_shared_from_this<OriginLimiter>
        {
          public:
            using Start = std::move_only_function<void()>;
            using Abort = std::move_only_function<void(std::error_code)>;

            explicit OriginLimiter(std::size_t maxPerOrigin) : m_max(maxPerOrigin)
            {
            }

            std::uint64_t NextTicket() noexcept
            {
                return ++m_nextTicket;
            }

            // Runs `start` now if the origin has capacity, otherwise queues it under `ticket` (see Cancel).
            void Run(const std::string& origin, std::uint64_t ticket, Start start, Abort abort)
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                auto& state = m_origins[origin];
                if (state.active < m_max)
                {
                    ++state.active;
                    lock.unlock();
                    start();
                    return;
                }
                state.waiters.push_back(Waiter{ticket, std::move(start), std::move(abort)});
            }

            // Hands the finished request's slot to the next waiter, or frees it.
            void Release(const std::string& origin)
            {
                Start next;
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    const auto it = m_origins.find(origin);
                    if (it == m_origins.end())
                    {
                        return;
                    }
                    if (it->second.waiters.empty())
                    {
                        if (--it->second.active == 0)
                        {
                            m_origins.erase(it);
                        }
                        return;
                    }
                    next = std::move(it->second.waiters.front().start);
                    it->second.waiters.pop_front();
                }
                next();
            }

            // Removes a queued request and fails it; a no-op if it already started.
            void Cancel(const std::string& origin, std::uint64_t ticket)
            {
                Abort abort;
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    const auto it = m_origins.find(origin);
                    if (it == m_origins.end())
                    {
                        return;
                    }
                    auto& waiters = it->second.waiters;
                    const auto waiter = std::ranges::find(waiters, ticket, &Waiter::ticket);
                    if (waiter == waiters.end())
                    {
                        return;
                    }
                    abort = std::move(waiter->abort);
                    waiters.erase(waiter);
                }
                abort(std::make_error_code(std::errc::operation_canceled));
            }

            void Close()
            {
                std::vector<Abort> aborts;
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    for (auto& [origin, state] : m_origins)
                    {
                        for (auto& waiter : state.waiters)
                        {
                            aborts.push_back(std::move(waiter.abort));
                        }
                        state.waiters.clear();
                    }
                }
                for (auto& abort : aborts)
                {
                    abort(std::make_error_code(std::errc::operation_canceled));
                }
            }

          private:
            struct Waiter
            {
                std::uint64_t ticket;
                Start start;
                Abort abort;
            };

            struct OriginState
            {
                std::size_t active = 0;
                std::deque<Waiter> waiters;
            };

            std::size_t m_max;
            std::mutex m_mutex;
            std::atomic<std::uint64_t> m_nextTicket{0};
            std::unordered_map<std::string, OriginState> m_origins;
        };

        class HttpClient final : public IHttpClient
        {
          public:
            HttpClient(asio::io_context& runtime, const HttpClientOptions& options)
                : context_(runtime), m_tlsContext(std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client)),
                  m_plainPool(std::make_shared<ConnectionPool<PlainStream>>(options.GetMaxIdleConnectionsPerHost(),
                      options.GetIdleConnectionTimeout())),
                  m_tlsPool(std::make_shared<ConnectionPool<TlsStream>>(options.GetMaxIdleConnectionsPerHost(),
                      options.GetIdleConnectionTimeout())),
                  m_sweeper(std::make_shared<IdleSweeper>(
                      [pool = m_plainPool] { return pool->Sweep(); },
                      [pool = m_tlsPool] { return pool->Sweep(); },
                      options.GetIdleConnectionTimeout()))
            {
                if (options.GetMaxConnectionsPerHost() > 0)
                {
                    m_limiter = std::make_shared<OriginLimiter>(options.GetMaxConnectionsPerHost());
                }
                m_sweeper->BindSelf(m_sweeper);
                // Weak, because a pool can outlive the client while in-flight requests still hold it.
                const auto notify = [weak = std::weak_ptr{m_sweeper}]
                {
                    if (auto sweeper = weak.lock())
                    {
                        sweeper->Notify();
                    }
                };
                m_plainPool->SetOnBecameNonEmpty(notify);
                m_tlsPool->SetOnBecameNonEmpty(notify);
                m_tlsContext->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                                          asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                                          asio::ssl::context::no_tlsv1_1);
                Private::ConfigureTlsContext(*m_tlsContext, options);
            }

            executor_type get_executor() const override
            {
                return asio::any_io_executor(context_.get_executor());
            }

            ~HttpClient() override
            {
                if (m_limiter)
                {
                    m_limiter->Close();
                }
            }

            void SendAsyncErased(HttpRequest request, CompletionHandler completion, HttpRequestOptions options) override
            {
                if (!completion)
                {
                    throw std::invalid_argument("AsyncSend requires a completion handler");
                }
                if (const auto origin = m_limiter ? OriginOf(request.GetUrl()) : std::nullopt)
                {
                    return SendLimited(*origin, std::move(request), std::move(completion), std::move(options));
                }
                SendUnlimited(std::move(request), std::move(completion), std::move(options));
            }

          private:
            // Unparseable URLs bypass the cap; they fail fast and never open a connection.
            static std::optional<std::string> OriginOf(std::string_view url)
            {
                const auto parsed = urls::parse_uri(url);
                if (!parsed)
                {
                    return std::nullopt;
                }
                const bool isTls = parsed->scheme_id() == urls::scheme::https;
                return std::string(parsed->scheme()) + "://" + std::string(parsed->host_address()) + ':' +
                       (parsed->has_port() ? std::string(parsed->port()) : (isTls ? "443" : "80"));
            }

            void SendLimited(std::string origin, HttpRequest request, CompletionHandler completion, HttpRequestOptions options)
            {
                struct Pending
                {
                    HttpRequest request;
                    CompletionHandler completion;
                    HttpRequestOptions options;
                };
                auto pending = std::make_shared<Pending>(Pending{std::move(request), std::move(completion), std::move(options)});
                const auto ticket = m_limiter->NextTicket();
                // Set before queueing so a cancellation cannot slip in between; a started request replaces it with
                // its own handler.
                if (auto slot = pending->options.GetCancellationSlot(); slot.is_connected())
                {
                    slot.assign([weak = std::weak_ptr{m_limiter}, origin, ticket](asio::cancellation_type type)
                    {
                        if (type != asio::cancellation_type::none)
                        {
                            if (auto limiter = weak.lock())
                            {
                                limiter->Cancel(origin, ticket);
                            }
                        }
                    });
                }
                m_limiter->Run(origin,
                    ticket,
                    [this, origin, pending]() mutable
                {
                    auto wrapped = [limiter = m_limiter, origin, inner = std::move(pending->completion)](
                                       std::error_code error, HttpResponse response) mutable
                    {
                        limiter->Release(origin);
                        inner(error, std::move(response));
                    };
                    SendUnlimited(std::move(pending->request), std::move(wrapped), std::move(pending->options));
                },
                    [this, pending](std::error_code error)
                {
                    // Posted because a completion must never run inside the call that triggered it.
                    asio::post(context_, [pending, error]() mutable
                    {
                        pending->completion(error, HttpResponse{});
                    });
                });
            }

            void SendUnlimited(HttpRequest request, CompletionHandler completion, HttpRequestOptions options)
            {
                // Invalid requests take the fresh-connection path, which fails them without closing a healthy
                // pooled connection.
                auto parsed = urls::parse_uri(request.GetUrl());
                if (parsed && IsPoolableRequest(request, options) && Private::IsValidRequestUrl(*parsed))
                {
                    const bool isTls = parsed->scheme_id() == urls::scheme::https;
                    if (isTls)
                    {
                        TlsConnectionKey key{parsed->host_address(),
                            parsed->has_port() ? std::string(parsed->port()) : "443"};
                        if (auto pooled = m_tlsPool->Acquire(key))
                        {
                            return ResumeWithPooledConnection<TlsStream>(std::move(*pooled),
                                std::move(key),
                                std::move(request),
                                std::move(completion),
                                std::move(options),
                                m_tlsPool);
                        }
                    }
                    else
                    {
                        BasicConnectionKey key{parsed->host_address(),
                            parsed->has_port() ? std::string(parsed->port()) : "80"};
                        if (auto pooled = m_plainPool->Acquire(key))
                        {
                            return ResumeWithPooledConnection<PlainStream>(std::move(*pooled),
                                std::move(key),
                                std::move(request),
                                std::move(completion),
                                std::move(options),
                                m_plainPool);
                        }
                    }
                }

                auto executor = asio::make_strand(context_);
                const auto& innerParsed = parsed;
                if (innerParsed && innerParsed->scheme_id() == urls::scheme::https)
                {
                    TlsConnectionKey key{innerParsed->host_address(),
                        innerParsed->has_port() ? std::string(innerParsed->port()) : "443"};
                    auto stream = std::make_unique<TlsStream>(executor, *m_tlsContext);
                    auto operation = std::make_shared<RequestOperation<TlsStream>>(m_tlsContext,
                        std::move(stream),
                        std::move(completion),
                        std::move(options),
                        m_tlsPool,
                        std::move(key));
                    operation->BindCancellationSlot();
                    asio::post(executor, [operation, request = std::move(request)]() mutable
                    {
                        operation->Start(std::move(request));
                    });
                    return;
                }

                BasicConnectionKey key{innerParsed ? innerParsed->host_address() : std::string{},
                    innerParsed && innerParsed->has_port() ? std::string(innerParsed->port()) : "80"};
                auto stream = std::make_unique<PlainStream>(executor);
                auto operation = std::make_shared<RequestOperation<PlainStream>>(m_tlsContext,
                    std::move(stream),
                    std::move(completion),
                    std::move(options),
                    m_plainPool,
                    std::move(key));
                operation->BindCancellationSlot();
                asio::post(executor, [operation, request = std::move(request)]() mutable
                {
                    operation->Start(std::move(request));
                });
            }

          private:
            static bool IsPoolableRequest(const HttpRequest& request, const HttpRequestOptions& options)
            {
                const HttpMethod method = request.GetMethod();
                if (ToString(method).empty() || method == HttpMethod::Connect ||
                    (method == HttpMethod::Trace && request.GetBodySize() != 0) || options.GetTimeout().count() <= 0)
                {
                    return false;
                }
                return std::all_of(request.GetHeaders().begin(),
                    request.GetHeaders().end(),
                    [](const HttpHeader& header)
                {
                    return Private::IsValidHeader(header.GetName(), header.GetValue());
                });
            }

            template <typename Stream>
            void ResumeWithPooledConnection(PooledConnection<Stream> pooled,
                ConnectionKey<Stream> key,
                HttpRequest request,
                CompletionHandler completion,
                HttpRequestOptions options,
                std::shared_ptr<ConnectionPool<Stream>> pool)
            {
                auto executor = pooled.stream->get_executor();
                auto operation = std::make_shared<RequestOperation<Stream>>(m_tlsContext,
                    std::move(pooled.stream),
                    std::move(completion),
                    std::move(options),
                    std::move(pool),
                    std::move(key),
                    std::move(pooled.buffer));
                operation->BindCancellationSlot();
                asio::post(executor,
                    [operation, request = std::move(request)]() mutable
                {
                    operation->StartReused(std::move(request));
                });
            }

            asio::io_context& context_;
            std::shared_ptr<asio::ssl::context> m_tlsContext;
            std::shared_ptr<ConnectionPool<PlainStream>> m_plainPool;
            std::shared_ptr<ConnectionPool<TlsStream>> m_tlsPool;
            std::shared_ptr<OriginLimiter> m_limiter; // Null when no per-origin cap is configured.
            std::shared_ptr<IdleSweeper> m_sweeper; // Last: stops scheduling sweeps before the pools it sweeps go away.
        };
    } // namespace

    std::unique_ptr<IHttpClient> IHttpClient::Create(boost::asio::io_context& context, HttpClientOptions options)
    {
        return std::make_unique<HttpClient>(context, options);
    }
} // namespace AVEVA
