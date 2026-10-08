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
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

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
                  m_interval(std::clamp(idleTimeout, std::chrono::seconds{1}, std::chrono::seconds{std::chrono::hours{24 * 365}})),
                  m_thread([this](std::stop_token stop)
            {
                Run(stop);
            })
            {
            }

            IdleSweeper(const IdleSweeper&) = delete;
            IdleSweeper& operator=(const IdleSweeper&) = delete;

            ~IdleSweeper()
            {
                m_thread.request_stop();
                m_wake.notify_all();
            }

            void Notify()
            {
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    m_hasIdle = true;
                }
                m_wake.notify_all();
            }

          private:
            void Run(std::stop_token stop)
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                while (!stop.stop_requested())
                {
                    if (!m_wake.wait(lock, stop, [this] { return m_hasIdle; }))
                    {
                        return;
                    }
                    m_hasIdle = false;
                    lock.unlock();
                    // Sweep after the interval; keep going while either pool still holds connections.
                    bool remaining = true;
                    while (remaining && !stop.stop_requested())
                    {
                        std::unique_lock<std::mutex> sleepLock(m_sleepMutex);
                        if (m_wake.wait_for(sleepLock, stop, m_interval, [] { return false; }) || stop.stop_requested())
                        {
                            break;
                        }
                        remaining = m_plainSweep() + m_tlsSweep() > 0;
                    }
                    lock.lock();
                }
            }

            SweepFunction m_plainSweep;
            SweepFunction m_tlsSweep;
            std::chrono::seconds m_interval;
            std::mutex m_mutex;
            std::mutex m_sleepMutex;
            std::condition_variable_any m_wake;
            bool m_hasIdle = false;
            std::jthread m_thread; // Declared last so every other member exists before the thread starts.
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

            void SendAsyncErased(HttpRequest request, CompletionHandler completion, HttpRequestOptions options) override
            {
                if (!completion)
                {
                    throw std::invalid_argument("AsyncSend requires a completion handler");
                }

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
                                key,
                                std::move(request),
                                std::move(completion),
                                options,
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
                                key,
                                std::move(request),
                                std::move(completion),
                                options,
                                m_plainPool);
                        }
                    }
                }

                auto executor = asio::make_strand(context_);
                auto innerParsed = urls::parse_uri(request.GetUrl());
                if (innerParsed && innerParsed->scheme_id() == urls::scheme::https)
                {
                    TlsConnectionKey key{innerParsed->host_address(),
                        innerParsed->has_port() ? std::string(innerParsed->port()) : "443"};
                    auto stream = std::make_unique<TlsStream>(executor, *m_tlsContext);
                    auto operation = std::make_shared<RequestOperation<TlsStream>>(m_tlsContext,
                        std::move(stream),
                        std::move(completion),
                        options,
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
                    options,
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
                    options,
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
            std::shared_ptr<IdleSweeper> m_sweeper; // Last: destroyed (and joined) before the pools it sweeps.
        };
    } // namespace

    std::unique_ptr<IHttpClient> IHttpClient::Create(boost::asio::io_context& context, HttpClientOptions options)
    {
        return std::make_unique<HttpClient>(context, options);
    }
} // namespace AVEVA
