// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/ConnectionPool.hpp"
#include "private/IdleSweeper.hpp"
#include "private/OriginLimiter.hpp"
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

        using Private::IdleSweeper;
        using Private::OriginLimiter;

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
                if (Private::ToBeastVerb(method) == boost::beast::http::verb::unknown || method == HttpMethod::Connect ||
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
