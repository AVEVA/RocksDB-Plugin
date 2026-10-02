#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/ConnectionPool.hpp"
#include "private/RequestOperation.hpp"
#include "private/StreamTypes.hpp"
#include "private/TlsContextConfigurator.hpp"

#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/url.hpp>

#include <memory>
#include <stdexcept>
#include <string>
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

        class HttpClient final : public IHttpClient
        {
          public:
            HttpClient(asio::io_context& runtime, const HttpClientOptions& options)
                : context_(runtime), m_tlsContext(std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client)),
                  m_plainPool(std::make_shared<ConnectionPool<PlainStream>>(options.GetMaxIdleConnectionsPerHost(),
                      options.GetIdleConnectionTimeout())),
                  m_tlsPool(std::make_shared<ConnectionPool<TlsStream>>(options.GetMaxIdleConnectionsPerHost(),
                      options.GetIdleConnectionTimeout()))
            {
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

                auto parsed = urls::parse_uri(request.GetUrl());
                if (parsed && parsed->has_authority() && !parsed->host().empty() &&
                    (parsed->scheme_id() == urls::scheme::http || parsed->scheme_id() == urls::scheme::https))
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
        };
    } // namespace

    std::unique_ptr<IHttpClient> IHttpClient::Create(boost::asio::io_context& context, HttpClientOptions options)
    {
        return std::make_unique<HttpClient>(context, options);
    }
} // namespace AVEVA
