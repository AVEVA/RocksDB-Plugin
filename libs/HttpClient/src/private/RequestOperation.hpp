#pragma once

#include "ConnectionPool.hpp"
#include "RequestValidation.hpp"
#include "StreamTypes.hpp"

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "AVEVA/HttpClient/HttpRequest.hpp"
#include "AVEVA/HttpClient/HttpRequestOptions.hpp"
#include "AVEVA/HttpClient/HttpResponse.hpp"

#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/steady_timer.hpp>

#include <openssl/ssl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

namespace AVEVA::Private
{
    template <typename Stream>
    class RequestOperation final : public std::enable_shared_from_this<RequestOperation<Stream>>
    {
      public:
        RequestOperation(std::shared_ptr<asio::ssl::context> tlsContext,
            std::unique_ptr<Stream> stream,
            IHttpClient::CompletionHandler completion,
            HttpRequestOptions options,
            std::shared_ptr<ConnectionPool<Stream>> pool,
            ConnectionKey<Stream> key,
            beast::flat_buffer buffer = {})
            : m_tlsContext(std::move(tlsContext)), m_executor(stream->get_executor()), m_stream(std::move(stream)),
              m_resolver(m_executor), m_timer(m_executor),
              m_completion(std::move(completion)), m_options(options), m_pool(std::move(pool)),
              m_key(std::move(key)), m_buffer(std::move(buffer))
        {
        }

        void BindCancellationSlot()
        {
            auto slot = m_options.GetCancellationSlot();
            if (!slot.is_connected())
            {
                return;
            }
            // The handler runs on whichever thread emits the signal, so it must only touch immutable or
            // atomic state: it posts to the operation's strand, where Cancel() may safely use the stream.
            slot.assign([weak = this->weak_from_this()](asio::cancellation_type cancellationType)
            {
                if (cancellationType == asio::cancellation_type::none)
                {
                    return;
                }
                if (auto self = weak.lock())
                {
                    self->RequestCancellation();
                }
            });
        }

        // Fresh connection: resolve, connect, (TLS handshake), then send.
        void Start(HttpRequest request)
        {
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            if (!BuildRequest(request))
            {
                return;
            }
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            m_reused = false;
            if constexpr (std::is_same_v<Stream, TlsStream>)
            {
                if (!ConfigureTlsForHost())
                {
                    return;
                }
            }
            ArmTimer();
            m_resolver.async_resolve(m_key.host,
                m_key.service,
                [self = this->shared_from_this()](boost::system::error_code error,
                    Tcp::resolver::results_type results)
            {
                self->OnResolve(error, std::move(results));
            });
        }

        // Reused pooled connection: already connected (and, for TLS, already handshaked).
        void StartReused(HttpRequest request)
        {
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            if (!BuildRequest(request))
            {
                return;
            }
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            m_reused = true;
            ArmTimer();
            Write();
        }

      private:
        bool BuildRequest(const HttpRequest& request)
        {
            auto parsed = urls::parse_uri(request.GetUrl());
            if (!parsed || !parsed->has_authority() || parsed->host().empty() || parsed->has_userinfo() ||
                (parsed->scheme_id() != urls::scheme::http && parsed->scheme_id() != urls::scheme::https) ||
                (parsed->has_port() && parsed->port_number() == 0))
            {
                Fail(HttpClientError::InvalidUrl);
                return false;
            }

            const auto& url = *parsed;
            const std::string host = url.host_address();
            if (std::any_of(host.begin(),
                    host.end(),
                    [](unsigned char character)
            {
                return character <= 32 || character >= 127;
            }))
            {
                Fail(HttpClientError::InvalidUrl);
                return false;
            }
            const std::string method = ToString(request.GetMethod());
            if (method.empty() || request.GetMethod() == HttpMethod::Connect ||
                (request.GetMethod() == HttpMethod::Trace && request.GetBodySize() != 0) ||
                m_options.GetTimeout().count() <= 0)
            {
                Fail(HttpClientError::InvalidRequest);
                return false;
            }

            m_request.version(11);
            m_request.method_string(method);
            std::string target(url.encoded_path());
            if (target.empty())
            {
                target = "/";
            }
            if (url.has_query())
            {
                target += "?";
                target += url.encoded_query();
            }
            m_request.target(target);
            for (const auto& header : request.GetHeaders())
            {
                if (!IsToken(header.GetName()) || std::any_of(header.GetValue().begin(),
                                                      header.GetValue().end(),
                                                      [](unsigned char character)
                {
                    return (character < 32 && character != '\t') || character == 127;
                }))
                {
                    Fail(HttpClientError::InvalidRequest);
                    return false;
                }
                if (beast::iequals(header.GetName(), "Host") ||
                    beast::iequals(header.GetName(), "Content-Length") ||
                    beast::iequals(header.GetName(), "Transfer-Encoding") ||
                    beast::iequals(header.GetName(), "Connection"))
                {
                    continue;
                }
                m_request.insert(header.GetName(), header.GetValue());
            }
            std::string authority(url.encoded_host());
            if (url.has_port())
            {
                authority += ":";
                authority += url.port();
            }
            m_request.set(http::field::host, authority);
            m_request.keep_alive(true);
            if (request.HasBodyView())
            {
                const auto view = request.GetBodyView();
                m_ownedBodyStorage.clear();
                m_request.body().data = const_cast<void*>(static_cast<const void*>(view.data()));
                m_request.body().size = view.size();
            }
            else
            {
                m_ownedBodyStorage = request.GetBody();
                m_request.body().data = m_ownedBodyStorage.empty()
                                            ? nullptr
                                            : const_cast<void*>(static_cast<const void*>(m_ownedBodyStorage.data()));
                m_request.body().size = m_ownedBodyStorage.size();
            }
            m_request.body().more = false;
            // buffer_body has no BodyWriter::size(), so message::prepare_payload() cannot be used;
            // set Content-Length manually instead (the same value it would have computed).
            m_request.set(http::field::content_length, std::to_string(m_request.body().size));
            ResetParser();
            m_hostIsName = url.host_type() == urls::host_type::name;
            return true;
        }

        void ArmTimer()
        {
            m_timer.expires_after(m_options.GetTimeout());
            auto self = this->shared_from_this();
            m_timer.async_wait([self](boost::system::error_code error)
            {
                if (!error)
                {
                    self->Fail(HttpClientError::TimedOut);
                }
            });
        }

        bool ConfigureTlsForHost()
        {
            if (SSL_get_verify_mode(m_stream->native_handle()) != SSL_VERIFY_NONE)
            {
                m_stream->set_verify_callback(asio::ssl::host_name_verification(m_key.host));
            }
            if (m_hostIsName && !SSL_set_tlsext_host_name(m_stream->native_handle(), m_key.host.c_str()))
            {
                Fail(HttpClientError::TlsFailed);
                return false;
            }
            return true;
        }

        // A pooled connection may have been closed by the peer while idle. If nothing has been
        // parsed yet, it is safe to silently retry once on a brand-new connection.
        // Because the request may already have been processed by the time the failure is seen, a request
        // that could have reached the server is only re-sent when its method is idempotent.
        bool MaybeRetryAfterReuseFailure(bool requestMayHaveBeenSent)
        {
            if (IsCancellationRequested() || !m_reused || m_retried || m_parser->got_some() ||
                (requestMayHaveBeenSent && !IsIdempotentMethod(m_request.method())))
            {
                return false;
            }
            m_retried = true;
            m_reused = false;

            boost::system::error_code ignored;
            beast::get_lowest_layer(*m_stream).socket().close(ignored);
            if constexpr (std::is_same_v<Stream, TlsStream>)
            {
                m_stream = std::make_unique<TlsStream>(m_executor, *m_tlsContext);
                if (!ConfigureTlsForHost())
                {
                    return true;
                }
            }
            else
            {
                m_stream = std::make_unique<PlainStream>(m_executor);
            }
            m_buffer.consume(m_buffer.size());
            ResetParser();

            m_resolver.async_resolve(m_key.host,
                m_key.service,
                [self = this->shared_from_this()](boost::system::error_code error,
                    Tcp::resolver::results_type results)
            {
                self->OnResolve(error, std::move(results));
            });
            return true;
        }

        static bool IsIdempotentMethod(http::verb method) noexcept
        {
            switch (method)
            {
            case http::verb::get:
            case http::verb::head:
            case http::verb::options:
            case http::verb::trace:
            case http::verb::put:
            case http::verb::delete_:
                return true;
            default:
                return false;
            }
        }

        void ResetParser()
        {
            m_parser = std::make_unique<http::response_parser<http::string_body>>();
            m_parser->body_limit(m_options.GetResponseBodyLimit());
            m_parser->skip(m_request.method() == http::verb::head);
        }

        void OnResolve(boost::system::error_code error, Tcp::resolver::results_type results)
        {
            if (finished_)
            {
                return;
            }
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            if (error)
            {
                return Fail(HttpClientError::ResolveFailed);
            }
            beast::get_lowest_layer(*m_stream).async_connect(results,
                [self = this->shared_from_this()](boost::system::error_code connectError, const Tcp::endpoint&)
            {
                self->OnConnect(connectError);
            });
        }

        void OnConnect(boost::system::error_code error)
        {
            if (finished_)
            {
                return;
            }
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            if (error)
            {
                return Fail(HttpClientError::ConnectFailed);
            }
            if constexpr (std::is_same_v<Stream, TlsStream>)
            {
                m_stream->async_handshake(asio::ssl::stream_base::client,
                    [self = this->shared_from_this()](boost::system::error_code handshakeError)
                {
                    if (self->finished_)
                    {
                        return;
                    }
                    if (self->IsCancellationRequested())
                    {
                        return self->Cancel();
                    }
                    if (handshakeError)
                    {
                        return self->Fail(HttpClientError::TlsFailed);
                    }
                    self->Write();
                });
            }
            else
            {
                Write();
            }
        }

        void Write()
        {
            if (finished_)
            {
                return;
            }
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            http::async_write(*m_stream,
                m_request,
                [self = this->shared_from_this()](boost::system::error_code error, std::size_t bytesWritten)
            {
                if (self->finished_)
                {
                    return;
                }
                if (self->IsCancellationRequested())
                {
                    return self->Cancel();
                }
                if (error)
                {
                    if (self->MaybeRetryAfterReuseFailure(bytesWritten != 0))
                    {
                        return;
                    }
                    return self->Fail(HttpClientError::WriteFailed);
                }
                self->Read();
            });
        }

        void Read()
        {
            if (finished_)
            {
                return;
            }
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            http::async_read(*m_stream,
                m_buffer,
                *m_parser,
                [self = this->shared_from_this()](boost::system::error_code error, std::size_t)
            {
                self->OnRead(error);
            });
        }

        void OnRead(boost::system::error_code error)
        {
            if (finished_)
            {
                return;
            }
            if (IsCancellationRequested())
            {
                return Cancel();
            }
            if (error)
            {
                if (error == http::error::body_limit)
                {
                    return Fail(HttpClientError::ResponseTooLarge);
                }
                // The request was fully written before the read started.
                if (MaybeRetryAfterReuseFailure(true))
                {
                    return;
                }
                return Fail(error.category() == make_error_code(http::error::bad_status).category()
                                ? HttpClientError::ProtocolError
                                : HttpClientError::ReadFailed);
            }
            const auto status = m_parser->get().result_int();
            if (status == 101)
            {
                return Fail(HttpClientError::ProtocolError);
            }
            if (status < 200)
            {
                ResetParser();
                return Read();
            }

            // The parser-level check also rejects EOF-delimited bodies; leftover bytes would be mistaken for the
            // next response on a pooled connection.
            const bool keepAlive = m_parser->keep_alive() && m_buffer.size() == 0;
            auto message = m_parser->release();
            HttpResponse response;
            response = HttpResponse(message.result_int(), {}, std::move(message.body()));
            for (const auto& header : message.base())
            {
                response.AddHeader({std::string(header.name_string()), std::string(header.value())});
            }

            m_timer.cancel();
            m_resolver.cancel();
            finished_ = true;
            ClearCancellationSlot();
            auto completion = std::move(m_completion);
            if (keepAlive)
            {
                m_pool->Release(m_key, PooledConnection<Stream>{std::move(m_stream), std::move(m_buffer)});
            }
            else if constexpr (std::is_same_v<Stream, TlsStream>)
            {
                beast::get_lowest_layer(*m_stream).expires_after(std::chrono::seconds(1));
                m_stream->async_shutdown([self = this->shared_from_this()](boost::system::error_code)
                {
                    self->Close();
                });
            }
            else
            {
                Close();
            }
            completion({}, std::move(response));
        }

        void Close()
        {
            boost::system::error_code ignored;
            auto& socket = beast::get_lowest_layer(*m_stream).socket();
            socket.shutdown(Tcp::socket::shutdown_both, ignored);
            socket.close(ignored);
        }

        void Fail(HttpClientError error)
        {
            Finish(error, {});
        }

        void Cancel()
        {
            Finish(std::make_error_code(std::errc::operation_canceled), {});
        }

        void Finish(std::error_code error, HttpResponse response)
        {
            if (finished_)
            {
                return;
            }
            finished_ = true;
            m_timer.cancel();
            m_resolver.cancel();
            ClearCancellationSlot();
            Close();
            auto completion = std::move(m_completion);
            completion(error, std::move(response));
        }

        void RequestCancellation()
        {
            if (m_cancellationRequested.exchange(true))
            {
                return;
            }
            asio::post(m_executor, [self = this->shared_from_this()]()
            {
                self->Cancel();
            });
        }

        [[nodiscard]] bool IsCancellationRequested() const noexcept
        {
            return m_cancellationRequested.load();
        }

        void ClearCancellationSlot()
        {
            auto slot = m_options.GetCancellationSlot();
            if (slot.is_connected())
            {
                slot.clear();
            }
        }

        std::shared_ptr<asio::ssl::context> m_tlsContext;
        // The operation's strand, fixed at construction so that threads other than the strand (the
        // cancellation handler) can post to it without touching m_stream, which is replaced and moved on it.
        typename Stream::executor_type m_executor;
        std::unique_ptr<Stream> m_stream;
        Tcp::resolver m_resolver;
        asio::steady_timer m_timer;
        IHttpClient::CompletionHandler m_completion;
        HttpRequestOptions m_options;
        std::shared_ptr<ConnectionPool<Stream>> m_pool;
        ConnectionKey<Stream> m_key;
        beast::flat_buffer m_buffer;
        http::request<http::buffer_body> m_request;
        // Backing storage for an owned (SetBody()) request body. buffer_body's writer only
        // stores a pointer/size pair, so something must own the bytes for the lifetime of the
        // write; for a SetBodyView() body the caller owns that storage instead (see the
        // lifetime contract on HttpRequest::SetBodyView()).
        std::string m_ownedBodyStorage;
        std::unique_ptr<http::response_parser<http::string_body>> m_parser;
        bool finished_ = false;
        bool m_reused = false;
        bool m_retried = false;
        bool m_hostIsName = false;
        std::atomic_bool m_cancellationRequested = false;
    };
} // namespace AVEVA::Private
