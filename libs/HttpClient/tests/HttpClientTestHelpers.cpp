#include "HttpClientTestHelpers.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <memory>
#include <stdexcept>
#include <utility>

namespace HttpClientTests
{
    namespace asio = boost::asio;
    namespace beast = boost::beast;
    using Tcp = asio::ip::tcp;

    namespace
    {
        class TestServer
        {
          public:
            TestServer(asio::io_context& context, std::string response, bool holdResponse)
                : acceptor_(context, {asio::ip::make_address("127.0.0.1"), 0}), socket_(context),
                  response_(std::move(response)), holdResponse_(holdResponse)
            {
                acceptor_.async_accept(socket_,
                    [this](boost::system::error_code error)
                {
                    if (error)
                    {
                        return;
                    }
                    http::async_read(socket_,
                        m_buffer,
                        received,
                        [this](boost::system::error_code readError, std::size_t)
                    {
                        if (readError || holdResponse_)
                        {
                            return;
                        }
                        asio::async_write(socket_,
                            asio::buffer(response_),
                            [this](boost::system::error_code, std::size_t)
                        {
                            Stop();
                        });
                    });
                });
            }

            std::string Authority() const
            {
                return "127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port());
            }

            void Stop()
            {
                boost::system::error_code ignored;
                acceptor_.close(ignored);
                socket_.shutdown(Tcp::socket::shutdown_both, ignored);
                socket_.close(ignored);
            }

            http::request<http::string_body> received;

          private:
            Tcp::acceptor acceptor_;
            Tcp::socket socket_;
            beast::flat_buffer m_buffer;
            std::string response_;
            bool holdResponse_;
        };
    }

    ExchangeResult Exchange(AVEVA::HttpRequest request,
        std::string wireResponse,
        AVEVA::HttpRequestOptions options,
        bool holdResponse)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        TestServer server(context, std::move(wireResponse), holdResponse);
        ExchangeResult result;
        result.authority = server.Authority();
        request.SetUrl("http://" + result.authority + request.GetUrl());
        int completions = 0;
        client->SendAsync(std::move(request),
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            ++completions;
            result.error = error;
            result.response = std::move(response);
            server.Stop();
        },
            options);
        if (completions != 0)
        {
            throw std::runtime_error("Completion must not run inline");
        }
        context.run();
        if (completions != 1)
        {
            throw std::runtime_error("Completion must run exactly once");
        }
        result.received = std::move(server.received);
        return result;
    }
} // namespace HttpClientTests