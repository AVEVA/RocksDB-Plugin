// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "HttpClientTestHelpers.hpp"

#include <gtest/gtest.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <vector>

namespace
{
    namespace asio = boost::asio;
    namespace beast = boost::beast;
    namespace http = boost::beast::http;
    using HttpClientTests::Exchange;
    using Tcp = asio::ip::tcp;

    TEST(HttpClientRequest, InvalidRequestCompletesAsynchronously)
    {
        boost::asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        const std::vector<std::string> invalidUrls{"/relative",
            "ftp://example.com/file",
            "http:///missing-host",
            "http://user:pass@example.com/",
            "http://example.com:99999/",
            "http://example.com:/",
            "http://localhost%00.example/"};
        std::size_t completions = 0;
        for (const auto& url : invalidUrls)
        {
            AVEVA::HttpRequest request;
            request.SetUrl(url);
            client->SendAsync(std::move(request),
                AVEVA::HttpRequestOptions{},
                [&](std::error_code error, AVEVA::HttpResponse response)
            {
                ++completions;
                EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidUrl));
                EXPECT_EQ(response.GetStatus(), 0u);
                EXPECT_TRUE(response.GetBody().empty());
                EXPECT_TRUE(response.GetHeaders().empty());
            });
        }

        AVEVA::HttpRequest request;
        request.SetUrl("http://127.0.0.1/");
        request.AddHeader({"X-Test", "value\r\nInjected: true"});
        client->SendAsync(request,
            AVEVA::HttpRequestOptions{},
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            ++completions;
            EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
        });
        request.GetHeaders().clear();

        AVEVA::HttpRequestOptions options;
        options.SetTimeout(std::chrono::milliseconds(0));
        client->SendAsync(request,
            options,
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            ++completions;
            EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
        });

        request.SetMethod(AVEVA::HttpMethod::Trace);
        request.SetBody("not-allowed");
        client->SendAsync(request,
            AVEVA::HttpRequestOptions{},
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            ++completions;
            EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
        });

        EXPECT_THROW(client->SendAsyncErased(request, {}, {}), std::invalid_argument);
        EXPECT_EQ(completions, 0u);
        context.run();
        EXPECT_EQ(completions, invalidUrls.size() + 3);
    }

    TEST(HttpClientRequest, OversizedHeadersFailWithInvalidRequest)
    {
        boost::asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        std::size_t completions = 0;

        AVEVA::HttpRequest longValue;
        longValue.SetUrl("http://127.0.0.1/");
        longValue.AddHeader({"X-Test", std::string(70000, 'a')});
        AVEVA::HttpRequest longName;
        longName.SetUrl("http://127.0.0.1/");
        longName.AddHeader({std::string(70000, 'a'), "value"});
        for (auto* request : {&longValue, &longName})
        {
            client->SendAsync(std::move(*request),
                AVEVA::HttpRequestOptions{},
                [&](std::error_code error, AVEVA::HttpResponse)
            {
                ++completions;
                EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
            });
        }

        context.run();
        EXPECT_EQ(completions, 2u);
    }

    TEST(HttpClientRequest, HeaderControlCharactersFailWithInvalidRequest)
    {
        boost::asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        std::size_t completions = 0;

        for (const std::string& value :
            {std::string{"bad\0value", 9}, std::string{"bad\x1Fvalue", 9}, std::string{"bad\x7Fvalue", 9}})
        {
            AVEVA::HttpRequest request;
            request.SetUrl("http://127.0.0.1/");
            request.AddHeader({"X-Test", value});
            client->SendAsync(std::move(request),
                AVEVA::HttpRequestOptions{},
                [&](std::error_code error, AVEVA::HttpResponse)
            {
                ++completions;
                EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
            });
        }

        context.run();
        EXPECT_EQ(completions, 3u);
    }

    TEST(HttpClientRequest, TokenBasedSendAsyncSupportsUseFutureAndReportsErrors)
    {
        boost::asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);

        AVEVA::HttpRequest request;
        request.SetUrl("ftp://example.com/file");
        auto future = client->SendAsync(std::move(request), AVEVA::HttpRequestOptions{}, asio::use_future);

        context.run();

        auto [error, response] = future.get();
        EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidUrl));
        EXPECT_EQ(response.GetStatus(), 0u);
    }

    TEST(HttpClientRequest, TokenBasedSendAsyncDispatchesCompletionOnTheBoundStrand)
    {
        boost::asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        auto strand = asio::make_strand(context);

        AVEVA::HttpRequest request;
        request.SetUrl("ftp://example.com/file");
        bool ranOnStrand = false;
        bool completed = false;
        client->SendAsync(std::move(request),
            AVEVA::HttpRequestOptions{},
            asio::bind_executor(strand,
                [&](std::error_code, AVEVA::HttpResponse)
        {
            ranOnStrand = strand.running_in_this_thread();
            completed = true;
        }));

        context.run();

        EXPECT_TRUE(completed);
        EXPECT_TRUE(ranOnStrand);
    }

    TEST(HttpClientRequest, HttpErrorPreservesRequestFraming)
    {
        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Post);
        request.SetUrl("/some%20path?key=value#not-sent");
        request.SetBody(std::string("one\0two", 7));
        request.SetHeaders({{"X-Test", "one"},
            {"X-Test", "two"},
            {"Host", "wrong.example"},
            {"Content-Length", "999"},
            {"Transfer-Encoding", "chunked"}});
        const std::string responseBody("a\0b", 3);
        auto result = Exchange(request,
            "HTTP/1.1 404 Not Found\r\nSet-Cookie: first=1\r\nSet-Cookie: second=2\r\n"
            "Content-Length: 3\r\nConnection: close\r\n\r\n" +
                responseBody);

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.response.GetStatus(), 404u);
        EXPECT_EQ(result.response.GetBody(), responseBody);
        ASSERT_EQ(result.response.GetHeaders().size(), 4u);
        EXPECT_EQ(result.response.GetHeaders()[0].GetValue(), "first=1");
        EXPECT_EQ(result.response.GetHeaders()[1].GetValue(), "second=2");
        EXPECT_EQ(result.received.method(), http::verb::post);
        EXPECT_EQ(result.received.body(), request.GetBody());
        EXPECT_EQ(result.received.target(), "/some%20path?key=value");
        EXPECT_EQ(result.received[http::field::host], result.authority);
        EXPECT_EQ(result.received[http::field::content_length], "7");
        EXPECT_FALSE(result.received.chunked());
        EXPECT_EQ(result.received.count("X-Test"), 2u);
    }

    TEST(HttpClientRequest, SetBodyViewTransmitsTheReferencedBytes)
    {
        std::vector<std::byte> payload{std::byte{'v'}, std::byte{'i'}, std::byte{'e'}, std::byte{'w'}, std::byte{'!'}};

        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Post);
        request.SetBodyView(payload);
        EXPECT_TRUE(request.HasBodyView());
        EXPECT_EQ(request.GetBodySize(), payload.size());

        auto result = Exchange(request, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.received.body(), "view!");
        EXPECT_EQ(result.received[http::field::content_length], "5");
    }

    TEST(HttpClientRequest, IPv6LiteralHostHeaderKeepsBrackets)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);

        boost::system::error_code ipv6Error;
        const auto loopback = asio::ip::make_address("::1", ipv6Error);
        if (ipv6Error)
        {
            GTEST_SKIP() << "IPv6 loopback is unavailable: " << ipv6Error.message();
        }

        Tcp::acceptor acceptor(context, {loopback.to_v6(), 0});
        const auto port = acceptor.local_endpoint().port();
        Tcp::socket socket(context);
        beast::flat_buffer buffer;
        auto received = std::make_shared<http::request<http::string_body>>();

        acceptor.async_accept(socket,
            [&](boost::system::error_code error)
        {
            ASSERT_FALSE(error);
            http::async_read(socket,
                buffer,
                *received,
                [&](boost::system::error_code readError, std::size_t)
            {
                ASSERT_FALSE(readError);
                auto response = std::make_shared<std::string>("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
                asio::async_write(socket,
                    asio::buffer(*response),
                    [&, response](boost::system::error_code writeError, std::size_t)
                {
                    ASSERT_FALSE(writeError);
                    boost::system::error_code ignored;
                    socket.shutdown(Tcp::socket::shutdown_both, ignored);
                    socket.close(ignored);
                    acceptor.close(ignored);
                });
            });
        });

        AVEVA::HttpRequest request;
        request.SetUrl("http://[::1]:" + std::to_string(port) + "/ipv6");
        std::error_code result;
        client->SendAsync(std::move(request),
            AVEVA::HttpRequestOptions{},
            [&](std::error_code error, AVEVA::HttpResponse)
        {
            result = error;
        });

        context.run();
        EXPECT_FALSE(result);
        ASSERT_TRUE(received);
        ASSERT_TRUE(received);
        EXPECT_EQ(received->base()[http::field::host], "[::1]:" + std::to_string(port));
    }

    // Proves SetBodyView() is genuinely non-owning (unlike SetBody(), which copies
    // synchronously): the request completes asynchronously, so mutating the referenced buffer
    // after SendAsync() returns but before the completion handler runs changes what is
    // transmitted on the wire. This is the documented lifetime contract on SetBodyView() -- if a
    // future change accidentally made the body path copy eagerly, this test would start failing
    // because the server would observe the *original* bytes instead.
    TEST(HttpClientRequest, SetBodyViewReadsTheBufferLazilyNotAtCallTime)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        Tcp::socket socket(context);
        beast::flat_buffer buffer;
        auto received = std::make_shared<http::request<http::string_body>>();

        acceptor.async_accept(socket,
            [&](boost::system::error_code error)
        {
            ASSERT_FALSE(error);
            http::async_read(socket,
                buffer,
                *received,
                [&](boost::system::error_code readError, std::size_t)
            {
                ASSERT_FALSE(readError);
                boost::system::error_code ignored;
                socket.shutdown(Tcp::socket::shutdown_both, ignored);
                socket.close(ignored);
                acceptor.close(ignored);
            });
        });

        std::vector<std::byte> mutableBuffer{std::byte{'A'}, std::byte{'A'}, std::byte{'A'}, std::byte{'A'}};

        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Post);
        request.SetUrl("http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");
        request.SetBodyView(mutableBuffer);

        bool completed = false;
        client->SendAsync(std::move(request),
            AVEVA::HttpRequestOptions{},
            [&](std::error_code, AVEVA::HttpResponse)
        {
            completed = true;
        });

        // Not yet run: proves the send is asynchronous and nothing has been read/copied yet.
        EXPECT_FALSE(completed);

        // Mutate the referenced buffer's content after initiating the send but before letting
        // the io_context process anything. If SetBodyView() had copied at call time, the server
        // would still see "AAAA".
        std::fill(mutableBuffer.begin(), mutableBuffer.end(), std::byte{'B'});

        context.run();

        EXPECT_EQ(received->body(), "BBBB");
    }

    TEST(HttpClientRequest, HeadDoesNotReadBody)
    {
        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Head);
        auto result = Exchange(request, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n");

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.response.GetStatus(), 200u);
        EXPECT_TRUE(result.response.GetBody().empty());
        EXPECT_EQ(result.received.target(), "/");
    }

    TEST(HttpClientRequest, ReusesConnectionForKeepAliveRequests)
    {
        asio::io_context context;
        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        Tcp::socket socket(context);
        beast::flat_buffer buffer;
        int acceptCount = 0;
        std::size_t requestsServed = 0;

        std::function<void()> serveNext;
        serveNext = [&]()
        {
            auto request = std::make_shared<http::request<http::string_body>>();
            http::async_read(socket,
                buffer,
                *request,
                [&, request](boost::system::error_code error, std::size_t)
            {
                if (error)
                {
                    return;
                }
                ++requestsServed;
                auto payload = std::make_shared<std::string>("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK");
                asio::async_write(socket,
                    asio::buffer(*payload),
                    [&, payload](boost::system::error_code, std::size_t)
                {
                    if (requestsServed < 2)
                    {
                        serveNext();
                    }
                });
            });
        };
        acceptor.async_accept(socket,
            [&](boost::system::error_code error)
        {
            if (error)
            {
                return;
            }
            ++acceptCount;
            serveNext();
        });

        auto client = AVEVA::IHttpClient::Create(context);
        const std::string authority = "127.0.0.1:" + std::to_string(acceptor.local_endpoint().port());
        int completions = 0;

        AVEVA::HttpRequest request1;
        request1.SetUrl("http://" + authority + "/first");
        client->SendAsync(request1,
            AVEVA::HttpRequestOptions{},
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            ++completions;
            EXPECT_FALSE(error);
            EXPECT_EQ(response.GetStatus(), 200u);

            AVEVA::HttpRequest request2;
            request2.SetUrl("http://" + authority + "/second");
            client->SendAsync(request2,
                AVEVA::HttpRequestOptions{},
                [&](std::error_code error2, AVEVA::HttpResponse response2)
            {
                ++completions;
                EXPECT_FALSE(error2);
                EXPECT_EQ(response2.GetStatus(), 200u);
                boost::system::error_code ignored;
                acceptor.close(ignored);
                socket.shutdown(Tcp::socket::shutdown_both, ignored);
                socket.close(ignored);
            });
        });

        context.run();
        EXPECT_EQ(completions, 2);
        EXPECT_EQ(acceptCount, 1);
        EXPECT_EQ(requestsServed, 2u);
    }

    TEST(HttpClientRequest, CancellationBeforeStartCompletesWithOperationCanceled)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        asio::cancellation_signal cancellation;
        AVEVA::HttpRequestOptions options;
        options.SetCancellationSlot(cancellation.slot());

        AVEVA::HttpRequest request;
        request.SetUrl("http://127.0.0.1:1/");

        int completions = 0;
        client->SendAsync(request,
            options,
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            ++completions;
            EXPECT_EQ(error, std::make_error_code(std::errc::operation_canceled));
            EXPECT_EQ(response.GetStatus(), 0u);
            EXPECT_TRUE(response.GetBody().empty());
            EXPECT_TRUE(response.GetHeaders().empty());
        });

        cancellation.emit(asio::cancellation_type::terminal);
        context.run();
        EXPECT_EQ(completions, 1);
    }

    TEST(HttpClientRequest, CancellationMidFlightCompletesWithOperationCanceled)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        Tcp::socket socket(context);
        beast::flat_buffer buffer;
        bool requestRead = false;
        asio::cancellation_signal cancellation;

        acceptor.async_accept(socket,
            [&](boost::system::error_code error)
        {
            ASSERT_FALSE(error);
            auto received = std::make_shared<http::request<http::string_body>>();
            http::async_read(socket,
                buffer,
                *received,
                [&, received](boost::system::error_code readError, std::size_t)
            {
                ASSERT_FALSE(readError);
                requestRead = true;
                // The server has the request and never answers: cancel now rather than after a fixed delay.
                cancellation.emit(asio::cancellation_type::terminal);
            });
        });

        AVEVA::HttpRequestOptions options;
        options.SetCancellationSlot(cancellation.slot());
        options.SetTimeout(std::chrono::seconds(5));

        AVEVA::HttpRequest request;
        request.SetUrl("http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");

        int completions = 0;
        client->SendAsync(request,
            options,
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            ++completions;
            EXPECT_EQ(error, std::make_error_code(std::errc::operation_canceled));
            EXPECT_EQ(response.GetStatus(), 0u);
            boost::system::error_code ignored;
            acceptor.close(ignored);
            socket.close(ignored);
        });

        context.run();
        EXPECT_TRUE(requestRead);
        EXPECT_EQ(completions, 1);
    }

    TEST(HttpClientRequest, ConnectedCancellationSlotDoesNotAffectNormalCompletion)
    {
        asio::cancellation_signal cancellation;
        AVEVA::HttpRequestOptions options;
        options.SetCancellationSlot(cancellation.slot());

        auto result = Exchange({}, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello", options);

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.response.GetStatus(), 200u);
        EXPECT_EQ(result.response.GetBody(), "hello");
    }

    TEST(HttpClientRequest, CancellingAfterCompletionIsANoop)
    {
        asio::io_context context;
        auto client = AVEVA::IHttpClient::Create(context);
        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        Tcp::socket socket(context);
        beast::flat_buffer buffer;
        auto payload = std::make_shared<std::string>("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK");

        acceptor.async_accept(socket,
            [&](boost::system::error_code error)
        {
            ASSERT_FALSE(error);
            auto received = std::make_shared<http::request<http::string_body>>();
            http::async_read(socket,
                buffer,
                *received,
                [&, received, payload](boost::system::error_code readError, std::size_t)
            {
                ASSERT_FALSE(readError);
                asio::async_write(socket,
                    asio::buffer(*payload),
                    [&, payload](boost::system::error_code writeError, std::size_t)
                {
                    ASSERT_FALSE(writeError);
                });
            });
        });

        asio::cancellation_signal cancellation;
        AVEVA::HttpRequestOptions options;
        options.SetCancellationSlot(cancellation.slot());

        AVEVA::HttpRequest request;
        request.SetUrl("http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");

        int completions = 0;
        client->SendAsync(request,
            options,
            [&](std::error_code error, AVEVA::HttpResponse response)
        {
            ++completions;
            EXPECT_FALSE(error);
            EXPECT_EQ(response.GetStatus(), 200u);
            boost::system::error_code ignored;
            acceptor.close(ignored);
            socket.shutdown(Tcp::socket::shutdown_both, ignored);
            socket.close(ignored);
        });

        context.run();
        EXPECT_EQ(completions, 1);

        cancellation.emit(asio::cancellation_type::terminal);
        context.restart();
        context.run();
        EXPECT_EQ(completions, 1);
    }

    TEST(HttpClientRequest, ReleaseBodyMovesTheOwnedBodyOut)
    {
        AVEVA::HttpRequest request;
        request.SetBody("payload");

        EXPECT_EQ(request.ReleaseBody(), "payload");
        EXPECT_TRUE(request.GetBody().empty());
        EXPECT_EQ(request.GetBodySize(), 0u);
    }

    TEST(HttpClientRequest, BodyViewKeepAliveIsSharedByCopiesAndDroppedByReplacingTheBody)
    {
        auto owner = std::make_shared<const std::string>("shared payload");
        const std::weak_ptr<const std::string> weak = owner;
        AVEVA::HttpRequest request;
        request.SetBodyView(std::as_bytes(std::span{owner->data(), owner->size()}), owner);
        owner.reset();

        AVEVA::HttpRequest copy = request;
        request.SetBody("other");
        EXPECT_FALSE(weak.expired());
        EXPECT_EQ(copy.GetBodySize(), 14u);

        copy.SetBody({});
        EXPECT_TRUE(weak.expired());
    }

    TEST(HttpClientRequest, SharedBodyViewIsTransmitted)
    {
        auto owner = std::make_shared<const std::string>("shared payload");
        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Post);
        request.SetBodyView(std::as_bytes(std::span{owner->data(), owner->size()}), owner);
        owner.reset();

        auto result = Exchange(request, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.received.body(), "shared payload");
    }
} // namespace
