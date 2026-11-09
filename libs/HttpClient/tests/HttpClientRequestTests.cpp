// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "HttpClientTestHelpers.hpp"
#include "private/RequestValidation.hpp"

#include <gtest/gtest.h>

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
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
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
using HttpClientTests::Exchange;
using HttpClientTests::RunBounded;
using HttpClientTests::ScriptedServer;
using HttpClientTests::ServerAction;
using HttpClientTests::ServerReply;
using HttpResult = AVEVA::HttpResult;
using Tcp = asio::ip::tcp;

constexpr const char* const OkNoBody = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
constexpr const char* const OkResponse = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

TEST(HttpClientRequest, InvalidRequestCompletesAsynchronously) {
    boost::asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    const std::vector<std::string> invalidUrls{"/relative",
                                               "ftp://example.com/file",
                                               "http:///missing-host",
                                               "http://user:pass@example.com/",
                                               "http://example.com:99999/",
                                               "http://example.com:/",
                                               "http://localhost%00.example/"};
    std::size_t completions = 0;
    for (const auto& url : invalidUrls) {
        AVEVA::HttpRequest request;
        request.SetUrl(AVEVA::HttpUrl{url});
        HttpClientTests::SendWith(*client, std::move(request), AVEVA::HttpRequestOptions{},
                                  [&](std::error_code error, AVEVA::HttpResponse response) {
                                      ++completions;
                                      EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidUrl));
                                      EXPECT_EQ(response.GetStatus(), AVEVA::HttpStatusCode{0});
                                      EXPECT_TRUE(response.GetBody().empty());
                                      EXPECT_TRUE(response.GetHeaders().empty());
                                  });
    }

    AVEVA::HttpRequest request;
    request.SetUrl(AVEVA::HttpUrl{"http://127.0.0.1/"});
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"X-Test"}, AVEVA::HeaderValue{"value\r\nInjected: true"}));
    HttpClientTests::SendWith(*client, request, AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  ++completions;
                                  EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
                              });
    request.RemoveHeaders(AVEVA::HeaderName{"X-Test"});

    request.SetMethod(AVEVA::HttpMethod::Trace);
    request.SetBody(AVEVA::HttpBody{"not-allowed"});
    HttpClientTests::SendWith(*client, request, AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  ++completions;
                                  EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
                              });

    EXPECT_EQ(completions, 0u);
    RunBounded(context);
    EXPECT_EQ(completions, invalidUrls.size() + 2);
}

TEST(HttpClientRequest, OversizedHeadersFailWithInvalidRequest) {
    boost::asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    std::size_t completions = 0;

    AVEVA::HttpRequest longValue;
    longValue.SetUrl(AVEVA::HttpUrl{"http://127.0.0.1/"});
    longValue.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"X-Test"}, AVEVA::HeaderValue{std::string(70000, 'a')}));
    AVEVA::HttpRequest longName;
    longName.SetUrl(AVEVA::HttpUrl{"http://127.0.0.1/"});
    longName.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{std::string(70000, 'a')}, AVEVA::HeaderValue{"value"}));
    for (auto* request : {&longValue, &longName}) {
        HttpClientTests::SendWith(*client, std::move(*request), AVEVA::HttpRequestOptions{},
                                  [&](std::error_code error, AVEVA::HttpResponse) {
                                      ++completions;
                                      EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
                                  });
    }

    RunBounded(context);
    EXPECT_EQ(completions, 2u);
}

TEST(HttpClientRequest, HeaderControlCharactersFailWithInvalidRequest) {
    boost::asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    std::size_t completions = 0;

    for (const std::string& value :
         {std::string{"bad\0value", 9}, std::string{"bad\x1Fvalue", 9}, std::string{"bad\x7Fvalue", 9}}) {
        AVEVA::HttpRequest request;
        request.SetUrl(AVEVA::HttpUrl{"http://127.0.0.1/"});
        request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"X-Test"}, AVEVA::HeaderValue{value}));
        HttpClientTests::SendWith(*client, std::move(request), AVEVA::HttpRequestOptions{},
                                  [&](std::error_code error, AVEVA::HttpResponse) {
                                      ++completions;
                                      EXPECT_EQ(error, AVEVA::make_error_code(AVEVA::HttpClientError::InvalidRequest));
                                  });
    }

    RunBounded(context);
    EXPECT_EQ(completions, 3u);
}

asio::awaitable<HttpResult> SendInvalid(AVEVA::HttpClient& client) {
    AVEVA::HttpRequest request;
    request.SetUrl(AVEVA::HttpUrl{"ftp://example.com/file"});
    co_return co_await client.SendAsync(std::move(request), AVEVA::HttpRequestOptions{});
}

TEST(HttpClientRequest, CoroutineSendAsyncReportsErrorsThroughUseFuture) {
    boost::asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);

    auto future = asio::co_spawn(context, SendInvalid(*client), asio::use_future);

    RunBounded(context);

    auto result = future.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Error(), AVEVA::make_error_code(AVEVA::HttpClientError::InvalidUrl));
}

asio::awaitable<void> SendInvalidOnStrand(AVEVA::HttpClient& client,
                                          asio::strand<asio::io_context::executor_type> strand, bool& ranOnStrand,
                                          bool& completed) {
    AVEVA::HttpRequest request;
    request.SetUrl(AVEVA::HttpUrl{"ftp://example.com/file"});
    (void)co_await client.SendAsync(std::move(request), AVEVA::HttpRequestOptions{});
    ranOnStrand = strand.running_in_this_thread();
    completed = true;
}

TEST(HttpClientRequest, CoroutineResumesOnTheAwaitingStrand) {
    boost::asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    auto strand = asio::make_strand(context);

    bool ranOnStrand = false;
    bool completed = false;
    asio::co_spawn(strand, SendInvalidOnStrand(*client, strand, ranOnStrand, completed), asio::detached);

    RunBounded(context);

    EXPECT_TRUE(completed);
    EXPECT_TRUE(ranOnStrand);
}

TEST(HttpClientRequest, HttpErrorPreservesRequestFraming) {
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Post);
    request.SetUrl(AVEVA::HttpUrl{"/some%20path?key=value#not-sent"});
    request.SetBody(AVEVA::HttpBody{std::string("one\0two", 7)});
    for (const auto& [name, value] : {std::pair{"X-Test", "one"},
                                      {"X-Test", "two"},
                                      {"Host", "wrong.example"},
                                      {"Content-Length", "999"},
                                      {"Transfer-Encoding", "chunked"}}) {
        request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{name}, AVEVA::HeaderValue{value}));
    }
    const std::string responseBody("a\0b", 3);
    auto result = Exchange(request, "HTTP/1.1 404 Not Found\r\nSet-Cookie: first=1\r\nSet-Cookie: second=2\r\n"
                                    "Content-Length: 3\r\nConnection: close\r\n\r\n" +
                                        responseBody);

    EXPECT_FALSE(result.error);
    EXPECT_EQ(result.response.GetStatus(), AVEVA::HttpStatusCode{404});
    EXPECT_EQ(result.response.GetBody(), responseBody);
    ASSERT_EQ(result.response.GetHeaders().size(), 4u);
    EXPECT_EQ(result.response.GetHeaders()[0].GetValue(), "first=1");
    EXPECT_EQ(result.response.GetHeaders()[1].GetValue(), "second=2");
    EXPECT_EQ(result.received.method(), http::verb::post);
    EXPECT_EQ(result.received.body(), std::string("one\0two", 7));
    EXPECT_EQ(result.received.target(), "/some%20path?key=value");
    EXPECT_EQ(result.received[http::field::host], result.authority);
    EXPECT_EQ(result.received[http::field::content_length], "7");
    EXPECT_FALSE(result.received.chunked());
    EXPECT_EQ(result.received.count("X-Test"), 1u);
    EXPECT_EQ(result.received["X-Test"], "two");
}

TEST(HttpClientRequest, AddHeaderReplacesSameNameCaseInsensitively) {
    AVEVA::HttpRequest request;
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"X-A"}, AVEVA::HeaderValue{"1"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"X-B"}, AVEVA::HeaderValue{"2"}));
    request.AddHeader(AVEVA::HttpHeader(AVEVA::HeaderName{"x-a"}, AVEVA::HeaderValue{"3"}));

    ASSERT_EQ(request.GetHeaders().size(), 2u);
    const auto replaced = request.GetHeaders().find(
        AVEVA::HttpHeader(AVEVA::HeaderName{"X-A"}, AVEVA::HeaderValue{""}));
    ASSERT_NE(replaced, request.GetHeaders().end());
    EXPECT_EQ(replaced->GetValue(), "3");
}

TEST(HttpClientRequest, ContentLengthIsSentOnlyWhenAppropriate) {
    const std::string ok = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    for (const auto method : {AVEVA::HttpMethod::Get, AVEVA::HttpMethod::Head, AVEVA::HttpMethod::Delete}) {
        AVEVA::HttpRequest request;
        request.SetMethod(method);
        auto result = Exchange(request, ok);
        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.received.count(http::field::content_length), 0u);
    }
    for (const auto method : {AVEVA::HttpMethod::Post, AVEVA::HttpMethod::Put, AVEVA::HttpMethod::Patch}) {
        AVEVA::HttpRequest request;
        request.SetMethod(method);
        auto result = Exchange(request, ok);
        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.received[http::field::content_length], "0");
    }
    AVEVA::HttpRequest withBody;
    withBody.SetMethod(AVEVA::HttpMethod::Delete);
    withBody.SetBody(AVEVA::HttpBody{"x"});
    auto result = Exchange(withBody, ok);
    EXPECT_EQ(result.received[http::field::content_length], "1");
}

TEST(HttpClientRequest, BorrowedBodyTransmitsTheReferencedBytes) {
    auto payload = std::make_shared<const std::vector<std::byte>>(
        std::vector<std::byte>{std::byte{'v'}, std::byte{'i'}, std::byte{'e'}, std::byte{'w'}, std::byte{'!'}});

    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Post);
    request.SetBody(AVEVA::HttpBody(*payload, payload));
    EXPECT_EQ(request.GetBody().Size(), AVEVA::ByteCount{payload->size()});
    const auto body = request.GetBody();
    EXPECT_EQ(body.Bytes().data(), payload->data());

    auto result = Exchange(request, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");

    EXPECT_FALSE(result.error);
    EXPECT_EQ(result.received.body(), "view!");
    EXPECT_EQ(result.received[http::field::content_length], "5");
}

TEST(HttpClientRequest, IPv6LiteralHostHeaderKeepsBrackets) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);

    boost::system::error_code ipv6Error;
    (void)asio::ip::make_address("::1", ipv6Error);
    if (ipv6Error) {
        GTEST_SKIP() << "IPv6 loopback is unavailable: " << ipv6Error.message();
    }

    ScriptedServer server(
        context,
        [](int, const http::request<http::string_body>&) {
            return ServerAction{OkNoBody, ServerReply::RespondAndClose};
        },
        "::1");

    AVEVA::HttpRequest request;
    const auto authority = server.Authority(); // the acceptor is closed by the time the test checks
    request.SetUrl(AVEVA::HttpUrl{server.Url("/ipv6")});
    std::error_code result;
    HttpClientTests::SendWith(*client, std::move(request), AVEVA::HttpRequestOptions{},
                              [&](std::error_code error, AVEVA::HttpResponse) {
                                  result = error;
                                  server.Stop();
                              });

    RunBounded(context);
    EXPECT_FALSE(result);
    ASSERT_EQ(server.Received().size(), 1u);
    EXPECT_EQ(server.Received().front().base()[http::field::host], authority);
}

// Proves a borrowed body is genuinely zero-copy: the request completes asynchronously, so mutating the owner's buffer
// after SendAsync() returns but before the exchange runs changes what is transmitted. If the body path started copying
// eagerly, the server would observe the original bytes instead.
TEST(HttpClientRequest, BorrowedBodyIsReadLazilyNotAtCallTime) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context, [](int, const http::request<http::string_body>&) {
        ServerAction action;
        action.Reply = ServerReply::Drop;
        return action;
    });

    auto mutableBuffer = std::make_shared<std::vector<std::byte>>(4, std::byte{'A'});

    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Post);
    request.SetUrl(AVEVA::HttpUrl{server.Url()});
    request.SetBody(AVEVA::HttpBody(*mutableBuffer, mutableBuffer));

    bool completed = false;
    HttpClientTests::SendWith(*client, std::move(request), AVEVA::HttpRequestOptions{},
                              [&](std::error_code, AVEVA::HttpResponse) {
                                  completed = true;
                                  server.Stop();
                              });

    // Not yet run: proves the send is asynchronous and nothing has been read/copied yet.
    EXPECT_FALSE(completed);

    std::fill(mutableBuffer->begin(), mutableBuffer->end(), std::byte{'B'});

    RunBounded(context);

    ASSERT_EQ(server.Received().size(), 1u);
    EXPECT_EQ(server.Received().front().body(), "BBBB");
}

TEST(HttpClientRequest, HeadDoesNotReadBody) {
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Head);
    auto result = Exchange(request, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n");

    EXPECT_FALSE(result.error);
    EXPECT_EQ(result.response.GetStatus(), AVEVA::HttpStatusCode{200});
    EXPECT_TRUE(result.response.GetBody().empty());
    EXPECT_EQ(result.received.target(), "/");
}

TEST(HttpClientRequest, ReusesConnectionForKeepAliveRequests) {
    asio::io_context context;
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) { return ServerAction{OkResponse}; });

    auto client = std::make_unique<AVEVA::HttpClient>(context);
    const std::string authority = server.Authority();
    int completions = 0;

    AVEVA::HttpRequest request1;
    request1.SetUrl(AVEVA::HttpUrl{"http://" + authority + "/first"});
    HttpClientTests::SendWith(
        *client, request1, AVEVA::HttpRequestOptions{}, [&](std::error_code error, AVEVA::HttpResponse response) {
            ++completions;
            EXPECT_FALSE(error);
            EXPECT_EQ(response.GetStatus(), AVEVA::HttpStatusCode{200});

            AVEVA::HttpRequest request2;
            request2.SetUrl(AVEVA::HttpUrl{"http://" + authority + "/second"});
            HttpClientTests::SendWith(*client, request2, AVEVA::HttpRequestOptions{},
                                      [&](std::error_code error2, AVEVA::HttpResponse response2) {
                                          ++completions;
                                          EXPECT_FALSE(error2);
                                          EXPECT_EQ(response2.GetStatus(), AVEVA::HttpStatusCode{200});
                                          server.Stop();
                                      });
        });

    RunBounded(context);
    EXPECT_EQ(completions, 2);
    EXPECT_EQ(server.Accepted(), 1);
    EXPECT_EQ(server.TotalRequests(), 2);
}

TEST(HttpClientRequest, ReadsLargeBodyIntact) {
    const std::string body(4 * 1024 * 1024, 'x');
    AVEVA::HttpRequest request;
    request.SetUrl(AVEVA::HttpUrl{"/"});
    AVEVA::HttpRequestOptions options;
    options.SetResponseBodyLimit(AVEVA::ByteCount{body.size() + 1});
    auto result = Exchange(
        request, "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body, options);
    EXPECT_FALSE(result.error);
    EXPECT_EQ(result.response.GetBody().size(), body.size());
}

TEST(HttpClientRequest, CancellationRightAfterStartCompletesWithOperationCanceled) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    asio::cancellation_signal cancellation;
    AVEVA::HttpRequestOptions options;

    AVEVA::HttpRequest request;
    request.SetUrl(AVEVA::HttpUrl{HttpClientTests::ClosedLocalUrl()});

    int completions = 0;
    HttpClientTests::SendWith(
        *client, request, options,
        [&](std::error_code error, AVEVA::HttpResponse response) {
            ++completions;
            EXPECT_EQ(error, std::make_error_code(std::errc::operation_canceled));
            EXPECT_EQ(response.GetStatus(), AVEVA::HttpStatusCode{0});
            EXPECT_TRUE(response.GetBody().empty());
            EXPECT_TRUE(response.GetHeaders().empty());
        },
        cancellation.slot());

    // The slot is only connected once the coroutine has started the request, so run that first.
    context.run_one();
    cancellation.emit(asio::cancellation_type::terminal);
    RunBounded(context);
    EXPECT_EQ(completions, 1);
}

TEST(HttpClientRequest, CancellationMidFlightCompletesWithOperationCanceled) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    bool requestRead = false;
    asio::cancellation_signal cancellation;
    ScriptedServer server(context, [&](int, const http::request<http::string_body>&) {
        requestRead = true;
        // The server has the request and never answers: cancel now rather than after a fixed delay.
        cancellation.emit(asio::cancellation_type::terminal);
        ServerAction action;
        action.Reply = ServerReply::Hold;
        return action;
    });

    AVEVA::HttpRequestOptions options;
    options.SetTimeout(AVEVA::Timeout{std::chrono::seconds(5)});

    AVEVA::HttpRequest request;
    request.SetUrl(AVEVA::HttpUrl{server.Url()});

    int completions = 0;
    HttpClientTests::SendWith(
        *client, request, options,
        [&](std::error_code error, AVEVA::HttpResponse response) {
            ++completions;
            EXPECT_EQ(error, std::make_error_code(std::errc::operation_canceled));
            EXPECT_EQ(response.GetStatus(), AVEVA::HttpStatusCode{0});
            server.Stop();
        },
        cancellation.slot());

    RunBounded(context);
    EXPECT_TRUE(requestRead);
    EXPECT_EQ(completions, 1);
}

TEST(HttpClientRequest, CancellingAfterCompletionIsANoop) {
    asio::io_context context;
    auto client = std::make_unique<AVEVA::HttpClient>(context);
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) { return ServerAction{OkResponse}; });

    asio::cancellation_signal cancellation;
    AVEVA::HttpRequestOptions options;

    AVEVA::HttpRequest request;
    request.SetUrl(AVEVA::HttpUrl{server.Url()});

    int completions = 0;
    HttpClientTests::SendWith(
        *client, request, options,
        [&](std::error_code error, AVEVA::HttpResponse response) {
            ++completions;
            EXPECT_FALSE(error);
            EXPECT_EQ(response.GetStatus(), AVEVA::HttpStatusCode{200});
            server.Stop();
        },
        cancellation.slot());

    RunBounded(context);
    EXPECT_EQ(completions, 1);

    cancellation.emit(asio::cancellation_type::terminal);
    context.restart();
    RunBounded(context);
    EXPECT_EQ(completions, 1);
}

TEST(HttpClientRequest, BodyOwnerIsSharedByCopiesAndDroppedByReplacingTheBody) {
    auto owner = std::make_shared<const std::string>("shared payload");
    const std::weak_ptr<const std::string> weak = owner;
    AVEVA::HttpRequest request;
    request.SetBody(AVEVA::HttpBody(std::as_bytes(std::span{owner->data(), owner->size()}), owner));
    owner.reset();

    AVEVA::HttpRequest copy = request;
    request.SetBody(AVEVA::HttpBody{"other"});
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(copy.GetBody().Size(), AVEVA::ByteCount{14});

    copy.SetBody(AVEVA::HttpBody{});
    EXPECT_TRUE(weak.expired());
}

TEST(HttpClientRequest, SharedBodyIsTransmittedAfterTheCallerDropsIt) {
    auto owner = std::make_shared<const std::string>("shared payload");
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Post);
    request.SetBody(AVEVA::HttpBody(std::as_bytes(std::span{owner->data(), owner->size()}), owner));
    owner.reset();

    auto result = Exchange(request, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");

    EXPECT_FALSE(result.error);
    EXPECT_EQ(result.received.body(), "shared payload");
}

TEST(HttpClientRequest, BuildFailuresAreNotAllReportedAsInvalidRequests) {
    using AVEVA::Private::ClassifyBuildFailure;
    const auto classify = [](auto exception) {
        try {
            throw exception;
        } catch (...) {
            return ClassifyBuildFailure(std::current_exception());
        }
    };

    EXPECT_EQ(classify(std::length_error("header too long")), AVEVA::HttpClientError::InvalidRequest);
    EXPECT_EQ(classify(std::invalid_argument("bad")), AVEVA::HttpClientError::InvalidRequest);
    EXPECT_EQ(classify(boost::system::system_error(http::error::bad_field)), AVEVA::HttpClientError::InvalidRequest);
    EXPECT_EQ(classify(boost::system::system_error(boost::asio::error::no_memory)),
              std::make_error_code(std::errc::not_enough_memory));
    EXPECT_EQ(classify(boost::system::system_error(boost::asio::error::connection_reset)),
              std::make_error_code(std::errc::state_not_recoverable));
    EXPECT_EQ(classify(std::bad_alloc()), std::make_error_code(std::errc::not_enough_memory));
    EXPECT_EQ(classify(std::runtime_error("unexpected")), std::make_error_code(std::errc::state_not_recoverable));
    EXPECT_EQ(classify(42), std::make_error_code(std::errc::state_not_recoverable));
}
} // namespace
