// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "HttpClientTestHelpers.hpp"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <system_error>

namespace {
namespace asio = boost::asio;
namespace http = boost::beast::http;
using HttpClientTests::RunBounded;
using HttpClientTests::ScriptedServer;
using HttpClientTests::ServerAction;
using HttpClientTests::ServerReply;

constexpr const char* const Ok = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
constexpr const char* const Unavailable = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
constexpr const char* const UnavailableRetryNow =
    "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 0\r\nContent-Length: 0\r\n\r\n";

struct Outcome {
    std::error_code error;
    AVEVA::HttpStatusCode status{0};
};

AVEVA::RetryOptions FastRetries(std::uint32_t maxRetries) {
    AVEVA::RetryOptions retry;
    retry.MaxRetries = AVEVA::RetryCount{maxRetries};
    retry.InitialDelay = std::chrono::milliseconds{1};
    retry.MaxDelay = std::chrono::milliseconds{5};
    return retry;
}

// Sends one request to `server` and runs the context until the server is stopped on completion.
Outcome Send(asio::io_context& context, ScriptedServer& server, AVEVA::HttpMethod method,
             AVEVA::HttpRequestOptions options) {
    AVEVA::HttpClient client(context);
    AVEVA::HttpRequest request;
    request.SetMethod(method);
    request.SetUrl(AVEVA::HttpUrl{server.Url()});
    Outcome outcome;
    HttpClientTests::SendWith(client, request, std::move(options),
                              [&](std::error_code error, AVEVA::HttpResponse response) {
                                  outcome = {error, response.GetStatus()};
                                  server.Stop();
                              });
    RunBounded(context);
    return outcome;
}

AVEVA::HttpRequestOptions WithRetries(std::uint32_t maxRetries) {
    AVEVA::HttpRequestOptions options;
    options.SetRetryOptions(FastRetries(maxRetries));
    return options;
}

TEST(HttpClientRetry, GetIsRetriedAfterA503) {
    asio::io_context context;
    int requests = 0;
    ScriptedServer flaky(context, [&](int, const http::request<http::string_body>&) {
        return ServerAction{++requests == 1 ? Unavailable : Ok};
    });

    const auto outcome = Send(context, flaky, AVEVA::HttpMethod::Get, WithRetries(3));

    EXPECT_FALSE(outcome.error);
    EXPECT_EQ(outcome.status, AVEVA::HttpStatusCode{200});
    EXPECT_EQ(flaky.TotalRequests(), 2);
}

TEST(HttpClientRetry, GivesUpAfterMaxRetriesAndReturnsTheLastResponse) {
    asio::io_context context;
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) { return ServerAction{Unavailable}; });

    const auto outcome = Send(context, server, AVEVA::HttpMethod::Get, WithRetries(2));

    EXPECT_FALSE(outcome.error);
    EXPECT_EQ(outcome.status, AVEVA::HttpStatusCode{503});
    EXPECT_EQ(server.TotalRequests(), 3);
}

TEST(HttpClientRetry, WithoutAPolicyTheRequestIsSentOnce) {
    asio::io_context context;
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) { return ServerAction{Unavailable}; });

    const auto outcome = Send(context, server, AVEVA::HttpMethod::Get, AVEVA::HttpRequestOptions{});

    EXPECT_EQ(outcome.status, AVEVA::HttpStatusCode{503});
    EXPECT_EQ(server.TotalRequests(), 1);
}

TEST(HttpClientRetry, PostIsNotRetriedOnAStatus) {
    asio::io_context context;
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) { return ServerAction{Unavailable}; });

    const auto outcome = Send(context, server, AVEVA::HttpMethod::Post, WithRetries(3));

    EXPECT_EQ(outcome.status, AVEVA::HttpStatusCode{503});
    EXPECT_EQ(server.TotalRequests(), 1);
}

TEST(HttpClientRetry, GetIsRetriedAfterTheConnectionIsDropped) {
    asio::io_context context;
    int requests = 0;
    ScriptedServer server(context, [&](int, const http::request<http::string_body>&) {
        return ++requests == 1 ? ServerAction{"", ServerReply::Drop} : ServerAction{Ok};
    });

    const auto outcome = Send(context, server, AVEVA::HttpMethod::Get, WithRetries(3));

    EXPECT_FALSE(outcome.error);
    EXPECT_EQ(outcome.status, AVEVA::HttpStatusCode{200});
}

TEST(HttpClientRetry, PostIsNotRetriedOnceItMayHaveReachedTheServer) {
    asio::io_context context;
    ScriptedServer server(
        context, [](int, const http::request<http::string_body>&) { return ServerAction{"", ServerReply::Drop}; });

    const auto outcome = Send(context, server, AVEVA::HttpMethod::Post, WithRetries(3));

    EXPECT_EQ(outcome.error, AVEVA::HttpClientError::ReadFailed);
    EXPECT_EQ(server.TotalRequests(), 1);
}

TEST(HttpClientRetry, RetryAfterOverridesTheBackoff) {
    asio::io_context context;
    int requests = 0;
    ScriptedServer server(context, [&](int, const http::request<http::string_body>&) {
        return ServerAction{++requests == 1 ? UnavailableRetryNow : Ok};
    });
    AVEVA::RetryOptions retry;
    retry.MaxRetries = AVEVA::RetryCount{1};
    retry.InitialDelay = std::chrono::seconds{30};
    retry.MaxDelay = std::chrono::seconds{30};
    AVEVA::HttpRequestOptions options;
    options.SetRetryOptions(retry);

    // Backing off for 30 s would trip RunBounded; the hint of 0 s must be used instead.
    const auto outcome = Send(context, server, AVEVA::HttpMethod::Get, options);

    EXPECT_EQ(outcome.status, AVEVA::HttpStatusCode{200});
    EXPECT_EQ(server.TotalRequests(), 2);
}

TEST(HttpClientRetry, RetryAfterIsCappedByTheCallersMaxDelay) {
    asio::io_context context;
    int requests = 0;
    ScriptedServer server(context, [&](int, const http::request<http::string_body>&) {
        return ServerAction{++requests == 1 ? "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 30\r\n"
                                              "Content-Length: 0\r\n\r\n"
                                            : Ok};
    });
    AVEVA::HttpRequestOptions options;
    options.SetRetryOptions(FastRetries(1));
    options.SetTimeout(AVEVA::Timeout{std::chrono::seconds{2}});

    const auto outcome = Send(context, server, AVEVA::HttpMethod::Get, options);

    EXPECT_FALSE(outcome.error) << outcome.error.message();
    EXPECT_EQ(outcome.status, AVEVA::HttpStatusCode{200});
}

TEST(HttpClientRetry, TheRequestTimeoutCapsTheRetries) {
    asio::io_context context;
    ScriptedServer server(context,
                          [](int, const http::request<http::string_body>&) { return ServerAction{Unavailable}; });
    AVEVA::RetryOptions retry;
    retry.MaxRetries = AVEVA::RetryCount{5};
    retry.InitialDelay = std::chrono::seconds{30};
    retry.MaxDelay = std::chrono::seconds{30};
    AVEVA::HttpRequestOptions options;
    options.SetRetryOptions(retry);
    options.SetTimeout(AVEVA::Timeout{std::chrono::milliseconds{300}});

    const auto outcome = Send(context, server, AVEVA::HttpMethod::Get, options);

    EXPECT_EQ(outcome.error, AVEVA::HttpClientError::TimedOut);
    EXPECT_EQ(server.TotalRequests(), 1);
}
} // namespace
