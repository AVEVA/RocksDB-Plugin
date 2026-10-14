// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "HttpClientTestHelpers.hpp"

#include <gtest/gtest.h>

#include <chrono>

namespace
{
    using HttpClientTests::Exchange;

    TEST(HttpClientResponse, InformationalResponseReadsFinalChunkedResponse)
    {
        auto result = Exchange({},
            "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n"
            "Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n");

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.response.GetStatus(), 200u);
        EXPECT_EQ(result.response.GetBody(), "hello");
    }

    TEST(HttpClientResponse, BodyLimitReturnsErrorWithoutPartialResponse)
    {
        AVEVA::HttpRequestOptions options;
        options.SetResponseBodyLimit(2);
        auto result = Exchange({}, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello", options);

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
        EXPECT_EQ(result.response.GetStatus(), 0u);
        EXPECT_TRUE(result.response.GetBody().empty());
        EXPECT_TRUE(result.response.GetHeaders().empty());
    }

    TEST(HttpClientResponse, MalformedResponseReturnsProtocolError)
    {
        auto result = Exchange({}, "NOT HTTP\r\n\r\n");

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::ProtocolError));
    }

    TEST(HttpClientResponse, SwitchingProtocolsReturnsProtocolError)
    {
        auto result =
            Exchange({}, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n");

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::ProtocolError));
    }

    TEST(HttpClientResponse, OversizedResponseHeadersReturnResponseTooLarge)
    {
        const std::string wire =
            "HTTP/1.1 200 OK\r\nX-Big: " + std::string(70000, 'a') + "\r\nContent-Length: 0\r\n\r\n";
        auto result = Exchange({}, wire);

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
        EXPECT_EQ(result.response.GetStatus(), 0U);
    }

    TEST(HttpClientResponse, LargeResponseHeaderBlockWithinLimitSucceeds)
    {
        const std::string wire =
            "HTTP/1.1 200 OK\r\nX-Big: " + std::string(16 * 1024, 'a') + "\r\nContent-Length: 0\r\n\r\n";
        auto result = Exchange({}, wire);

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.response.GetStatus(), 200U);
    }

    TEST(HttpClientResponse, ConfiguredHeaderLimitIsEnforced)
    {
        AVEVA::HttpRequestOptions options;
        options.SetResponseHeaderLimit(1024);
        const std::string wire =
            "HTTP/1.1 200 OK\r\nX-Big: " + std::string(4096, 'a') + "\r\nContent-Length: 0\r\n\r\n";
        auto result = Exchange({}, wire, options);

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
    }

    TEST(HttpClientResponse, HeaderBytesDoNotCountAgainstTheBodyLimit)
    {
        AVEVA::HttpRequestOptions options;
        options.SetResponseBodyLimit(4);
        const std::string wire =
            "HTTP/1.1 200 OK\r\nX-Big: " + std::string(8 * 1024, 'a') + "\r\nContent-Length: 4\r\n\r\nabcd";
        auto result = Exchange({}, wire, options);

        EXPECT_FALSE(result.error);
        EXPECT_EQ(result.response.GetBody(), "abcd");
    }

    TEST(HttpClientResponse, BodyExactlyAtTheLimitSucceedsAndOneByteOverFails)
    {
        AVEVA::HttpRequestOptions options;
        options.SetResponseBodyLimit(4);

        auto atLimit = Exchange({}, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nabcd", options);
        EXPECT_FALSE(atLimit.error);
        EXPECT_EQ(atLimit.response.GetBody(), "abcd");

        auto overLimit = Exchange({}, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabcde", options);
        EXPECT_EQ(overLimit.error, AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
    }

    TEST(HttpClientResponse, ChunkedBodyOverTheBodyLimitReturnsResponseTooLarge)
    {
        AVEVA::HttpRequestOptions options;
        options.SetResponseBodyLimit(4);
        auto result = Exchange(
            {}, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n", options);

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
        EXPECT_TRUE(result.response.GetBody().empty());
    }

    TEST(HttpClientResponse, HeaderLimitIsEnforcedEvenWhenTheBodyLimitIsGenerous)
    {
        AVEVA::HttpRequestOptions options;
        options.SetResponseHeaderLimit(1024);
        options.SetResponseBodyLimit(1024 * 1024);
        const std::string wire =
            "HTTP/1.1 200 OK\r\nX-Big: " + std::string(4096, 'a') + "\r\nContent-Length: 2\r\n\r\nok";
        auto result = Exchange({}, wire, options);

        EXPECT_EQ(result.error, AVEVA::make_error_code(AVEVA::HttpClientError::ResponseTooLarge));
        EXPECT_TRUE(result.response.GetBody().empty());
    }
} // namespace