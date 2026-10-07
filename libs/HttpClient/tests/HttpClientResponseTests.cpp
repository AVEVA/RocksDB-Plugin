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
        auto result = Exchange({}, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n");

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
} // namespace