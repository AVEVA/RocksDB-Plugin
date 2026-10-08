// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "BlobRequestHelpers.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <gtest/gtest.h>

#include <boost/asio/awaitable.hpp>

#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <utility>

namespace AVEVA::AzureClient::Tests
{
    // Awaits the deferred operation produced by `makeOperation()` and stores its result through
    // `out`. This is a free coroutine taking a pointer rather than a capturing coroutine lambda, so
    // the coroutine frame never outlives captured references (cppcoreguidelines-avoid-capturing-
    // lambda-coroutines / cppcoreguidelines-avoid-reference-coroutine-parameters).
    template <class Result, class MakeOperation>
    boost::asio::awaitable<void> AwaitInto(std::optional<Result>* out, MakeOperation makeOperation)
    {
        *out = co_await makeOperation();
    }

    // Same idea, but only records whether the awaited result was engaged.
    template <class MakeOperation> boost::asio::awaitable<void> AwaitHasValue(bool* out, MakeOperation makeOperation)
    {
        const auto result = co_await makeOperation();
        *out = result.has_value();
    }

    class CallbackExpectation
    {
      public:
        CallbackExpectation() = default;
        CallbackExpectation(const CallbackExpectation&) = delete;
        CallbackExpectation& operator=(const CallbackExpectation&) = delete;
        CallbackExpectation(CallbackExpectation&&) = delete;
        CallbackExpectation& operator=(CallbackExpectation&&) = delete;

        ~CallbackExpectation()
        {
            EXPECT_TRUE(m_invoked) << "Completion callback was not invoked.";
        }

        void MarkInvoked()
        {
            m_invoked = true;
        }

      private:
        bool m_invoked = false;
    };

    inline void ExpectOptionalBodyMatches(const HttpRequest& request, std::optional<std::string_view> expectedBody)
    {
        if (expectedBody.has_value())
        {
            EXPECT_EQ(request.GetBody(), *expectedBody);
        }
    }

    inline void ExpectRequestContract(const HttpRequest& request,
        HttpMethod expectedMethod,
        std::string_view expectedUrl,
        std::optional<std::string_view> expectedBody = std::nullopt,
        std::string_view expectedApiVersion = DefaultApiVersion)
    {
        EXPECT_EQ(request.GetMethod(), expectedMethod);
        EXPECT_EQ(request.GetUrl(), expectedUrl);
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-version"), expectedApiVersion);

        const std::string dateHeader = FakeHttpClient::FindHeaderValue(request, "x-ms-date");
        EXPECT_FALSE(dateHeader.empty());
        EXPECT_TRUE(Private::ParseHttpDateHeader(dateHeader).has_value());

        const std::string requestId = FakeHttpClient::FindHeaderValue(request, "x-ms-client-request-id");
        EXPECT_TRUE(std::regex_match(requestId,
            std::regex{"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"}));

        ExpectOptionalBodyMatches(request, expectedBody);
    }

    [[nodiscard]] inline AccessToken MakeToken(std::string token, std::chrono::seconds expiresIn)
    {
        return AccessToken{.Token = std::move(token), .ExpiresOn = std::chrono::system_clock::now() + expiresIn};
    }
} // namespace AVEVA::AzureClient::Tests
