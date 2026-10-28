// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <thread>

using AVEVA::HttpHeader;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::AccessToken;
using AVEVA::AzureClient::AzurePipelinesCredentialOptions;
using AVEVA::AzureClient::BasicAzurePipelinesCredential;
using AVEVA::AzureClient::ClientSecretCredentialOptions;
using AVEVA::AzureClient::Tests::ClientSecretCredential;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::StaticTokenCredential;
using AVEVA::RocksDB::Plugin::Azure::Impl::ChainedTokenCredential;
using AVEVA::RocksDB::Plugin::Azure::Impl::CredentialVariant;
using PipelinesCredential = BasicAzurePipelinesCredential<FakeHttpClient>;
using TestSource = CredentialVariant<ClientSecretCredential, StaticTokenCredential>;
using TestChain = ChainedTokenCredential<TestSource>;

namespace {
class TokenCredentialsTests : public ::testing::Test {
  protected:
    // Runs the token request on the fake HTTP client's executor, which also runs retry timers, until it completes.
    template <class TCred> std::pair<std::error_code, AccessToken> GetToken(TCred& credential) {
        auto result = AVEVA::AzureClient::Tests::RunAwaitable(
            m_httpClient, credential.GetTokenAsync({"https://storage.azure.com/.default"}));
        if (!result) {
            return {result.error(), AccessToken{}};
        }
        return {std::error_code{}, std::move(*result)};
    }
    AzurePipelinesCredentialOptions PipelineOptions() const {
        AzurePipelinesCredentialOptions options;
        options.TenantId = "tenant";
        options.ClientId = "client";
        options.ServiceConnectionId = "connection";
        options.SystemAccessToken = "system";
        options.OidcRequestUri = "https://pipelines.example/oidc";
        options.Retry.MaxRetries = 2;
        return options;
    }

    static HttpResponse OidcResponse() { return HttpResponse{200, {}, R"({"oidcToken":"oidc"})"}; }

    static HttpResponse TokenResponse() {
        return HttpResponse{200, {}, R"({"access_token":"tok","expires_in":"3600"})"};
    }

    FakeHttpClient m_httpClient;
};

TEST_F(TokenCredentialsTests, ChainFallsThroughToLaterSource) {
    auto failing = std::make_shared<ClientSecretCredential>(m_httpClient, ClientSecretCredentialOptions{});
    auto working = std::make_shared<StaticTokenCredential>("static");
    auto chain = std::make_shared<TestChain>(std::vector<std::shared_ptr<TestSource>>{
        std::make_shared<TestSource>(failing), std::make_shared<TestSource>(working)});

    const auto [error, token] = GetToken(*chain);

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "static");
}

TEST_F(TokenCredentialsTests, ChainReportsLastErrorWhenAllSourcesFail) {
    auto first = std::make_shared<ClientSecretCredential>(m_httpClient, ClientSecretCredentialOptions{});
    auto second = std::make_shared<ClientSecretCredential>(m_httpClient, ClientSecretCredentialOptions{});
    auto chain = std::make_shared<TestChain>(std::vector<std::shared_ptr<TestSource>>{
        std::make_shared<TestSource>(first), std::make_shared<TestSource>(second)});

    const auto [error, token] = GetToken(*chain);

    EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument));
}

TEST_F(TokenCredentialsTests, RetryAfterIsHonoredOn429) {
    m_httpClient.EnqueueResponse(HttpResponse{429, {HttpHeader{"Retry-After", "1"}}, ""});
    m_httpClient.EnqueueResponse(OidcResponse());
    m_httpClient.EnqueueResponse(TokenResponse());
    auto credential = std::make_shared<PipelinesCredential>(m_httpClient, PipelineOptions());

    const auto [error, token] = GetToken(*credential);

    // The retry is observable by the request count rather than by wall-clock elapsed time.
    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "tok");
    EXPECT_EQ(m_httpClient.RequestCount(), 3U);
}

TEST_F(TokenCredentialsTests, TokenLifetimeBeyondMaximumIsClampedNotRejected) {
    m_httpClient.EnqueueResponse(OidcResponse());
    m_httpClient.EnqueueResponse(HttpResponse{200, {}, R"({"access_token":"tok","expires_in":"86401"})"});
    auto credential = std::make_shared<PipelinesCredential>(m_httpClient, PipelineOptions());

    const auto [error, token] = GetToken(*credential);

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "tok");
}

TEST_F(TokenCredentialsTests, NonTransientFailureIsNotRetried) {
    m_httpClient.EnqueueResponse(HttpResponse{401, {}, ""});
    auto credential = std::make_shared<PipelinesCredential>(m_httpClient, PipelineOptions());

    const auto [error, token] = GetToken(*credential);

    EXPECT_TRUE(error);
    EXPECT_EQ(m_httpClient.RequestCount(), 1U);
}
} // namespace
