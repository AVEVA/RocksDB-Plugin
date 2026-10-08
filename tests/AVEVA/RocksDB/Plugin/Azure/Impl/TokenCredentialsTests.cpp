// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"

#include "FakeHttpClient.hpp"
#include "FakeHttpPump.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <thread>

using AVEVA::HttpHeader;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::AccessToken;
using AVEVA::AzureClient::ClientSecretCredential;
using AVEVA::AzureClient::ClientSecretCredentialOptions;
using AVEVA::AzureClient::StaticTokenCredential;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::RocksDB::Plugin::Azure::Impl::AzurePipelinesCredential;
using AVEVA::RocksDB::Plugin::Azure::Impl::AzurePipelinesCredentialOptions;
using AVEVA::RocksDB::Plugin::Azure::Impl::ChainedTokenCredential;

namespace {
class TokenCredentialsTests : public ::testing::Test {
  protected:
    void SetUp() override {
        m_httpClient.CompleteInline() = true;
        m_pump = AVEVA::RocksDB::Plugin::Azure::Impl::Tests::StartFakeHttpPump(m_httpClient);
    }

    std::pair<std::error_code, AccessToken> GetToken(AVEVA::AzureClient::ITokenCredential& credential) {
        auto promise = std::make_shared<std::promise<std::pair<std::error_code, AccessToken>>>();
        auto future = promise->get_future();
        credential.GetTokenAsync(
            {"https://storage.azure.com/.default"},
            [promise](std::error_code error, AccessToken token) { promise->set_value({error, std::move(token)}); });
        if (future.wait_for(std::chrono::seconds(20)) != std::future_status::ready) {
            ADD_FAILURE() << "token request did not complete";
            return {std::make_error_code(std::errc::timed_out), AccessToken{}};
        }
        return future.get();
    }

    static AzurePipelinesCredentialOptions PipelineOptions() {
        AzurePipelinesCredentialOptions options;
        options.TenantId = "tenant";
        options.ClientId = "client";
        options.ServiceConnectionId = "connection";
        options.SystemAccessToken = "system";
        options.OidcRequestUri = "https://pipelines.example/oidc";
        options.MaxRetries = 2;
        return options;
    }

    static HttpResponse OidcResponse() { return HttpResponse{200, {}, R"({"oidcToken":"oidc"})"}; }

    static HttpResponse TokenResponse() {
        return HttpResponse{200, {}, R"({"access_token":"tok","expires_in":"3600"})"};
    }

    FakeHttpClient m_httpClient;
    std::jthread m_pump;
};

TEST_F(TokenCredentialsTests, ChainFallsThroughToLaterSource) {
    auto failing = std::make_shared<ClientSecretCredential>(m_httpClient, ClientSecretCredentialOptions{});
    auto working = std::make_shared<StaticTokenCredential>("static");
    auto chain = std::make_shared<ChainedTokenCredential>(
        std::vector<std::shared_ptr<AVEVA::AzureClient::ITokenCredential>>{failing, working});

    const auto [error, token] = GetToken(*chain);

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "static");
}

TEST_F(TokenCredentialsTests, ChainReportsLastErrorWhenAllSourcesFail) {
    auto first = std::make_shared<ClientSecretCredential>(m_httpClient, ClientSecretCredentialOptions{});
    auto second = std::make_shared<ClientSecretCredential>(m_httpClient, ClientSecretCredentialOptions{});
    auto chain = std::make_shared<ChainedTokenCredential>(
        std::vector<std::shared_ptr<AVEVA::AzureClient::ITokenCredential>>{first, second});

    const auto [error, token] = GetToken(*chain);

    EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument));
}

TEST_F(TokenCredentialsTests, RetryAfterIsHonoredOn429) {
    m_httpClient.EnqueueResponse(HttpResponse{429, {HttpHeader{"Retry-After", "1"}}, ""});
    m_httpClient.EnqueueResponse(OidcResponse());
    m_httpClient.EnqueueResponse(TokenResponse());
    auto credential = std::make_shared<AzurePipelinesCredential>(m_httpClient, PipelineOptions());

    const auto start = std::chrono::steady_clock::now();
    const auto [error, token] = GetToken(*credential);
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "tok");
    EXPECT_GE(elapsed, std::chrono::milliseconds(900));
}

class DeferredCredential final : public AVEVA::AzureClient::ITokenCredential {
  public:
    void GetTokenAsync(std::vector<std::string>, GetTokenCompletionHandler completion) override {
        Pending = std::move(completion);
    }
    GetTokenCompletionHandler Pending;
};

TEST(RuntimeBoundCredentialTests, PendingRefreshKeepsRuntimeAliveUntilCompletion) {
    boost::asio::io_context context;
    auto runtime = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime>(context);
    const std::weak_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime> weak = runtime;
    auto inner = std::make_shared<DeferredCredential>();
    auto bound = std::make_unique<AVEVA::RocksDB::Plugin::Azure::Impl::RuntimeBoundCredential>(runtime, inner);
    bool completed = false;
    bound->GetTokenAsync({"scope"}, [&completed](std::error_code, AccessToken) { completed = true; });

    // The filesystem goes away while the refresh is pending.
    bound.reset();
    runtime.reset();
    EXPECT_FALSE(weak.expired());

    inner->Pending(std::error_code{}, AccessToken{"tok"});
    EXPECT_TRUE(completed);
    inner.reset();
    context.run();
    EXPECT_TRUE(weak.expired());
}

TEST_F(TokenCredentialsTests, TokenLifetimeBeyondMaximumIsClampedNotRejected) {
    m_httpClient.EnqueueResponse(OidcResponse());
    m_httpClient.EnqueueResponse(HttpResponse{200, {}, R"({"access_token":"tok","expires_in":"86401"})"});
    auto credential = std::make_shared<AzurePipelinesCredential>(m_httpClient, PipelineOptions());

    const auto [error, token] = GetToken(*credential);

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "tok");
}

TEST_F(TokenCredentialsTests, NonTransientFailureIsNotRetried) {
    m_httpClient.EnqueueResponse(HttpResponse{401, {}, ""});
    auto credential = std::make_shared<AzurePipelinesCredential>(m_httpClient, PipelineOptions());

    const auto [error, token] = GetToken(*credential);

    EXPECT_TRUE(error);
    EXPECT_EQ(m_httpClient.RequestCount(), 1U);
}
} // namespace
