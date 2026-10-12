// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"

#include "FakeHttpBridge.hpp"
#include "FakeHttpClient.hpp"

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
using AVEVA::RocksDB::Plugin::Azure::Impl::CredentialVariant;
using TestSource = CredentialVariant<ClientSecretCredential, StaticTokenCredential>;
using TestChain = ChainedTokenCredential<TestSource>;

namespace {
class TokenCredentialsTests : public ::testing::Test {
  protected:
    void SetUp() override {
        m_httpClient.CompleteInline() = true;
        m_bridge = std::make_unique<AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeHttpBridge>(m_httpClient);
    }

    template <class TCred> std::pair<std::error_code, AccessToken> GetToken(TCred& credential) {
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

    AzurePipelinesCredentialOptions PipelineOptions() const {
        AzurePipelinesCredentialOptions options;
        options.TenantId = "tenant";
        options.ClientId = "client";
        options.ServiceConnectionId = "connection";
        options.SystemAccessToken = "system";
        options.OidcRequestUri = m_bridge->Endpoint() + "/oidc";
        options.AuthorityHost = m_bridge->Endpoint() + "/";
        options.MaxRetries = 2;
        return options;
    }

    static HttpResponse OidcResponse() { return HttpResponse{200, {}, R"({"oidcToken":"oidc"})"}; }

    static HttpResponse TokenResponse() {
        return HttpResponse{200, {}, R"({"access_token":"tok","expires_in":"3600"})"};
    }

    FakeHttpClient m_httpClient;
    std::unique_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeHttpBridge> m_bridge;
};

TEST_F(TokenCredentialsTests, ChainFallsThroughToLaterSource) {
    auto failing = std::make_shared<ClientSecretCredential>(m_bridge->Client(), ClientSecretCredentialOptions{});
    auto working = std::make_shared<StaticTokenCredential>("static");
    auto chain = std::make_shared<TestChain>(std::vector<std::shared_ptr<TestSource>>{
        std::make_shared<TestSource>(failing), std::make_shared<TestSource>(working)});

    const auto [error, token] = GetToken(*chain);

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "static");
}

TEST_F(TokenCredentialsTests, ChainReportsLastErrorWhenAllSourcesFail) {
    auto first = std::make_shared<ClientSecretCredential>(m_bridge->Client(), ClientSecretCredentialOptions{});
    auto second = std::make_shared<ClientSecretCredential>(m_bridge->Client(), ClientSecretCredentialOptions{});
    auto chain = std::make_shared<TestChain>(std::vector<std::shared_ptr<TestSource>>{
        std::make_shared<TestSource>(first), std::make_shared<TestSource>(second)});

    const auto [error, token] = GetToken(*chain);

    EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument));
}

TEST_F(TokenCredentialsTests, RetryAfterIsHonoredOn429) {
    m_httpClient.EnqueueResponse(HttpResponse{429, {HttpHeader{"Retry-After", "1"}}, ""});
    m_httpClient.EnqueueResponse(OidcResponse());
    m_httpClient.EnqueueResponse(TokenResponse());
    auto credential = std::make_shared<AzurePipelinesCredential>(m_bridge->Client(), PipelineOptions());

    const auto start = std::chrono::steady_clock::now();
    const auto [error, token] = GetToken(*credential);
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "tok");
    EXPECT_GE(elapsed, std::chrono::milliseconds(900));
}

class DeferredCredential final {
  public:
    template <boost::asio::completion_token_for<AVEVA::AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<AVEVA::AzureClient::TokenSignature>(
            [this](auto handler, std::vector<std::string>) { Pending = std::move(handler); }, token, std::move(scopes));
    }
    std::move_only_function<AVEVA::AzureClient::TokenSignature> Pending;
};

TEST(RuntimeBoundCredentialTests, PendingRefreshKeepsRuntimeAliveUntilCompletion) {
    boost::asio::io_context context;
    auto runtime = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime>(context);
    const std::weak_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime> weak = runtime;
    auto inner = std::make_shared<DeferredCredential>();
    auto bound = std::make_unique<AVEVA::RocksDB::Plugin::Azure::Impl::RuntimeBoundCredential<DeferredCredential>>(
        runtime, inner);
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
    auto credential = std::make_shared<AzurePipelinesCredential>(m_bridge->Client(), PipelineOptions());

    const auto [error, token] = GetToken(*credential);

    EXPECT_FALSE(error);
    EXPECT_EQ(token.Token, "tok");
}

TEST_F(TokenCredentialsTests, NonTransientFailureIsNotRetried) {
    m_httpClient.EnqueueResponse(HttpResponse{401, {}, ""});
    auto credential = std::make_shared<AzurePipelinesCredential>(m_bridge->Client(), PipelineOptions());

    const auto [error, token] = GetToken(*credential);

    EXPECT_TRUE(error);
    EXPECT_EQ(m_httpClient.RequestCount(), 1U);
}
} // namespace
