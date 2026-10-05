// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/Credentials.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <optional>
#include <thread>
#include <typeinfo>
#include <vector>

using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobContainerClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::BlobHelpers;

namespace {
class BlobHelpersTests : public ::testing::Test {
  protected:
    void SetUp() override {
        m_httpClient.CompleteInline() = true;
        m_pump = std::jthread([this](const std::stop_token& stop) {
            while (!stop.stop_requested()) {
                m_httpClient.Poll();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    FakeHttpClient m_httpClient;
    std::jthread m_pump;
};

TEST_F(BlobHelpersTests, ForbiddenIsNotRetried) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(403, "AuthorizationFailure", "no", "id"));
    BlobContainerClient client(m_httpClient, MakeBlobContainerClientOptions("cont"));

    EXPECT_THROW(BlobHelpers::CreateContainerIfNotExists(client, 5, std::chrono::milliseconds(1)),
                 RequestFailedException);
    EXPECT_EQ(m_httpClient.RequestCount(), 1U);
}

TEST_F(BlobHelpersTests, ServerBusyIsRetriedUntilSuccess) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(503, "ServerBusy", "busy", "id"));
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(503, "ServerBusy", "busy", "id"));
    m_httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({}), ""});
    BlobContainerClient client(m_httpClient, MakeBlobContainerClientOptions("cont"));

    EXPECT_NO_THROW(BlobHelpers::CreateContainerIfNotExists(client, 5, std::chrono::milliseconds(1)));
    EXPECT_EQ(m_httpClient.RequestCount(), 3U);
}

TEST_F(BlobHelpersTests, RetriesAreBounded) {
    for (int i = 0; i < 3; ++i) {
        m_httpClient.EnqueueResponse(MakeAzureErrorResponse(503, "ServerBusy", "busy", "id"));
    }
    BlobContainerClient client(m_httpClient, MakeBlobContainerClientOptions("cont"));

    EXPECT_THROW(BlobHelpers::CreateContainerIfNotExists(client, 3, std::chrono::milliseconds(1)),
                 RequestFailedException);
    EXPECT_EQ(m_httpClient.RequestCount(), 3U);
}
class ScopedEnv {
  public:
    ScopedEnv(const char* name, const char* value) : m_name(name) { Set(value); }
    ~ScopedEnv() { Set(nullptr); }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

  private:
    void Set(const char* value) const {
#ifdef _WIN32
        _putenv_s(m_name, value == nullptr ? "" : value);
#else
        if (value == nullptr) {
            unsetenv(m_name);
        } else {
            setenv(m_name, value, 1);
        }
#endif
    }
    const char* m_name;
};

std::vector<const std::type_info*> SourceTypes(const std::optional<std::string>& managedIdentityId) {
    boost::asio::io_context context;
    AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime runtime(context);
    const AVEVA::RocksDB::Plugin::Azure::Models::ChainedCredentialInfo info(
        "db", "https://acct.blob.core.windows.net", "sp", "secret", "tenant", managedIdentityId);
    std::vector<const std::type_info*> types;
    for (const auto& source : BlobHelpers::CreateCredentialSources(runtime, info)) {
        types.push_back(&typeid(*source));
    }
    return types;
}

TEST(CredentialChainTests, DefaultsToServicePrincipalThenSystemAssignedManagedIdentity) {
    const ScopedEnv tenant("AZURE_TENANT_ID", nullptr);
    const ScopedEnv client("AZURE_CLIENT_ID", nullptr);
    const ScopedEnv secret("AZURE_CLIENT_SECRET", nullptr);
    const ScopedEnv tokenFile("AZURE_FEDERATED_TOKEN_FILE", nullptr);

    const auto types = SourceTypes(std::nullopt);

    ASSERT_EQ(types.size(), 2U);
    EXPECT_EQ(*types[0], typeid(AVEVA::AzureClient::ClientSecretCredential));
    EXPECT_EQ(*types[1], typeid(AVEVA::AzureClient::ManagedIdentityCredential));
}

TEST(CredentialChainTests, EnvironmentAndWorkloadIdentityAreAddedWhenConfigured) {
    const ScopedEnv tenant("AZURE_TENANT_ID", "t");
    const ScopedEnv client("AZURE_CLIENT_ID", "c");
    const ScopedEnv secret("AZURE_CLIENT_SECRET", "s");
    const ScopedEnv tokenFile("AZURE_FEDERATED_TOKEN_FILE", "/tmp/token");

    const auto types = SourceTypes("managed-id");

    ASSERT_EQ(types.size(), 4U);
    EXPECT_EQ(*types[0], typeid(AVEVA::AzureClient::ClientSecretCredential));
    EXPECT_EQ(*types[1], typeid(AVEVA::AzureClient::ManagedIdentityCredential));
    EXPECT_EQ(*types[2], typeid(AVEVA::AzureClient::ClientSecretCredential));
    EXPECT_EQ(*types[3], typeid(AVEVA::AzureClient::WorkloadIdentityCredential));
}

TEST(CredentialChainTests, PartialEnvironmentIsIgnored) {
    const ScopedEnv tenant("AZURE_TENANT_ID", "t");
    const ScopedEnv client("AZURE_CLIENT_ID", "c");
    const ScopedEnv secret("AZURE_CLIENT_SECRET", nullptr);
    const ScopedEnv tokenFile("AZURE_FEDERATED_TOKEN_FILE", nullptr);

    EXPECT_EQ(SourceTypes(std::nullopt).size(), 2U);
}
} // namespace
