// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeHttpPump.hpp"
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
        m_pump = AVEVA::RocksDB::Plugin::Azure::Impl::Tests::StartFakeHttpPump(m_httpClient);
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

TEST_F(BlobHelpersTests, ConflictIsRetriedUntilSuccess) {
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "ContainerBeingDeleted", "busy", "id"));
    m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "ContainerBeingDeleted", "busy", "id"));
    m_httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({}), ""});
    BlobContainerClient client(m_httpClient, MakeBlobContainerClientOptions("cont"));

    EXPECT_NO_THROW(BlobHelpers::CreateContainerIfNotExists(client, 5, std::chrono::milliseconds(1)));
    EXPECT_EQ(m_httpClient.RequestCount(), 3U);
}

TEST_F(BlobHelpersTests, RetriesAreBounded) {
    for (int i = 0; i < 3; ++i) {
        m_httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "ContainerBeingDeleted", "busy", "id"));
    }
    BlobContainerClient client(m_httpClient, MakeBlobContainerClientOptions("cont"));

    EXPECT_THROW(BlobHelpers::CreateContainerIfNotExists(client, 3, std::chrono::milliseconds(1)),
                 RequestFailedException);
    EXPECT_EQ(m_httpClient.RequestCount(), 3U);
}
TEST(FileSizeMetadataTests, ParsesValidAndMissingValues) {
    AVEVA::AzureClient::Models::BlobProperties properties;
    EXPECT_EQ(0, BlobHelpers::FileSizeFromProperties(properties));
    properties.Metadata.emplace("filesize", "4096");
    EXPECT_EQ(4096, BlobHelpers::FileSizeFromProperties(properties));
}

TEST(FileSizeMetadataTests, MalformedValuesThrowDescriptiveError) {
    for (const char* value : {"abc", "12x", "", "-5", "99999999999999999999"}) {
        AVEVA::AzureClient::Models::BlobProperties properties;
        properties.Metadata.emplace("filesize", value);
        try {
            BlobHelpers::FileSizeFromProperties(properties, "000001.sst");
            ADD_FAILURE() << "expected throw for '" << value << "'";
        } catch (const std::runtime_error& ex) {
            EXPECT_NE(std::string(ex.what()).find(value), std::string::npos);
            EXPECT_NE(std::string(ex.what()).find("000001.sst"), std::string::npos);
        }
    }
}

class ScopedEnv {
  public:
    ScopedEnv(const char* name, const char* value) : m_name(name) {
        // Restore the previous value on exit so later tests in the same process still see it.
        if (const char* previous = std::getenv(name)) {
            m_previous = previous;
        }
        Set(value);
    }
    ~ScopedEnv() { Set(m_previous ? m_previous->c_str() : nullptr); }
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
    std::optional<std::string> m_previous;
};

std::vector<const std::type_info*> SourceTypes(const std::optional<std::string>& managedIdentityId) {
    boost::asio::io_context context;
    const auto runtime = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime>(context);
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
TEST(BlobHelpersCredentialLifetimeTests, ServicePrincipalCredentialKeepsRuntimeAlive) {
    boost::asio::io_context context;
    auto runtime = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime>(context);
    const std::weak_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime> weak = runtime;
    auto credential = BlobHelpers::CreateClientSecretCredential(runtime, "tenant", "client", "secret");

    runtime.reset();
    EXPECT_FALSE(weak.expired());

    credential.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(BlobHelpersCredentialLifetimeTests, PipelinesCredentialKeepsRuntimeAlive) {
    boost::asio::io_context context;
    auto runtime = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime>(context);
    const std::weak_ptr<AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime> weak = runtime;
    auto credential = BlobHelpers::CreatePipelinesCredential(runtime, "tenant", "client", "connection", "token");

    runtime.reset();
    EXPECT_FALSE(weak.expired());

    credential.reset();
    EXPECT_TRUE(weak.expired());
}

} // namespace
