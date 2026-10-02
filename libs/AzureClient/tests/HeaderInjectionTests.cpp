#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/ITokenCredential.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace
{
    using AVEVA::AzureClient::BlobClient;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::ManagedIdentityCredential;
    using AVEVA::AzureClient::ManagedIdentityCredentialOptions;
    using AVEVA::AzureClient::SetBlobMetadataOptions;
    using AVEVA::AzureClient::UploadBlockBlobOptions;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;

    const std::string Injected = "a\r\nx-ms-evil: 1";

    template <class TResult> void ExpectRejected(FakeHttpClient& httpClient, const std::optional<TResult>& result)
    {
        httpClient.Poll();
        ASSERT_TRUE(result.has_value());
        ASSERT_FALSE(result->has_value());
        EXPECT_EQ(result->error().Code, std::errc::invalid_argument);
        EXPECT_EQ(httpClient.RequestCount(), 0U);
    }
} // namespace

TEST(HeaderInjectionTests, MetadataValueWithCrLfIsRejected)
{
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    SetBlobMetadataOptions options;
    options.Metadata.emplace("key", Injected);

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>,
        AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.SetMetadataAsync(std::move(options),
        [&](Result value)
    {
        result = std::move(value);
    });
    ExpectRejected(httpClient, result);
}

TEST(HeaderInjectionTests, ContentTypeWithCrLfIsRejected)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    UploadBlockBlobOptions options;
    options.HttpHeaders.ContentType = Injected;

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::UploadBlockBlobResult>,
        AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.UploadAsync(std::string{"data"},
        std::move(options),
        [&](Result value)
    {
        result = std::move(value);
    });
    ExpectRejected(httpClient, result);
}

TEST(HeaderInjectionTests, LeaseIdWithCrLfIsRejected)
{
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    SetBlobMetadataOptions options;
    options.Conditions.LeaseId = Injected;

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>,
        AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.SetMetadataAsync(std::move(options),
        [&](Result value)
    {
        result = std::move(value);
    });
    ExpectRejected(httpClient, result);
}

TEST(HeaderInjectionTests, NulInHeaderValueIsRejected)
{
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    SetBlobMetadataOptions options;
    options.Metadata.emplace("key", std::string{"a\0b", 3});

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>,
        AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.SetMetadataAsync(std::move(options),
        [&](Result value)
    {
        result = std::move(value);
    });
    ExpectRejected(httpClient, result);
}

TEST(HeaderInjectionTests, ManagedIdentityHeaderWithCrLfIsRejected)
{
    FakeHttpClient httpClient;
    ManagedIdentityCredentialOptions options;
    options.IdentityEndpoint = "https://identity.example/msi/token";
    options.IdentityHeader = Injected;
    ManagedIdentityCredential credential{httpClient, std::move(options)};

    std::optional<std::error_code> error;
    credential.GetTokenAsync({"https://storage.azure.com/.default"},
        [&](std::error_code code, AVEVA::AzureClient::AccessToken)
    {
        error = code;
    });
    httpClient.Poll();

    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(*error, std::errc::invalid_argument);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}
