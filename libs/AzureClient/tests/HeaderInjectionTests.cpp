// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/TokenCredential.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {
using AVEVA::AzureClient::CreatePageBlobOptions;
using AVEVA::AzureClient::ManagedIdentityCredentialOptions;
using AVEVA::AzureClient::SetBlobMetadataOptions;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::ManagedIdentityCredential;
using AVEVA::AzureClient::Tests::PageBlobClient;

const std::string Injected = "a\r\nx-ms-evil: 1";

template <class TResult> void ExpectRejected(FakeHttpClient& httpClient, const std::optional<TResult>& result) {
    httpClient.Poll();
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::errc::invalid_argument);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}
} // namespace

TEST(HeaderInjectionTests, MetadataValueWithCrLfIsRejected) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    SetBlobMetadataOptions options;
    options.Metadata.emplace("key", Injected);

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>,
                                 AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.SetMetadataAsync(std::move(options), [&](Result value) { result = std::move(value); });
    ExpectRejected(httpClient, result);
}

TEST(HeaderInjectionTests, LeaseIdWithCrLfIsRejected) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    SetBlobMetadataOptions options;
    options.Conditions.LeaseId = Injected;

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>,
                                 AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.SetMetadataAsync(std::move(options), [&](Result value) { result = std::move(value); });
    ExpectRejected(httpClient, result);
}

TEST(HeaderInjectionTests, NulInHeaderValueIsRejected) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    SetBlobMetadataOptions options;
    options.Metadata.emplace("key", std::string{"a\0b", 3});

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>,
                                 AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.SetMetadataAsync(std::move(options), [&](Result value) { result = std::move(value); });
    ExpectRejected(httpClient, result);
}

TEST(HeaderInjectionTests, ManagedIdentityHeaderWithCrLfIsRejected) {
    FakeHttpClient httpClient;
    ManagedIdentityCredentialOptions options;
    options.IdentityEndpoint = "https://identity.example/msi/token";
    options.IdentityHeader = Injected;
    ManagedIdentityCredential credential{httpClient, std::move(options)};

    std::optional<std::error_code> error;
    credential.GetTokenAsync({"https://storage.azure.com/.default"},
                             [&](std::error_code code, AVEVA::AzureClient::AccessToken) { error = code; });
    httpClient.Poll();

    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(*error, std::errc::invalid_argument);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(HeaderInjectionTests, OtherControlCharactersAreRejectedButTabIsAllowed) {
    using AVEVA::AzureClient::Private::HasHeaderControlCharacters;
    for (const char c : {'\0', '\x01', '\x08', '\x0b', '\x0c', '\x1f', '\x7f', '\r', '\n'}) {
        EXPECT_TRUE(HasHeaderControlCharacters(std::string{"a"} + c + "b")) << static_cast<int>(c);
    }
    EXPECT_FALSE(HasHeaderControlCharacters("a\tb c"));
    EXPECT_FALSE(HasHeaderControlCharacters(""));
}

TEST(HeaderInjectionTests, NonAsciiMetadataValueIsRejected) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    SetBlobMetadataOptions options;
    options.Metadata.emplace("key", "caf\xc3\xa9");

    using Result = std::expected<AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>,
                                 AVEVA::AzureClient::BlobStorageError>;
    std::optional<Result> result;
    client.SetMetadataAsync(std::move(options), [&](Result value) { result = std::move(value); });
    httpClient.Poll();
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidMetadata);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}