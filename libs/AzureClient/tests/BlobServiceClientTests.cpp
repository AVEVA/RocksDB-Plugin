// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <stdexcept>
#include <string>

namespace {
using AVEVA::AzureClient::BlobServiceClientOptions;
using AVEVA::AzureClient::Tests::BlobServiceClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;

[[nodiscard]] BlobServiceClientOptions BuildOptions() { return MakeBlobServiceClientOptions(); }

void ExpectSignedAuthorization(const FakeHttpClient::RequestRecord& record) {
    AVEVA::HttpRequest unsignedRequest = record.Request;
    std::erase_if(unsignedRequest.GetHeaders(),
                  [](const AVEVA::HttpHeader& header) { return header.GetName() == "Authorization"; });
    AVEVA::AzureClient::Private::AuthorizeRequest(
        AVEVA::AzureClient::SharedKeyCredentialOptions{.AccountName = "storageaccount",
                                                       .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="},
        unsignedRequest);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(record.Request, "Authorization"),
              FakeHttpClient::FindHeaderValue(unsignedRequest, "Authorization"))
        << record.Request.GetUrl();
}
} // namespace

TEST(BlobServiceClientTests, Constructor_ThrowsForInvalidEndpointSchemeAndPartialSharedKeys) {
    FakeHttpClient httpClient;

    BlobServiceClientOptions badScheme = BuildOptions();
    badScheme.ServiceEndpoint = "ftp://storageaccount.blob.core.windows.net";
    EXPECT_THROW((BlobServiceClient{httpClient, badScheme}), std::invalid_argument);

    BlobServiceClientOptions accountOnly = BuildOptions();
    accountOnly.SasToken.clear();
    accountOnly.SharedKey = {.AccountName = "storageaccount"};
    EXPECT_THROW((BlobServiceClient{httpClient, accountOnly}), std::invalid_argument);

    BlobServiceClientOptions keyOnly = BuildOptions();
    keyOnly.SasToken.clear();
    keyOnly.SharedKey = {.AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    EXPECT_THROW((BlobServiceClient{httpClient, keyOnly}), std::invalid_argument);
}

// Blob clients derived from a Shared Key service client sign with the connection's shared signer; the
// signature must match a fresh one-off signature of the same request.
TEST(BlobServiceClientTests, DerivedClientsSignWithTheSharedConnectionSigner) {
    FakeHttpClient httpClient;
    BlobServiceClientOptions serviceOptions;
    serviceOptions.ServiceEndpoint = "https://storageaccount.blob.core.windows.net";
    serviceOptions.SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    BlobServiceClient service{httpClient, std::move(serviceOptions)};

    AVEVA::AzureClient::Tests::BlobContainerClient container = service.GetBlobContainerClient("images");
    auto blob = container.GetPageBlobClient("folder/photo.png");

    int completions = 0;
    container.ListBlobsAsync([&](auto) { ++completions; });
    blob.DeleteAsync([&](auto result) {
        EXPECT_TRUE(result.has_value());
        ++completions;
    });
    httpClient.Poll();
    EXPECT_EQ(completions, 2);

    ASSERT_EQ(httpClient.Requests().size(), 2U);
    for (const auto& record : httpClient.Requests()) {
        ExpectSignedAuthorization(record);
    }
    EXPECT_EQ(httpClient.Requests().at(1).Request.GetUrl(),
              "https://storageaccount.blob.core.windows.net/images/folder/photo.png");
}

TEST(BlobServiceClientTests, GetBlobContainerClient_ValidatesTheContainerName) {
    FakeHttpClient httpClient;
    BlobServiceClient service{httpClient, BuildOptions()};
    EXPECT_THROW(static_cast<void>(service.GetBlobContainerClient("Invalid_Name")), std::invalid_argument);
    const auto container = service.GetBlobContainerClient("valid-name");
    EXPECT_THROW(static_cast<void>(container.GetPageBlobClient("")), std::invalid_argument);
}

TEST(BlobServiceClientTests, OptionsDefaultApiVersionMatchesSharedDefault) {
    EXPECT_EQ(AVEVA::AzureClient::BlobServiceClientOptions{}.ApiVersion,
              std::string(AVEVA::AzureClient::DefaultApiVersion));
}
