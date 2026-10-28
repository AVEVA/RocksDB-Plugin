// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <cstddef>
#include <future>
#include <gtest/gtest.h>

#include <boost/asio/detached.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <expected>
#include <optional>
#include <ranges>
#include <regex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace {
using AVEVA::HttpMethod;
using AVEVA::HttpRequestOptions;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobContainerClientOptions;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::Models::CreateBlobContainerResult;
using AVEVA::AzureClient::Models::ListBlobsResult;
using AVEVA::AzureClient::Private::ParseHttpDateHeader;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::BlobContainerClient;
using AVEVA::AzureClient::Tests::DefaultETag;
using AVEVA::AzureClient::Tests::ExpectRequestContract;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::RunAwaitable;

[[nodiscard]] BlobContainerClientOptions BuildOptions() { return MakeBlobContainerClientOptions(); }

void VerifyMalformedListBlobsErrorCode(const BlobStorageError& error) {
    EXPECT_EQ(error.Code, BlobStorageErrorCode::InvalidResponse);
    EXPECT_EQ(error.Code, std::errc::bad_message);
}

void VerifyMalformedListBlobsStatusAndMessage(const BlobStorageError& error) {
    EXPECT_EQ(error.StatusCode, 200U);
    EXPECT_FALSE(error.Message.empty());
}

void VerifyListBlobsWithOptionsMarkers(const ListBlobsResult& result) { EXPECT_EQ(result.Prefix, "pref/"); }

void VerifyListBlobsWithOptionsPaging(const ListBlobsResult& result) {
    EXPECT_EQ(result.Marker, "page-1");
    EXPECT_EQ(result.NextMarker, "page-2");
}

void VerifyListBlobsWithOptionsItems(const ListBlobsResult& result) {
    ASSERT_EQ(result.Blobs.size(), 1U);
    EXPECT_EQ(result.Blobs.at(0).Name, "pref/blob.txt");
}

void ListBlobsAndVerifyMalformedXml(FakeHttpClient& httpClient, BlobContainerClient& client) {
    const auto result = RunAwaitable(httpClient, client.ListBlobsAsync());
    ASSERT_FALSE(result.has_value());
    VerifyMalformedListBlobsErrorCode(result.error());
    VerifyMalformedListBlobsStatusAndMessage(result.error());
}

void ListBlobsWithOptionsAndVerifyResult(FakeHttpClient& httpClient, BlobContainerClient& client) {
    AVEVA::AzureClient::ListBlobsOptions options;
    options.Prefix = "pref/";
    options.Marker = "page-1";
    options.MaxResults = 7U;
    options.IncludeMetadata = true;

    const auto result = RunAwaitable(httpClient, client.ListBlobsAsync(options));
    ASSERT_TRUE(result.has_value());
    VerifyListBlobsWithOptionsMarkers(result->Value());
    VerifyListBlobsWithOptionsPaging(result->Value());
    VerifyListBlobsWithOptionsItems(result->Value());
}

void CreateContainerIfMissingAndVerifyExpectedConflict(FakeHttpClient& httpClient, BlobContainerClient& client) {
    AVEVA::AzureClient::CreateBlobContainerOptions createOptions;
    createOptions.Metadata["project"] = "tests";
    const auto result = RunAwaitable(httpClient, client.CreateIfNotExistsAsync(createOptions));
    ASSERT_TRUE(result.has_value());
    const Response<CreateBlobContainerResult>& response = *result;
    ASSERT_TRUE(response.Error().has_value());
    EXPECT_EQ(response.Error()->RequestId, "create-request");
    EXPECT_TRUE(response.Value().ETag.empty());
}
} // namespace

TEST(BlobContainerClientTests, Constructor_ThrowsForMissingEndpoint) {
    FakeHttpClient httpClient;

    BlobContainerClientOptions options;
    options.ContainerName = "images";
    options.ApiVersion = "2023-11-03";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForInvalidContainerName) {
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ContainerName = "Bad--Name";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForEndpointWithoutScheme) {
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ServiceEndpoint = "storageaccount.blob.core.windows.net";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForEndpointWithPathOrQuery) {
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ServiceEndpoint = "https://storageaccount.blob.core.windows.net/account?x=1";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForEndpointWithFragment) {
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ServiceEndpoint = "https://storageaccount.blob.core.windows.net#fragment";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForInvalidSharedKeyBase64) {
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "bad=="};

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForPartialSharedKeys) {
    FakeHttpClient httpClient;

    BlobContainerClientOptions accountOnly = BuildOptions();
    accountOnly.SasToken.clear();
    accountOnly.SharedKey = {.AccountName = "storageaccount"};
    EXPECT_THROW((BlobContainerClient{httpClient, accountOnly}), std::invalid_argument);

    BlobContainerClientOptions keyOnly = BuildOptions();
    keyOnly.SasToken.clear();
    keyOnly.SharedKey = {.AccountName = "", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    EXPECT_THROW((BlobContainerClient{httpClient, keyOnly}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_EnforcesContainerNameBoundariesAndDashRules) {
    FakeHttpClient httpClient;

    BlobContainerClientOptions shortName = BuildOptions();
    shortName.ContainerName = "abc";
    EXPECT_NO_THROW((BlobContainerClient{httpClient, shortName}));

    BlobContainerClientOptions maxName = BuildOptions();
    maxName.ContainerName = std::string(63U, 'a');
    EXPECT_NO_THROW((BlobContainerClient{httpClient, maxName}));

    BlobContainerClientOptions leadingDash = BuildOptions();
    leadingDash.ContainerName = "-abc";
    EXPECT_THROW((BlobContainerClient{httpClient, leadingDash}), std::invalid_argument);

    BlobContainerClientOptions trailingDash = BuildOptions();
    trailingDash.ContainerName = "abc-";
    EXPECT_THROW((BlobContainerClient{httpClient, trailingDash}), std::invalid_argument);

    BlobContainerClientOptions consecutiveDash = BuildOptions();
    consecutiveDash.ContainerName = "ab--cd";
    EXPECT_THROW((BlobContainerClient{httpClient, consecutiveDash}), std::invalid_argument);
}

TEST(BlobContainerClientTests, ListBlobsAsync_PreservesSpecialSasValuesAlongsideOperationQuery) {
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.SasToken = "?sv=2025-01-05&sig=a%2Bb%26c%3D1&se=2025-01-05T00%3A00%3A00Z";
    BlobContainerClient client{httpClient, options};

    AVEVA::AzureClient::ListBlobsOptions listOptions;
    listOptions.Marker = "next+page&marker=%";
    listOptions.IncludeMetadata = true;
    (void)RunAwaitable(httpClient, client.ListBlobsAsync(listOptions));

    EXPECT_EQ(
        httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/"
        "images?restype=container&comp=list&marker=next%2Bpage%26marker%3D%25&include=metadata&sv=2025-01-05&sig=a%2Bb%"
        "26c%3D1&se=2025-01-05T00%3A00%3A00Z");
    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization").empty());
}

TEST(BlobContainerClientTests, ListBlobsAsync_ReportsMalformedXmlAsInvalidResponse) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {}, "<EnumerationResults><Blobs>"};
    BlobContainerClient client{httpClient, BuildOptions()};

    ListBlobsAndVerifyMalformedXml(httpClient, client);
}

TEST(BlobContainerClientTests, GetPageBlobClient_BuildsBlobScopedClient) {
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    auto pageBlobClient = client.GetPageBlobClient("disk.vhd");
    (void)RunAwaitable(httpClient, pageBlobClient.DeleteAsync());

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
              "https://storageaccount.blob.core.windows.net/images/disk.vhd?sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, ListBlobsAsync_OptionsOverloadIncludesSupportedQueryParametersAndParsesPagination) {
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(),
                                                R"(<?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
              <Prefix>pref/</Prefix>
              <Delimiter>/</Delimiter>
              <Marker>page-1</Marker>
              <NextMarker>page-2</NextMarker>
              <Blobs>
                <Blob><Name>pref/blob.txt</Name><Properties><BlobType>BlockBlob</BlobType><Content-Length>3</Content-Length></Properties></Blob>
                <BlobPrefix><Name>pref/folder/</Name></BlobPrefix>
              </Blobs>
            </EnumerationResults>)"};
    BlobContainerClient client{httpClient, BuildOptions()};

    ListBlobsWithOptionsAndVerifyResult(httpClient, client);

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
              "https://storageaccount.blob.core.windows.net/"
              "images?restype=container&comp=list&prefix=pref%2F&marker=page-1&maxresults=7&include=metadata&"
              "sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, CreateIfNotExists_TreatsAlreadyExistsAsNonFatal) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "ContainerAlreadyExists", "", "create-request"));
    BlobContainerClient client{httpClient, BuildOptions()};

    CreateContainerIfMissingAndVerifyExpectedConflict(httpClient, client);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-meta-project"), "tests");
}

// Compile-only proof that the "options" and "no options" overloads are unambiguous. Never run; only needs to compile.
namespace {
[[maybe_unused]] void BlobContainerClientNoTokenOverloadsCompile(BlobContainerClient& client) {
    using AVEVA::AzureClient::CreateBlobContainerOptions;
    using AVEVA::AzureClient::ListBlobsOptions;

    static_cast<void>(client.ListBlobsAsync());
    static_cast<void>(client.ListBlobsAsync(ListBlobsOptions{}));

    static_cast<void>(client.CreateIfNotExistsAsync());
    static_cast<void>(client.CreateIfNotExistsAsync(CreateBlobContainerOptions{}));
}
} // namespace
