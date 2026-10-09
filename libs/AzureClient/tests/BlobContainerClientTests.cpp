// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "BlobRequestHelpers.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"
#include "ValueOrFail.hpp"

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

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::HttpMethod;
    using AVEVA::HttpRequestOptions;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobClient;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobContainerClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::CreateBlobContainerResult;
    using AVEVA::AzureClient::Models::ListBlobsResult;
    using AVEVA::AzureClient::Private::ParseHttpDateHeader;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::DefaultETag;
    using AVEVA::AzureClient::Tests::ExpectRequestContract;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;

    [[nodiscard]] BlobContainerClientOptions BuildOptions()
    {
        return MakeBlobContainerClientOptions();
    }

    void VerifyMalformedListBlobsErrorCode(const BlobStorageError& error)
    {
        EXPECT_EQ(error.Code, BlobStorageErrorCode::InvalidResponse);
        EXPECT_EQ(error.Code, std::errc::bad_message);
    }

    void VerifyMalformedListBlobsStatusAndMessage(const BlobStorageError& error, bool& callbackInvoked)
    {
        EXPECT_EQ(error.StatusCode, 200U);
        EXPECT_FALSE(error.Message.empty());
        callbackInvoked = true;
    }

    void VerifyListBlobsWithOptionsMarkers(const ListBlobsResult& result)
    {
        EXPECT_EQ(result.Prefix, "pref/");
        EXPECT_EQ(result.Delimiter, "/");
    }

    void VerifyListBlobsWithOptionsPaging(const ListBlobsResult& result)
    {
        EXPECT_EQ(result.Marker, "page-1");
        EXPECT_EQ(result.NextMarker, "page-2");
    }

    void VerifyListBlobsWithOptionsItems(const ListBlobsResult& result)
    {
        ASSERT_EQ(result.Blobs.size(), 1U);
        EXPECT_EQ(result.Blobs.at(0).Name, "pref/blob.txt");
    }

    void VerifyListBlobsWithOptionsPrefixes(const ListBlobsResult& result, CallbackExpectation& callback)
    {
        ASSERT_EQ(result.BlobPrefixes.size(), 1U);
        EXPECT_EQ(result.BlobPrefixes.at(0), "pref/folder/");
        callback.MarkInvoked();
    }

    void CreateContainerAndVerifySuccess(BlobContainerClient& client,
        const HttpRequestOptions& requestOptions,
        bool& callbackInvoked)
    {
        client.CreateAsync(
            [&](std::expected<Response<CreateBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<CreateBlobContainerResult>& response = *result;
            EXPECT_EQ(response.RawResponse().GetStatus(), 201U);
            EXPECT_EQ(response.Value().ETag, DefaultETag);
            EXPECT_NE(response.Value().LastModified.time_since_epoch().count(), 0);
            callbackInvoked = true;
        },
            requestOptions);
    }

    void CreateContainerAndVerifyMappedServiceError(BlobContainerClient& client, bool& callbackInvoked)
    {
        client.CreateAsync([&](std::expected<Response<CreateBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ContainerAlreadyExists);
            callbackInvoked = true;
        });
    }

    void ListBlobsAndVerifyMalformedXml(BlobContainerClient& client, bool& callbackInvoked)
    {
        client.ListBlobsAsync([&](std::expected<Response<ListBlobsResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            VerifyMalformedListBlobsErrorCode(result.error());
            VerifyMalformedListBlobsStatusAndMessage(result.error(), callbackInvoked);
        });
    }

    void VerifyBlobClientAndContainerOperations(BlobContainerClient& container,
        BlobClient& blob,
        FakeHttpClient& httpClient,
        int& completions)
    {
        container.CreateAsync([&](auto result)
        {
            EXPECT_TRUE(result.has_value());
            ++completions;
        });
        blob.GetPropertiesAsync([&](auto result)
        {
            EXPECT_TRUE(result.has_value());
            ++completions;
        });
        httpClient.Poll();
    }

    void VerifySharedKeyAuthorizationForBlobClientRequests(FakeHttpClient& httpClient)
    {
        for (const auto& record : httpClient.Requests())
        {
            AVEVA::HttpRequest unsignedRequest = record.Request;
            std::erase_if(unsignedRequest.GetHeaders(),
                [](const AVEVA::HttpHeader& header)
            {
                return header.GetName() == "Authorization";
            });
            AVEVA::AzureClient::Private::AuthorizeRequest(
                AVEVA::AzureClient::SharedKeyCredentialOptions{.AccountName = "storageaccount",
                    .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="},
                unsignedRequest);
            EXPECT_EQ(FakeHttpClient::FindHeaderValue(record.Request, "Authorization"),
                FakeHttpClient::FindHeaderValue(unsignedRequest, "Authorization"))
                << record.Request.GetUrl();
        }
    }

    void ListBlobsWithOptionsAndVerifyResult(BlobContainerClient& client, CallbackExpectation& callback)
    {
        AVEVA::AzureClient::ListBlobsOptions options;
        options.Prefix = "pref/";
        options.Delimiter = "/";
        options.Marker = "page-1";
        options.MaxResults = 7U;
        options.IncludeMetadata = true;

        client.ListBlobsAsync(options,
            [&](std::expected<Response<ListBlobsResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            VerifyListBlobsWithOptionsMarkers(result->Value());
            VerifyListBlobsWithOptionsPaging(result->Value());
            VerifyListBlobsWithOptionsItems(result->Value());
            VerifyListBlobsWithOptionsPrefixes(result->Value(), callback);
        });
    }

    void CreateContainerIfMissingAndVerifyExpectedConflict(BlobContainerClient& client, CallbackExpectation& callback)
    {
        AVEVA::AzureClient::CreateBlobContainerOptions createOptions;
        createOptions.Metadata["project"] = "tests";
        client.CreateIfNotExistsAsync(createOptions,
            [&](std::expected<Response<CreateBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<CreateBlobContainerResult>& response = *result;
            ASSERT_TRUE(response.Error().has_value());
            EXPECT_EQ(response.Error()->RequestId, "create-request");
            EXPECT_TRUE(response.Value().ETag.empty());
            callback.MarkInvoked();
        });
    }

} // namespace

TEST(BlobContainerClientTests, CreateAsync_BuildsPutRequestAndParsesResult)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlobContainerClient client{httpClient, BuildOptions()};

    HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::milliseconds{1234});

    bool callbackInvoked = false;
    CreateContainerAndVerifySuccess(client, requestOptions, callbackInvoked);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    ASSERT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 1U);

    const auto& request = httpClient.LastRequest();
    ExpectRequestContract(request,
        HttpMethod::Put,
        "https://storageaccount.blob.core.windows.net/images?restype=container&sv=2025-01-05&sig=fakesig");

    const std::string dateHeader = FakeHttpClient::FindHeaderValue(request, "x-ms-date");
    EXPECT_TRUE(std::regex_match(dateHeader,
        std::regex{"^[A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT$"}));
    EXPECT_TRUE(ParseHttpDateHeader(dateHeader).has_value());

    const std::string requestId = FakeHttpClient::FindHeaderValue(request, "x-ms-client-request-id");
    EXPECT_TRUE(std::regex_match(requestId,
        std::regex{"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"}));
    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), std::chrono::milliseconds{1234});
}

TEST(BlobContainerClientTests, CreateAsync_BuildsUrlWhenEndpointHasNoTrailingSlash)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ServiceEndpoint = "https://storageaccount.blob.core.windows.net";
    BlobContainerClient client{httpClient, options};

    client.CreateAsync([](std::expected<Response<CreateBlobContainerResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images?restype=container&sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, CreateAsync_MapsServiceErrorCodeWithoutParsingResult)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{409, {{"x-ms-error-code", "ContainerAlreadyExists"}, {"ETag", "\"should-not-parse\""}}, ""};

    BlobContainerClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    CreateContainerAndVerifyMappedServiceError(client, callbackInvoked);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_TRUE(callbackInvoked);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForMissingEndpoint)
{
    FakeHttpClient httpClient;

    BlobContainerClientOptions options;
    options.ContainerName = "images";
    options.ApiVersion = "2023-11-03";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForInvalidContainerName)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ContainerName = "Bad--Name";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForEndpointWithoutScheme)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ServiceEndpoint = "storageaccount.blob.core.windows.net";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForEndpointWithPathOrQuery)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ServiceEndpoint = "https://storageaccount.blob.core.windows.net/account?x=1";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForEndpointWithFragment)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.ServiceEndpoint = "https://storageaccount.blob.core.windows.net#fragment";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForConflictingCredentials)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.BearerToken = "bearer-token";

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForInvalidSharedKeyBase64)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "bad=="};

    EXPECT_THROW((BlobContainerClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobContainerClientTests, Constructor_ThrowsForPartialSharedKeys)
{
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

TEST(BlobContainerClientTests, Constructor_EnforcesContainerNameBoundariesAndDashRules)
{
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

TEST(BlobContainerClientTests, CreateAsync_UsesBearerAuthorizationExactly)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.BearerToken = "container-token";
    BlobContainerClient client{httpClient, options};

    client.CreateAsync([](std::expected<Response<CreateBlobContainerResult>, BlobStorageError>) {});

    // BearerToken/TokenCredential completions that finish on the same stack are deferred
    // via post() (never completing before the initiating call returns); poll the fake
    // client's executor to observe them.
    httpClient.Poll();

    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization"), "Bearer container-token");
}

TEST(BlobContainerClientTests, ListBlobsAsync_PreservesSpecialSasValuesAlongsideOperationQuery)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.SasToken = "?sv=2025-01-05&sig=a%2Bb%26c%3D1&se=2025-01-05T00%3A00%3A00Z";
    BlobContainerClient client{httpClient, options};

    AVEVA::AzureClient::ListBlobsOptions listOptions;
    listOptions.Marker = "next+page&marker=%";
    listOptions.IncludeMetadata = true;
    client.ListBlobsAsync(listOptions,
        [](std::expected<Response<AVEVA::AzureClient::Models::ListBlobsResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/"
        "images?restype=container&comp=list&marker=next%2Bpage%26marker%3D%25&include=metadata&sv=2025-01-05&sig=a%2Bb%"
        "26c%3D1&se=2025-01-05T00%3A00%3A00Z");
    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization").empty());
}

TEST(BlobContainerClientTests, ListBlobsAsync_ReportsMalformedXmlAsInvalidResponse)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {}, "<EnumerationResults><Blobs>"};
    BlobContainerClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    ListBlobsAndVerifyMalformedXml(client, callbackInvoked);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_TRUE(callbackInvoked);
}

TEST(BlobContainerClientTests, GetBlockBlobClient_BuildsBlobScopedClient)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    auto blockBlobClient = client.GetBlockBlobClient("photo.png");
    blockBlobClient->DeleteAsync(
        [](std::expected<Response<AVEVA::AzureClient::Models::DeleteBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/photo.png?sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, GetPageBlobClient_BuildsBlobScopedClient)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    auto pageBlobClient = client.GetPageBlobClient("disk.vhd");
    pageBlobClient->DeleteAsync(
        [](std::expected<Response<AVEVA::AzureClient::Models::DeleteBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/disk.vhd?sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, GetBlockBlobClient_UrlEncodesBlobNameAndPreservesSlashes)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    auto blockBlobClient = client.GetBlockBlobClient("folder name/file?.txt");
    blockBlobClient->DeleteAsync(
        [](std::expected<Response<AVEVA::AzureClient::Models::DeleteBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/folder%20name/file%3F.txt?sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, GetBlobClient_TargetsTheBlobAndSharesTheConnection)
{
    FakeHttpClient httpClient;
    BlobContainerClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    BlobContainerClient container{httpClient, options};

    const auto blobPtr = container.GetBlobClient("folder name/photo.png");
    AVEVA::AzureClient::BlobClient& blob = *blobPtr;
    EXPECT_EQ(blob.get_executor(), httpClient.get_executor());

    int completions = 0;
    VerifyBlobClientAndContainerOperations(container, blob, httpClient, completions);
    EXPECT_EQ(completions, 2);

    ASSERT_EQ(httpClient.Requests().size(), 2U);
    VerifySharedKeyAuthorizationForBlobClientRequests(httpClient);
    EXPECT_EQ(httpClient.Requests().at(1).Request.GetMethod(), HttpMethod::Head);
    EXPECT_EQ(httpClient.Requests().at(1).Request.GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/folder%20name/photo.png");
    EXPECT_THROW(static_cast<void>(container.GetBlobClient("")), std::invalid_argument);
}

TEST(BlobContainerClientTests, GetBlockBlobClient_ThrowsForEmptyBlobName)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    EXPECT_THROW(
        [&client]
    {
        auto unusedClient = client.GetBlockBlobClient("");
        static_cast<void>(unusedClient);
    }(),
        std::invalid_argument);
}

TEST(BlobContainerClientTests, ListBlobsAsync_OptionsOverloadIncludesAllSupportedQueryParametersAndParsesPagination)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        MakeCanonicalSuccessHeaders(),
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

    CallbackExpectation callback;
    ListBlobsWithOptionsAndVerifyResult(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/"
        "images?restype=container&comp=list&prefix=pref%2F&delimiter=%2F&marker=page-1&maxresults=7&include=metadata&"
        "sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, CreateIfNotExists_TreatsAlreadyExistsAsNonFatal)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "ContainerAlreadyExists", "", "create-request"));
    BlobContainerClient client{httpClient, BuildOptions()};

    CallbackExpectation createCallback;
    CreateContainerIfMissingAndVerifyExpectedConflict(client, createCallback);

    httpClient.Poll();
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-meta-project"), "tests");
}

TEST(BlobContainerClientTests, CreateAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlobContainerClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<CreateBlobContainerResult>, BlobStorageError>> future =
        client.CreateAsync(AVEVA::AzureClient::CreateBlobContainerOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{30}), std::future_status::ready);
    std::expected<Response<CreateBlobContainerResult>, BlobStorageError> const result = future.get();
    EXPECT_TRUE(result.has_value());
}

// Task 14: omitting the completion token entirely yields a deferred, directly co_await-able
// operation (see BlockBlobClientTests.cpp for the detailed rationale).
TEST(BlobContainerClientTests, ListBlobsAsyncSupportsCoAwaitWithNoExplicitCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{200, MakeCanonicalSuccessHeaders(), "<EnumerationResults></EnumerationResults>"};
    BlobContainerClient client{httpClient, BuildOptions()};

    std::optional<std::expected<Response<AVEVA::AzureClient::Models::ListBlobsResult>, BlobStorageError>> result;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitInto(&result,
            [&]
    {
        return client.ListBlobsAsync();
    }),
        boost::asio::detached);

    httpClient.Poll();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(ValueOrFail(result).has_value());
}

// Compile-only proof that defaulting CompletionToken did not introduce ambiguity between the
// "options" and "no options" overloads. Never run; only needs to compile.
namespace
{
    [[maybe_unused]] void BlobContainerClientNoTokenOverloadsCompile(BlobContainerClient& client)
    {
        using AVEVA::AzureClient::CreateBlobContainerOptions;
        using AVEVA::AzureClient::ListBlobsOptions;

        static_cast<void>(client.CreateAsync());
        static_cast<void>(client.CreateAsync(CreateBlobContainerOptions{}));

        static_cast<void>(client.ListBlobsAsync());
        static_cast<void>(client.ListBlobsAsync(ListBlobsOptions{}));

        static_cast<void>(client.CreateIfNotExistsAsync());
        static_cast<void>(client.CreateIfNotExistsAsync(CreateBlobContainerOptions{}));
    }
} // namespace
