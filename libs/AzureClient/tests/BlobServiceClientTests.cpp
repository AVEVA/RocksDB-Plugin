#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlockBlobClient.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobServiceModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"

#include "BlobRequestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <chrono>
#include <gtest/gtest.h>

#include <boost/asio/detached.hpp>
#include <boost/asio/use_future.hpp>

#include <expected>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobServiceClient;
    using AVEVA::AzureClient::BlobServiceClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::GetUserDelegationKeyOptions;
    using AVEVA::AzureClient::ListBlobContainersOptions;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::AccountInfo;
    using AVEVA::AzureClient::Models::BlobServiceProperties;
    using AVEVA::AzureClient::Models::ListBlobContainersResult;
    using AVEVA::AzureClient::Models::UserDelegationKey;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::ExpectRequestContract;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;

    [[nodiscard]] BlobServiceClientOptions BuildOptions()
    {
        return MakeBlobServiceClientOptions();
    }

    using ListContainersExpected = std::expected<Response<ListBlobContainersResult>, BlobStorageError>;

    void AssertInvalidResponseResult(const ListContainersExpected& result, bool& callbackInvoked)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::InvalidResponse);
        EXPECT_EQ(result.error().Code, std::errc::bad_message);
        EXPECT_EQ(result.error().StatusCode, 200U);
        EXPECT_FALSE(result.error().Message.empty());
        callbackInvoked = true;
    }

    void AssertMalformedServiceErrorResult(const ListContainersExpected& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(result.error().StatusCode, 500U);
        EXPECT_TRUE(result.error().ErrorCode.empty());
        EXPECT_TRUE(result.error().Message.empty());
        EXPECT_TRUE(result.error().RequestId.empty());
    }

    void AssertUnknownServiceErrorDetailsResult(const ListContainersExpected& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(result.error().ErrorCode, "OddServiceProblem");
        EXPECT_EQ(result.error().Message, "Unexpected detail");
        EXPECT_EQ(result.error().RequestId, "service-409");
    }

    void ExpectInvalidResponseError(BlobServiceClient& client, FakeHttpClient& httpClient)
    {
        bool callbackInvoked = false;
        client.ListBlobContainersAsync([&](ListContainersExpected result)
        {
            AssertInvalidResponseResult(result, callbackInvoked);
        });

        httpClient.Poll();
        EXPECT_TRUE(callbackInvoked);
    }

    void ExpectTransportError(BlobServiceClient& client, FakeHttpClient& httpClient)
    {
        CallbackExpectation callback;
        client.ListBlobContainersAsync([&](ListContainersExpected result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::timed_out));
            callback.MarkInvoked();
        });

        httpClient.Poll();
    }

    void ExpectRedirectServiceError(BlobServiceClient& client, FakeHttpClient& httpClient)
    {
        CallbackExpectation callback;
        client.ListBlobContainersAsync([&](ListContainersExpected result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
            EXPECT_EQ(result.error().StatusCode, 307U);
            EXPECT_EQ(result.error().RequestId, "service-redirect");
            callback.MarkInvoked();
        });

        httpClient.Poll();
    }

    void ExpectMalformedServiceError(BlobServiceClient& client, FakeHttpClient& httpClient)
    {
        CallbackExpectation callback;
        client.ListBlobContainersAsync([&](ListContainersExpected result)
        {
            AssertMalformedServiceErrorResult(result);
            callback.MarkInvoked();
        });

        httpClient.Poll();
    }

    void ExpectUnknownServiceErrorDetailsPreserved(BlobServiceClient& client, FakeHttpClient& httpClient)
    {
        CallbackExpectation callback;
        client.ListBlobContainersAsync([&](ListContainersExpected result)
        {
            AssertUnknownServiceErrorDetailsResult(result);
            callback.MarkInvoked();
        });

        httpClient.Poll();
    }

    [[nodiscard]] ListBlobContainersResult ListBlobContainers(BlobServiceClient& client,
        FakeHttpClient& httpClient,
        const ListBlobContainersOptions& options)
    {
        std::optional<ListBlobContainersResult> listed;
        CallbackExpectation callback;
        client.ListBlobContainersAsync(options,
            [&](ListContainersExpected result)
        {
            ASSERT_TRUE(result.has_value());
            listed = result->Value();
            callback.MarkInvoked();
        });

        httpClient.Poll();
        EXPECT_TRUE(listed.has_value());
        return *listed;
    }

    void ExpectSignedAuthorization(const FakeHttpClient::RequestRecord& record)
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

    void ExpectRequestsSignedWithSharedConnectionSigner(const FakeHttpClient& httpClient)
    {
        for (const auto& record : httpClient.Requests())
        {
            ExpectSignedAuthorization(record);
        }
    }

    [[nodiscard]] ListBlobContainersResult ListAllBlobContainers(BlobServiceClient& client,
        FakeHttpClient& httpClient,
        const ListBlobContainersOptions& options)
    {
        std::optional<ListBlobContainersResult> listed;
        client.ListBlobContainersAllAsync(options,
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            listed = result->Value();
        });
        httpClient.Poll();
        EXPECT_TRUE(listed.has_value());
        return *listed;
    }

    [[nodiscard]] AccountInfo GetAccountInfo(BlobServiceClient& client, FakeHttpClient& httpClient)
    {
        std::optional<AccountInfo> info;
        client.GetAccountInfoAsync([&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            info = result->Value();
        });
        httpClient.Poll();
        EXPECT_TRUE(info.has_value());
        return *info;
    }

    [[nodiscard]] BlobServiceProperties GetProperties(BlobServiceClient& client, FakeHttpClient& httpClient)
    {
        std::optional<BlobServiceProperties> properties;
        client.GetPropertiesAsync([&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            properties = result->Value();
        });
        httpClient.Poll();
        EXPECT_TRUE(properties.has_value());
        return *properties;
    }

    [[nodiscard]] UserDelegationKey GetUserDelegationKey(BlobServiceClient& client,
        FakeHttpClient& httpClient,
        const GetUserDelegationKeyOptions& options)
    {
        std::optional<UserDelegationKey> key;
        client.GetUserDelegationKeyAsync(options,
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            key = result->Value();
        });
        httpClient.Poll();
        EXPECT_TRUE(key.has_value());
        return *key;
    }
} // namespace

TEST(BlobServiceClientTests, ListBlobContainersAsync_PreservesSpecialSasValuesWithOperationQuery)
{
    FakeHttpClient httpClient;
    BlobServiceClientOptions options = BuildOptions();
    options.SasToken = "?sv=2025-01-05&sig=a%2Bb%26c%3D1&se=2025-01-05T00%3A00%3A00Z";
    BlobServiceClient client{httpClient, std::move(options)};

    AVEVA::AzureClient::ListBlobContainersOptions listOptions;
    listOptions.Prefix = "team/containers";
    listOptions.Marker = "next+page&marker=%";
    listOptions.MaxResults = 10U;
    listOptions.IncludeMetadata = true;
    client.ListBlobContainersAsync(listOptions,
        [](std::expected<Response<ListBlobContainersResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://"
        "storageaccount.blob.core.windows.net?comp=list&prefix=team%2Fcontainers&marker=next%2Bpage%26marker%3D%25&"
        "maxresults=10&include=metadata&sv=2025-01-05&sig=a%2Bb%26c%3D1&se=2025-01-05T00%3A00%3A00Z");
    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization").empty());
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_UsesBearerAuthorizationExactly)
{
    FakeHttpClient httpClient;
    BlobServiceClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.BearerToken = "service-token";
    BlobServiceClient client{httpClient, std::move(options)};

    client.ListBlobContainersAsync([](std::expected<Response<ListBlobContainersResult>, BlobStorageError>) {});

    // BearerToken/TokenCredential completions that finish on the same stack are deferred
    // via post() (never completing before the initiating call returns); poll the fake
    // client's executor to observe them.
    httpClient.Poll();

    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization"), "Bearer service-token");
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_UsesSharedKeyAuthorizationWhenConfigured)
{
    FakeHttpClient httpClient;
    BlobServiceClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    BlobServiceClient client{httpClient, std::move(options)};

    client.ListBlobContainersAsync([](std::expected<Response<ListBlobContainersResult>, BlobStorageError>) {});

    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization")
            .starts_with("SharedKey storageaccount:"));
}

TEST(BlobServiceClientTests, Constructor_ThrowsForConflictingCredentials)
{
    FakeHttpClient httpClient;
    BlobServiceClientOptions options = BuildOptions();
    options.BearerToken = "token";

    EXPECT_THROW((BlobServiceClient{httpClient, options}), std::invalid_argument);
}

TEST(BlobServiceClientTests, Constructor_ThrowsForInvalidEndpointSchemeAndPartialSharedKeys)
{
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

TEST(BlobServiceClientTests, ListBlobContainersAsync_ReportsMalformedXmlAsInvalidResponse)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {}, "<EnumerationResults><Containers>"};
    BlobServiceClient client{httpClient, BuildOptions()};

    ExpectInvalidResponseError(client, httpClient);
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_PropagatesTransportErrorWithoutInspectingHttpStatus)
{
    FakeHttpClient httpClient;
    httpClient.DefaultError() = std::make_error_code(std::errc::timed_out);
    httpClient.DefaultResponse() = HttpResponse{404, {{"x-ms-error-code", "ContainerNotFound"}}, ""};
    BlobServiceClient client{httpClient, BuildOptions()};

    ExpectTransportError(client, httpClient);
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_TreatsRedirectAsServiceError)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{307, {{"x-ms-request-id", "service-redirect"}}, ""};
    BlobServiceClient client{httpClient, BuildOptions()};

    ExpectRedirectServiceError(client, httpClient);
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_FallsBackToServiceErrorForMalformedXmlAndMissingRequestId)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{500, {}, "<Error><Message>broken"};
    BlobServiceClient client{httpClient, BuildOptions()};

    ExpectMalformedServiceError(client, httpClient);
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_UnrecognizedStorageCodeFallsBackToServiceErrorButPreservesDetails)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{409,
        {{"x-ms-error-code", "OddServiceProblem"}, {"x-ms-request-id", "service-409"}},
        "<Error><Message>Unexpected detail</Message></Error>"};
    BlobServiceClient client{httpClient, BuildOptions()};

    ExpectUnknownServiceErrorDetailsPreserved(client, httpClient);
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_OptionsOverloadParsesPaginationAndProperties)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        MakeCanonicalSuccessHeaders(),
        R"(<?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
              <Prefix>pref</Prefix>
              <Marker>page-1</Marker>
              <NextMarker>page-2</NextMarker>
              <Containers>
                <Container>
                  <Name>images</Name>
                  <Properties>
                    <Etag>"etag"</Etag>
                    <Last-Modified>Fri, 26 Jun 2015 18:59:17 GMT</Last-Modified>
                  </Properties>
                  <Metadata><Project>aveva</Project></Metadata>
                </Container>
              </Containers>
            </EnumerationResults>)"};
    BlobServiceClient client{httpClient, BuildOptions()};

    ListBlobContainersOptions options;
    options.Prefix = "pref";
    options.Marker = "page-1";
    options.MaxResults = 5U;
    options.IncludeMetadata = true;

    const ListBlobContainersResult listed = ListBlobContainers(client, httpClient, options);
    EXPECT_EQ(listed.Prefix, "pref");
    EXPECT_EQ(listed.Marker, "page-1");
    EXPECT_EQ(listed.NextMarker, "page-2");
    ASSERT_EQ(listed.Containers.size(), 1U);
    EXPECT_EQ(listed.Containers.at(0).Name, "images");
    EXPECT_EQ(listed.Containers.at(0).Properties.Metadata.at("project"), "aveva");
    ExpectRequestContract(httpClient.LastRequest(),
        AVEVA::HttpMethod::Get,
        "https://"
        "storageaccount.blob.core.windows.net?comp=list&prefix=pref&marker=page-1&maxresults=5&include=metadata&sv="
        "2025-01-05&sig=fakesig");
}

TEST(BlobServiceClientTests, ListBlobContainersAsync_UseFutureReturnsExpectedResult)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        MakeCanonicalSuccessHeaders(),
        R"(<?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
              <Containers>
                <Container>
                  <Name>images</Name>
                </Container>
              </Containers>
            </EnumerationResults>)"};
    BlobServiceClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<ListBlobContainersResult>, BlobStorageError>> future =
        client.ListBlobContainersAsync(AVEVA::AzureClient::ListBlobContainersOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    std::expected<Response<ListBlobContainersResult>, BlobStorageError> result = future.get();
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->Value().Containers.size(), 1U);
    EXPECT_EQ(result->Value().Containers.at(0).Name, "images");
}

// Task 14: omitting the completion token entirely yields a deferred, directly co_await-able
// operation (see BlockBlobClientTests.cpp for the detailed rationale).
TEST(BlobServiceClientTests, ListBlobContainersAsyncSupportsCoAwaitWithNoExplicitCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{200, MakeCanonicalSuccessHeaders(), "<EnumerationResults></EnumerationResults>"};
    BlobServiceClient client{httpClient, BuildOptions()};

    std::optional<std::expected<Response<ListBlobContainersResult>, BlobStorageError>> result;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitInto(&result,
            [&]
    {
        return client.ListBlobContainersAsync();
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
    [[maybe_unused]] void BlobServiceClientNoTokenOverloadsCompile(BlobServiceClient& client)
    {
        static_cast<void>(client.ListBlobContainersAsync());
        static_cast<void>(client.ListBlobContainersAsync(AVEVA::AzureClient::ListBlobContainersOptions{}));
        static_cast<void>(client.GetBlobContainerClient("images"));
    }

    // T12/T14: blob clients derived from a Shared Key service client sign with the connection's shared
    // signer; the signature must match a fresh one-off signature of the same request.
    TEST(BlobServiceClientTests, DerivedClientsSignWithTheSharedConnectionSigner)
    {
        FakeHttpClient httpClient;
        BlobServiceClient service{httpClient,
            BlobServiceClientOptions{.ServiceEndpoint = "https://storageaccount.blob.core.windows.net",
                .SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="}}};

        AVEVA::AzureClient::BlobContainerClient container = service.GetBlobContainerClient("images");
        AVEVA::AzureClient::BlockBlobClient blob = container.GetBlockBlobClient("folder/photo.png");

        int completions = 0;
        container.ExistsAsync([&](auto result)
        {
            EXPECT_TRUE(result.has_value());
            ++completions;
        });
        blob.DeleteAsync([&](auto result)
        {
            EXPECT_TRUE(result.has_value());
            ++completions;
        });
        httpClient.Poll();
        EXPECT_EQ(completions, 2);

        ASSERT_EQ(httpClient.Requests().size(), 2U);
        ExpectRequestsSignedWithSharedConnectionSigner(httpClient);
        EXPECT_EQ(httpClient.Requests().at(1).Request.GetUrl(),
            "https://storageaccount.blob.core.windows.net/images/folder/photo.png");
    }

    TEST(BlobServiceClientTests, GetBlobContainerClient_ValidatesTheContainerName)
    {
        FakeHttpClient httpClient;
        BlobServiceClient service{httpClient, BuildOptions()};
        EXPECT_THROW(static_cast<void>(service.GetBlobContainerClient("Invalid_Name")), std::invalid_argument);
        const auto container = service.GetBlobContainerClient("valid-name");
        EXPECT_THROW(static_cast<void>(container.GetBlockBlobClient("")), std::invalid_argument);
    }

    TEST(BlobServiceClientTests, ListBlobContainersAllFollowsNextMarker)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200,
            MakeCanonicalSuccessHeaders(),
            "<EnumerationResults><Prefix>c</Prefix><Containers><Container><Name>c1</Name></"
            "Container><Container><Name>c2</Name></Container>"
            "</Containers><NextMarker>next</NextMarker></EnumerationResults>"});
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200,
            MakeCanonicalSuccessHeaders(),
            "<EnumerationResults><Containers><Container><Name>c3</Name></Container></Containers><NextMarker/></"
            "EnumerationResults>"});
        BlobServiceClient client{httpClient, MakeBlobServiceClientOptions()};

        ListBlobContainersOptions options;
        options.Prefix = "c";
        const ListBlobContainersResult listed = ListAllBlobContainers(client, httpClient, options);

        ASSERT_EQ(httpClient.RequestCount(), 2U);
        EXPECT_NE(httpClient.RequestAt(1).Request.GetUrl().find("marker=next"), std::string::npos);
        EXPECT_NE(httpClient.RequestAt(1).Request.GetUrl().find("prefix=c"), std::string::npos);
        ASSERT_EQ(listed.Containers.size(), 3U);
        EXPECT_EQ(listed.Containers.at(0).Name, "c1");
        EXPECT_EQ(listed.Containers.at(2).Name, "c3");
        EXPECT_EQ(listed.Prefix, "c");
        EXPECT_TRUE(listed.NextMarker.empty());
    }

    TEST(BlobServiceClientTests, GetAccountInfoParsesHeaders)
    {
        FakeHttpClient httpClient;
        std::vector<AVEVA::HttpHeader> headers = MakeCanonicalSuccessHeaders();
        headers.emplace_back("x-ms-sku-name", "Standard_RAGRS");
        headers.emplace_back("x-ms-account-kind", "StorageV2");
        headers.emplace_back("x-ms-is-hns-enabled", "true");
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200, std::move(headers), ""});
        BlobServiceClient client{httpClient, MakeBlobServiceClientOptions()};

        const AccountInfo info = GetAccountInfo(client, httpClient);
        const AVEVA::HttpRequest& request = httpClient.LastRequest();
        EXPECT_EQ(request.GetMethod(), AVEVA::HttpMethod::Get);
        EXPECT_NE(request.GetUrl().find("restype=account&comp=properties"), std::string::npos);
        EXPECT_EQ(info.SkuName, "Standard_RAGRS");
        EXPECT_EQ(info.AccountKind, "StorageV2");
        EXPECT_TRUE(info.IsHierarchicalNamespaceEnabled);
    }

    TEST(BlobServiceClientTests, GetPropertiesParsesServicePropertiesXml)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200,
            MakeCanonicalSuccessHeaders(),
            R"(<?xml version="1.0" encoding="utf-8"?>
<StorageServiceProperties>
  <Logging><Version>1.0</Version><Delete>true</Delete><Read>false</Read><Write>true</Write>
    <RetentionPolicy><Enabled>true</Enabled><Days>7</Days></RetentionPolicy></Logging>
  <HourMetrics><Version>1.0</Version><Enabled>true</Enabled><IncludeAPIs>false</IncludeAPIs>
    <RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></HourMetrics>
  <MinuteMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></MinuteMetrics>
  <Cors><CorsRule><AllowedOrigins>https://a.example</AllowedOrigins><AllowedMethods>GET,PUT</AllowedMethods>
    <MaxAgeInSeconds>500</MaxAgeInSeconds><ExposedHeaders>x-ms-meta-*</ExposedHeaders><AllowedHeaders>x-ms-meta-abc</AllowedHeaders></CorsRule></Cors>
  <DefaultServiceVersion>2023-11-03</DefaultServiceVersion>
  <DeleteRetentionPolicy><Enabled>true</Enabled><Days>14</Days></DeleteRetentionPolicy>
  <StaticWebsite><Enabled>true</Enabled><IndexDocument>index.html</IndexDocument><ErrorDocument404Path>404.html</ErrorDocument404Path></StaticWebsite>
</StorageServiceProperties>)"});
        BlobServiceClient client{httpClient, MakeBlobServiceClientOptions()};

        const BlobServiceProperties properties = GetProperties(client, httpClient);
        EXPECT_NE(httpClient.LastRequest().GetUrl().find("restype=service&comp=properties"), std::string::npos);
        EXPECT_EQ(properties.Logging.Version, "1.0");
        EXPECT_TRUE(properties.Logging.Delete);
        EXPECT_FALSE(properties.Logging.Read);
        EXPECT_TRUE(properties.Logging.Write);
        EXPECT_TRUE(properties.Logging.RetentionPolicy.Enabled);
        EXPECT_EQ(properties.Logging.RetentionPolicy.Days, 7);
        EXPECT_TRUE(properties.HourMetrics.Enabled);
        EXPECT_EQ(properties.HourMetrics.IncludeApis, false);
        EXPECT_FALSE(properties.MinuteMetrics.Enabled);
        EXPECT_FALSE(properties.MinuteMetrics.IncludeApis.has_value());
        ASSERT_EQ(properties.Cors.size(), 1U);
        EXPECT_EQ(properties.Cors.at(0).AllowedOrigins, "https://a.example");
        EXPECT_EQ(properties.Cors.at(0).AllowedMethods, "GET,PUT");
        EXPECT_EQ(properties.Cors.at(0).AllowedHeaders, "x-ms-meta-abc");
        EXPECT_EQ(properties.Cors.at(0).ExposedHeaders, "x-ms-meta-*");
        EXPECT_EQ(properties.Cors.at(0).MaxAgeInSeconds, 500);
        EXPECT_EQ(properties.DefaultServiceVersion, "2023-11-03");
        EXPECT_TRUE(properties.DeleteRetentionPolicy.Enabled);
        EXPECT_EQ(properties.DeleteRetentionPolicy.Days, 14);
        EXPECT_TRUE(properties.StaticWebsite.Enabled);
        EXPECT_EQ(properties.StaticWebsite.IndexDocument, "index.html");
        EXPECT_EQ(properties.StaticWebsite.ErrorDocument404Path, "404.html");
    }

    TEST(BlobServiceClientTests, GetUserDelegationKeySendsKeyInfoAndParsesKey)
    {
        using namespace std::chrono;
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200,
            MakeCanonicalSuccessHeaders(),
            R"(<?xml version="1.0" encoding="utf-8"?><UserDelegationKey><SignedOid>oid</SignedOid><SignedTid>tid</SignedTid>
<SignedStart>2024-05-01T10:00:00Z</SignedStart><SignedExpiry>2024-05-02T10:00:00.5Z</SignedExpiry>
<SignedService>b</SignedService><SignedVersion>2023-11-03</SignedVersion><Value>a2V5</Value></UserDelegationKey>)"});
        BlobServiceClient client{httpClient, MakeBlobServiceClientOptions()};

        const auto start = sys_days{year{2024} / 5 / 1} + 10h;
        const UserDelegationKey key = GetUserDelegationKey(client,
            httpClient,
            GetUserDelegationKeyOptions{.StartsOn = start, .ExpiresOn = start + 24h});
        const AVEVA::HttpRequest& request = httpClient.LastRequest();
        EXPECT_EQ(request.GetMethod(), AVEVA::HttpMethod::Post);
        EXPECT_NE(request.GetUrl().find("restype=service&comp=userdelegationkey"), std::string::npos);
        EXPECT_NE(std::string{request.GetBody()}.find(
                      "<KeyInfo><Start>2024-05-01T10:00:00Z</Start><Expiry>2024-05-02T10:00:00Z</Expiry></KeyInfo>"),
            std::string::npos);
        EXPECT_EQ(key.SignedObjectId, "oid");
        EXPECT_EQ(key.SignedTenantId, "tid");
        EXPECT_EQ(key.SignedStartsOn, start);
        EXPECT_EQ(key.SignedExpiresOn, start + 24h + 500ms);
        EXPECT_EQ(key.SignedService, "b");
        EXPECT_EQ(key.SignedVersion, "2023-11-03");
        EXPECT_EQ(key.Value, "a2V5");
    }

    TEST(BlobServiceClientTests, GetUserDelegationKeyValidatesExpiryAndRejectsMalformedResponse)
    {
        using namespace std::chrono;
        FakeHttpClient httpClient;
        BlobServiceClient client{httpClient, MakeBlobServiceClientOptions()};

        std::optional<std::error_code> error;
        client.GetUserDelegationKeyAsync(AVEVA::AzureClient::GetUserDelegationKeyOptions{},
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
        httpClient.Poll();
        EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument));
        EXPECT_EQ(httpClient.RequestCount(), 0U);

        httpClient.EnqueueResponse(AVEVA::HttpResponse{200,
            MakeCanonicalSuccessHeaders(),
            "<UserDelegationKey><SignedStart>yesterday</SignedStart><Value>a2V5</Value></UserDelegationKey>"});
        bool failed = false;
        client.GetUserDelegationKeyAsync(AVEVA::AzureClient::GetUserDelegationKeyOptions{.StartsOn = std::nullopt,
                                             .ExpiresOn = system_clock::now() + 1h},
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            failed = true;
        });
        httpClient.Poll();
        EXPECT_TRUE(failed);
    }
} // namespace

TEST(BlobServiceClientTests, OptionsDefaultApiVersionMatchesSharedDefault)
{
    EXPECT_EQ(AVEVA::AzureClient::BlobServiceClientOptions{}.ApiVersion,
        std::string(AVEVA::AzureClient::DefaultApiVersion));
}
