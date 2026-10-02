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
    using AVEVA::AzureClient::Models::BlobContainerProperties;
    using AVEVA::AzureClient::Models::CreateBlobContainerResult;
    using AVEVA::AzureClient::Models::DeleteBlobContainerResult;
    using AVEVA::AzureClient::Models::LeaseDurationType;
    using AVEVA::AzureClient::Models::LeaseState;
    using AVEVA::AzureClient::Models::LeaseStatus;
    using AVEVA::AzureClient::Models::ListBlobsResult;
    using AVEVA::AzureClient::Models::PublicAccessType;
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

    void VerifyLeaseAccessType(const BlobContainerProperties& properties,
        unsigned int& statusCode,
        unsigned int responseStatus)
    {
        statusCode = responseStatus;
        EXPECT_EQ(properties.AccessType, PublicAccessType::Blob);
    }

    void VerifyLeasePolicyFlags(const BlobContainerProperties& properties)
    {
        EXPECT_TRUE(properties.HasImmutabilityPolicy);
        EXPECT_TRUE(properties.HasLegalHold);
    }

    void VerifyLeaseStateProperties(const BlobContainerProperties& properties)
    {
        EXPECT_EQ(properties.Status, LeaseStatus::Locked);
        EXPECT_EQ(properties.State, LeaseState::Breaking);
        EXPECT_EQ(properties.DurationType, LeaseDurationType::Fixed);
    }

    void VerifyEncryptionScopeProperties(const BlobContainerProperties& properties)
    {
        EXPECT_EQ(properties.DefaultEncryptionScope, "scope-a");
        EXPECT_TRUE(properties.PreventEncryptionScopeOverride);
    }

    void VerifyLeaseMetadataEntries(const BlobContainerProperties& properties)
    {
        EXPECT_EQ(properties.Metadata.at("project"), "aveva");
        EXPECT_EQ(properties.Metadata.at("owner"), "storage");
    }

    void VerifyLeaseMetadataResult(const Response<BlobContainerProperties>& response,
        unsigned int& statusCode,
        CallbackExpectation& callback)
    {
        VerifyLeaseAccessType(response.Value(), statusCode, response.RawResponse().GetStatus());
        VerifyLeasePolicyFlags(response.Value());
        VerifyLeaseStateProperties(response.Value());
        VerifyEncryptionScopeProperties(response.Value());
        VerifyLeaseMetadataEntries(response.Value());
        callback.MarkInvoked();
    }

    void VerifyUnknownAccessAndStatus(const BlobContainerProperties& properties)
    {
        EXPECT_EQ(properties.AccessType, PublicAccessType::Unknown);
        EXPECT_EQ(properties.Status, LeaseStatus::Unknown);
    }

    void VerifyUnknownStateAndDuration(const BlobContainerProperties& properties)
    {
        EXPECT_EQ(properties.State, LeaseState::Unknown);
        EXPECT_EQ(properties.DurationType, LeaseDurationType::Unknown);
    }

    void VerifyUnknownEnumPropertiesResult(const Response<BlobContainerProperties>& response)
    {
        VerifyUnknownAccessAndStatus(response.Value());
        VerifyUnknownStateAndDuration(response.Value());
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

    void VerifyDeleteMalformedXmlFallbackStatus(const BlobStorageError& error)
    {
        EXPECT_EQ(error.Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(error.StatusCode, 500U);
    }

    void VerifyDeleteMalformedXmlFallbackPayload(const BlobStorageError& error, CallbackExpectation& callback)
    {
        EXPECT_TRUE(error.ErrorCode.empty());
        EXPECT_TRUE(error.Message.empty());
        EXPECT_TRUE(error.RequestId.empty());
        callback.MarkInvoked();
    }

    void VerifyDeleteUnknownCodeFallbackCodeAndId(const BlobStorageError& error)
    {
        EXPECT_EQ(error.Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(error.RequestId, "container-409");
    }

    void VerifyDeleteUnknownCodeFallbackDetails(const BlobStorageError& error, CallbackExpectation& callback)
    {
        EXPECT_EQ(error.ErrorCode, "OddContainerProblem");
        EXPECT_EQ(error.Message, "Unexpected detail");
        callback.MarkInvoked();
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

    void GetPropertiesAndVerifyLeaseMetadata(BlobContainerClient& client,
        unsigned int& statusCode,
        CallbackExpectation& callback)
    {
        client.GetPropertiesAsync([&](std::expected<Response<BlobContainerProperties>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            VerifyLeaseMetadataResult(*result, statusCode, callback);
        });
    }

    void GetPropertiesAndVerifyUnknownEnums(BlobContainerClient& client)
    {
        client.GetPropertiesAsync([&](std::expected<Response<BlobContainerProperties>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            VerifyUnknownEnumPropertiesResult(*result);
        });
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

    void DeleteContainerAndVerifyTransportError(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::connection_reset));
            EXPECT_TRUE(result.error().ErrorCode.empty());
            callback.MarkInvoked();
        });
    }

    void DeleteContainerAndVerifyRedirect(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
            EXPECT_EQ(result.error().StatusCode, 307U);
            EXPECT_EQ(result.error().RequestId, "container-redirect");
            callback.MarkInvoked();
        });
    }

    void DeleteContainerAndVerifyMalformedXmlFallback(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            VerifyDeleteMalformedXmlFallbackStatus(result.error());
            VerifyDeleteMalformedXmlFallbackPayload(result.error(), callback);
        });
    }

    void DeleteContainerAndVerifyUnknownCodeFallback(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            VerifyDeleteUnknownCodeFallbackCodeAndId(result.error());
            VerifyDeleteUnknownCodeFallbackDetails(result.error(), callback);
        });
    }

    void VerifyBlobClientAndContainerOperations(BlobContainerClient& container,
        BlobClient& blob,
        FakeHttpClient& httpClient,
        int& completions)
    {
        container.ExistsAsync([&](auto result)
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

    void VerifyContainerExists(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.ExistsAsync([&](std::expected<Response<bool>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<bool>& response = *result;
            EXPECT_TRUE(response.Value());
            callback.MarkInvoked();
        });
    }

    void VerifyMissingContainerExists(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.ExistsAsync([&](std::expected<Response<bool>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<bool>& response = *result;
            EXPECT_FALSE(response.Value());
            ASSERT_TRUE(response.Error().has_value());
            EXPECT_EQ(response.Error()->RequestId, "missing-request");
            callback.MarkInvoked();
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

    void DeleteContainerIfExistsAndVerifyExpectedMissing(BlobContainerClient& client, CallbackExpectation& callback)
    {
        client.DeleteIfExistsAsync({},
            [&](std::expected<Response<DeleteBlobContainerResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<DeleteBlobContainerResult>& response = *result;
            ASSERT_TRUE(response.Error().has_value());
            EXPECT_EQ(response.Error()->RequestId, "delete-request");
            callback.MarkInvoked();
        });
    }

    void ListBlobsAndCaptureResult(BlobContainerClient& client, std::optional<ListBlobsResult>& listed)
    {
        client.ListBlobsAsync([&](std::expected<Response<ListBlobsResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            listed = result->Value();
        });
    }

    void ListAllBlobsAndCaptureResult(BlobContainerClient& client,
        const AVEVA::AzureClient::ListBlobsOptions& options,
        std::optional<ListBlobsResult>& listed,
        int& completions)
    {
        client.ListBlobsAllAsync(options,
            [&](std::expected<Response<ListBlobsResult>, BlobStorageError> result)
        {
            ++completions;
            ASSERT_TRUE(result.has_value());
            listed = result->Value();
        });
    }

    void ExpectPagedListRequestsUseSharedPrefixAndMaxResults(const FakeHttpClient& httpClient)
    {
        for (std::size_t index = 0; index < 3U; ++index)
        {
            EXPECT_NE(httpClient.RequestAt(index).Request.GetUrl().find("prefix=x"), std::string::npos);
            EXPECT_NE(httpClient.RequestAt(index).Request.GetUrl().find("maxresults=2"), std::string::npos);
        }
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

TEST(BlobContainerClientTests, DeleteAsync_UsesDeleteMethod)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    client.DeleteAsync([](std::expected<Response<DeleteBlobContainerResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Delete);
    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images?restype=container&sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, GetPropertiesAsync_ParsesLeaseMetadataAndEncryptionScopeFields)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        {
            {"ETag", "\"0x8D1234\""},
            {"Last-Modified", "Fri, 26 Jun 2015 18:59:17 GMT"},
            {"x-ms-blob-public-access", "blob"},
            {"x-ms-lease-status", "locked"},
            {"x-ms-lease-state", "breaking"},
            {"x-ms-lease-duration", "fixed"},
            {"x-ms-has-immutability-policy", "true"},
            {"x-ms-has-legal-hold", "true"},
            {"x-ms-default-encryption-scope", "scope-a"},
            {"x-ms-deny-encryption-scope-override", "true"},
            {"x-ms-meta-Project", "aveva"},
            {"X-Ms-MeTa-OWNER", "storage"},
        },
        ""};

    BlobContainerClient client{httpClient, BuildOptions()};

    unsigned int statusCode = 0;
    CallbackExpectation callback;
    GetPropertiesAndVerifyLeaseMetadata(client, statusCode, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Head);
    EXPECT_EQ(statusCode, 200U);
}

TEST(BlobContainerClientTests, GetPropertiesAsync_PreservesUnknownProtocolEnumValues)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        {
            {"x-ms-blob-public-access", "mystery-access"},
            {"x-ms-lease-status", "half-locked"},
            {"x-ms-lease-state", "teleporting"},
            {"x-ms-lease-duration", "elastic"},
        },
        ""};

    BlobContainerClient client{httpClient, BuildOptions()};

    GetPropertiesAndVerifyUnknownEnums(client);
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

TEST(BlobContainerClientTests, DeleteAsync_PropagatesTransportErrorWithoutInspectingHttpStatus)
{
    FakeHttpClient httpClient;
    httpClient.DefaultError() = std::make_error_code(std::errc::connection_reset);
    httpClient.DefaultResponse() = HttpResponse{404, {{"x-ms-error-code", "ContainerNotFound"}}, ""};
    BlobContainerClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    DeleteContainerAndVerifyTransportError(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlobContainerClientTests, DeleteAsync_TreatsRedirectAsServiceError)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{307, {{"x-ms-request-id", "container-redirect"}}, ""};
    BlobContainerClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    DeleteContainerAndVerifyRedirect(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlobContainerClientTests, DeleteAsync_FallsBackToServiceErrorForMalformedXmlAndMissingRequestId)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{500, {}, "<Error><Message>broken"};
    BlobContainerClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    DeleteContainerAndVerifyMalformedXmlFallback(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlobContainerClientTests, DeleteAsync_UnrecognizedStorageCodeFallsBackToServiceErrorButPreservesDetails)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{409,
        {{"x-ms-error-code", "OddContainerProblem"}, {"x-ms-request-id", "container-409"}},
        "<Error><Message>Unexpected detail</Message></Error>"};
    BlobContainerClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    DeleteContainerAndVerifyUnknownCodeFallback(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlobContainerClientTests, GetBlockBlobClient_BuildsBlobScopedClient)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    auto blockBlobClient = client.GetBlockBlobClient("photo.png");
    blockBlobClient.DeleteAsync(
        [](std::expected<Response<AVEVA::AzureClient::Models::DeleteBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/photo.png?sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, GetPageBlobClient_BuildsBlobScopedClient)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    auto pageBlobClient = client.GetPageBlobClient("disk.vhd");
    pageBlobClient.DeleteAsync(
        [](std::expected<Response<AVEVA::AzureClient::Models::DeleteBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/disk.vhd?sv=2025-01-05&sig=fakesig");
}

TEST(BlobContainerClientTests, GetBlockBlobClient_UrlEncodesBlobNameAndPreservesSlashes)
{
    FakeHttpClient httpClient;
    BlobContainerClient client{httpClient, BuildOptions()};

    auto blockBlobClient = client.GetBlockBlobClient("folder name/file?.txt");
    blockBlobClient.DeleteAsync(
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

    AVEVA::AzureClient::BlobClient blob = container.GetBlobClient("folder name/photo.png");
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

TEST(BlobContainerClientTests, ExistsCreateIfNotExistsAndDeleteIfExistsTreatExpectedStatusesAsNonFatal)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(MakeAzureErrorResponse(404, "ContainerNotFound", "", "missing-request"));
    httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "ContainerAlreadyExists", "", "create-request"));
    httpClient.EnqueueResponse(MakeAzureErrorResponse(404, "ContainerNotFound", "", "delete-request"));
    BlobContainerClient client{httpClient, BuildOptions()};

    CallbackExpectation existsCallback;
    VerifyContainerExists(client, existsCallback);

    CallbackExpectation missingExistsCallback;
    VerifyMissingContainerExists(client, missingExistsCallback);

    CallbackExpectation createCallback;
    CreateContainerIfMissingAndVerifyExpectedConflict(client, createCallback);

    CallbackExpectation deleteCallback;
    DeleteContainerIfExistsAndVerifyExpectedMissing(client, deleteCallback);

    httpClient.Poll(); // drive the four posted (async) completions (T26)
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "x-ms-meta-project"), "tests");
}

TEST(BlobContainerClientTests, CreateAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlobContainerClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<CreateBlobContainerResult>, BlobStorageError>> future =
        client.CreateAsync(AVEVA::AzureClient::CreateBlobContainerOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
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
        using AVEVA::AzureClient::DeleteBlobContainerOptions;
        using AVEVA::AzureClient::GetBlobContainerPropertiesOptions;
        using AVEVA::AzureClient::ListBlobsOptions;

        static_cast<void>(client.CreateAsync());
        static_cast<void>(client.CreateAsync(CreateBlobContainerOptions{}));

        static_cast<void>(client.DeleteAsync());
        static_cast<void>(client.DeleteAsync(DeleteBlobContainerOptions{}));

        static_cast<void>(client.GetPropertiesAsync());
        static_cast<void>(client.GetPropertiesAsync(GetBlobContainerPropertiesOptions{}));

        static_cast<void>(client.ListBlobsAsync());
        static_cast<void>(client.ListBlobsAsync(ListBlobsOptions{}));

        static_cast<void>(client.ExistsAsync());
        static_cast<void>(client.CreateIfNotExistsAsync());
        static_cast<void>(client.CreateIfNotExistsAsync(CreateBlobContainerOptions{}));
        static_cast<void>(client.DeleteIfExistsAsync());
        static_cast<void>(client.DeleteIfExistsAsync(DeleteBlobContainerOptions{}));
    }

    TEST(BlobContainerClientTests, ListBlobsSendsIncludeFlagsInServiceOrder)
    {
        FakeHttpClient httpClient;
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        AVEVA::AzureClient::ListBlobsOptions options;
        options.IncludeVersions = true;
        options.IncludeMetadata = true;
        options.IncludeTags = true;
        options.IncludeDeleted = true;
        options.IncludeSnapshots = true;
        options.IncludeUncommittedBlobs = true;
        options.IncludeCopy = true;
        client.ListBlobsAsync(options, [](auto) {});
        httpClient.Poll();
        const std::string url = httpClient.LastRequest().GetUrl();
        const bool encodedCommas =
            url.contains("include=copy%2Cdeleted%2Cmetadata%2Csnapshots%2Ctags%2Cuncommittedblobs%2Cversions");
        const bool plainCommas = url.contains("include=copy,deleted,metadata,snapshots,tags,uncommittedblobs,versions");
        EXPECT_TRUE(encodedCommas || plainCommas) << url;

        client.ListBlobsAsync([](auto) {});
        httpClient.Poll();
        EXPECT_EQ(httpClient.LastRequest().GetUrl().find("include="), std::string::npos);
    }

    TEST(BlobContainerClientTests, ListBlobsParsesEncodedNamesDeletedFlagTagsAndVersions)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200,
            MakeCanonicalSuccessHeaders(),
            R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://storageaccount.blob.core.windows.net/" ContainerName="images">
  <Blobs>
    <Blob>
      <Name Encoded="true">dir%2Fodd%01name.txt</Name>
      <VersionId>2024-01-01T00:00:00.0000000Z</VersionId>
      <IsCurrentVersion>true</IsCurrentVersion>
      <Deleted>true</Deleted>
      <Properties><Content-Length>4</Content-Length></Properties>
      <Tags><TagSet><Tag><Key>project</Key><Value>alpha</Value></Tag><Tag><Key>Stage</Key><Value>raw</Value></Tag></TagSet></Tags>
    </Blob>
    <Blob>
      <Name>plain.txt</Name>
      <Properties><Content-Length>1</Content-Length></Properties>
    </Blob>
    <BlobPrefix><Name Encoded="false">dir%2F</Name></BlobPrefix>
  </Blobs>
  <NextMarker />
</EnumerationResults>)"});
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        std::optional<ListBlobsResult> listed;
        ListBlobsAndCaptureResult(client, listed);
        httpClient.Poll();

        ASSERT_TRUE(listed.has_value());
        ASSERT_EQ(ValueOrFail(listed).Blobs.size(), 2U);
        const auto& odd = ValueOrFail(listed).Blobs.at(0);
        EXPECT_EQ(odd.Name, std::string{"dir/odd\x01name.txt"});
        EXPECT_TRUE(odd.Deleted);
        EXPECT_EQ(odd.Properties.VersionId, "2024-01-01T00:00:00.0000000Z");
        EXPECT_EQ(odd.Properties.IsCurrentVersion, true);
        ASSERT_EQ(odd.Tags.size(), 2U);
        EXPECT_EQ(odd.Tags.at("project"), "alpha");
        EXPECT_EQ(odd.Tags.at("Stage"), "raw");
        EXPECT_EQ(ValueOrFail(listed).Blobs.at(1).Name, "plain.txt");
        EXPECT_FALSE(ValueOrFail(listed).Blobs.at(1).Deleted);
        EXPECT_TRUE(ValueOrFail(listed).Blobs.at(1).Tags.empty());
        ASSERT_EQ(ValueOrFail(listed).BlobPrefixes.size(), 1U);
        EXPECT_EQ(ValueOrFail(listed).BlobPrefixes.at(0), "dir%2F");
    }

    TEST(BlobContainerClientTests, ListBlobsRejectsInvalidEncodedName)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200,
            MakeCanonicalSuccessHeaders(),
            R"(<EnumerationResults><Blobs><Blob><Name Encoded="true">bad%zz</Name></Blob></Blobs></EnumerationResults>)"});
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        bool failed = false;
        client.ListBlobsAsync([&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            failed = true;
        });
        httpClient.Poll();
        EXPECT_TRUE(failed);
    }

    namespace
    {
        // Tags the next-marker with its own type so it's never adjacent-and-same-type with the
        // comma-separated blob names parameter (see bugprone-easily-swappable-parameters).
        struct NextMarker
        {
            std::string_view Value;

            constexpr NextMarker(const char* value) noexcept : Value(value)
            {
            }

            constexpr NextMarker(std::string_view value) noexcept : Value(value)
            {
            }
        };

        [[nodiscard]] AVEVA::HttpResponse ListBlobsPage(std::string_view names, NextMarker nextMarker)
        {
            std::string body = "<EnumerationResults><Blobs>";
            for (const auto part : std::views::split(names, ','))
            {
                body += "<Blob><Name>" + std::string{std::string_view{part}} + "</Name></Blob>";
            }
            body += "<BlobPrefix><Name>p-" + std::string{nextMarker.Value} + "</Name></BlobPrefix>";
            body += "</Blobs><NextMarker>" + std::string{nextMarker.Value} + "</NextMarker></EnumerationResults>";
            return AVEVA::HttpResponse{200, MakeCanonicalSuccessHeaders(), body};
        }
    } // namespace

    TEST(BlobContainerClientTests, ListBlobsAllFollowsNextMarkerAndMergesPages)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(ListBlobsPage("a,b", "m1"));
        httpClient.EnqueueResponse(ListBlobsPage("c", "m2"));
        httpClient.EnqueueResponse(ListBlobsPage("d", ""));
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        AVEVA::AzureClient::ListBlobsOptions options;
        options.Prefix = "x";
        options.MaxResults = 2;
        std::optional<ListBlobsResult> listed;
        int completions = 0;
        ListAllBlobsAndCaptureResult(client, options, listed, completions);
        httpClient.Poll();

        EXPECT_EQ(completions, 1);
        ASSERT_EQ(httpClient.RequestCount(), 3U);
        EXPECT_EQ(httpClient.RequestAt(0).Request.GetUrl().find("marker="), std::string::npos);
        EXPECT_NE(httpClient.RequestAt(1).Request.GetUrl().find("marker=m1"), std::string::npos);
        EXPECT_NE(httpClient.RequestAt(2).Request.GetUrl().find("marker=m2"), std::string::npos);
        ExpectPagedListRequestsUseSharedPrefixAndMaxResults(httpClient);
        ASSERT_TRUE(listed.has_value());
        ASSERT_EQ(ValueOrFail(listed).Blobs.size(), 4U);
        EXPECT_EQ(ValueOrFail(listed).Blobs.at(0).Name, "a");
        EXPECT_EQ(ValueOrFail(listed).Blobs.at(3).Name, "d");
        EXPECT_EQ(ValueOrFail(listed).BlobPrefixes, (std::vector<std::string>{"p-m1", "p-m2", "p-"}));
        EXPECT_TRUE(ValueOrFail(listed).NextMarker.empty());
    }

    TEST(BlobContainerClientTests, ListBlobsAllFailsAndStopsWhenMaxItemsExceeded)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(ListBlobsPage("a,b", "m1"));
        httpClient.EnqueueResponse(ListBlobsPage("c", "m2"));
        httpClient.EnqueueResponse(ListBlobsPage("d", ""));
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        AVEVA::AzureClient::ListBlobsOptions options;
        options.MaxItems = 3;
        std::optional<std::error_code> error;
        client.ListBlobsAllAsync(options,
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
        httpClient.Poll();
        EXPECT_EQ(error, std::make_error_code(std::errc::value_too_large));
        EXPECT_EQ(httpClient.RequestCount(), 2U);
    }

    TEST(BlobContainerClientTests, ListBlobsAllStopsAtFirstPageError)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(ListBlobsPage("a", "m1"));
        httpClient.EnqueueResponse(MakeAzureErrorResponse(403, "AuthorizationFailure", "denied", "req-2"));
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        std::optional<std::error_code> error;
        client.ListBlobsAllAsync([&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
        httpClient.Poll();
        EXPECT_EQ(error, std::error_code{BlobStorageErrorCode::AuthorizationFailure});
        EXPECT_EQ(httpClient.RequestCount(), 2U);
    }

    TEST(BlobContainerClientTests, ListBlobsAllRejectsRepeatedMarker)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(ListBlobsPage("a", "m1"));
        httpClient.EnqueueResponse(ListBlobsPage("b", "m1"));
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        bool failed = false;
        client.ListBlobsAllAsync([&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            failed = true;
        });
        httpClient.Poll();
        EXPECT_TRUE(failed);
        EXPECT_EQ(httpClient.RequestCount(), 2U);
    }

    TEST(BlobContainerClientTests, ListBlobsAllSurvivesClientDestructionMidEnumeration)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(ListBlobsPage("a", "m1"));
        httpClient.EnqueueResponse(ListBlobsPage("b", ""));
        std::optional<std::size_t> count;
        {
            auto client = std::make_unique<BlobContainerClient>(httpClient, MakeBlobContainerClientOptions());
            client->ListBlobsAllAsync([&](auto result)
            {
                ASSERT_TRUE(result.has_value());
                count = result->Value().Blobs.size();
            });
        }
        httpClient.Poll();
        EXPECT_EQ(count, 2U);
    }

    TEST(BlobContainerClientTests, ListBlobsAllWorksWithUseFuture)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(ListBlobsPage("a", "m1"));
        httpClient.EnqueueResponse(ListBlobsPage("b", ""));
        BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

        auto future = client.ListBlobsAllAsync(boost::asio::use_future);
        httpClient.Poll();
        auto result = future.get();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().Blobs.size(), 2U);
    }
} // namespace
