#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "SuppressDeprecated.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/AppendBlobClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <cstddef>
#include <gtest/gtest.h>

#include <boost/asio/detached.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <expected>
#include <filesystem>
#include <future>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::HttpMethod;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::AppendBlobClient;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::AppendBlockResult;
    using AVEVA::AzureClient::Models::BlobType;
    using AVEVA::AzureClient::Models::CreateAppendBlobResult;
    using AVEVA::AzureClient::Models::DeleteBlobResult;
    using AVEVA::AzureClient::Models::DownloadBlobResult;
    using AVEVA::AzureClient::Models::DownloadBlobToResult;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::DefaultETag;
    using AVEVA::AzureClient::Tests::ExpectRequestContract;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;

    [[nodiscard]] BlobClientOptions BuildOptions()
    {
        return MakeBlobClientOptions("logs", "append.txt");
    }

    void VerifyDeleteUnknownCodeFallbackIdentity(const BlobStorageError& error)
    {
        EXPECT_EQ(error.Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(error.RequestId, "append-409");
    }

    void VerifyDeleteUnknownCodeFallbackDetails(const BlobStorageError& error, CallbackExpectation& callback)
    {
        EXPECT_EQ(error.ErrorCode, "OddAppendProblem");
        EXPECT_EQ(error.Message, "Unexpected detail");
        callback.MarkInvoked();
    }

    void VerifyDownloadedAppendBlobProperties(const DownloadBlobResult& result)
    {
        EXPECT_EQ(result.Properties.Type, BlobType::AppendBlob);
        EXPECT_EQ(result.Properties.ContentLength, 4U);
    }

    void VerifyDownloadedAppendBlobContent(const DownloadBlobResult& result, CallbackExpectation& callback)
    {
        EXPECT_EQ(result.Content, "data");
        callback.MarkInvoked();
    }

    void VerifyAppendBlobPropertiesBasics(const AVEVA::AzureClient::Models::BlobProperties& properties)
    {
        EXPECT_EQ(properties.Type, BlobType::AppendBlob);
        EXPECT_EQ(properties.ContentLength, 7U);
    }

    void VerifyAppendBlobMetadata(const AVEVA::AzureClient::Models::BlobProperties& properties,
        CallbackExpectation& callback)
    {
        EXPECT_EQ(properties.Metadata.at("owner"), "ops");
        callback.MarkInvoked();
    }

    void VerifyMissingAppendBlobError(const BlobStorageError& error, CallbackExpectation& callback)
    {
        EXPECT_EQ(error.RequestId, "exists-404");
        callback.MarkInvoked();
    }

    void CreateAppendBlobAndVerifySuccess(AppendBlobClient& client,
        const AVEVA::AzureClient::CreateAppendBlobOptions& options,
        CallbackExpectation& callback)
    {
        client.CreateAsync(options,
            [&](std::expected<Response<CreateAppendBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void AppendStringBlockAndVerifySuccess(AppendBlobClient& client, std::string content, CallbackExpectation& callback)
    {
        client.AppendBlockAsync(std::move(content),
            [&](std::expected<Response<AppendBlockResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void AppendSpanBlockAndVerifySuccess(AppendBlobClient& client,
        std::span<const std::byte> content,
        const AVEVA::AzureClient::AppendBlockOptions& options,
        CallbackExpectation& callback)
    {
        client.AppendBlockAsync(content,
            options,
            [&](std::expected<Response<AppendBlockResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void DownloadBlobAndVerifyResult(AppendBlobClient& client,
        const AVEVA::AzureClient::DownloadBlobOptions& options,
        CallbackExpectation& callback)
    {
        client.DownloadAsync(options,
            [&](std::expected<Response<DownloadBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            VerifyDownloadedAppendBlobProperties(result->Value());
            VerifyDownloadedAppendBlobContent(result->Value(), callback);
        });
    }

    void DownloadBlobToStreamAndVerifyResult(AppendBlobClient& client,
        std::ostringstream& stream,
        CallbackExpectation& callback)
    {
        client.DownloadToAsync(stream,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, 4U);
            callback.MarkInvoked();
        });
    }

    void DownloadBlobToPathAndVerifyResult(AppendBlobClient& client,
        const std::filesystem::path& path,
        CallbackExpectation& callback)
    {
        client.DownloadToAsync(path,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, 5U);
            callback.MarkInvoked();
        });
    }

    void DeleteBlobAndVerifyTransportError(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::connection_reset));
            EXPECT_EQ(result.error().StatusCode, 0U);
            callback.MarkInvoked();
        });
    }

    void DeleteBlobAndVerifyRedirect(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
            EXPECT_EQ(result.error().RequestId, "append-redirect");
            callback.MarkInvoked();
        });
    }

    void DeleteBlobAndVerifyMalformedXmlFallback(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
            EXPECT_EQ(result.error().StatusCode, 500U);
            EXPECT_TRUE(result.error().Message.empty());
            callback.MarkInvoked();
        });
    }

    void DeleteBlobAndVerifyUnknownCodeFallback(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            VerifyDeleteUnknownCodeFallbackIdentity(result.error());
            VerifyDeleteUnknownCodeFallbackDetails(result.error(), callback);
        });
    }

    void SetBlobMetadataAndVerifySuccess(AppendBlobClient& client,
        const AVEVA::AzureClient::SetBlobMetadataOptions& options,
        CallbackExpectation& callback)
    {
        client.SetMetadataAsync(options,
            [&](std::expected<Response<AVEVA::AzureClient::Models::SetBlobMetadataResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void GetBlobPropertiesAndVerifyAppendBlob(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.GetPropertiesAsync(
            [&](std::expected<Response<AVEVA::AzureClient::Models::BlobProperties>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            VerifyAppendBlobPropertiesBasics(result->Value());
            VerifyAppendBlobMetadata(result->Value(), callback);
        });
    }

    void VerifyAppendBlobExists(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.ExistsAsync([&](std::expected<Response<bool>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->Value());
            callback.MarkInvoked();
        });
    }

    void VerifyMissingAppendBlob(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.ExistsAsync([&](std::expected<Response<bool>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_FALSE(result->Value());
            ASSERT_TRUE(result->Error().has_value());
            VerifyMissingAppendBlobError(*result->Error(), callback);
        });
    }

    void ExpectAppendBlobHeadRequests(const FakeHttpClient& httpClient)
    {
        for (std::size_t index = 1; index < 4; ++index)
        {
            ExpectRequestContract(httpClient.RequestAt(index).Request,
                HttpMethod::Head,
                "https://storageaccount.blob.core.windows.net/logs/append.txt?sv=2025-01-05&sig=fakesig");
        }
    }

    void ReleaseLeaseAndVerifySuccess(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.ReleaseLeaseAsync(AVEVA::AzureClient::ReleaseLeaseOptions{.LeaseId = "lease-1"},
            [&](std::expected<Response<AVEVA::AzureClient::Models::ReleaseBlobLeaseResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void DeleteBlobIfExistsAndVerifyExpectedMissing(AppendBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteIfExistsAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->Error().has_value());
            EXPECT_EQ(result->Error()->RequestId, "delete-404");
            callback.MarkInvoked();
        });
    }

    void AppendBlockAndCaptureResult(AppendBlobClient& client,
        AVEVA::AzureClient::AppendBlockOptions options,
        std::optional<AppendBlockResult>& appended)
    {
        client.AppendBlockAsync(std::string{"data"},
            std::move(options),
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            appended = result->Value();
        });
    }

    void SealBlobAndCaptureState(AppendBlobClient& client,
        AVEVA::AzureClient::SealAppendBlobOptions options,
        std::optional<bool>& sealed)
    {
        client.SealAsync(std::move(options),
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            sealed = result->Value().IsSealed;
        });
    }

    void CreateBlobIfNotExistsAndVerifyCreated(AppendBlobClient& client, int& created)
    {
        client.CreateIfNotExistsAsync([&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_FALSE(result->Error().has_value());
            ++created;
        });
    }

    void CreateBlobIfNotExistsAndVerifyExisting(AppendBlobClient& client, int& created)
    {
        client.CreateIfNotExistsAsync([&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->Error().has_value());
            EXPECT_EQ(result->Error()->RequestId, "req-2");
            ++created;
        });
    }

    void CreateBlobIfNotExistsAndCaptureFailure(AppendBlobClient& client, std::optional<std::error_code>& error)
    {
        client.CreateIfNotExistsAsync([&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
    }
} // namespace

TEST(AppendBlobClientTests, Constructor_ThrowsForConflictingCredentials)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.BearerToken = "token";

    EXPECT_THROW((AppendBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(AppendBlobClientTests, Constructor_ThrowsForMissingBlobName)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.BlobName.clear();

    EXPECT_THROW((AppendBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(AppendBlobClientTests, CreateAsync_UsesBearerAuthorizationExactly)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.BearerToken = "append-token";
    AppendBlobClient client{httpClient, options};

    client.CreateAsync([](std::expected<Response<CreateAppendBlobResult>, BlobStorageError>) {});

    // BearerToken/TokenCredential completions that finish on the same stack are deferred
    // via post() (never completing before the initiating call returns); poll the fake
    // client's executor to observe them.
    httpClient.Poll();

    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization"), "Bearer append-token");
}

TEST(AppendBlobClientTests, CreateAndAppendBlockOverloadsParseResponsesAndRespectOptions)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});

    AppendBlobClient client{httpClient, BuildOptions()};

    AVEVA::AzureClient::CreateAppendBlobOptions createOptions;
    createOptions.Metadata["owner"] = "tests";
    CallbackExpectation createCallback;
    CreateAppendBlobAndVerifySuccess(client, createOptions, createCallback);
    ExpectRequestContract(httpClient.RequestAt(0).Request,
        HttpMethod::Put,
        "https://storageaccount.blob.core.windows.net/logs/append.txt?sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-blob-type"), "AppendBlob");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-meta-owner"), "tests");

    CallbackExpectation stringAppend;
    AppendStringBlockAndVerifySuccess(client, "abc", stringAppend);
    ExpectRequestContract(httpClient.RequestAt(1).Request,
        HttpMethod::Put,
        "https://storageaccount.blob.core.windows.net/logs/append.txt?comp=appendblock&sv=2025-01-05&sig=fakesig",
        "abc");

    const std::vector<std::byte> bytes{std::byte{'x'}, std::byte{'y'}};
    AVEVA::AzureClient::AppendBlockOptions appendOptions;
    appendOptions.Conditions.LeaseId = "lease-1";
    CallbackExpectation spanAppend;
    AppendSpanBlockAndVerifySuccess(client, std::span<const std::byte>{bytes}, appendOptions, spanAppend);
    ExpectRequestContract(httpClient.RequestAt(2).Request,
        HttpMethod::Put,
        "https://storageaccount.blob.core.windows.net/logs/append.txt?comp=appendblock&sv=2025-01-05&sig=fakesig",
        "xy");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "x-ms-lease-id"), "lease-1");

    httpClient.Poll(); // drive the three posted (async) completions so each callback runs (T26)
}

TEST(AppendBlobClientTests, AppendBlockAsync_PreservesSpecialSasValuesWithOperationQuery)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken = "?sv=2025-01-05&sig=a%2Bb%26c%3D1";
    AppendBlobClient client{httpClient, options};

    client.AppendBlockAsync("abc", [](std::expected<Response<AppendBlockResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/logs/"
        "append.txt?comp=appendblock&sv=2025-01-05&sig=a%2Bb%26c%3D1");
    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization").empty());
}

TEST(AppendBlobClientTests, DownloadAsyncAndDownloadToAsyncCoverResponseParsingAndPathOverloads)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(
        HttpResponse{200,
            MakeCanonicalSuccessHeaders(
                {{"Content-Length", "4"}, {"Content-Type", "text/plain"}, {"x-ms-blob-type", "AppendBlob"}}),
            "data"},
        {},
        true);
    httpClient.EnqueueResponse(
        HttpResponse{200,
            MakeCanonicalSuccessHeaders({{"Content-Length", "4"}, {"x-ms-blob-type", "AppendBlob"}}),
            "more"},
        {},
        true);
    httpClient.EnqueueResponse(
        HttpResponse{200,
            MakeCanonicalSuccessHeaders({{"Content-Length", "5"}, {"x-ms-blob-type", "AppendBlob"}}),
            "bytes"},
        {},
        true);
    AppendBlobClient client{httpClient, BuildOptions()};

    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 4U};
    CallbackExpectation downloadCallback;
    DownloadBlobAndVerifyResult(client, options, downloadCallback);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "Range"), "bytes=0-3");

    std::ostringstream stream;
    CallbackExpectation streamCallback;
    DownloadBlobToStreamAndVerifyResult(client, stream, streamCallback);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(stream.str(), "more");

    const std::filesystem::path fsPath = std::filesystem::temp_directory_path() / "append-blob-download-output.txt";
    CallbackExpectation pathCallback;
    DownloadBlobToPathAndVerifyResult(client, fsPath, pathCallback);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(std::filesystem::file_size(fsPath), 5U);

    std::error_code ignored;
    std::filesystem::remove(fsPath, ignored);
}

TEST(AppendBlobClientTests, DeleteAsync_PropagatesTransportAndServiceErrors)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""},
        std::make_error_code(std::errc::connection_reset));
    httpClient.EnqueueResponse(HttpResponse{307, {{"x-ms-request-id", "append-redirect"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{500, {}, "<Error><Message>broken"});
    httpClient.EnqueueResponse(HttpResponse{409,
        {{"x-ms-error-code", "OddAppendProblem"}, {"x-ms-request-id", "append-409"}},
        "<Error><Message>Unexpected detail</Message></Error>"});
    AppendBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation transportCallback;
    DeleteBlobAndVerifyTransportError(client, transportCallback);

    CallbackExpectation redirectCallback;
    DeleteBlobAndVerifyRedirect(client, redirectCallback);

    CallbackExpectation malformedCallback;
    DeleteBlobAndVerifyMalformedXmlFallback(client, malformedCallback);

    CallbackExpectation serviceCallback;
    DeleteBlobAndVerifyUnknownCodeFallback(client, serviceCallback);

    httpClient.Poll(); // drive the four posted (async) completions so each callback runs (T26)
}

TEST(AppendBlobClientTests, DownloadToAsync_PathOpenFailureReportsIoError)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {{"Content-Length", "4"}}, "data"};
    AppendBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    client.DownloadToAsync(std::filesystem::temp_directory_path(), // a directory cannot be opened as the output file
        [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::is_a_directory));
        callback.MarkInvoked();
    });

    httpClient.Poll();
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(AppendBlobClientTests, DownloadAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), "hello"};
    AppendBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<DownloadBlobResult>, BlobStorageError>> future =
        client.DownloadAsync(AVEVA::AzureClient::DownloadBlobOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<DownloadBlobResult>, BlobStorageError> result = future.get();
    ASSERT_TRUE(result.has_value());
    const auto& content = result->Value().Content;
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(content.data()), content.size()), "hello");
}

TEST(AppendBlobClientTests, AppendBlockAsyncAcceptsUseFutureCompletionTokenAndReportsFailure)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{409,
        {{"x-ms-error-code", "OddAppendProblem"}, {"x-ms-request-id", "append-request"}},
        "<Error><Message>Unexpected detail</Message></Error>"});
    AppendBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<AppendBlockResult>, BlobStorageError>> future =
        client.AppendBlockAsync(std::span<const std::byte>{},
            AVEVA::AzureClient::AppendBlockOptions{},
            boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<AppendBlockResult>, BlobStorageError> result = future.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
    EXPECT_EQ(result.error().RequestId, "append-request");
}

TEST(AppendBlobClientTests, CreateAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    AppendBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<CreateAppendBlobResult>, BlobStorageError>> future =
        client.CreateAsync(AVEVA::AzureClient::CreateAppendBlobOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<CreateAppendBlobResult>, BlobStorageError> const result = future.get();
    EXPECT_TRUE(result.has_value());
}

// Task 14: omitting the completion token entirely yields a deferred, directly co_await-able
// operation (see BlockBlobClientTests.cpp for the detailed rationale).
TEST(AppendBlobClientTests, AppendBlockAsyncSupportsCoAwaitWithNoExplicitCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    AppendBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    std::optional<std::expected<Response<AppendBlockResult>, BlobStorageError>> result;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitInto(&result,
            [&]
    {
        return client.AppendBlockAsync(std::span<const std::byte>{content});
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
    [[maybe_unused]] void AppendBlobClientNoTokenOverloadsCompile(AppendBlobClient& client)
    {
        using AVEVA::AzureClient::AppendBlockOptions;
        using AVEVA::AzureClient::CreateAppendBlobOptions;
        using AVEVA::AzureClient::DeleteBlobOptions;
        using AVEVA::AzureClient::DownloadBlobOptions;
        using AVEVA::AzureClient::GetBlobPropertiesOptions;

        const std::vector<std::byte> bytes;
        const std::span<const std::byte> span{bytes};
        std::string const str;
        std::ostringstream stream;
        std::filesystem::path const path;

        static_cast<void>(client.CreateAsync());
        static_cast<void>(client.CreateAsync(CreateAppendBlobOptions{}));

        static_cast<void>(client.AppendBlockAsync(str));
        static_cast<void>(client.AppendBlockAsync(str, AppendBlockOptions{}));
        static_cast<void>(client.AppendBlockAsync(span));
        static_cast<void>(client.AppendBlockAsync(span, AppendBlockOptions{}));

        static_cast<void>(client.DownloadAsync());
        static_cast<void>(client.DownloadAsync(DownloadBlobOptions{}));

        static_cast<void>(client.DownloadToAsync(stream));
        static_cast<void>(client.DownloadToAsync(stream, DownloadBlobOptions{}));
        static_cast<void>(client.DownloadToAsync(path));
        static_cast<void>(client.DownloadToAsync(path, DownloadBlobOptions{}));
        AVEVA_TEST_ALLOW_DEPRECATED_BEGIN
        static_cast<void>(client.DownloadToAsync(str));
        static_cast<void>(client.DownloadToAsync(str, DownloadBlobOptions{}));
        AVEVA_TEST_ALLOW_DEPRECATED_END

        static_cast<void>(client.DeleteAsync());
        static_cast<void>(client.DeleteAsync(DeleteBlobOptions{}));

        static_cast<void>(client.GetPropertiesAsync());
        static_cast<void>(client.GetPropertiesAsync(GetBlobPropertiesOptions{}));
    }

    // T20: AppendBlobClient inherits the common blob operations from BlobClient.
    TEST(AppendBlobClientTests, InheritedSetMetadataGetPropertiesAndExistsTargetTheAppendBlob)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
        httpClient.EnqueueResponse(HttpResponse{200,
            MakeCanonicalSuccessHeaders(
                {{"Content-Length", "7"}, {"x-ms-blob-type", "AppendBlob"}, {"x-ms-meta-owner", "ops"}}),
            ""});
        httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
        httpClient.EnqueueResponse(
            HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}, {"x-ms-request-id", "exists-404"}}, ""});
        AppendBlobClient client{httpClient, BuildOptions()};

        AVEVA::AzureClient::SetBlobMetadataOptions metadataOptions;
        metadataOptions.Metadata["owner"] = "ops";
        CallbackExpectation metadataCallback;
        SetBlobMetadataAndVerifySuccess(client, metadataOptions, metadataCallback);

        CallbackExpectation propertiesCallback;
        GetBlobPropertiesAndVerifyAppendBlob(client, propertiesCallback);

        CallbackExpectation existsCallback;
        VerifyAppendBlobExists(client, existsCallback);

        CallbackExpectation missingCallback;
        VerifyMissingAppendBlob(client, missingCallback);

        ExpectRequestContract(httpClient.RequestAt(0).Request,
            HttpMethod::Put,
            "https://storageaccount.blob.core.windows.net/logs/append.txt?comp=metadata&sv=2025-01-05&sig=fakesig");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-meta-owner"), "ops");
        ExpectAppendBlobHeadRequests(httpClient);
        httpClient.Poll(); // drive the four posted (async) completions (T26)
    }

    TEST(AppendBlobClientTests, InheritedLeaseOperationsAndDeleteIfExistsUseLeaseHeaders)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease-1"}}), ""});
        httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
        httpClient.EnqueueResponse(
            HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}, {"x-ms-request-id", "delete-404"}}, ""});
        AppendBlobClient client{httpClient, BuildOptions()};

        AVEVA::AzureClient::AcquireLeaseOptions acquireOptions;
        acquireOptions.Duration = std::chrono::seconds{30};
        acquireOptions.ProposedLeaseId = "lease-1";
        std::future<std::expected<Response<AVEVA::AzureClient::Models::AcquireBlobLeaseResult>, BlobStorageError>>
            acquired = client.AcquireLeaseAsync(acquireOptions, boost::asio::use_future);

        CallbackExpectation releaseCallback;
        ReleaseLeaseAndVerifySuccess(client, releaseCallback);

        CallbackExpectation deleteCallback;
        DeleteBlobIfExistsAndVerifyExpectedMissing(client, deleteCallback);

        httpClient.Poll();
        ASSERT_EQ(acquired.wait_for(std::chrono::seconds{1}), std::future_status::ready);
        auto acquireResult = acquired.get();
        ASSERT_TRUE(acquireResult.has_value());
        EXPECT_EQ(acquireResult->Value().LeaseId, "lease-1");

        const AVEVA::HttpRequest& acquireRequest = httpClient.RequestAt(0).Request;
        ExpectRequestContract(acquireRequest,
            HttpMethod::Put,
            "https://storageaccount.blob.core.windows.net/logs/append.txt?comp=lease&sv=2025-01-05&sig=fakesig");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(acquireRequest, "x-ms-lease-action"), "acquire");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(acquireRequest, "x-ms-lease-duration"), "30");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(acquireRequest, "x-ms-proposed-lease-id"), "lease-1");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "x-ms-lease-action"), "release");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "x-ms-lease-id"), "lease-1");
        EXPECT_EQ(httpClient.RequestAt(2).Request.GetMethod(), HttpMethod::Delete);
    }

    TEST(AppendBlobClientTests, AppendBlockSendsAppendConditionsAndParsesOffsets)
    {
        FakeHttpClient httpClient;
        std::vector<AVEVA::HttpHeader> headers = MakeCanonicalSuccessHeaders();
        headers.emplace_back("x-ms-blob-append-offset", "4096");
        headers.emplace_back("x-ms-blob-committed-block-count", "7");
        httpClient.EnqueueResponse(AVEVA::HttpResponse{201, std::move(headers), ""});
        AppendBlobClient client{httpClient, MakeBlobClientOptions()};

        AVEVA::AzureClient::AppendBlockOptions options;
        options.IfAppendPositionEqual = 4096;
        options.IfMaxSizeLessThanOrEqual = 1048576;
        std::optional<AppendBlockResult> appended;
        AppendBlockAndCaptureResult(client, std::move(options), appended);
        httpClient.Poll();

        ASSERT_TRUE(appended.has_value());
        EXPECT_EQ(ValueOrFail(appended).AppendOffset, 4096U);
        EXPECT_EQ(ValueOrFail(appended).CommittedBlockCount, 7U);
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-blob-condition-appendpos"), "4096");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-blob-condition-maxsize"), "1048576");
    }

    TEST(AppendBlobClientTests, AppendBlockOmitsAppendConditionsByDefaultAndSurfacesConditionFailures)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(
            AVEVA::AzureClient::Tests::MakeAzureErrorResponse(412, "AppendPositionConditionNotMet", "", "req"));
        AppendBlobClient client{httpClient, MakeBlobClientOptions()};

        std::optional<std::error_code> error;
        client.AppendBlockAsync(std::string{"data"},
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
        httpClient.Poll();

        EXPECT_EQ(error, std::error_code{BlobStorageErrorCode::AppendPositionConditionNotMet});
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-blob-condition-appendpos"), "");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-blob-condition-maxsize"), "");
    }

    TEST(AppendBlobClientTests, SealSendsSealRequestAndParsesSealedFlag)
    {
        FakeHttpClient httpClient;
        std::vector<AVEVA::HttpHeader> headers = MakeCanonicalSuccessHeaders();
        headers.emplace_back("x-ms-blob-sealed", "true");
        httpClient.EnqueueResponse(AVEVA::HttpResponse{200, std::move(headers), ""});
        AppendBlobClient client{httpClient, MakeBlobClientOptions()};

        AVEVA::AzureClient::SealAppendBlobOptions options;
        options.IfAppendPositionEqual = 12;
        options.Conditions.IfMatch = DefaultETag;
        std::optional<bool> sealed;
        SealBlobAndCaptureState(client, std::move(options), sealed);
        httpClient.Poll();

        EXPECT_EQ(sealed, true);
        const AVEVA::HttpRequest& request = httpClient.LastRequest();
        EXPECT_EQ(request.GetMethod(), AVEVA::HttpMethod::Put);
        EXPECT_NE(request.GetUrl().find("comp=seal"), std::string::npos);
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-condition-appendpos"), "12");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "If-Match"), DefaultETag);
    }

    TEST(AppendBlobClientTests, CreateIfNotExistsSendsIfNoneMatchAndTreatsExistingBlobAsSuccess)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(AVEVA::HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});
        httpClient.EnqueueResponse(
            AVEVA::AzureClient::Tests::MakeAzureErrorResponse(409, "BlobAlreadyExists", "exists", "req-2"));
        httpClient.EnqueueResponse(
            AVEVA::AzureClient::Tests::MakeAzureErrorResponse(403, "AuthorizationFailure", "denied", "req-3"));
        AppendBlobClient client{httpClient, MakeBlobClientOptions()};

        int created = 0;
        CreateBlobIfNotExistsAndVerifyCreated(client, created);
        httpClient.Poll();
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "If-None-Match"), "*");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-blob-type"), "AppendBlob");

        CreateBlobIfNotExistsAndVerifyExisting(client, created);
        httpClient.Poll();

        std::optional<std::error_code> error;
        CreateBlobIfNotExistsAndCaptureFailure(client, error);
        httpClient.Poll();
        EXPECT_EQ(created, 2);
        EXPECT_EQ(error, std::error_code{BlobStorageErrorCode::AuthorizationFailure});
    }
} // namespace
