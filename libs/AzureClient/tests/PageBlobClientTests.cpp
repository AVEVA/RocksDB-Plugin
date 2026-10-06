#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "SuppressDeprecated.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>

#include <boost/asio/detached.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <expected>
#include <filesystem>
#include <future>
#include <limits>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::HttpMethod;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::PageBlobPageSize;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::AcquireBlobLeaseResult;
    using AVEVA::AzureClient::Models::BlobProperties;
    using AVEVA::AzureClient::Models::BlobType;
    using AVEVA::AzureClient::Models::ClearPagesResult;
    using AVEVA::AzureClient::Models::CreatePageBlobResult;
    using AVEVA::AzureClient::Models::DeleteBlobResult;
    using AVEVA::AzureClient::Models::DownloadBlobResult;
    using AVEVA::AzureClient::Models::DownloadBlobToResult;
    using AVEVA::AzureClient::Models::ReleaseBlobLeaseResult;
    using AVEVA::AzureClient::Models::ResizePageBlobResult;
    using AVEVA::AzureClient::Models::SetBlobMetadataResult;
    using AVEVA::AzureClient::Models::UploadPagesResult;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::DefaultETag;
    using AVEVA::AzureClient::Tests::ExpectRequestContract;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;

    [[nodiscard]] BlobClientOptions BuildOptions()
    {
        return MakeBlobClientOptions("disks", "disk.vhd");
    }

    [[nodiscard]] std::size_t CountHeader(const AVEVA::HttpRequest& request, std::string_view headerName)
    {
        std::size_t count = 0;
        for (const auto& header : request.GetHeaders())
        {
            if (header.GetName() == headerName)
            {
                ++count;
            }
        }
        return count;
    }

    void VerifyMalformedGetPageRangesResult(
        const std::expected<Response<AVEVA::AzureClient::Models::GetPageRangesResult>, BlobStorageError>& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
        EXPECT_EQ(result.error().Code, std::errc::bad_message);
        EXPECT_EQ(result.error().StatusCode, 200U);
        EXPECT_FALSE(result.error().Message.empty());
    }

    void VerifyMalformedDeleteServiceErrorResult(
        const std::expected<Response<DeleteBlobResult>, BlobStorageError>& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(result.error().StatusCode, 500U);
        EXPECT_TRUE(result.error().ErrorCode.empty());
        EXPECT_TRUE(result.error().Message.empty());
        EXPECT_TRUE(result.error().RequestId.empty());
    }

    void VerifyUnrecognizedDeleteServiceErrorResult(
        const std::expected<Response<DeleteBlobResult>, BlobStorageError>& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(result.error().ErrorCode, "OddProblem");
        EXPECT_EQ(result.error().Message, "Unexpected detail");
        EXPECT_EQ(result.error().RequestId, "page-409");
    }

    void StartCreateAsyncExpectingUnalignedLength(PageBlobClient& client, bool& callbackInvoked)
    {
        client.CreateAsync(100,
            [&](std::expected<Response<CreatePageBlobResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
            EXPECT_EQ(result.error().Message, "contentLength must be a multiple of 512 bytes.");
            callbackInvoked = true;
        });
    }

    void StartUploadPagesAsyncExpectingUnalignedOffset(PageBlobClient& client,
        const std::string& content,
        bool& callbackInvoked)
    {
        client.UploadPagesAsync(100,
            content,
            [&](std::expected<Response<UploadPagesResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
            EXPECT_EQ(result.error().Message, "offset must be a multiple of 512 bytes.");
            callbackInvoked = true;
        });
    }

    void StartUploadPagesAsyncExpectingZeroLengthContent(PageBlobClient& client, bool& callbackInvoked)
    {
        client.UploadPagesAsync(0,
            std::string{},
            [&](std::expected<Response<UploadPagesResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
            EXPECT_EQ(result.error().Message, "length must be greater than zero.");
            callbackInvoked = true;
        });
    }

    void StartClearPagesAsyncExpectingZeroLengthRange(PageBlobClient& client, bool& callbackInvoked)
    {
        client.ClearPagesAsync(0,
            0,
            [&](std::expected<Response<ClearPagesResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
            EXPECT_EQ(result.error().Message, "length must be greater than zero.");
            callbackInvoked = true;
        });
    }

    void StartGetPageRangesAsyncExpectingMalformedXml(PageBlobClient& client, bool& callbackInvoked)
    {
        client.GetPageRangesAsync(
            [&](std::expected<Response<AVEVA::AzureClient::Models::GetPageRangesResult>, BlobStorageError> result)
        {
            VerifyMalformedGetPageRangesResult(result);
            callbackInvoked = true;
        });
    }

    void StartDeleteAsyncExpectingTransportError(PageBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::timed_out));
            EXPECT_TRUE(result.error().RequestId.empty());
            callback.MarkInvoked();
        });
    }

    void StartDeleteAsyncExpectingRedirectServiceError(PageBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
            EXPECT_EQ(result.error().StatusCode, 307U);
            EXPECT_EQ(result.error().RequestId, "page-redirect");
            callback.MarkInvoked();
        });
    }

    void StartDeleteAsyncExpectingMalformedXmlServiceError(PageBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            VerifyMalformedDeleteServiceErrorResult(result);
            callback.MarkInvoked();
        });
    }

    void StartDeleteAsyncExpectingUnrecognizedServiceError(PageBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            VerifyUnrecognizedDeleteServiceErrorResult(result);
            callback.MarkInvoked();
        });
    }

    void StartDownloadAsyncAndVerify(PageBlobClient& client, CallbackExpectation& downloadCallback)
    {
        client.DownloadAsync([&](std::expected<Response<DownloadBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().Properties.Type, BlobType::PageBlob);
            EXPECT_EQ(result->Value().Properties.ContentLength, 3U);
            EXPECT_EQ(result->Value().Content, "abc");
            downloadCallback.MarkInvoked();
        });
    }

    void StartDownloadToStreamAsyncAndVerify(PageBlobClient& client,
        std::ostringstream& stream,
        CallbackExpectation& streamCallback)
    {
        client.DownloadToAsync(stream,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, 4U);
            streamCallback.MarkInvoked();
        });
    }

    void StartDownloadToPathAsyncAndVerify(PageBlobClient& client,
        const std::filesystem::path& path,
        const AVEVA::AzureClient::DownloadBlobOptions& options,
        CallbackExpectation& pathCallback)
    {
        client.DownloadToAsync(path,
            options,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, 5U);
            pathCallback.MarkInvoked();
        });
    }

    void StartGetPageRangesAsyncAndVerify(PageBlobClient& client,
        const AVEVA::AzureClient::GetPageRangesOptions& options,
        CallbackExpectation& callback)
    {
        client.GetPageRangesAsync(options,
            [&](std::expected<Response<AVEVA::AzureClient::Models::GetPageRangesResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            ASSERT_EQ(result->Value().PageRanges.size(), 1U);
            EXPECT_EQ(result->Value().PageRanges.at(0).End, 511U);
            callback.MarkInvoked();
        });
    }

    void StartSetMetadataAsyncAndVerify(PageBlobClient& client,
        const AVEVA::AzureClient::SetBlobMetadataOptions& options,
        CallbackExpectation& callback)
    {
        client.SetMetadataAsync(options,
            [&](std::expected<Response<SetBlobMetadataResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void StartAcquireLeaseAsyncAndVerify(PageBlobClient& client, CallbackExpectation& callback)
    {
        client.AcquireLeaseAsync([&](std::expected<Response<AcquireBlobLeaseResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().LeaseId, "lease-1");
            callback.MarkInvoked();
        });
    }

    void StartReleaseLeaseAsyncAndVerify(PageBlobClient& client,
        const AVEVA::AzureClient::ReleaseLeaseOptions& options,
        CallbackExpectation& callback)
    {
        client.ReleaseLeaseAsync(options,
            [&](std::expected<Response<ReleaseBlobLeaseResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void StartExistsAsyncAndVerify(PageBlobClient& client, CallbackExpectation& callback)
    {
        client.ExistsAsync([&](std::expected<Response<bool>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->Value());
            callback.MarkInvoked();
        });
    }

    void StartDeleteIfExistsAsyncAndVerify(PageBlobClient& client,
        const AVEVA::AzureClient::DeleteBlobOptions& options,
        CallbackExpectation& callback)
    {
        client.DeleteIfExistsAsync(options,
            [&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->Error().has_value());
            EXPECT_EQ(result->Error()->RequestId, "delete-request");
            callback.MarkInvoked();
        });
    }
} // namespace

TEST(PageBlobClientTests, CreateAsync_BuildsPutRequestWithPageBlobType)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    client.CreateAsync(2U * PageBlobPageSize,
        [&](std::expected<Response<CreatePageBlobResult>, BlobStorageError> result)
    {
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().ETag, DefaultETag);
        callback.MarkInvoked();
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
    const auto& request = httpClient.LastRequest();
    ExpectRequestContract(request,
        HttpMethod::Put,
        "https://storageaccount.blob.core.windows.net/disks/disk.vhd?sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-type"), "PageBlob");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-content-length"), "1024");
}

TEST(PageBlobClientTests, CreateAsync_ReportsUnalignedLengthViaCompletion)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    StartCreateAsyncExpectingUnalignedLength(client, callbackInvoked);

    EXPECT_FALSE(callbackInvoked);
    httpClient.Poll();
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, UploadPagesAsync_SetsRangeAndPageWriteHeaders)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize, 'x');
    client.UploadPagesAsync(PageBlobPageSize,
        content,
        [](std::expected<Response<UploadPagesResult>, BlobStorageError>) {});

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetUrl(),
        "https://storageaccount.blob.core.windows.net/disks/disk.vhd?comp=page&sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-page-write"), "update");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-range"), "bytes=512-1023");
    EXPECT_EQ(request.GetBody(), content);
}

TEST(PageBlobClientTests, UploadPagesAsync_SpanOverloadUsesSingleRequestPath)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content(PageBlobPageSize, std::byte{'x'});
    client.UploadPagesAsync(0,
        std::span<const std::byte>{content},
        [](std::expected<Response<UploadPagesResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetBody().size(), PageBlobPageSize);
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsUnalignedOffsetViaCompletion)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize, 'x');
    bool callbackInvoked = false;
    StartUploadPagesAsyncExpectingUnalignedOffset(client, content, callbackInvoked);

    EXPECT_FALSE(callbackInvoked);
    httpClient.Poll();
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsZeroLengthContentViaCompletion)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    StartUploadPagesAsyncExpectingZeroLengthContent(client, callbackInvoked);

    EXPECT_FALSE(callbackInvoked);
    httpClient.Poll();
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, ClearPagesAsync_SetsRangeAndPageWriteHeaders)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    client.ClearPagesAsync(0, PageBlobPageSize, [](std::expected<Response<ClearPagesResult>, BlobStorageError>) {});

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-page-write"), "clear");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-range"), "bytes=0-511");
}

TEST(PageBlobClientTests, ClearPagesAsync_ReportsZeroLengthRangeViaCompletion)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    StartClearPagesAsyncExpectingZeroLengthRange(client, callbackInvoked);

    EXPECT_FALSE(callbackInvoked);
    httpClient.Poll();
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, ResizeAsync_SetsContentLengthHeader)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    client.ResizeAsync(2048, [](std::expected<Response<ResizePageBlobResult>, BlobStorageError>) {});

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetUrl(),
        "https://storageaccount.blob.core.windows.net/disks/disk.vhd?comp=properties&sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-content-length"), "2048");
}

TEST(PageBlobClientTests, DeleteAsync_UsesDeleteMethod)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    client.DeleteAsync([](std::expected<Response<DeleteBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Delete);
}

TEST(PageBlobClientTests, GetPropertiesAsync_ParsesBlobType)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {{"x-ms-blob-type", "PageBlob"}, {"Content-Length", "2048"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    client.GetPropertiesAsync([](std::expected<Response<BlobProperties>, BlobStorageError> result)
    {
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().Type, BlobType::PageBlob);
        EXPECT_EQ(result->Value().ContentLength, 2048U);
    });

    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Head);
}

TEST(PageBlobClientTests, CreateAsync_MapsServiceErrorCode)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    client.CreateAsync(PageBlobPageSize,
        [&](std::expected<Response<CreatePageBlobResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobNotFound);
        callbackInvoked = true;
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_TRUE(callbackInvoked);
}

TEST(PageBlobClientTests, Constructor_ThrowsForMissingBlobName)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.BlobName.clear();

    EXPECT_THROW((PageBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(PageBlobClientTests, Constructor_ThrowsForConflictingCredentials)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.BearerToken = "bearer-token";

    EXPECT_THROW((PageBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(PageBlobClientTests, Constructor_ThrowsForInvalidSharedKeyBase64)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "bad=="};

    EXPECT_THROW((PageBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(PageBlobClientTests, CreateAsync_UsesSharedKeyAuthorizationWhenConfigured)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    PageBlobClient client{httpClient, options};

    client.CreateAsync(PageBlobPageSize, [](std::expected<Response<CreatePageBlobResult>, BlobStorageError>) {});

    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization")
            .starts_with("SharedKey storageaccount:"));
}

TEST(PageBlobClientTests, UploadPagesAsync_ReportsOverflowingRangeViaCompletion)
{
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, BuildOptions()};

    std::string const content(PageBlobPageSize * 2U, 'x');
    bool callbackInvoked = false;
    client.UploadPagesAsync(std::numeric_limits<std::uint64_t>::max() -
                                static_cast<std::uint64_t>(PageBlobPageSize - 1U),
        content,
        [&](std::expected<Response<UploadPagesResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
        callbackInvoked = true;
    });

    EXPECT_FALSE(callbackInvoked);
    httpClient.Poll();
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(PageBlobClientTests, GetPageRangesAsync_ReportsMalformedXmlAsInvalidResponse)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {}, "<PageList><PageRange><Start>0</Start>"};
    PageBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    StartGetPageRangesAsyncExpectingMalformedXml(client, callbackInvoked);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_TRUE(callbackInvoked);
}

TEST(PageBlobClientTests, DeleteAsync_PropagatesTransportErrorWithoutInspectingHttpStatus)
{
    FakeHttpClient httpClient;
    httpClient.DefaultError() = std::make_error_code(std::errc::timed_out);
    httpClient.DefaultResponse() = HttpResponse{409, {{"x-ms-error-code", "BlobNotFound"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    StartDeleteAsyncExpectingTransportError(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(PageBlobClientTests, DeleteAsync_TreatsRedirectAsServiceError)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{307, {{"x-ms-request-id", "page-redirect"}}, ""};
    PageBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    StartDeleteAsyncExpectingRedirectServiceError(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(PageBlobClientTests, DeleteAsync_FallsBackToServiceErrorForMalformedXmlAndMissingRequestId)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{500, {}, "<Error><Message>broken"};
    PageBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    StartDeleteAsyncExpectingMalformedXmlServiceError(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(PageBlobClientTests, DeleteAsync_UnrecognizedStorageCodeFallsBackToServiceErrorButPreservesDetails)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{409,
        {{"x-ms-error-code", "OddProblem"}, {"x-ms-request-id", "page-409"}},
        "<Error><Message>Unexpected detail</Message></Error>"};
    PageBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    StartDeleteAsyncExpectingUnrecognizedServiceError(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(PageBlobClientTests, DownloadAsyncAndDownloadToAsyncParseContentAndProperties)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200,
                                   MakeCanonicalSuccessHeaders({{"Content-Length", "3"},
                                       {"Content-Type", "application/octet-stream"},
                                       {"x-ms-blob-type", "PageBlob"}}),
                                   "abc"},
        {},
        true);
    httpClient.EnqueueResponse(
        HttpResponse{200,
            MakeCanonicalSuccessHeaders({{"Content-Length", "4"}, {"x-ms-blob-type", "PageBlob"}}),
            "data"},
        {},
        true);
    httpClient.EnqueueResponse(
        HttpResponse{200,
            MakeCanonicalSuccessHeaders({{"Content-Length", "5"}, {"x-ms-blob-type", "PageBlob"}}),
            "bytes"},
        {},
        true);
    PageBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation downloadCallback;
    StartDownloadAsyncAndVerify(client, downloadCallback);
    EXPECT_TRUE(httpClient.CompleteNext());

    std::ostringstream stream;
    CallbackExpectation streamCallback;
    StartDownloadToStreamAsyncAndVerify(client, stream, streamCallback);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(stream.str(), "data");

    const std::filesystem::path fsPath = std::filesystem::current_path() / "azure-client-page-download.bin";
    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 5U};
    CallbackExpectation pathCallback;
    StartDownloadToPathAsyncAndVerify(client, fsPath, options, pathCallback);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(std::filesystem::file_size(fsPath), 5U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "Range"), "bytes=0-4");

    std::error_code ignored;
    std::filesystem::remove(fsPath, ignored);
}

TEST(PageBlobClientTests, GetPageRangesFollowsNextMarkerAndMergesPages)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200,
        MakeCanonicalSuccessHeaders(),
        R"(<PageList><PageRange><Start>0</Start><End>511</End></PageRange><NextMarker>m1</NextMarker></PageList>)"});
    httpClient.EnqueueResponse(HttpResponse{200,
        MakeCanonicalSuccessHeaders(),
        R"(<PageList><PageRange><Start>1024</Start><End>1535</End></PageRange><NextMarker/></PageList>)"});
    PageBlobClient client{httpClient, BuildOptions()};

    std::size_t rangeCount = 0U;
    int completions = 0;
    client.GetPageRangesAsync(
        [&](std::expected<Response<AVEVA::AzureClient::Models::GetPageRangesResult>, BlobStorageError> result)
    {
        ASSERT_TRUE(result.has_value());
        rangeCount = result->Value().PageRanges.size();
        EXPECT_EQ(result->Value().PageRanges.at(1).Start, 1024U);
        ++completions;
    });
    httpClient.Poll();

    EXPECT_EQ(completions, 1);
    EXPECT_EQ(rangeCount, 2U);
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(httpClient.RequestAt(0).Request.GetUrl().find("marker="), std::string::npos);
    EXPECT_NE(httpClient.RequestAt(1).Request.GetUrl().find("marker=m1"), std::string::npos);
}

TEST(PageBlobClientTests, DownloadAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        MakeCanonicalSuccessHeaders({{"Content-Length", "5"}, {"x-ms-blob-type", "PageBlob"}}),
        "hello"};
    PageBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<DownloadBlobResult>, BlobStorageError>> future =
        client.DownloadAsync(AVEVA::AzureClient::DownloadBlobOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{30}), std::future_status::ready);
    std::expected<Response<DownloadBlobResult>, BlobStorageError> result = future.get();
    ASSERT_TRUE(result.has_value());
    const auto& content = result->Value().Content;
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(content.data()), content.size()), "hello");
}

TEST(PageBlobClientTests, UploadPagesAsyncAcceptsUseFutureCompletionTokenAndReportsFailure)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "BlobAlreadyExists", "conflict", "upload-pages-request"));
    PageBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content(PageBlobPageSize, std::byte{'x'});
    std::future<std::expected<Response<UploadPagesResult>, BlobStorageError>> future = client.UploadPagesAsync(0,
        std::span<const std::byte>{content},
        AVEVA::AzureClient::UploadPagesOptions{},
        boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{30}), std::future_status::ready);
    std::expected<Response<UploadPagesResult>, BlobStorageError> result = future.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobAlreadyExists);
    EXPECT_EQ(result.error().RequestId, "upload-pages-request");
}

TEST(PageBlobClientTests, ResizeAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<ResizePageBlobResult>, BlobStorageError>> future =
        client.ResizeAsync(PageBlobPageSize, AVEVA::AzureClient::ResizePageBlobOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{30}), std::future_status::ready);
    std::expected<Response<ResizePageBlobResult>, BlobStorageError> const result = future.get();
    EXPECT_TRUE(result.has_value());
}

// Task 14: omitting the completion token entirely yields a deferred, directly co_await-able
// operation (see BlockBlobClientTests.cpp for the detailed rationale).
TEST(PageBlobClientTests, UploadPagesAsyncSupportsCoAwaitWithNoExplicitCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    PageBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content(PageBlobPageSize, std::byte{'x'});
    std::optional<std::expected<Response<UploadPagesResult>, BlobStorageError>> result;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitInto(&result,
            [&]
    {
        return client.UploadPagesAsync(0, std::span<const std::byte>{content});
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
    [[maybe_unused]] void PageBlobClientNoTokenOverloadsCompile(PageBlobClient& client)
    {
        using AVEVA::AzureClient::AcquireLeaseOptions;
        using AVEVA::AzureClient::ClearPagesOptions;
        using AVEVA::AzureClient::CreatePageBlobOptions;
        using AVEVA::AzureClient::DeleteBlobOptions;
        using AVEVA::AzureClient::DownloadBlobOptions;
        using AVEVA::AzureClient::GetBlobPropertiesOptions;
        using AVEVA::AzureClient::GetPageRangesOptions;
        using AVEVA::AzureClient::UploadPagesOptions;

        const std::vector<std::byte> bytes;
        const std::span<const std::byte> span{bytes};
        std::string const str;
        std::ostringstream stream;
        std::filesystem::path const path;

        static_cast<void>(client.CreateAsync(PageBlobPageSize));
        static_cast<void>(client.CreateAsync(PageBlobPageSize, CreatePageBlobOptions{}));

        static_cast<void>(client.UploadPagesAsync(0, str));
        static_cast<void>(client.UploadPagesAsync(0, str, UploadPagesOptions{}));
        static_cast<void>(client.UploadPagesAsync(0, span));
        static_cast<void>(client.UploadPagesAsync(0, span, UploadPagesOptions{}));

        static_cast<void>(client.ClearPagesAsync(0, PageBlobPageSize));
        static_cast<void>(client.ClearPagesAsync(0, PageBlobPageSize, ClearPagesOptions{}));

        static_cast<void>(client.ResizeAsync(PageBlobPageSize));
        static_cast<void>(client.ResizeAsync(PageBlobPageSize, AVEVA::AzureClient::ResizePageBlobOptions{}));

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

        static_cast<void>(client.GetPageRangesAsync());
        static_cast<void>(client.GetPageRangesAsync(GetPageRangesOptions{}));

        static_cast<void>(client.DeleteAsync());
        static_cast<void>(client.DeleteAsync(DeleteBlobOptions{}));

        static_cast<void>(client.GetPropertiesAsync());
        static_cast<void>(client.GetPropertiesAsync(GetBlobPropertiesOptions{}));



        static_cast<void>(client.AcquireLeaseAsync());
        static_cast<void>(client.AcquireLeaseAsync(AcquireLeaseOptions{}));


        static_cast<void>(client.ExistsAsync());
        static_cast<void>(client.DeleteIfExistsAsync());
        static_cast<void>(client.DeleteIfExistsAsync(AVEVA::AzureClient::DeleteBlobOptions{}));
    }
} // namespace
