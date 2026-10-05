#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "BlobRequestHelpers.hpp"
#include "FakeHttpClient.hpp"
#include "SuppressDeprecated.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <cstddef>
#include <gtest/gtest.h>

#include <boost/asio/detached.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <future>
#include <ios>
#include <istream>
#include <limits>
#include <optional>
#include <regex>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::HttpMethod;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::AcquireBlobLeaseResult;
    using AVEVA::AzureClient::Models::BlobProperties;
    using AVEVA::AzureClient::Models::BlobType;
    using AVEVA::AzureClient::Models::BreakBlobLeaseResult;
    using AVEVA::AzureClient::Models::CommitBlockListResult;
    using AVEVA::AzureClient::Models::CreateBlobSnapshotResult;
    using AVEVA::AzureClient::Models::DeleteBlobResult;
    using AVEVA::AzureClient::Models::DownloadBlobResult;
    using AVEVA::AzureClient::Models::DownloadBlobToResult;
    using AVEVA::AzureClient::Models::ReleaseBlobLeaseResult;
    using AVEVA::AzureClient::Models::SetBlobAccessTierResult;
    using AVEVA::AzureClient::Models::SetBlobHttpHeadersResult;
    using AVEVA::AzureClient::Models::SetBlobMetadataResult;
    using AVEVA::AzureClient::Models::StageBlockResult;
    using AVEVA::AzureClient::Models::StartBlobCopyFromUriResult;
    using AVEVA::AzureClient::Models::UploadBlockBlobResult;
    using AVEVA::AzureClient::Private::ParseHttpDateHeader;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::DefaultETag;
    using AVEVA::AzureClient::Tests::ExpectRequestContract;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeAzureErrorResponse;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;

    [[nodiscard]] BlobClientOptions BuildOptions()
    {
        return MakeBlobClientOptions();
    }

    void ExpectUuidV4(std::string_view value)
    {
        EXPECT_TRUE(std::regex_match(std::string{value},
            std::regex{"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"}));
    }

    void ExpectBearerAuthorization(std::string_view value, std::string_view token)
    {
        EXPECT_FALSE(value.empty());
        EXPECT_EQ(value, "Bearer " + std::string(token));
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

    void AssertBlobPropertiesResultHasValue(const std::expected<Response<BlobProperties>, BlobStorageError>& result)
    {
        ASSERT_TRUE(result.has_value());
    }

    void VerifyBlobPropertiesEdgeCasesValue(const Response<BlobProperties>& response)
    {
        EXPECT_EQ(response.Value().Type, BlobType::Unknown);
        EXPECT_EQ(response.Value().ContentLength, 0U);
        EXPECT_EQ(response.Value().ContentType, "image/png");
        EXPECT_EQ(response.Value().Metadata.at("project"), "aveva");
        EXPECT_EQ(response.Value().Metadata.at("owner"), "storage");
        EXPECT_NE(response.Value().LastModified, std::chrono::system_clock::time_point{});
    }

    void VerifyDeleteMissingStorageCodeServiceErrorResult(
        const std::expected<Response<DeleteBlobResult>, BlobStorageError>& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(result.error().StatusCode, 500U);
        EXPECT_TRUE(result.error().ErrorCode.empty());
        EXPECT_EQ(result.error().Message, "boom");
        EXPECT_EQ(result.error().RequestId, "req-500");
    }

    void VerifyDeleteUnrecognizedServiceErrorResult(
        const std::expected<Response<DeleteBlobResult>, BlobStorageError>& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(result.error().ErrorCode, "WeirdProblem");
        EXPECT_EQ(result.error().Message, "Unexpected detail");
        EXPECT_EQ(result.error().RequestId, "req-409");
    }

    void VerifyMalformedBlockListResult(
        const std::expected<Response<AVEVA::AzureClient::Models::GetBlockListResult>, BlobStorageError>& result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
        EXPECT_EQ(result.error().Code, std::errc::bad_message);
        EXPECT_EQ(result.error().StatusCode, 200U);
        EXPECT_FALSE(result.error().Message.empty());
    }

    void AssertBlockListResultHasValue(
        const std::expected<Response<AVEVA::AzureClient::Models::GetBlockListResult>, BlobStorageError>& result)
    {
        ASSERT_TRUE(result.has_value());
    }

    void VerifyCommittedBlock(const AVEVA::AzureClient::Models::GetBlockListResult& result)
    {
        ASSERT_EQ(result.CommittedBlocks.size(), 1U);
        EXPECT_EQ(result.CommittedBlocks.at(0).Name, "alpha");
        EXPECT_EQ(result.CommittedBlocks.at(0).Size, 4U);
    }

    void VerifyUncommittedBlock(const AVEVA::AzureClient::Models::GetBlockListResult& result)
    {
        ASSERT_EQ(result.UncommittedBlocks.size(), 1U);
        EXPECT_EQ(result.UncommittedBlocks.at(0).Name, "beta");
        EXPECT_EQ(result.UncommittedBlocks.at(0).Size, 2U);
    }

    void VerifyDefaultDownloadResult(const std::expected<Response<DownloadBlobResult>, BlobStorageError>& result)
    {
        ASSERT_TRUE(result.has_value());
        const Response<DownloadBlobResult>& response = *result;
        EXPECT_EQ(response.Value().Properties.Type, BlobType::BlockBlob);
        EXPECT_EQ(response.Value().Properties.ContentLength, 4U);
        EXPECT_EQ(response.Value().Properties.ContentType, "text/plain");
        EXPECT_EQ(response.Value().Content, "data");
    }

    void StartCommitBlockListAsyncAndVerify(BlockBlobClient& client,
        const std::vector<std::string>& blockIds,
        bool& callbackInvoked)
    {
        client.CommitBlockListAsync(blockIds,
            [&](std::expected<Response<CommitBlockListResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<CommitBlockListResult>& response = *result;
            EXPECT_EQ(response.Value().ETag, "\"0x8D1234\"");
            callbackInvoked = true;
        });
    }

    void StartUploadFromAsyncAndVerify(BlockBlobClient& client,
        std::istream& stream,
        const AVEVA::AzureClient::UploadFromOptions& options,
        bool& callbackInvoked)
    {
        client.UploadFromAsync(stream,
            options,
            [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            callbackInvoked = true;
        });
    }

    void StartGetPropertiesAsyncAndVerifyEdgeCases(BlockBlobClient& client)
    {
        client.GetPropertiesAsync([](std::expected<Response<BlobProperties>, BlobStorageError> result)
        {
            AssertBlobPropertiesResultHasValue(result);
            VerifyBlobPropertiesEdgeCasesValue(*result);
        });
    }

    void StartDeleteAsyncExpectingMissingStorageCodeServiceError(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            VerifyDeleteMissingStorageCodeServiceErrorResult(result);
            callback.MarkInvoked();
        });
    }

    void StartDeleteAsyncExpectingUnrecognizedServiceError(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            VerifyDeleteUnrecognizedServiceErrorResult(result);
            callback.MarkInvoked();
        });
    }

    void StartGetBlockListAsyncExpectingMalformedXml(BlockBlobClient& client, bool& callbackInvoked)
    {
        client.GetBlockListAsync(
            [&](std::expected<Response<AVEVA::AzureClient::Models::GetBlockListResult>, BlobStorageError> result)
        {
            VerifyMalformedBlockListResult(result);
            callbackInvoked = true;
        });
    }

    void StartGetBlockListAsyncAndVerifyParsedBlocks(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.GetBlockListAsync(
            [&](std::expected<Response<AVEVA::AzureClient::Models::GetBlockListResult>, BlobStorageError> result)
        {
            AssertBlockListResultHasValue(result);
            VerifyCommittedBlock(result->Value());
            VerifyUncommittedBlock(result->Value());
            callback.MarkInvoked();
        });
    }

    void StartDownloadAsyncAndVerifyDefaultContent(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.DownloadAsync([&](std::expected<Response<DownloadBlobResult>, BlobStorageError> result)
        {
            VerifyDefaultDownloadResult(result);
            callback.MarkInvoked();
        });
    }

    void StartDownloadAsyncAndVerifyRangedContent(BlockBlobClient& client,
        const AVEVA::AzureClient::DownloadBlobOptions& options,
        CallbackExpectation& callback)
    {
        client.DownloadAsync(options,
            [&](std::expected<Response<DownloadBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<DownloadBlobResult>& response = *result;
            EXPECT_EQ(response.Value().Properties.ContentLength, 2U);
            callback.MarkInvoked();
        });
    }

    void StartDownloadToStreamAsyncAndVerify(BlockBlobClient& client,
        std::ostringstream& stream,
        CallbackExpectation& callback)
    {
        client.DownloadToAsync(stream,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<DownloadBlobToResult>& response = *result;
            EXPECT_EQ(response.Value().BytesWritten, 4U);
            callback.MarkInvoked();
        });
    }

    AVEVA_TEST_ALLOW_DEPRECATED_BEGIN
    void StartDownloadToStringPathAsyncAndVerify(BlockBlobClient& client,
        const std::string& path,
        CallbackExpectation& callback)
    {
        client.DownloadToAsync(path,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<DownloadBlobToResult>& response = *result;
            EXPECT_EQ(response.Value().BytesWritten, 4U);
            callback.MarkInvoked();
        });
    }

    AVEVA_TEST_ALLOW_DEPRECATED_END

    void StartDownloadToFilesystemPathAsyncAndVerify(BlockBlobClient& client,
        const std::filesystem::path& path,
        CallbackExpectation& callback)
    {
        client.DownloadToAsync(path,
            [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<DownloadBlobToResult>& response = *result;
            EXPECT_EQ(response.Value().BytesWritten, 5U);
            callback.MarkInvoked();
        });
    }

    void StartSetMetadataAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::SetBlobMetadataOptions& options,
        CallbackExpectation& callback)
    {
        client.SetMetadataAsync(options,
            [&](std::expected<Response<SetBlobMetadataResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<SetBlobMetadataResult>& response = *result;
            EXPECT_EQ(response.Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void StartSetHttpHeadersAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::SetBlobHttpHeadersOptions& options,
        CallbackExpectation& callback)
    {
        client.SetHttpHeadersAsync(options,
            [&](std::expected<Response<SetBlobHttpHeadersResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<SetBlobHttpHeadersResult>& response = *result;
            EXPECT_EQ(response.Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void StartSetAccessTierAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::SetBlobAccessTierOptions& options,
        CallbackExpectation& callback)
    {
        client.SetAccessTierAsync(options,
            [&](std::expected<Response<SetBlobAccessTierResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<SetBlobAccessTierResult>& response = *result;
            EXPECT_EQ(response.Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void StartCopyFromUriAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::StartCopyFromUriOptions& options,
        CallbackExpectation& callback)
    {
        client.StartCopyFromUriAsync("https://source.example.com/container/blob.txt?sig=x",
            options,
            [&](std::expected<Response<StartBlobCopyFromUriResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<StartBlobCopyFromUriResult>& response = *result;
            EXPECT_EQ(response.Value().CopyId, "copy-1");
            EXPECT_EQ(response.Value().CopyStatus, "success");
            callback.MarkInvoked();
        });
    }

    void StartSnapshotAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::SnapshotBlobOptions& options,
        CallbackExpectation& callback)
    {
        client.SnapshotAsync(options,
            [&](std::expected<Response<CreateBlobSnapshotResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<CreateBlobSnapshotResult>& response = *result;
            EXPECT_EQ(response.Value().Snapshot, "snapshot-1");
            callback.MarkInvoked();
        });
    }

    void StartAcquireLeaseAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::AcquireLeaseOptions& options,
        CallbackExpectation& callback)
    {
        client.AcquireLeaseAsync(options,
            [&](std::expected<Response<AcquireBlobLeaseResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<AcquireBlobLeaseResult>& response = *result;
            EXPECT_EQ(response.Value().LeaseId, "lease-1");
            callback.MarkInvoked();
        });
    }

    void StartReleaseLeaseAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::ReleaseLeaseOptions& options,
        CallbackExpectation& callback)
    {
        client.ReleaseLeaseAsync(options,
            [&](std::expected<Response<ReleaseBlobLeaseResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<ReleaseBlobLeaseResult>& response = *result;
            EXPECT_EQ(response.Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void StartBreakLeaseAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::BreakLeaseOptions& options,
        CallbackExpectation& callback)
    {
        client.BreakLeaseAsync(options,
            [&](std::expected<Response<BreakBlobLeaseResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<BreakBlobLeaseResult>& response = *result;
            ASSERT_TRUE(response.Value().LeaseTimeSeconds.has_value());
            EXPECT_EQ(*response.Value().LeaseTimeSeconds, 15);
            callback.MarkInvoked();
        });
    }

    void StartExistsAsyncAndVerifyTrue(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.ExistsAsync([&](std::expected<Response<bool>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<bool>& response = *result;
            EXPECT_TRUE(response.Value());
            callback.MarkInvoked();
        });
    }

    void StartExistsAsyncAndVerifyFalse(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.ExistsAsync([&](std::expected<Response<bool>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            EXPECT_FALSE(result->Value());
            ASSERT_TRUE(result->Error().has_value());
            EXPECT_EQ(result->Error()->RequestId, "missing-request");
            callback.MarkInvoked();
        });
    }

    void StartCreateIfNotExistsAsyncAndVerify(BlockBlobClient& client,
        const AVEVA::AzureClient::UploadBlockBlobOptions& options,
        CallbackExpectation& callback)
    {
        client.CreateIfNotExistsAsync(options,
            [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->Error().has_value());
            EXPECT_EQ(result->Error()->RequestId, "exists-request");
            EXPECT_TRUE(result->Value().ETag.empty());
            callback.MarkInvoked();
        });
    }

    void StartDeleteIfExistsAsyncAndVerify(BlockBlobClient& client,
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

    void StartUploadFromPathAsyncAndVerify(BlockBlobClient& client,
        const std::filesystem::path& path,
        const AVEVA::AzureClient::UploadFromOptions& options,
        CallbackExpectation& callback)
    {
        client.UploadFromAsync(path,
            options,
            [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            const Response<UploadBlockBlobResult>& response = *result;
            EXPECT_EQ(response.Value().ETag, DefaultETag);
            callback.MarkInvoked();
        });
    }

    void StartUploadAsyncWithTransactionalHashes(BlockBlobClient& client,
        const AVEVA::AzureClient::UploadBlockBlobOptions& options)
    {
        client.UploadAsync(std::string{"data"},
            options,
            [](auto result)
        {
            ASSERT_TRUE(result.has_value());
        });
    }

    void StartStageBlockAsyncWithTransactionalHashes(BlockBlobClient& client,
        const AVEVA::AzureClient::StageBlockOptions& options,
        std::optional<std::string>& md5)
    {
        client.StageBlockAsync(AVEVA::AzureClient::Models::EncodeBlockId(0),
            std::string{"data"},
            options,
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            md5 = result->Value().ContentMd5;
        });
    }

    void StartUploadAsyncWithoutTransactionalHashes(BlockBlobClient& client)
    {
        client.UploadAsync(std::string{"data"}, [](auto) {});
    }

    void StartStageBlockFromUriAsyncAndVerify(BlockBlobClient& client,
        const std::string& blockId,
        const AVEVA::AzureClient::StageBlockFromUriOptions& options,
        bool& staged)
    {
        client.StageBlockFromUriAsync(blockId,
            "https://src.example.com/c/b?sig=x",
            options,
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            staged = true;
        });
    }

    void StartStageBlockFromUriAsyncWithoutLength(BlockBlobClient& client,
        const std::string& blockId,
        const AVEVA::AzureClient::StageBlockFromUriOptions& options)
    {
        client.StageBlockFromUriAsync(blockId, "https://src.example.com/c/b", options, [](auto) {});
    }

    void StartStageBlockFromUriAsyncWithoutRange(BlockBlobClient& client, const std::string& blockId)
    {
        client.StageBlockFromUriAsync(blockId, "https://src.example.com/c/b", [](auto) {});
    }

    void StartStageBlockFromUriAsyncExpectingInvalidArgument(BlockBlobClient& client,
        const std::string& blockId,
        const std::string& sourceUri,
        int& rejected)
    {
        client.StageBlockFromUriAsync(blockId,
            sourceUri,
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
            ++rejected;
        });
    }

    void StartStageBlockFromUriAsyncExpectingInvalidArgument(BlockBlobClient& client,
        const std::string& blockId,
        const std::string& sourceUri,
        const AVEVA::AzureClient::StageBlockFromUriOptions& options,
        int& rejected)
    {
        client.StageBlockFromUriAsync(blockId,
            sourceUri,
            options,
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
            ++rejected;
        });
    }

    void StartCopyFromUriAsyncAndCaptureResult(BlockBlobClient& client,
        const AVEVA::AzureClient::CopyFromUriOptions& options,
        std::optional<AVEVA::AzureClient::Models::CopyBlobFromUriResult>& copied)
    {
        client.CopyFromUriAsync("https://src.example.com/c/b?sig=x",
            options,
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            copied = result->Value();
        });
    }

    void StartAbortCopyFromUriAsyncAndVerify(BlockBlobClient& client, bool& aborted)
    {
        client.AbortCopyFromUriAsync("copy id/1",
            AVEVA::AzureClient::AbortCopyFromUriOptions{.LeaseId = "lease-1"},
            [&](auto result)
        {
            ASSERT_TRUE(result.has_value());
            aborted = true;
        });
    }

    void StartAbortCopyFromUriAsyncExpectingInvalidArgument(BlockBlobClient& client,
        std::optional<std::error_code>& error)
    {
        client.AbortCopyFromUriAsync("",
            [&](auto result)
        {
            ASSERT_FALSE(result.has_value());
            error = result.error().Code;
        });
    }
} // namespace

TEST(BlockBlobClientTests, UploadAsync_BuildsPutRequestWithBlockBlobType)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    client.UploadAsync("hello world",
        [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
    {
        ASSERT_TRUE(result.has_value());
        const Response<UploadBlockBlobResult>& response = *result;
        EXPECT_EQ(response.Value().ETag, DefaultETag);
        callback.MarkInvoked();
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    const auto& request = httpClient.LastRequest();
    ExpectRequestContract(request,
        HttpMethod::Put,
        "https://storageaccount.blob.core.windows.net/images/photo.png?sv=2025-01-05&sig=fakesig",
        "hello world");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-type"), "BlockBlob");
}

TEST(BlockBlobClientTests, UploadAsync_UsesBearerAuthorizationAndRequiredHeaders)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.ServiceEndpoint = "https://storageaccount.blob.core.windows.net";
    options.SasToken.clear();
    options.BearerToken = "bearer-token";
    BlockBlobClient client{httpClient, options};

    client.UploadAsync("hello", [](std::expected<Response<UploadBlockBlobResult>, BlobStorageError>) {});

    // BearerToken/TokenCredential completions that finish on the same stack are deferred
    // via post() (never completing before the initiating call returns); poll the fake
    // client's executor to observe them.
    httpClient.Poll();

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-version"), "2023-11-03");
    ExpectUuidV4(FakeHttpClient::FindHeaderValue(request, "x-ms-client-request-id"));

    const std::string dateHeader = FakeHttpClient::FindHeaderValue(request, "x-ms-date");
    EXPECT_TRUE(std::regex_match(dateHeader,
        std::regex{"^[A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT$"}));
    EXPECT_TRUE(ParseHttpDateHeader(dateHeader).has_value());

    ExpectBearerAuthorization(FakeHttpClient::FindHeaderValue(request, "Authorization"), "bearer-token");
}

TEST(BlockBlobClientTests, UploadAsync_SpanOverloadUsesSingleRequestPath)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    client.UploadAsync(std::span<const std::byte>{content},
        [](std::expected<Response<UploadBlockBlobResult>, BlobStorageError>) {});

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.LastRequest().GetBody(), "abc");
}

TEST(BlockBlobClientTests, StageBlockAsync_EncodesBlockIdInQuery)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    client.StageBlockAsync("AAAAAA==", "chunk-data", [](std::expected<Response<StageBlockResult>, BlobStorageError>) {
    });

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetMethod(), HttpMethod::Put);
    EXPECT_EQ(request.GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/"
        "photo.png?comp=block&blockid=AAAAAA%3D%3D&sv=2025-01-05&sig=fakesig");
    EXPECT_EQ(request.GetBody(), "chunk-data");
}

TEST(BlockBlobClientTests, StageBlockAsync_EncodesPlusSlashAndEqualsInBlockIdQueryAndPreservesSasToken)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken = "?sv=2025-01-05&sig=a%2Bb%26c%3D1";
    BlockBlobClient client{httpClient, options};

    client.StageBlockAsync("++//AA==", "chunk-data", [](std::expected<Response<StageBlockResult>, BlobStorageError>) {
    });

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.LastRequest().GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/"
        "photo.png?comp=block&blockid=%2B%2B%2F%2FAA%3D%3D&sv=2025-01-05&sig=a%2Bb%26c%3D1");
    EXPECT_TRUE(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Authorization").empty());
}

TEST(BlockBlobClientTests, StageBlockAsync_ReportsInvalidBase64BlockIdViaCompletion)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    client.StageBlockAsync("not-base64!",
        "data",
        [&](std::expected<Response<StageBlockResult>, BlobStorageError> result)
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

TEST(BlockBlobClientTests, CommitBlockListAsync_BuildsXmlBody)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, {{"ETag", "\"0x8D1234\""}}, ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::string> blockIds{"YmxvY2swMQ==", "YmxvY2swMg=="};

    bool callbackInvoked = false;
    StartCommitBlockListAsyncAndVerify(client, blockIds, callbackInvoked);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_TRUE(callbackInvoked);
    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(request.GetUrl(),
        "https://storageaccount.blob.core.windows.net/images/photo.png?comp=blocklist&sv=2025-01-05&sig=fakesig");
    EXPECT_NE(request.GetBody().find("<Latest>YmxvY2swMQ==</Latest>"), std::string::npos);
    EXPECT_NE(request.GetBody().find("<Latest>YmxvY2swMg==</Latest>"), std::string::npos);
}

TEST(BlockBlobClientTests, CommitBlockListAsync_AllowsEmptyBlockLists)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    client.CommitBlockListAsync({}, [](std::expected<Response<CommitBlockListResult>, BlobStorageError>) {});

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.LastRequest().GetBody(), "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList></BlockList>");
}

TEST(BlockBlobClientTests, UploadFromAsync_StagesStreamBlockByBlockBeforeCommit)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    std::istringstream stream("ABCDEFGHI");
    AVEVA::AzureClient::UploadFromOptions options;
    options.BlockSize = 4U;

    bool callbackInvoked = false;
    StartUploadFromAsyncAndVerify(client, stream, options, callbackInvoked);

    httpClient.Poll(); // drive the posted (async) chained stage/commit completions (T26)
    ASSERT_TRUE(callbackInvoked);
    ASSERT_EQ(httpClient.RequestCount(), 4U);
    EXPECT_EQ(httpClient.Requests().at(0).Body, "ABCD");
    EXPECT_EQ(httpClient.Requests().at(1).Body, "EFGH");
    EXPECT_EQ(httpClient.Requests().at(2).Body, "I");
    EXPECT_NE(httpClient.Requests().at(3).Request.GetUrl().find("comp=blocklist"), std::string::npos);
}

TEST(BlockBlobClientTests, UploadFromAsync_PathOpenFailureReportsNoSuchFile)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    client.UploadFromAsync((std::filesystem::temp_directory_path() / "azc-does-not-exist.bin").string(),
        [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::no_such_file_or_directory));
        callbackInvoked = true;
    });

    EXPECT_FALSE(callbackInvoked);
    httpClient.Poll();
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(BlockBlobClientTests, CommitBlockListAsync_ReportsDifferentBlockIdLengthsViaCompletion)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    client.CommitBlockListAsync({"YQ==", "YmxvY2swMQ=="},
        [&](std::expected<Response<CommitBlockListResult>, BlobStorageError> result)
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

TEST(BlockBlobClientTests, DeleteAsync_UsesDeleteMethod)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    client.DeleteAsync([](std::expected<Response<DeleteBlobResult>, BlobStorageError>) {});

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Delete);
}

TEST(BlockBlobClientTests, SetHttpHeadersAsync_SendsEmptyHeadersForProperties)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    client.SetHttpHeadersAsync({},
        [](std::expected<Response<AVEVA::AzureClient::Models::SetBlobHttpHeadersResult>, BlobStorageError>) {});

    const auto& request = httpClient.LastRequest();
    EXPECT_EQ(CountHeader(request, "x-ms-blob-content-type"), 1U);
    EXPECT_EQ(CountHeader(request, "x-ms-blob-content-md5"), 1U);
    EXPECT_EQ(CountHeader(request, "x-ms-blob-cache-control"), 1U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-content-type"), "");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-content-md5"), "");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-blob-cache-control"), "");
}

TEST(BlockBlobClientTests, GetPropertiesAsync_ParsesBlobPropertiesAcrossEdgeCases)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        {
            {"Last-Modified", "Fri, 26 Jun 2015 18:59:17 GMT"},
            {"x-ms-blob-type", "SomethingUnexpected"},
            {"Content-Type", "image/png"},
            {"x-ms-meta-Project", "aveva"},
            {"X-Ms-MeTa-OWNER", "storage"},
        },
        ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    StartGetPropertiesAsyncAndVerifyEdgeCases(client);

    EXPECT_EQ(httpClient.LastRequest().GetMethod(), HttpMethod::Head);
}

TEST(BlockBlobClientTests, UploadAsync_MapsBlobNotFoundErrorWithoutParsingResult)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}, {"ETag", "\"should-not-parse\""}}, ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    client.UploadAsync("data",
        [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobNotFound);
        callbackInvoked = true;
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_TRUE(callbackInvoked);
}

TEST(BlockBlobClientTests, UploadAsync_SharedBufferIsSentWithoutCopyAndNullIsRejected)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    auto buffer =
        std::make_shared<const std::vector<std::byte>>(std::vector<std::byte>{std::byte{'a'}, std::byte{'b'}});
    bool succeeded = false;
    client.UploadAsync(buffer,
        AVEVA::AzureClient::UploadBlockBlobOptions{},
        [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
    {
        succeeded = result.has_value();
    });
    httpClient.Poll();
    EXPECT_TRUE(succeeded);
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(FakeHttpClient::BodyAsString(httpClient.LastRequest()), "ab");

    bool rejected = false;
    client.UploadAsync(std::shared_ptr<const std::vector<std::byte>>{},
        AVEVA::AzureClient::UploadBlockBlobOptions{},
        [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
        rejected = true;
    });
    httpClient.Poll();
    EXPECT_TRUE(rejected);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(BlockBlobClientTests, DeleteAsync_PropagatesTransportErrorWithoutInspectingHttpStatus)
{
    FakeHttpClient httpClient;
    httpClient.DefaultError() = std::make_error_code(std::errc::connection_reset);
    httpClient.DefaultResponse() = HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::connection_reset));
        callback.MarkInvoked();
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlockBlobClientTests, DeleteAsync_TreatsRedirectAsServiceError)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{307, {{"x-ms-request-id", "redirect-1"}}, ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    client.DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::ServiceError);
        EXPECT_EQ(result.error().StatusCode, 307U);
        EXPECT_EQ(result.error().RequestId, "redirect-1");
        callback.MarkInvoked();
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlockBlobClientTests, DeleteAsync_FallsBackToServiceErrorWhenNoStorageCodeIsPresent)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() =
        HttpResponse{500, {{"x-ms-request-id", "req-500"}}, "<Error><Message>boom</Message></Error>"};
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    StartDeleteAsyncExpectingMissingStorageCodeServiceError(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlockBlobClientTests, DeleteAsync_UnrecognizedStorageCodeFallsBackToServiceErrorButPreservesDetails)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{409,
        {{"x-ms-error-code", "WeirdProblem"}, {"x-ms-request-id", "req-409"}},
        "<Error><Message>Unexpected detail</Message></Error>"};
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    StartDeleteAsyncExpectingUnrecognizedServiceError(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
}

TEST(BlockBlobClientTests, GetBlockListAsync_ReportsMalformedXmlAsInvalidResponse)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {}, "<BlockList><CommittedBlocks>"};
    BlockBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    StartGetBlockListAsyncExpectingMalformedXml(client, callbackInvoked);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_TRUE(callbackInvoked);
}

TEST(BlockBlobClientTests, StartCopyFromUriAsync_SetsCopySourceHeaderExactly)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    const std::string sourceUri = "https://source.example.com/container/blob.txt?sv=1&sig=a%2Bb&marker=copy+me";
    client.StartCopyFromUriAsync(sourceUri,
        [](std::expected<Response<AVEVA::AzureClient::Models::StartBlobCopyFromUriResult>, BlobStorageError>) {});

    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-copy-source"), sourceUri);
}

TEST(BlockBlobClientTests, DownloadToAsync_PathOpenFailureReportsIoError)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, {{"Content-Length", "4"}}, "data"};
    BlockBlobClient client{httpClient, BuildOptions()};

    bool callbackInvoked = false;
    client.DownloadToAsync(std::filesystem::temp_directory_path(), // a directory cannot be opened as the output file
        [&](std::expected<Response<AVEVA::AzureClient::Models::DownloadBlobToResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::is_a_directory));
        callbackInvoked = true;
    });

    httpClient.Poll(); // early failures are posted, never invoked inline (T26)
    EXPECT_TRUE(callbackInvoked);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(BlockBlobClientTests, Constructor_ThrowsForMissingBlobName)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.BlobName.clear();

    EXPECT_THROW((BlockBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(BlockBlobClientTests, Constructor_ThrowsForConflictingCredentials)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.BearerToken = "bearer-token";

    EXPECT_THROW((BlockBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(BlockBlobClientTests, Constructor_ThrowsForInvalidSharedKeyBase64)
{
    FakeHttpClient httpClient;
    BlobClientOptions options = BuildOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "bad=="};

    EXPECT_THROW((BlockBlobClient{httpClient, options}), std::invalid_argument);
}

TEST(BlockBlobClientTests, BuildRequest_GeneratesUuidV4ClientRequestId)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};

    client.DeleteAsync([](std::expected<Response<DeleteBlobResult>, BlobStorageError>) {});

    ExpectUuidV4(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-client-request-id"));
}

TEST(BlockBlobClientTests, BuildRequest_NormalizesEndpointsWithAndWithoutTrailingSlash)
{
    FakeHttpClient withSlashHttpClient;
    BlockBlobClient withSlashClient{withSlashHttpClient, BuildOptions()};
    withSlashClient.DeleteAsync([](std::expected<Response<DeleteBlobResult>, BlobStorageError>) {});

    FakeHttpClient withoutSlashHttpClient;
    BlobClientOptions optionsWithoutSlash = BuildOptions();
    optionsWithoutSlash.ServiceEndpoint = "https://storageaccount.blob.core.windows.net";
    BlockBlobClient withoutSlashClient{withoutSlashHttpClient, optionsWithoutSlash};
    withoutSlashClient.DeleteAsync([](std::expected<Response<DeleteBlobResult>, BlobStorageError>) {});

    EXPECT_EQ(withSlashHttpClient.LastRequest().GetUrl(), withoutSlashHttpClient.LastRequest().GetUrl());
}

TEST(BlockBlobClientTests, GetBlockListAsync_ParsesCommittedAndUncommittedBlocks)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200,
        MakeCanonicalSuccessHeaders(),
        R"(<?xml version="1.0" encoding="utf-8"?>
            <BlockList>
              <CommittedBlocks><Block><Name>alpha</Name><Size>4</Size></Block></CommittedBlocks>
              <UncommittedBlocks><Block><Name>beta</Name><Size>2</Size></Block></UncommittedBlocks>
            </BlockList>)"};
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation callback;
    StartGetBlockListAsyncAndVerifyParsedBlocks(client, callback);

    httpClient.Poll(); // drive the posted (async) completion (T26)
    ExpectRequestContract(httpClient.LastRequest(),
        HttpMethod::Get,
        "https://storageaccount.blob.core.windows.net/images/"
        "photo.png?comp=blocklist&blocklisttype=all&sv=2025-01-05&sig=fakesig");
}

TEST(BlockBlobClientTests, DownloadAsync_DefaultAndOptionsOverloadsParseContent)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200,
        MakeCanonicalSuccessHeaders(
            {{"Content-Length", "4"}, {"Content-Type", "text/plain"}, {"x-ms-blob-type", "BlockBlob"}}),
        "data"});
    httpClient.EnqueueResponse(HttpResponse{206,
        MakeCanonicalSuccessHeaders({{"Content-Length", "2"}, {"x-ms-blob-type", "BlockBlob"}}),
        "ta"});
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation firstCallback;
    StartDownloadAsyncAndVerifyDefaultContent(client, firstCallback);

    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 2U, .Length = 2U};
    options.Conditions.IfMatch = "\"etag\"";
    CallbackExpectation secondCallback;
    StartDownloadAsyncAndVerifyRangedContent(client, options, secondCallback);

    const auto& request = httpClient.RequestAt(1).Request;
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "Range"), "bytes=2-3");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "If-Match"), "\"etag\"");
    httpClient.Poll(); // drive the two posted (async) completions (T26)
}

TEST(BlockBlobClientTests, DownloadToAsync_StreamStringPathAndFilesystemPathWriteBytes)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "4"}}), "data"},
        {},
        true);
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "4"}}), "more"},
        {},
        true);
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "5"}}), "bytes"},
        {},
        true);
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream stream;
    CallbackExpectation streamCallback;
    StartDownloadToStreamAsyncAndVerify(client, stream, streamCallback);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(stream.str(), "data");

    const std::filesystem::path stringPath =
        std::filesystem::temp_directory_path() / "azure-client-block-download-string.txt";
    const std::filesystem::path fsPath =
        std::filesystem::temp_directory_path() / "azure-client-block-download-path.txt";

    CallbackExpectation stringPathCallback;
    StartDownloadToStringPathAsyncAndVerify(client, stringPath.string(), stringPathCallback);
    EXPECT_TRUE(httpClient.CompleteNext());

    CallbackExpectation fsPathCallback;
    StartDownloadToFilesystemPathAsyncAndVerify(client, fsPath, fsPathCallback);
    EXPECT_TRUE(httpClient.CompleteNext());

    EXPECT_EQ(std::filesystem::file_size(stringPath), 4U);
    EXPECT_EQ(std::filesystem::file_size(fsPath), 5U);

    std::error_code ignored;
    std::filesystem::remove(stringPath, ignored);
    std::filesystem::remove(fsPath, ignored);
}

TEST(BlockBlobClientTests, DownloadToAsync_LargeBodyStreamsDirectlyWithoutDuplicatingContent)
{
    // Regression test for the interim Task 3 fix: the body must reach the destination stream
    // byte-for-byte via the direct-streaming path (Private::DownloadBlobToAsync), without ever
    // being materialized into Models::DownloadBlobResult::Content.
    const std::string largeBody(std::size_t{8} * 1024U * 1024U, 'x');
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200,
        MakeCanonicalSuccessHeaders({{"Content-Length", std::to_string(largeBody.size())}}),
        largeBody});
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream stream;
    CallbackExpectation callback;
    client.DownloadToAsync(stream,
        [&](std::expected<Response<DownloadBlobToResult>, BlobStorageError> result)
    {
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().BytesWritten, largeBody.size());
        callback.MarkInvoked();
    });

    httpClient.Poll(); // drive the posted (async) completion (T26)
    EXPECT_EQ(stream.str(), largeBody);
}

TEST(BlockBlobClientTests, SetMetadataHeadersAndAccessTierSendExpectedRequestsAndParseResponses)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    BlockBlobClient client{httpClient, BuildOptions()};

    AVEVA::AzureClient::SetBlobMetadataOptions metadataOptions;
    metadataOptions.Metadata["Project"] = "aveva";
    metadataOptions.Conditions.LeaseId = "lease-1";
    CallbackExpectation metadataCallback;
    StartSetMetadataAsyncAndVerify(client, metadataOptions, metadataCallback);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-meta-Project"), "aveva");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-lease-id"), "lease-1");

    AVEVA::AzureClient::SetBlobHttpHeadersOptions headerOptions;
    headerOptions.HttpHeaders.ContentType = "image/png";
    headerOptions.HttpHeaders.ContentMd5 = "md5";
    headerOptions.HttpHeaders.CacheControl = "no-cache";
    CallbackExpectation headersCallback;
    StartSetHttpHeadersAsyncAndVerify(client, headerOptions, headersCallback);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "x-ms-blob-content-type"), "image/png");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "x-ms-blob-content-md5"), "md5");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "x-ms-blob-cache-control"), "no-cache");

    AVEVA::AzureClient::SetBlobAccessTierOptions tierOptions;
    tierOptions.AccessTier = "Cool";
    CallbackExpectation tierCallback;
    StartSetAccessTierAsyncAndVerify(client, tierOptions, tierCallback);
    httpClient.Poll(); // drive the three posted (async) completions (T26)
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "x-ms-access-tier"), "Cool");
}

TEST(BlockBlobClientTests, StartCopySnapshotAndLeaseOperationsParseResponsesAndOptions)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{202,
        MakeCanonicalSuccessHeaders({{"x-ms-copy-id", "copy-1"}, {"x-ms-copy-status", "success"}}),
        ""});
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-snapshot", "snapshot-1"}}), ""});
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease-1"}}), ""});
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(HttpResponse{202, MakeCanonicalSuccessHeaders({{"x-ms-lease-time", "15"}}), ""});
    BlockBlobClient client{httpClient, BuildOptions()};

    AVEVA::AzureClient::StartCopyFromUriOptions copyOptions;
    copyOptions.Metadata["source"] = "tests";
    copyOptions.AccessTier = "Hot";
    CallbackExpectation copyCallback;
    StartCopyFromUriAsyncAndVerify(client, copyOptions, copyCallback);

    AVEVA::AzureClient::SnapshotBlobOptions snapshotOptions;
    snapshotOptions.Metadata["snapshot"] = "true";
    CallbackExpectation snapshotCallback;
    StartSnapshotAsyncAndVerify(client, snapshotOptions, snapshotCallback);

    AVEVA::AzureClient::AcquireLeaseOptions acquireOptions;
    acquireOptions.ProposedLeaseId = "proposed-lease";
    acquireOptions.Duration = std::chrono::seconds{30};
    CallbackExpectation acquireCallback;
    StartAcquireLeaseAsyncAndVerify(client, acquireOptions, acquireCallback);

    AVEVA::AzureClient::ReleaseLeaseOptions releaseOptions;
    releaseOptions.LeaseId = "lease-1";
    CallbackExpectation releaseCallback;
    StartReleaseLeaseAsyncAndVerify(client, releaseOptions, releaseCallback);

    AVEVA::AzureClient::BreakLeaseOptions breakOptions;
    breakOptions.BreakPeriod = std::chrono::seconds{10};
    CallbackExpectation breakCallback;
    StartBreakLeaseAsyncAndVerify(client, breakOptions, breakCallback);

    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-copy-source"),
        "https://source.example.com/container/blob.txt?sig=x");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-meta-source"), "tests");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-access-tier"), "Hot");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "x-ms-meta-snapshot"), "true");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "x-ms-lease-action"), "acquire");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "x-ms-proposed-lease-id"),
        "proposed-lease");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(3).Request, "x-ms-lease-id"), "lease-1");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(4).Request, "x-ms-lease-break-period"), "10");
    httpClient.Poll(); // drive the five posted (async) completions (T26)
}

TEST(BlockBlobClientTests, ExistsCreateIfNotExistsAndDeleteIfExistsTreatExpectedErrorsAsNonFatal)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(MakeAzureErrorResponse(404, "BlobNotFound", "", "missing-request"));
    httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "BlobAlreadyExists", "", "exists-request"));
    httpClient.EnqueueResponse(MakeAzureErrorResponse(404, "BlobNotFound", "", "delete-request"));
    BlockBlobClient client{httpClient, BuildOptions()};

    CallbackExpectation existsCallback;
    StartExistsAsyncAndVerifyTrue(client, existsCallback);

    CallbackExpectation missingExistsCallback;
    StartExistsAsyncAndVerifyFalse(client, missingExistsCallback);

    AVEVA::AzureClient::UploadBlockBlobOptions createOptions;
    createOptions.HttpHeaders.ContentType = "text/plain";
    CallbackExpectation createCallback;
    StartCreateIfNotExistsAsyncAndVerify(client, createOptions, createCallback);

    AVEVA::AzureClient::DeleteBlobOptions deleteOptions;
    deleteOptions.DeleteSnapshotsOption = "include";
    CallbackExpectation deleteCallback;
    StartDeleteIfExistsAsyncAndVerify(client, deleteOptions, deleteCallback);

    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(2).Request, "If-None-Match"), "*");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(3).Request, "x-ms-delete-snapshots"), "include");
    httpClient.Poll(); // drive the four posted (async) completions (T26)
}

TEST(BlockBlobClientTests, UploadFromAsyncFilesystemPathStagesBlocksAndCommitsInOrder)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    const std::filesystem::path path = std::filesystem::temp_directory_path() / "azure-client-block-upload-from.txt";
    {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output << "12345";
    }

    AVEVA::AzureClient::UploadFromOptions options;
    options.BlockSize = 2U;

    CallbackExpectation callback;
    StartUploadFromPathAsyncAndVerify(client, path, options, callback);

    httpClient.Poll(); // drive the posted (async) chained stage/commit completions (T26)
    ASSERT_EQ(httpClient.RequestCount(), 4U);
    EXPECT_EQ(httpClient.RequestAt(0).Body, "12");
    EXPECT_EQ(httpClient.RequestAt(1).Body, "34");
    EXPECT_EQ(httpClient.RequestAt(2).Body, "5");
    const std::string commitBody = httpClient.RequestAt(3).Body;
    ASSERT_EQ(httpClient.StagedBlockIds().size(), 3U);
    std::string expectedList;
    for (const std::string& id : httpClient.StagedBlockIds())
    {
        expectedList += "<Latest>" + id + "</Latest>";
    }
    EXPECT_NE(commitBody.find(expectedList), std::string::npos) << commitBody;

    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

TEST(BlockBlobClientTests, ExistsAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<bool>, BlobStorageError>> future = client.ExistsAsync(boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<bool>, BlobStorageError> result = future.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->Value());
}

TEST(BlockBlobClientTests, DownloadAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), "hello"};
    BlockBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<DownloadBlobResult>, BlobStorageError>> future =
        client.DownloadAsync(AVEVA::AzureClient::DownloadBlobOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<DownloadBlobResult>, BlobStorageError> result = future.get();
    ASSERT_TRUE(result.has_value());
    const auto& content = result->Value().Content;
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(content.data()), content.size()), "hello");
}

TEST(BlockBlobClientTests, UploadAsyncAcceptsUseFutureCompletionTokenAndReportsFailure)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(MakeAzureErrorResponse(409, "BlobAlreadyExists", "conflict", "upload-request"));
    BlockBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<UploadBlockBlobResult>, BlobStorageError>> future =
        client.UploadAsync(std::span<const std::byte>{},
            AVEVA::AzureClient::UploadBlockBlobOptions{},
            boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result = future.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobAlreadyExists);
    EXPECT_EQ(result.error().RequestId, "upload-request");
}

TEST(BlockBlobClientTests, DeleteAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{202, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<DeleteBlobResult>, BlobStorageError>> future =
        client.DeleteAsync(AVEVA::AzureClient::DeleteBlobOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<DeleteBlobResult>, BlobStorageError> const result = future.get();
    EXPECT_TRUE(result.has_value());
}

TEST(BlockBlobClientTests, GetPropertiesAsyncAcceptsUseFutureCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    std::future<std::expected<Response<BlobProperties>, BlobStorageError>> future =
        client.GetPropertiesAsync(AVEVA::AzureClient::GetBlobPropertiesOptions{}, boost::asio::use_future);

    httpClient.Poll(); // drive the posted (async) completion so the future becomes ready (T26)
    ASSERT_EQ(future.wait_for(std::chrono::seconds{1}), std::future_status::ready);
    std::expected<Response<BlobProperties>, BlobStorageError> const result = future.get();
    EXPECT_TRUE(result.has_value());
}

// Task 14: every ...Async overload defaults its CompletionToken parameter to
// boost::asio::default_completion_token_t<executor_type> (boost::asio::deferred_t, since
// executor_type does not specialize default_completion_token_type). Omitting the token entirely
// therefore yields a deferred, directly co_await-able operation -- this is the ergonomics win the
// task set out to prove: no explicit boost::asio::use_awaitable is required at the call site.
TEST(BlockBlobClientTests, ExistsAsyncSupportsCoAwaitWithNoExplicitCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    std::optional<std::expected<Response<bool>, BlobStorageError>> result;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitInto(&result,
            [&]
    {
        return client.ExistsAsync();
    }),
        boost::asio::detached);

    httpClient.Poll();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(ValueOrFail(result).has_value());
    EXPECT_TRUE((*result)->Value());
}

// Same as above, but for an overload that takes both a leading content parameter and an options
// struct (UploadAsync), the exact shape Task 14's ambiguity-avoidance requires clauses target.
TEST(BlockBlobClientTests, UploadAsyncSupportsCoAwaitWithNoExplicitCompletionToken)
{
    FakeHttpClient httpClient;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, BuildOptions()};

    const std::vector<std::byte> content{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    std::optional<std::expected<Response<UploadBlockBlobResult>, BlobStorageError>> result;
    boost::asio::co_spawn(httpClient.get_executor(),
        AVEVA::AzureClient::Tests::AwaitInto(&result,
            [&]
    {
        return client.UploadAsync(std::span<const std::byte>{content});
    }),
        boost::asio::detached);

    httpClient.Poll();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(ValueOrFail(result).has_value());
}

// Compile-only proof that defaulting CompletionToken did not introduce ambiguity between the
// "options" and "no options" overloads of UploadAsync/StageBlockAsync (and analogues): every
// combination below must resolve to exactly one overload. Never run; only needs to compile.
namespace
{
    [[maybe_unused]] void BlockBlobClientNoTokenOverloadsCompile(BlockBlobClient& client)
    {
        using AVEVA::AzureClient::CommitBlockListOptions;
        using AVEVA::AzureClient::StageBlockOptions;
        using AVEVA::AzureClient::UploadBlockBlobOptions;
        using AVEVA::AzureClient::UploadFromOptions;

        const std::vector<std::byte> bytes;
        const std::span<const std::byte> span{bytes};
        std::string const str;
        std::vector<std::string> const blockIds;
        std::filesystem::path const path;
        std::istringstream stream;

        static_cast<void>(client.UploadAsync(str));
        static_cast<void>(client.UploadAsync(str, UploadBlockBlobOptions{}));
        static_cast<void>(client.UploadAsync(span));
        static_cast<void>(client.UploadAsync(span, UploadBlockBlobOptions{}));

        static_cast<void>(client.StageBlockAsync(str, str));
        static_cast<void>(client.StageBlockAsync(str, str, StageBlockOptions{}));
        static_cast<void>(client.StageBlockAsync(str, span));
        static_cast<void>(client.StageBlockAsync(str, span, StageBlockOptions{}));

        static_cast<void>(client.CommitBlockListAsync(blockIds));
        static_cast<void>(client.CommitBlockListAsync(blockIds, CommitBlockListOptions{}));

        static_cast<void>(client.GetBlockListAsync());
        static_cast<void>(client.DownloadAsync());
        static_cast<void>(client.DownloadAsync(AVEVA::AzureClient::DownloadBlobOptions{}));

        static_cast<void>(client.DeleteAsync());
        static_cast<void>(client.DeleteAsync(AVEVA::AzureClient::DeleteBlobOptions{}));

        static_cast<void>(client.GetPropertiesAsync());
        static_cast<void>(client.GetPropertiesAsync(AVEVA::AzureClient::GetBlobPropertiesOptions{}));

        static_cast<void>(client.SnapshotAsync());
        static_cast<void>(client.SnapshotAsync(AVEVA::AzureClient::SnapshotBlobOptions{}));

        static_cast<void>(client.AcquireLeaseAsync());
        static_cast<void>(client.AcquireLeaseAsync(AVEVA::AzureClient::AcquireLeaseOptions{}));

        static_cast<void>(client.BreakLeaseAsync());
        static_cast<void>(client.BreakLeaseAsync(AVEVA::AzureClient::BreakLeaseOptions{}));

        static_cast<void>(client.StartCopyFromUriAsync(str));
        static_cast<void>(client.StartCopyFromUriAsync(str, AVEVA::AzureClient::StartCopyFromUriOptions{}));

        static_cast<void>(client.UploadFromAsync(path));
        static_cast<void>(client.UploadFromAsync(path, UploadFromOptions{}));
        static_cast<void>(client.UploadFromAsync(str));
        static_cast<void>(client.UploadFromAsync(str, UploadFromOptions{}));
        static_cast<void>(client.UploadFromAsync(stream));
        static_cast<void>(client.UploadFromAsync(stream, UploadFromOptions{}));

        static_cast<void>(client.ExistsAsync());
        static_cast<void>(client.CreateIfNotExistsAsync());
        static_cast<void>(client.CreateIfNotExistsAsync(UploadBlockBlobOptions{}));
        static_cast<void>(client.DeleteIfExistsAsync());
        static_cast<void>(client.DeleteIfExistsAsync(AVEVA::AzureClient::DeleteBlobOptions{}));
    }

    TEST(BlockBlobClientTests, UploadAndStageBlockSendTransactionalHashes)
    {
        FakeHttpClient httpClient;
        std::vector<AVEVA::HttpHeader> stageHeaders = MakeCanonicalSuccessHeaders();
        stageHeaders.emplace_back("Content-MD5", "bWQ1");
        httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});
        httpClient.EnqueueResponse(HttpResponse{201, std::move(stageHeaders), ""});
        BlockBlobClient client{httpClient, BuildOptions()};

        AVEVA::AzureClient::UploadBlockBlobOptions options;
        options.TransactionalContentMd5 = "bWQ1";
        options.TransactionalContentCrc64 = "Y3JjNjQ=";
        StartUploadAsyncWithTransactionalHashes(client, options);
        httpClient.Poll();
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Content-MD5"), "bWQ1");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-content-crc64"), "Y3JjNjQ=");

        AVEVA::AzureClient::StageBlockOptions stageOptions;
        stageOptions.TransactionalContentMd5 = options.TransactionalContentMd5;
        stageOptions.TransactionalContentCrc64 = options.TransactionalContentCrc64;
        std::optional<std::string> md5;
        StartStageBlockAsyncWithTransactionalHashes(client, stageOptions, md5);
        httpClient.Poll();
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Content-MD5"), "bWQ1");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-content-crc64"), "Y3JjNjQ=");
        EXPECT_EQ(md5, "bWQ1");

        StartUploadAsyncWithoutTransactionalHashes(client);
        httpClient.Poll();
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "Content-MD5"), "");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-content-crc64"), "");
    }

    TEST(BlockBlobClientTests, StageBlockFromUriSendsSourceRangeAndValidatesArguments)
    {
        FakeHttpClient httpClient;
        BlockBlobClient client{httpClient, BuildOptions()};
        const std::string blockId = AVEVA::AzureClient::Models::EncodeBlockId(3);

        AVEVA::AzureClient::StageBlockFromUriOptions options;
        options.SourceOffset = 100;
        options.SourceLength = 50;
        options.SourceContentMd5 = "c3JjbWQ1";
        options.Conditions.LeaseId = "lease-1";
        bool staged = false;
        StartStageBlockFromUriAsyncAndVerify(client, blockId, options, staged);
        httpClient.Poll();
        EXPECT_TRUE(staged);
        const AVEVA::HttpRequest& request = httpClient.LastRequest();
        EXPECT_EQ(request.GetMethod(), AVEVA::HttpMethod::Put);
        EXPECT_NE(request.GetUrl().find("comp=block&blockid="), std::string::npos);
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-copy-source"), "https://src.example.com/c/b?sig=x");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-source-range"), "bytes=100-149");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-source-content-md5"), "c3JjbWQ1");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-lease-id"), "lease-1");

        options.SourceLength.reset();
        StartStageBlockFromUriAsyncWithoutLength(client, blockId, options);
        httpClient.Poll();
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-source-range"), "bytes=100-");

        StartStageBlockFromUriAsyncWithoutRange(client, blockId);
        httpClient.Poll();
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-source-range"), "");
        const std::size_t sent = httpClient.RequestCount();

        int rejected = 0;
        StartStageBlockFromUriAsyncExpectingInvalidArgument(client,
            "not base64!",
            "https://src.example.com/c/b",
            rejected);
        StartStageBlockFromUriAsyncExpectingInvalidArgument(client, blockId, "", rejected);
        AVEVA::AzureClient::StageBlockFromUriOptions lengthOnly;
        lengthOnly.SourceLength = 10;
        StartStageBlockFromUriAsyncExpectingInvalidArgument(client,
            blockId,
            "https://src.example.com/c/b",
            lengthOnly,
            rejected);
        AVEVA::AzureClient::StageBlockFromUriOptions zeroLength;
        zeroLength.SourceOffset = 0;
        zeroLength.SourceLength = 0;
        StartStageBlockFromUriAsyncExpectingInvalidArgument(client,
            blockId,
            "https://src.example.com/c/b",
            zeroLength,
            rejected);
        AVEVA::AzureClient::StageBlockFromUriOptions overflow;
        overflow.SourceOffset = std::numeric_limits<std::uint64_t>::max() - 3U;
        overflow.SourceLength = 8;
        StartStageBlockFromUriAsyncExpectingInvalidArgument(client,
            blockId,
            "https://src.example.com/c/b",
            overflow,
            rejected);
        httpClient.Poll();
        EXPECT_EQ(rejected, 5);
        EXPECT_EQ(httpClient.RequestCount(), sent);
    }

    TEST(BlockBlobClientTests, CopyFromUriIsSynchronousAndParsesResult)
    {
        FakeHttpClient httpClient;
        std::vector<AVEVA::HttpHeader> headers = MakeCanonicalSuccessHeaders();
        headers.emplace_back("x-ms-copy-id", "copy-1");
        headers.emplace_back("x-ms-copy-status", "success");
        headers.emplace_back("x-ms-content-crc64", "Y3JjNjQ=");
        httpClient.EnqueueResponse(HttpResponse{202, std::move(headers), ""});
        BlockBlobClient client{httpClient, BuildOptions()};

        AVEVA::AzureClient::CopyFromUriOptions options;
        options.Metadata = {{"origin", "copy"}};
        options.AccessTier = AVEVA::AzureClient::Models::AccessTier::Cool();
        options.SourceContentMd5 = "c3JjbWQ1";
        options.Conditions.IfNoneMatch = "*";
        std::optional<AVEVA::AzureClient::Models::CopyBlobFromUriResult> copied;
        StartCopyFromUriAsyncAndCaptureResult(client, options, copied);
        httpClient.Poll();

        ASSERT_TRUE(copied.has_value());
        EXPECT_EQ(ValueOrFail(copied).ETag, DefaultETag);
        EXPECT_EQ(ValueOrFail(copied).CopyId, "copy-1");
        EXPECT_EQ(ValueOrFail(copied).CopyStatus, AVEVA::AzureClient::Models::CopyStatus::Success());
        EXPECT_EQ(ValueOrFail(copied).ContentCrc64, "Y3JjNjQ=");
        const AVEVA::HttpRequest& request = httpClient.LastRequest();
        EXPECT_EQ(request.GetMethod(), AVEVA::HttpMethod::Put);
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-copy-source"), "https://src.example.com/c/b?sig=x");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-requires-sync"), "true");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-meta-origin"), "copy");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-access-tier"), "Cool");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-source-content-md5"), "c3JjbWQ1");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "If-None-Match"), "*");
    }

    TEST(BlockBlobClientTests, AbortCopyFromUriSendsAbortActionAndRejectsEmptyCopyId)
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{204, MakeCanonicalSuccessHeaders(), ""});
        BlockBlobClient client{httpClient, BuildOptions()};

        bool aborted = false;
        StartAbortCopyFromUriAsyncAndVerify(client, aborted);
        httpClient.Poll();
        EXPECT_TRUE(aborted);
        const AVEVA::HttpRequest& request = httpClient.LastRequest();
        EXPECT_EQ(request.GetMethod(), AVEVA::HttpMethod::Put);
        EXPECT_NE(request.GetUrl().find("comp=copy&copyid=copy%20id%2F1"), std::string::npos);
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-copy-action"), "abort");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-lease-id"), "lease-1");

        std::optional<std::error_code> error;
        StartAbortCopyFromUriAsyncExpectingInvalidArgument(client, error);
        httpClient.Poll();
        EXPECT_EQ(error, std::make_error_code(std::errc::invalid_argument));
        EXPECT_EQ(httpClient.RequestCount(), 1U);
    }
} // namespace
