#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <ios>
#include <istream>
#include <map>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>

using AVEVA::HttpRequestOptions;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlockBlobClient;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::UploadFromOptions;
using AVEVA::AzureClient::Models::UploadBlockBlobResult;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;

namespace
{
    [[nodiscard]] AVEVA::AzureClient::BlobClientOptions BuildOptions()
    {
        return MakeBlobClientOptions();
    }

    using UploadResult = std::expected<Response<UploadBlockBlobResult>, BlobStorageError>;

    void StartUploadFromAsyncAndExpectSuccess(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        int& callbackCount);
    void StartUploadFromAsyncAndExpectSuccess(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        bool& callbackInvoked);
    void StartUploadFromAsyncAndExpectSuccess(BlockBlobClient& client,
        const std::filesystem::path& path,
        const UploadFromOptions& options,
        int& callbackCount);
    void StartUploadFromAsyncAndExpectCode(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        int& callbackCount,
        const std::error_code& expectedCode);
    void StartUploadFromAsyncAndExpectStatus(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        int& callbackCount,
        int expectedStatus);
    void PollUntilCallback(FakeHttpClient& httpClient, int& callbackCount, int maxPolls);
    void WaitForNextStageRequest(FakeHttpClient& httpClient, std::size_t expectedRequestCount);
    [[nodiscard]] std::string MakeTripledAlphabetData();
    [[nodiscard]] std::size_t CompletePendingRequestsInReverseOrderUntilCallback(FakeHttpClient& httpClient,
        std::size_t maxPending,
        int& callbackCount);
    void VerifyCommitReassemblesData(FakeHttpClient& httpClient, const std::string& data, std::size_t blockSize);
    void VerifySingleBlockBoundarySize(std::size_t size);
    void VerifyMultiBlockBoundarySize(std::size_t size);
    void VerifyLeaseAndConditionHeaders(const FakeHttpClient& httpClient);
    void VerifyPathUploadUsesSingleUpload(const std::filesystem::path& path, std::uint64_t threshold);
    void VerifyPathUploadUsesChunkedUpload(const std::filesystem::path& path, std::uint64_t threshold);
} // namespace

TEST(T10_ConcurrentUploadTests, UploadFromAsync_Concurrent_IssuesAndCompletesInFlight)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, BuildOptions()};

    std::istringstream stream("ABCDEFGHI");
    UploadFromOptions options;
    options.BlockSize = 4U;
    options.Concurrency = 2U;

    bool callbackInvoked = false;
    StartUploadFromAsyncAndExpectSuccess(client, stream, options, callbackInvoked);

    // Two initial stage requests should be issued.
    ASSERT_EQ(httpClient.RequestCount(), 2U);

    // Complete the first staged request; this should cause the next block to be staged.
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    // Allow the fake client's executor to schedule any follow-up work that may issue the next stage.
    WaitForNextStageRequest(httpClient, 3U);
    ASSERT_EQ(httpClient.RequestCount(), 3U);

    // Complete the remaining stage requests.
    ASSERT_TRUE(httpClient.CompleteRequest(1U));
    ASSERT_TRUE(httpClient.CompleteRequest(2U));
    httpClient.Poll();

    // Commit should be issued as the final request.
    ASSERT_EQ(httpClient.RequestCount(), 4U);
    EXPECT_NE(httpClient.Requests().at(3).Request.GetUrl().find("comp=blocklist"), std::string::npos);

    // Verify blocks were staged in order with the correct bodies.
    EXPECT_EQ(httpClient.Requests().at(0).Body, "ABCD");
    EXPECT_EQ(httpClient.Requests().at(1).Body, "EFGH");
    EXPECT_EQ(httpClient.Requests().at(2).Body, "I");

    ASSERT_TRUE(httpClient.CompleteRequest(3U));
    httpClient.Poll();
    EXPECT_TRUE(callbackInvoked);
}

TEST(T10_ConcurrentUploadTests, UploadFromAsync_Concurrent_FailureStopsAndDoesNotCommit)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, BuildOptions()};

    std::istringstream stream("ABCDEFGHI");
    UploadFromOptions options;
    options.BlockSize = 4U;
    options.Concurrency = 2U;

    int callbackCount = 0;
    StartUploadFromAsyncAndExpectCode(client,
        stream,
        options,
        callbackCount,
        std::make_error_code(std::errc::connection_reset));

    ASSERT_EQ(httpClient.RequestCount(), 2U);
    // Fail the second pending staged request.
    EXPECT_TRUE(httpClient.FailPending(1U, std::make_error_code(std::errc::connection_reset)));
    httpClient.Poll();

    // Only two requests should have been made (no commit), and callback invoked once with failure.
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(callbackCount, 1);
}

TEST(T10_ConcurrentUploadTests, UploadFromAsync_Concurrent_CancellationAbortsOutstandingRequests)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, BuildOptions()};

    // Build a stream large enough to require multiple blocks
    std::string data;
    for (int i = 0; i < 100; ++i)
    {
        data += std::string(1024, 'A'); // ~100 KiB
    }
    std::istringstream stream(data);

    UploadFromOptions options;
    options.BlockSize = 4096U; // 4 KiB blocks
    options.Concurrency = 3U;

    boost::asio::cancellation_signal cancelSignal;
    HttpRequestOptions requestOptions;
    requestOptions.SetCancellationSlot(cancelSignal.slot());

    int callbackCount = 0;
    client.UploadFromAsync(stream,
        options,
        [&](std::expected<Response<UploadBlockBlobResult>, BlobStorageError> result)
    {
        ++callbackCount;
        ASSERT_FALSE(result.has_value());
        // Cancellation maps to operation_aborted-like std::errc::operation_canceled
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::operation_canceled));
    },
        requestOptions);

    // There should be up to Concurrency outstanding staged requests.
    EXPECT_LE(httpClient.PendingCount(), options.Concurrency);
    EXPECT_GT(httpClient.PendingCount(), 0U);

    // Emitting the cancellation signal alone -- with no manual FakeHttpClient bookkeeping --
    // must forward into FakeHttpClient's cancellation-slot handling (see SendAsyncErased) and
    // cause the overall UploadFromAsync operation to complete with operation_canceled.
    cancelSignal.emit(boost::asio::cancellation_type::all);
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
}

namespace
{
    void StartUploadFromAsyncAndExpectSuccess(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        int& callbackCount)
    {
        client.UploadFromAsync(stream,
            options,
            [&](UploadResult result)
        {
            ++callbackCount;
            EXPECT_TRUE(result.has_value());
        });
    }

    void StartUploadFromAsyncAndExpectSuccess(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        bool& callbackInvoked)
    {
        client.UploadFromAsync(stream,
            options,
            [&](UploadResult result)
        {
            ASSERT_TRUE(result.has_value());
            callbackInvoked = true;
        });
    }

    void StartUploadFromAsyncAndExpectCode(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        int& callbackCount,
        const std::error_code& expectedCode)
    {
        client.UploadFromAsync(stream,
            options,
            [&](UploadResult result)
        {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, expectedCode);
        });
    }

    void StartUploadFromAsyncAndExpectStatus(BlockBlobClient& client,
        std::istream& stream,
        const UploadFromOptions& options,
        int& callbackCount,
        int expectedStatus)
    {
        client.UploadFromAsync(stream,
            options,
            [&callbackCount, expectedStatus](UploadResult result)
        {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().StatusCode, expectedStatus);
        });
    }

    void StartUploadFromAsyncAndExpectSuccess(BlockBlobClient& client,
        const std::filesystem::path& path,
        const UploadFromOptions& options,
        int& callbackCount)
    {
        client.UploadFromAsync(path,
            options,
            [&](UploadResult result)
        {
            ++callbackCount;
            EXPECT_TRUE(result.has_value());
        });
    }

    void PollUntilCallback(FakeHttpClient& httpClient, int& callbackCount, int maxPolls)
    {
        for (int i = 0; i < maxPolls && callbackCount == 0; ++i)
        {
            httpClient.Poll();
        }
    }

    void WaitForNextStageRequest(FakeHttpClient& httpClient, std::size_t expectedRequestCount)
    {
        for (int i = 0; i < 10 && httpClient.RequestCount() < expectedRequestCount; ++i)
        {
            httpClient.Poll();
        }
    }

    [[nodiscard]] bool IsStageBlock(const FakeHttpClient::RequestRecord& record)
    {
        return record.Request.GetUrl().contains("comp=block&") || record.Request.GetUrl().ends_with("comp=block");
    }

    [[nodiscard]] bool IsCommit(const FakeHttpClient::RequestRecord& record)
    {
        return record.Request.GetUrl().contains("comp=blocklist");
    }

    [[nodiscard]] std::string MakeTripledAlphabetData()
    {
        std::string data;
        for (char c = 'a'; c <= 'z'; ++c)
        {
            data += std::string(3, c);
        }
        return data;
    }

    [[nodiscard]] std::size_t CompletePendingRequestsInReverseOrderUntilCallback(FakeHttpClient& httpClient,
        std::size_t maxPending,
        int& callbackCount)
    {
        std::size_t peak = 0;
        for (int guard = 0; guard < 1000 && callbackCount == 0; ++guard)
        {
            peak = std::max(peak, httpClient.PendingCount());
            EXPECT_LE(httpClient.PendingCount(), maxPending);
            if (httpClient.PendingCount() > 0U)
            {
                bool completed = false;
                for (std::size_t index = httpClient.RequestCount(); index-- > 0U && !completed;)
                {
                    completed = httpClient.CompleteRequest(index);
                }
                EXPECT_TRUE(completed) << (httpClient.RequestCount() - 1U);
            }
            httpClient.Poll();
        }
        return peak;
    }

    [[nodiscard]] std::map<std::string, std::string> CollectBlockBodiesById(FakeHttpClient& httpClient,
        std::size_t expectedBlocks)
    {
        std::map<std::string, std::string> bodyByBlockId;
        for (std::size_t index = 0; index < expectedBlocks; ++index)
        {
            const auto& record = httpClient.RequestAt(index);
            EXPECT_TRUE(IsStageBlock(record));
            const std::string& url = record.Request.GetUrl();
            const std::size_t idPos = url.find("blockid=");
            EXPECT_NE(idPos, std::string::npos);
            std::string blockId = url.substr(idPos + 8U);
            blockId = blockId.substr(0, blockId.find('&'));
            bodyByBlockId[blockId] = record.Body;
        }
        return bodyByBlockId;
    }

    [[nodiscard]] std::string EncodeBlockId(const std::string& id)
    {
        std::string encoded;
        for (char const c : id)
        {
            if (c == '=')
            {
                encoded += "%3D";
                continue;
            }
            if (c == '+')
            {
                encoded += "%2B";
                continue;
            }
            if (c == '/')
            {
                encoded += "%2F";
                continue;
            }
            encoded += c;
        }
        return encoded;
    }

    [[nodiscard]] std::pair<std::string, std::size_t> ReassembleCommittedBlocks(
        const FakeHttpClient::RequestRecord& commit,
        const std::map<std::string, std::string>& bodyByBlockId)
    {
        std::string reassembled;
        std::size_t pos = 0;
        std::size_t committed = 0;
        while ((pos = commit.Body.find("<Latest>", pos)) != std::string::npos)
        {
            pos += 8U;
            const std::size_t end = commit.Body.find("</Latest>", pos);
            const std::string id = commit.Body.substr(pos, end - pos);
            const auto it = bodyByBlockId.contains(id) ? bodyByBlockId.find(id) : bodyByBlockId.find(EncodeBlockId(id));
            EXPECT_NE(it, bodyByBlockId.end()) << id;
            if (it != bodyByBlockId.end())
            {
                reassembled += it->second;
                ++committed;
            }
        }
        return {reassembled, committed};
    }

    void VerifyCommitReassemblesData(FakeHttpClient& httpClient, const std::string& data, std::size_t blockSize)
    {
        const std::size_t expectedBlocks = (data.size() + blockSize - 1U) / blockSize;
        ASSERT_EQ(httpClient.RequestCount(), expectedBlocks + 1U);
        const auto& commit = httpClient.Requests().back();
        ASSERT_TRUE(IsCommit(commit));

        const auto bodyByBlockId = CollectBlockBodiesById(httpClient, expectedBlocks);
        const auto [reassembled, committed] = ReassembleCommittedBlocks(commit, bodyByBlockId);
        EXPECT_EQ(committed, expectedBlocks);
        EXPECT_EQ(reassembled, data);
    }

    [[nodiscard]] std::string MakeSequentialData(std::size_t size)
    {
        std::string data;
        for (std::size_t i = 0; i < size; ++i)
        {
            data += static_cast<char>('a' + i);
        }
        return data;
    }

    void VerifySingleBlockBoundarySize(std::size_t size)
    {
        FakeHttpClient httpClient;
        BlockBlobClient client{httpClient, BuildOptions()};
        const std::string data = MakeSequentialData(size);
        std::istringstream stream(data);
        UploadFromOptions options;
        options.BlockSize = 4U;
        options.Concurrency = 2U;

        int callbackCount = 0;
        StartUploadFromAsyncAndExpectSuccess(client, stream, options, callbackCount);
        PollUntilCallback(httpClient, callbackCount, 20);

        ASSERT_EQ(callbackCount, 1) << size;
        ASSERT_EQ(httpClient.RequestCount(), 1U) << size;
        EXPECT_EQ(httpClient.RequestAt(0).Request.GetUrl().find("comp="), std::string::npos);
        EXPECT_EQ(httpClient.RequestAt(0).Body, data);
    }

    void VerifyMultiBlockBoundarySize(std::size_t size)
    {
        FakeHttpClient httpClient;
        BlockBlobClient client{httpClient, BuildOptions()};
        const std::string data = MakeSequentialData(size);
        std::istringstream stream(data);
        UploadFromOptions options;
        options.BlockSize = 4U;
        options.Concurrency = 2U;

        int callbackCount = 0;
        StartUploadFromAsyncAndExpectSuccess(client, stream, options, callbackCount);
        PollUntilCallback(httpClient, callbackCount, 20);

        ASSERT_EQ(callbackCount, 1) << size;
        const std::size_t blocks = (size + 3U) / 4U;
        ASSERT_EQ(httpClient.RequestCount(), blocks + 1U) << size;
        std::string staged;
        for (std::size_t i = 0; i < blocks; ++i)
        {
            EXPECT_TRUE(IsStageBlock(httpClient.RequestAt(i))) << size;
            staged += httpClient.RequestAt(i).Body;
        }
        EXPECT_EQ(staged, data);
        EXPECT_TRUE(IsCommit(httpClient.Requests().back()));
    }

    void VerifyStageRequestCarriesOnlyLease(const FakeHttpClient& httpClient, std::size_t index)
    {
        const auto& request = httpClient.RequestAt(index).Request;
        EXPECT_TRUE(IsStageBlock(httpClient.RequestAt(index)));
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "If-None-Match"), "");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-lease-id"), "lease-1");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-meta-k"), "");
    }

    void VerifyCommitCarriesAllConditions(const FakeHttpClient& httpClient)
    {
        const auto& commit = httpClient.RequestAt(3U).Request;
        EXPECT_TRUE(IsCommit(httpClient.RequestAt(3U)));
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(commit, "If-None-Match"), "*");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(commit, "x-ms-lease-id"), "lease-1");
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(commit, "x-ms-meta-k"), "v");
    }

    void VerifyLeaseAndConditionHeaders(const FakeHttpClient& httpClient)
    {
        VerifyStageRequestCarriesOnlyLease(httpClient, 0U);
        VerifyStageRequestCarriesOnlyLease(httpClient, 1U);
        VerifyStageRequestCarriesOnlyLease(httpClient, 2U);
        VerifyCommitCarriesAllConditions(httpClient);
    }

    void VerifyPathUploadUsesSingleUpload(const std::filesystem::path& path, std::uint64_t threshold)
    {
        FakeHttpClient httpClient;
        BlockBlobClient client{httpClient, BuildOptions()};
        UploadFromOptions options;
        options.BlockSize = 4U;
        options.Concurrency = 2U;
        options.SingleUploadThreshold = threshold;

        int callbackCount = 0;
        StartUploadFromAsyncAndExpectSuccess(client, path, options, callbackCount);
        PollUntilCallback(httpClient, callbackCount, 20);

        ASSERT_EQ(callbackCount, 1) << threshold;
        ASSERT_EQ(httpClient.RequestCount(), 1U) << threshold;
        EXPECT_EQ(httpClient.RequestAt(0).Request.GetUrl().find("comp="), std::string::npos);
        EXPECT_EQ(httpClient.RequestAt(0).Body, "ABCDEFGHI");
    }

    void VerifyPathUploadUsesChunkedUpload(const std::filesystem::path& path, std::uint64_t threshold)
    {
        FakeHttpClient httpClient;
        BlockBlobClient client{httpClient, BuildOptions()};
        UploadFromOptions options;
        options.BlockSize = 4U;
        options.Concurrency = 2U;
        options.SingleUploadThreshold = threshold;

        int callbackCount = 0;
        StartUploadFromAsyncAndExpectSuccess(client, path, options, callbackCount);
        PollUntilCallback(httpClient, callbackCount, 20);

        ASSERT_EQ(callbackCount, 1) << threshold;
        ASSERT_EQ(httpClient.RequestCount(), 4U) << threshold;
        EXPECT_TRUE(IsCommit(httpClient.RequestAt(3U)));
    }
} // namespace

TEST(T10_ConcurrentUploadTests, OutOfOrderCompletionStillCommitsBlocksInOrder)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, BuildOptions()};

    std::string const data = MakeTripledAlphabetData();
    std::istringstream stream(data);
    UploadFromOptions options;
    options.BlockSize = 5U;
    options.Concurrency = 4U;

    int callbackCount = 0;
    StartUploadFromAsyncAndExpectSuccess(client, stream, options, callbackCount);

    const std::size_t peak =
        CompletePendingRequestsInReverseOrderUntilCallback(httpClient, options.Concurrency, callbackCount);
    ASSERT_EQ(callbackCount, 1);
    EXPECT_EQ(peak, options.Concurrency);
    VerifyCommitReassemblesData(httpClient, data, options.BlockSize);
}

TEST(T10_ConcurrentUploadTests, FailureCancelsOtherInFlightStagesAndReportsOnce)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, BuildOptions()};

    std::istringstream stream(std::string(40, 'x'));
    UploadFromOptions options;
    options.BlockSize = 4U;
    options.Concurrency = 3U;

    int callbackCount = 0;
    StartUploadFromAsyncAndExpectCode(client,
        stream,
        options,
        callbackCount,
        std::make_error_code(std::errc::connection_reset));

    ASSERT_EQ(httpClient.PendingCount(), 3U);
    ASSERT_TRUE(httpClient.FailPending(1U, std::make_error_code(std::errc::connection_reset)));
    httpClient.Poll();

    // The remaining in-flight stages were cancelled through their cancellation slots.
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 3U);
    for (const auto& record : httpClient.Requests())
    {
        EXPECT_FALSE(IsCommit(record));
    }
}

TEST(T10_ConcurrentUploadTests, ServiceErrorFailsTheUploadWithTheServiceStatus)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{201, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{403, {{"x-ms-error-code", "AuthorizationFailure"}}, ""});
    BlockBlobClient client{httpClient, BuildOptions()};

    std::istringstream stream("ABCDEFGHIJKL");
    UploadFromOptions options;
    options.BlockSize = 4U;

    int callbackCount = 0;
    StartUploadFromAsyncAndExpectStatus(client, stream, options, callbackCount, 403);
    PollUntilCallback(httpClient, callbackCount, 10);
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}

TEST(T10_ConcurrentUploadTests, BlockBoundariesForExactMultipleAndOneByteOver)
{
    VerifyMultiBlockBoundarySize(8U);
    VerifyMultiBlockBoundarySize(9U);
    VerifySingleBlockBoundarySize(4U);
    VerifyMultiBlockBoundarySize(5U);
}

TEST(T10_ConcurrentUploadTests, EmptyStreamUploadsWithASinglePutBlob)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};
    std::istringstream stream("");
    UploadFromOptions options;
    options.BlockSize = 4U;
    options.Concurrency = 4U;

    int callbackCount = 0;
    StartUploadFromAsyncAndExpectSuccess(client, stream, options, callbackCount);
    PollUntilCallback(httpClient, callbackCount, 10);
    ASSERT_EQ(callbackCount, 1);
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.RequestAt(0).Request.GetUrl().find("comp="), std::string::npos);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "x-ms-blob-type"), "BlockBlob");
    EXPECT_TRUE(httpClient.RequestAt(0).Body.empty());
}

TEST(T10_ConcurrentUploadTests, StageRequestsCarryOnlyTheLeaseAndCommitCarriesAllConditions)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, BuildOptions()};
    std::istringstream stream("ABCDEFGHI");
    UploadFromOptions options;
    options.BlockSize = 4U;
    options.Concurrency = 2U;
    options.UploadOptions.Conditions.IfNoneMatch = "*";
    options.UploadOptions.Conditions.LeaseId = "lease-1";
    options.UploadOptions.Metadata["k"] = "v";

    int callbackCount = 0;
    StartUploadFromAsyncAndExpectSuccess(client, stream, options, callbackCount);
    PollUntilCallback(httpClient, callbackCount, 20);
    ASSERT_EQ(callbackCount, 1);
    ASSERT_EQ(httpClient.RequestCount(), 4U);
    VerifyLeaseAndConditionHeaders(httpClient);
}

TEST(T10_ConcurrentUploadTests, PathUploadHonoursSingleUploadThreshold)
{
    const std::filesystem::path tempPath =
        std::filesystem::temp_directory_path() / "azure-client-t10-single-upload-threshold.bin";
    {
        std::ofstream output(tempPath, std::ios::binary | std::ios::trunc);
        output << "ABCDEFGHI";
    }

    VerifyPathUploadUsesChunkedUpload(tempPath, 0U);
    VerifyPathUploadUsesChunkedUpload(tempPath, 8U);
    VerifyPathUploadUsesSingleUpload(tempPath, 9U);
    VerifyPathUploadUsesSingleUpload(tempPath, 1024U);

    std::error_code ignored;
    std::filesystem::remove(tempPath, ignored);
}
