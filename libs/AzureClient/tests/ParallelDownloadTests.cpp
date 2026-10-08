// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "DownloadTestSupport.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <cstddef>
#include <gtest/gtest.h>

#include <algorithm>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

// T11: parallel chunked DownloadToAsync (Concurrency > 1).

namespace
{
    using AVEVA::HttpRequestOptions;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::DownloadToOptions;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::DownloadBlobToResult;
    using AVEVA::AzureClient::Tests::EnqueueChunks;
    using AVEVA::AzureClient::Tests::EnqueueForRange;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::FindRequestByRange;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
    using AVEVA::AzureClient::Tests::RangeHeader;
    using AVEVA::AzureClient::Tests::RangeResponse;

    using DownloadToResult = std::expected<Response<DownloadBlobToResult>, BlobStorageError>;

    [[nodiscard]] BlobClientOptions BuildOptions()
    {
        return MakeBlobClientOptions("logs", "t11.txt");
    }

    [[nodiscard]] std::string MakeContent(std::size_t size)
    {
        std::string content;
        for (std::size_t i = 0; i < size; ++i)
        {
            content += static_cast<char>('a' + (i % 26U));
        }
        return content;
    }

    // Tags the concurrency parameter with its own type so it's never adjacent-and-same-type with
    // the chunk size parameter (see bugprone-easily-swappable-parameters).
    struct Concurrency
    {
        std::size_t Value;

        constexpr Concurrency(std::size_t value) noexcept : Value(value)
        {
        }
    };

    [[nodiscard]] DownloadToOptions Parallel(std::size_t chunkSize, Concurrency concurrency)
    {
        DownloadToOptions options;
        options.ChunkSize = chunkSize;
        options.Concurrency = concurrency.Value;
        return options;
    }

    // Writes only through sputc/xsputn; any seek fails, so out-of-order writes would be detected.
    class AppendOnlyBuffer final : public std::stringbuf
    {
      protected:
        pos_type seekoff(off_type /*_Off*/, std::ios_base::seekdir /*_Way*/, std::ios_base::openmode /*_Mode*/) override
        {
            return {static_cast<off_type>(-1)};
        }

        pos_type seekpos(pos_type /*_Pos*/, std::ios_base::openmode /*_Mode*/) override
        {
            return {static_cast<off_type>(-1)};
        }
    };

    // Completes the most recently issued pending request.
    [[nodiscard]] bool CompleteNewest(FakeHttpClient& httpClient)
    {
        for (std::size_t index = httpClient.RequestCount(); index-- > 0U;)
        {
            if (httpClient.CompleteRequest(index))
            {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::string ReadFile(const std::filesystem::path& path)
    {
        std::ifstream input(path, std::ios::binary);
        return std::string{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    }

    [[nodiscard]] bool HasPartialFiles(const std::filesystem::path& target)
    {
        const std::string prefix = target.filename().string() + ".partial-";
        const std::filesystem::directory_iterator entries(target.parent_path());
        return std::any_of(begin(entries),
            end(entries),
            [&prefix](const auto& entry)
        {
            return entry.path().filename().string().starts_with(prefix);
        });
    }

    void StartDownloadToAsyncAndExpectSuccess(BlockBlobClient& client,
        std::ostream& out,
        const DownloadToOptions& options,
        int& callbackCount,
        std::size_t expectedBytes)
    {
        client.DownloadToAsync(out,
            options,
            [&callbackCount, expectedBytes](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, expectedBytes);
        });
    }

    void StartDownloadToAsyncAndExpectSuccess(PageBlobClient& client,
        const std::filesystem::path& target,
        const DownloadToOptions& options,
        int& callbackCount,
        std::size_t expectedBytes)
    {
        client.DownloadToAsync(target,
            options,
            [&callbackCount, expectedBytes](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, expectedBytes);
        });
    }

    void StartDownloadToAsyncAndExpectStatus(BlockBlobClient& client,
        std::ostream& out,
        const DownloadToOptions& options,
        int& callbackCount,
        unsigned expectedStatus,
        std::string_view expectedErrorCode = {})
    {
        client.DownloadToAsync(out,
            options,
            [&callbackCount, expectedStatus, expectedErrorCode](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().StatusCode, expectedStatus);
            if (!expectedErrorCode.empty())
            {
                EXPECT_EQ(result.error().ErrorCode, expectedErrorCode);
            }
        });
    }

    void StartDownloadToAsyncAndExpectStatus(BlockBlobClient& client,
        const std::filesystem::path& target,
        const DownloadToOptions& options,
        int& callbackCount,
        unsigned expectedStatus)
    {
        client.DownloadToAsync(target,
            options,
            [&callbackCount, expectedStatus](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().StatusCode, expectedStatus);
        });
    }

    void StartDownloadToAsyncAndExpectCancellation(BlockBlobClient& client,
        std::ostream& out,
        const DownloadToOptions& options,
        int& callbackCount,
        const HttpRequestOptions& requestOptions)
    {
        client.DownloadToAsync(out,
            options,
            [&](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::operation_canceled));
        },
            requestOptions);
    }

    [[nodiscard]] std::size_t CompleteNewestUntilCallback(FakeHttpClient& httpClient,
        std::size_t maxPending,
        int& callbackCount)
    {
        std::size_t peakPending = 0;
        for (int guard = 0; guard < 100 && callbackCount == 0; ++guard)
        {
            peakPending = std::max(peakPending, httpClient.PendingCount());
            EXPECT_LE(httpClient.PendingCount(), maxPending);
            EXPECT_TRUE(CompleteNewest(httpClient));
            httpClient.Poll();
        }
        return peakPending;
    }

    void VerifyIfMatchHeaderOnChunkRequests(const FakeHttpClient& httpClient, std::string_view etag)
    {
        for (std::size_t index = 1; index < httpClient.RequestCount(); ++index)
        {
            EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(index).Request, "If-Match"), etag);
        }
    }

    void CreateFileWithContents(const std::filesystem::path& path, std::string_view contents)
    {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output << contents;
    }
} // namespace

TEST(T11_ParallelDownloadAcceptanceTests, OutOfOrderCompletionIsWrittenInOrderToANonSeekableStream)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlockBlobClient client{httpClient, BuildOptions()};

    AppendOnlyBuffer buffer;
    std::ostream out(&buffer);
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, out, Parallel(3U, 3U), callbackCount, content.size());

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);

    const std::size_t peakPending = CompleteNewestUntilCallback(httpClient, 3U, callbackCount);

    ASSERT_EQ(callbackCount, 1);
    EXPECT_EQ(peakPending, 3U);
    EXPECT_EQ(buffer.str(), content);
    EXPECT_EQ(httpClient.RequestCount(), 6U);
    VerifyIfMatchHeaderOnChunkRequests(httpClient, "\"etag-a\"");
}

TEST(T11_ParallelDownloadAcceptanceTests, ReorderWindowBoundsBufferedChunksToConcurrency)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(30);
    EnqueueChunks(httpClient, content, 3U);
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, out, Parallel(3U, 3U), callbackCount, content.size());

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.RequestCount(), 4U); // probe + 3 chunks (3-5, 6-8, 9-11)

    // The two later chunks arrive while the first is still outstanding: they are buffered, and the
    // window (1 in flight + 2 buffered) is full, so nothing new is requested.
    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(6, 3))));
    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(9, 3))));
    EXPECT_EQ(httpClient.RequestCount(), 4U);
    EXPECT_EQ(out.str(), content.substr(0, 3));

    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(3, 3))));
    EXPECT_EQ(out.str(), content.substr(0, 12));
    EXPECT_EQ(httpClient.RequestCount(), 7U);

    httpClient.CompleteAll();
    httpClient.Poll();
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(out.str(), content);
}

TEST(T11_ParallelDownloadAcceptanceTests, ETagChangeMidDownloadReportsConditionNotMet)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content.substr(0, 6), content.size(), "\"etagA\""));
    EnqueueForRange(httpClient,
        RangeHeader(6, 6),
        HttpResponse{412,
            MakeCanonicalSuccessHeaders({{"x-ms-error-code", "ConditionNotMet"}, {"ETag", "\"etagB\""}}),
            ""});
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectStatus(client, out, Parallel(6U, 2U), callbackCount, 412U, "ConditionNotMet");
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "If-Match"), "\"etagA\"");
}

TEST(T11_ParallelDownloadAcceptanceTests, ChunkFailureCancelsInFlightChunksAndReportsOnce)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    EnqueueForRange(httpClient, RangeHeader(0, 4), RangeResponse(0, content.substr(0, 4), content.size()));
    EnqueueForRange(httpClient, RangeHeader(4, 4), RangeResponse(4, content.substr(4, 4), content.size()));
    EnqueueForRange(httpClient,
        RangeHeader(8, 4),
        HttpResponse{500, MakeCanonicalSuccessHeaders({{"x-ms-error-code", "InternalError"}}), ""});
    EnqueueForRange(httpClient, RangeHeader(12, 4), RangeResponse(12, content.substr(12, 4), content.size()));
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectStatus(client, out, Parallel(4U, 3U), callbackCount, 500U);

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);
    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(8, 4))));
    httpClient.Poll();

    // The other in-flight chunks were cancelled through their cancellation slots and drained.
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 4U);
}

TEST(T11_ParallelDownloadAcceptanceTests, ParentCancellationCancelsAllInFlightChunks)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(40);
    EnqueueChunks(httpClient, content, 4U);
    BlockBlobClient client{httpClient, BuildOptions()};

    boost::asio::cancellation_signal cancel;
    HttpRequestOptions requestOptions;
    requestOptions.SetCancellationSlot(cancel.slot());
    std::ostringstream out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectCancellation(client, out, Parallel(4U, 4U), callbackCount, requestOptions);

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 4U);
    cancel.emit(boost::asio::cancellation_type::all);
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(httpClient.RequestCount(), 5U);
}

TEST(T11_ParallelDownloadAcceptanceTests, ParallelFileDownloadReplacesTheTargetOnSuccess)
{
    const std::filesystem::path target =
        std::filesystem::temp_directory_path() / "azure-client-t11-parallel-success.bin";
    CreateFileWithContents(target, "old contents that are longer than the new blob");

    FakeHttpClient httpClient;
    const std::string content = MakeContent(25);
    EnqueueChunks(httpClient, content, 4U);
    PageBlobClient client{httpClient, BuildOptions()};

    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, target, Parallel(4U, 3U), callbackCount, content.size());
    const std::size_t peakPending = CompleteNewestUntilCallback(httpClient, 3U, callbackCount);

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(peakPending, 3U);
    EXPECT_EQ(ReadFile(target), content);
    EXPECT_FALSE(HasPartialFiles(target));
    std::error_code ignored;
    std::filesystem::remove(target, ignored);
}

TEST(T11_ParallelDownloadAcceptanceTests, ParallelFileDownloadFailureLeavesTheTargetUntouched)
{
    const std::filesystem::path target =
        std::filesystem::temp_directory_path() / "azure-client-t11-parallel-failure.bin";
    CreateFileWithContents(target, "previous");

    FakeHttpClient httpClient;
    const std::string content = MakeContent(16);
    EnqueueForRange(httpClient, RangeHeader(0, 4), RangeResponse(0, content.substr(0, 4), content.size()));
    EnqueueForRange(httpClient, RangeHeader(4, 4), RangeResponse(4, content.substr(4, 4), content.size()));
    EnqueueForRange(httpClient,
        RangeHeader(8, 4),
        HttpResponse{412, MakeCanonicalSuccessHeaders({{"x-ms-error-code", "ConditionNotMet"}}), ""});
    EnqueueForRange(httpClient, RangeHeader(12, 4), RangeResponse(12, content.substr(12, 4), content.size()));
    BlockBlobClient client{httpClient, BuildOptions()};

    int callbackCount = 0;
    StartDownloadToAsyncAndExpectStatus(client, target, Parallel(4U, 2U), callbackCount, 412U);
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(ReadFile(target), "previous");
    EXPECT_FALSE(HasPartialFiles(target));
    std::error_code ignored;
    std::filesystem::remove(target, ignored);
}

TEST(B05_DownloadIgnoredRangeTests, OffsetRangeAnswered200IsInvalidResponse)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), content};
    BlockBlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 10U};
    std::ostringstream out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out,
        options,
        [&](DownloadToResult value)
    {
        result = std::move(value);
    });
    ASSERT_TRUE(httpClient.RunUntil([&]
    {
        return result.has_value();
    }));

    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
    EXPECT_TRUE(out.str().empty());
}

TEST(B05_DownloadIgnoredRangeTests, LengthRangeAnswered200IsTruncatedToTheRequestedLength)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), content};
    BlockBlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 5U};
    std::ostringstream out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out,
        options,
        [&](DownloadToResult value)
    {
        result = std::move(value);
    });
    ASSERT_TRUE(httpClient.RunUntil([&]
    {
        return result.has_value();
    }));

    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(result->value().Value().BytesWritten, 5U);
    EXPECT_EQ(out.str(), content.substr(0, 5U));
}
