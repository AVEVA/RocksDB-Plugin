// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "DownloadTestSupport.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <cstddef>
#include <gtest/gtest.h>

#include <expected>
#include <ios>
#include <ostream>
#include <sstream>
#include <streambuf>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

using AVEVA::AzureClient::Tests::ValueOrFail;

// T04: chunked DownloadToAsync / DownloadAsync (sequential behaviour, Concurrency = 1).

namespace
{
    using AVEVA::HttpRequestOptions;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::DownloadToOptions;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::DownloadBlobResult;
    using AVEVA::AzureClient::Models::DownloadBlobToResult;
    using AVEVA::AzureClient::Tests::DefaultETag;
    using AVEVA::AzureClient::Tests::EnqueueChunks;
    using AVEVA::AzureClient::Tests::EnqueueForRange;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
    using AVEVA::AzureClient::Tests::RangeHeader;
    using AVEVA::AzureClient::Tests::RangeResponse;

    using DownloadToResult = std::expected<Response<DownloadBlobToResult>, BlobStorageError>;

    [[nodiscard]] BlobClientOptions BuildOptions()
    {
        return MakeBlobClientOptions("logs", "t04.txt");
    }

    [[nodiscard]] std::string MakeContent(std::size_t size)
    {
        std::string content;
        for (std::size_t i = 0; i < size; ++i)
        {
            content += static_cast<char>('0' + (i % 10U));
        }
        return content;
    }

    [[nodiscard]] std::string Header(const FakeHttpClient& httpClient, std::size_t index, std::string_view name)
    {
        return FakeHttpClient::FindHeaderValue(httpClient.RequestAt(index).Request, name);
    }

    void VerifySequentialChunksSizeMetrics(const Response<DownloadBlobToResult>& response)
    {
        EXPECT_EQ(response.Value().BytesWritten, 10U);
        EXPECT_EQ(response.Value().Properties.ContentLength, 10U);
    }

    void VerifySequentialChunksContentRange(const Response<DownloadBlobToResult>& response)
    {
        ASSERT_TRUE(response.Value().ContentRange.has_value());
        EXPECT_EQ(ValueOrFail(response.Value().ContentRange).Offset, 0U);
        EXPECT_EQ(ValueOrFail(response.Value().ContentRange).Length, 10U);
    }

    void VerifySequentialChunksETag(const Response<DownloadBlobToResult>& response)
    {
        EXPECT_EQ(response.Value().Properties.ETag, "\"etag-1\"");
    }

    void VerifySequentialChunksDownloadResult(const DownloadToResult& result, int& callbackCount)
    {
        ++callbackCount;
        ASSERT_TRUE(result.has_value());
        VerifySequentialChunksSizeMetrics(result.value());
        VerifySequentialChunksContentRange(result.value());
        VerifySequentialChunksETag(result.value());
    }

    void StartDownloadToAndVerifySequentialChunks(PageBlobClient& client,
        std::ostringstream& out,
        DownloadToOptions options,
        int& callbackCount)
    {
        client.DownloadToAsync(out,
            std::move(options),
            [&](DownloadToResult result)
        {
            VerifySequentialChunksDownloadResult(result, callbackCount);
        });
    }

    void StartDownloadToAndCountSuccess(BlockBlobClient& client,
        std::ostringstream& out,
        DownloadToOptions options,
        int& callbackCount)
    {
        client.DownloadToAsync(out,
            std::move(options),
            [&](DownloadToResult result)
        {
            ++callbackCount;
            EXPECT_TRUE(result.has_value());
        });
    }

    void StartDownloadToAndVerifyEmptyBlob(BlockBlobClient& client, std::ostringstream& out, int& callbackCount)
    {
        client.DownloadToAsync(out,
            [&](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, 0U);
            EXPECT_FALSE(result->Value().ContentRange.has_value());
        });
    }

    void PollSeveralTimes(FakeHttpClient& httpClient, int times)
    {
        for (int i = 0; i < times; ++i)
        {
            httpClient.Poll();
        }
    }

    void VerifyExplicitRangeDownloadResult(const DownloadToResult& result, int& callbackCount)
    {
        ++callbackCount;
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().BytesWritten, 7U);
        ASSERT_TRUE(result->Value().ContentRange.has_value());
        EXPECT_EQ(ValueOrFail(result->Value().ContentRange).Offset, 2U);
        EXPECT_EQ(ValueOrFail(result->Value().ContentRange).Length, 7U);
    }

    void StartDownloadToAndVerifyExplicitRange(BlockBlobClient& client,
        std::ostringstream& out,
        DownloadToOptions options,
        int& callbackCount)
    {
        client.DownloadToAsync(out,
            std::move(options),
            [&](DownloadToResult result)
        {
            VerifyExplicitRangeDownloadResult(result, callbackCount);
        });
    }

    void StartDownloadToAndVerifyIoError(BlockBlobClient& client,
        std::ostream& out,
        DownloadToOptions options,
        int& callbackCount)
    {
        client.DownloadToAsync(out,
            std::move(options),
            [&](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::io_error));
        });
    }

    void StartDownloadToAndVerifyCancellation(BlockBlobClient& client,
        std::ostringstream& out,
        DownloadToOptions options,
        int& callbackCount,
        HttpRequestOptions requestOptions)
    {
        client.DownloadToAsync(out,
            std::move(options),
            [&](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::operation_canceled));
        },
            requestOptions);
    }

    void StartDownloadToAndVerifyDefaultOptions(BlockBlobClient& client,
        std::ostringstream& out,
        int& callbackCount,
        std::size_t contentSize)
    {
        client.DownloadToAsync(out,
            [&callbackCount, contentSize](DownloadToResult result)
        {
            ++callbackCount;
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, contentSize);
        });
    }

    void PollUntilCallbackInvoked(FakeHttpClient& httpClient, int& callbackCount, int maxPolls)
    {
        for (int i = 0; i < maxPolls && callbackCount == 0; ++i)
        {
            httpClient.Poll();
        }
    }

    void VerifyDefaultChunkBodyLimits(const FakeHttpClient& httpClient, std::size_t minimumBodyLimit)
    {
        for (std::size_t index = 0; index < httpClient.RequestCount(); ++index)
        {
            // Each chunk's body limit leaves headroom above the 4 MiB chunk (the default limit is 8 MiB).
            EXPECT_GT(httpClient.RequestAt(index).Options.GetResponseBodyLimit(), minimumBodyLimit);
        }
    }

    using DownloadResult = std::expected<Response<DownloadBlobResult>, BlobStorageError>;

    void StartDownloadAsyncAndVerifyContent(BlockBlobClient& client, int& callbackCount, const std::string& content)
    {
        client.DownloadAsync([&](DownloadResult result)
        {
            ++callbackCount;
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->Value().Content == content);
            EXPECT_EQ(result->Value().Properties.ContentLength, content.size());
            EXPECT_EQ(result->Value().Properties.ETag, DefaultETag);
        });
    }

    // A stream that rejects every write.
    class FailingBuffer final : public std::streambuf
    {
      protected:
        int_type overflow(int_type /*unused*/) override
        {
            return traits_type::eof();
        }

        std::streamsize xsputn(const char* /*_Ptr*/, std::streamsize /*_Count*/) override
        {
            return 0;
        }
    };
} // namespace

TEST(T04_ConcurrencyTests, SequentialChunksAreRequestedOneAtATimeWithTheProbeETag)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(10);
    EnqueueChunks(httpClient, content, 4U, "\"etag-1\"");
    PageBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    int callbackCount = 0;
    DownloadToOptions options;
    options.ChunkSize = 4U;
    StartDownloadToAndVerifySequentialChunks(client, out, options, callbackCount);

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(Header(httpClient, 0, "Range"), "bytes=0-3");
    EXPECT_EQ(Header(httpClient, 0, "If-Match"), "");

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(Header(httpClient, 1, "Range"), "bytes=4-7");
    EXPECT_EQ(Header(httpClient, 1, "If-Match"), "\"etag-1\"");

    ASSERT_TRUE(httpClient.CompleteRequest(1U));
    ASSERT_EQ(httpClient.RequestCount(), 3U);
    EXPECT_EQ(Header(httpClient, 2, "Range"), "bytes=8-9");
    EXPECT_EQ(Header(httpClient, 2, "If-Match"), "\"etag-1\"");

    ASSERT_TRUE(httpClient.CompleteRequest(2U));
    httpClient.Poll();
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(out.str(), content);
    EXPECT_EQ(httpClient.RequestCount(), 3U);
}

TEST(T04_ConcurrencyTests, ExactMultipleOfTheChunkSizeNeedsNoExtraRequest)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(8);
    EnqueueChunks(httpClient, content, 4U);
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    int callbackCount = 0;
    DownloadToOptions options;
    options.ChunkSize = 4U;
    StartDownloadToAndCountSuccess(client, out, options, callbackCount);
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(out.str(), content);
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}

TEST(T04_ConcurrencyTests, CallerIfMatchIsSentOnEveryChunk)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(6);
    EnqueueChunks(httpClient, content, 4U);
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    DownloadToOptions options;
    options.ChunkSize = 4U;
    options.Conditions.IfMatch = "\"mine\"";
    int callbackCount = 0;
    StartDownloadToAndCountSuccess(client, out, options, callbackCount);
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_EQ(callbackCount, 1);
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(Header(httpClient, 0, "If-Match"), "\"mine\"");
    EXPECT_EQ(Header(httpClient, 1, "If-Match"), "\"mine\"");
}

TEST(T04_ConcurrencyTests, EmptyBlobFallsBackToAnUnrangedGetAfter416)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{416,
        MakeCanonicalSuccessHeaders({{"x-ms-error-code", "InvalidRange"}, {"Content-Range", "bytes */0"}}),
        ""});
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "0"}}), ""});
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    int callbackCount = 0;
    StartDownloadToAndVerifyEmptyBlob(client, out, callbackCount);
    PollSeveralTimes(httpClient, 5);

    EXPECT_EQ(callbackCount, 1);
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(Header(httpClient, 0, "Range"), "bytes=0-4194303");
    EXPECT_EQ(Header(httpClient, 1, "Range"), "");
    EXPECT_TRUE(out.str().empty());
}

TEST(T04_ConcurrencyTests, ExplicitRangeIsDownloadedInChunks)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(100);
    EnqueueForRange(httpClient, "bytes=2-5", RangeResponse(2, content.substr(2, 4), content.size()));
    EnqueueForRange(httpClient, "bytes=6-8", RangeResponse(6, content.substr(6, 3), content.size()));
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    DownloadToOptions options;
    options.ChunkSize = 4U;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 2U, .Length = 7U};
    int callbackCount = 0;
    StartDownloadToAndVerifyExplicitRange(client, out, options, callbackCount);
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(out.str(), content.substr(2, 7));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}

TEST(T04_ConcurrencyTests, StreamWriteFailureReportsIoError)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(10);
    EnqueueChunks(httpClient, content, 4U);
    BlockBlobClient client{httpClient, BuildOptions()};

    FailingBuffer buffer;
    std::ostream out(&buffer);
    DownloadToOptions options;
    options.ChunkSize = 4U;
    options.Concurrency = 2U;
    int callbackCount = 0;
    StartDownloadToAndVerifyIoError(client, out, options, callbackCount);
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
}

TEST(T04_ConcurrencyTests, CancellationBetweenChunksStopsTheDownload)
{
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueChunks(httpClient, content, 4U);
    BlockBlobClient client{httpClient, BuildOptions()};

    boost::asio::cancellation_signal cancel;
    HttpRequestOptions requestOptions;
    requestOptions.SetCancellationSlot(cancel.slot());

    std::ostringstream out;
    DownloadToOptions options;
    options.ChunkSize = 4U;
    int callbackCount = 0;
    StartDownloadToAndVerifyCancellation(client, out, options, callbackCount, requestOptions);

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    cancel.emit(boost::asio::cancellation_type::all);
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(out.str(), content.substr(0, 4));
}

TEST(T04_ConcurrencyTests, DefaultOptionsDownload20MiBWithFiveRangedRequests)
{
    constexpr std::size_t MiB = std::size_t{1024} * 1024U;
    const std::string content(20U * MiB, 'z');
    FakeHttpClient httpClient;
    for (std::size_t offset = 0; offset < content.size(); offset += 4U * MiB)
    {
        EnqueueForRange(httpClient,
            RangeHeader(offset, 4U * MiB),
            RangeResponse(offset, content.substr(offset, 4U * MiB), content.size()),
            false);
    }
    BlockBlobClient client{httpClient, BuildOptions()};

    std::ostringstream out;
    int callbackCount = 0;
    StartDownloadToAndVerifyDefaultOptions(client, out, callbackCount, content.size());
    PollUntilCallbackInvoked(httpClient, callbackCount, 20);

    ASSERT_EQ(callbackCount, 1);
    ASSERT_EQ(httpClient.RequestCount(), 5U);
    VerifyDefaultChunkBodyLimits(httpClient, 4U * MiB);
    EXPECT_TRUE(out.str() == content);
}

TEST(T04_ConcurrencyTests, InMemoryDownloadAboveTheDefaultBodyLimitIsChunked)
{
    constexpr std::size_t MiB = std::size_t{1024} * 1024U;
    const std::string content = MakeContent((9U * MiB) + 3U);
    FakeHttpClient httpClient;
    EnqueueChunks(httpClient, content, 4U * MiB);
    BlockBlobClient client{httpClient, BuildOptions()};

    int callbackCount = 0;
    StartDownloadAsyncAndVerifyContent(client, callbackCount, content);
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 3U);
}
