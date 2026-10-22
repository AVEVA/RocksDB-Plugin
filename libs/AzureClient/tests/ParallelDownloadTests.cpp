// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/BlobDownload.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <future>
#include <ios>
#include <iterator>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

// T11: parallel chunked DownloadToAsync (Concurrency > 1).

namespace {
using AVEVA::HttpRequestOptions;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobClientOptions;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::DownloadToOptions;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::Models::DownloadBlobToResult;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::EnqueueChunks;
using AVEVA::AzureClient::Tests::EnqueueForRange;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::FindRequestByRange;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RangeHeader;
using AVEVA::AzureClient::Tests::RangeResponse;

using DownloadToResult = std::expected<Response<DownloadBlobToResult>, BlobStorageError>;

[[nodiscard]] BlobClientOptions BuildOptions() { return MakeBlobClientOptions("logs", "t11.txt"); }

[[nodiscard]] std::string MakeContent(std::size_t size) {
    std::string content;
    for (std::size_t i = 0; i < size; ++i) {
        content += static_cast<char>('a' + (i % 26U));
    }
    return content;
}

// Tags the concurrency parameter with its own type so it's never adjacent-and-same-type with
// the chunk size parameter (see bugprone-easily-swappable-parameters).
struct Concurrency {
    std::size_t Value;

    constexpr Concurrency(std::size_t value) noexcept : Value(value) {}
};

[[nodiscard]] DownloadToOptions Parallel(std::size_t chunkSize, Concurrency concurrency) {
    DownloadToOptions options;
    options.ChunkSize = chunkSize;
    options.Concurrency = concurrency.Value;
    return options;
}

// Download target in the temp directory, named after the running test and removed on scope exit.
struct TempFile {
    std::filesystem::path Path;

    TempFile() : Path(std::filesystem::temp_directory_path() / Name()) { Remove(); }

    ~TempFile() { Remove(); }

    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;
    TempFile(TempFile&&) = delete;
    TempFile& operator=(TempFile&&) = delete;

  private:
    [[nodiscard]] static std::string Name() {
        return std::string("azure-client-pd-") + ::testing::UnitTest::GetInstance()->current_test_info()->name();
    }

    void Remove() const {
        std::error_code ignored;
        std::filesystem::remove(Path, ignored);
    }
};

// Completes the most recently issued pending request.
[[nodiscard]] bool CompleteNewest(FakeHttpClient& httpClient) {
    for (std::size_t index = httpClient.RequestCount(); index-- > 0U;) {
        if (httpClient.CompleteRequest(index)) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return std::string{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

[[nodiscard]] bool HasPartialFiles(const std::filesystem::path& target) {
    const std::string prefix = target.filename().string() + ".partial-";
    const std::filesystem::directory_iterator entries(target.parent_path());
    return std::any_of(begin(entries), end(entries),
                       [&prefix](const auto& entry) { return entry.path().filename().string().starts_with(prefix); });
}

void StartDownloadToAsyncAndExpectSuccess(BlobClient& client, const std::filesystem::path& target,
                                          const DownloadToOptions& options, int& callbackCount,
                                          std::size_t expectedBytes) {
    client.DownloadToAsync(target, options, [&callbackCount, expectedBytes](DownloadToResult result) {
        ++callbackCount;
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().BytesWritten, expectedBytes);
    });
}

void StartDownloadToAsyncAndExpectStatus(BlobClient& client, const std::filesystem::path& target,
                                         const DownloadToOptions& options, int& callbackCount, unsigned expectedStatus,
                                         std::string_view expectedErrorCode = {}) {
    client.DownloadToAsync(target, options,
                           [&callbackCount, expectedStatus, expectedErrorCode](DownloadToResult result) {
                               ++callbackCount;
                               ASSERT_FALSE(result.has_value());
                               EXPECT_EQ(result.error().StatusCode, expectedStatus);
                               if (!expectedErrorCode.empty()) {
                                   EXPECT_EQ(result.error().ErrorCode, expectedErrorCode);
                               }
                           });
}

void StartDownloadToAsyncAndExpectCancellation(BlobClient& client, const std::filesystem::path& target,
                                               const DownloadToOptions& options, int& callbackCount,
                                               const HttpRequestOptions& requestOptions) {
    client.DownloadToAsync(
        target, options,
        [&](DownloadToResult result) {
            ++callbackCount;
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::operation_canceled));
        },
        requestOptions);
}

[[nodiscard]] std::size_t CompleteNewestUntilCallback(FakeHttpClient& httpClient, std::size_t maxPending,
                                                      int& callbackCount) {
    std::size_t peakPending = 0;
    for (int guard = 0; guard < 100 && callbackCount == 0; ++guard) {
        peakPending = std::max(peakPending, httpClient.PendingCount());
        EXPECT_LE(httpClient.PendingCount(), maxPending);
        EXPECT_TRUE(CompleteNewest(httpClient));
        httpClient.Poll();
    }
    return peakPending;
}

void VerifyIfMatchHeaderOnChunkRequests(const FakeHttpClient& httpClient, std::string_view etag) {
    for (std::size_t index = 1; index < httpClient.RequestCount(); ++index) {
        EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(index).Request, "If-Match"), etag);
    }
}

void CreateFileWithContents(const std::filesystem::path& path, std::string_view contents) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << contents;
}
} // namespace

TEST(T11_ParallelDownloadAcceptanceTests, OutOfOrderCompletionIsWrittenInOrderToAFile) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, out.Path, Parallel(3U, 3U), callbackCount, content.size());

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);

    const std::size_t peakPending = CompleteNewestUntilCallback(httpClient, 3U, callbackCount);

    ASSERT_EQ(callbackCount, 1);
    EXPECT_EQ(peakPending, 3U);
    EXPECT_EQ(ReadFile(out.Path), content);
    EXPECT_EQ(httpClient.RequestCount(), 6U);
    VerifyIfMatchHeaderOnChunkRequests(httpClient, "\"etag-a\"");
}

TEST(T11_ParallelDownloadAcceptanceTests, FileWritesRunOnTheInjectedWriteScheduler) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    boost::asio::thread_pool pool{1U};
    const std::size_t clientWritesBefore = AVEVA::AzureClient::Tests::DownloadWriters().Started.load();
    auto options = Parallel(3U, 3U);
    options.WriteScheduler = pool.get_executor();

    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, out.Path, options, callbackCount, content.size());
    // The injected pool is not observed by Poll(), so wait for its work explicitly.
    ASSERT_TRUE(httpClient.WaitForRequest(1U));
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    for (int guard = 0; guard < 100 && callbackCount == 0; ++guard) {
        ASSERT_TRUE(httpClient.RunUntil([&] { return httpClient.PendingCount() > 0U || callbackCount > 0; }));
        if (callbackCount == 0) {
            ASSERT_TRUE(CompleteNewest(httpClient));
        }
    }

    ASSERT_EQ(callbackCount, 1);
    // The injected scheduler takes precedence over the HTTP client's own writer.
    EXPECT_EQ(AVEVA::AzureClient::Tests::DownloadWriters().Started.load(), clientWritesBefore);
    EXPECT_EQ(ReadFile(out.Path), content);
}

TEST(T11_ParallelDownloadAcceptanceTests, ThrowWhileStartingALaterChunkFailsOnceWithoutEscaping) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U);
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    client.DownloadToAsync(out.Path, Parallel(3U, 3U), [&callbackCount](DownloadToResult result) {
        ++callbackCount;
        EXPECT_FALSE(result.has_value());
    });

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    httpClient.FailNextInitiation(std::make_exception_ptr(std::bad_alloc{}));
    EXPECT_NO_THROW({
        EXPECT_TRUE(httpClient.CompleteRequest(0U));
        httpClient.CompleteAll();
        httpClient.Poll();
    });

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_FALSE(std::filesystem::exists(out.Path));
    EXPECT_TRUE(httpClient.RunUntil([&] { return !HasPartialFiles(out.Path); }));
}

TEST(T11_ParallelDownloadAcceptanceTests, ReorderWindowBoundsBufferedChunksToConcurrency) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(30);
    EnqueueChunks(httpClient, content, 3U);
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, out.Path, Parallel(3U, 3U), callbackCount, content.size());

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.RequestCount(), 4U); // probe + 3 chunks (3-5, 6-8, 9-11)

    // The two later chunks arrive while the first is still outstanding: they are buffered, and the
    // window (1 in flight + 2 buffered) is full, so nothing new is requested.
    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(6, 3))));
    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(9, 3))));
    EXPECT_EQ(httpClient.RequestCount(), 4U);

    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(3, 3))));
    EXPECT_EQ(httpClient.RequestCount(), 7U);

    httpClient.CompleteAll();
    httpClient.Poll();
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(ReadFile(out.Path), content);
}

TEST(T11_ParallelDownloadAcceptanceTests, ETagChangeMidDownloadReportsConditionNotMet) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content.substr(0, 6), content.size(), "\"etagA\""));
    EnqueueForRange(
        httpClient, RangeHeader(6, 6),
        HttpResponse{412, MakeCanonicalSuccessHeaders({{"x-ms-error-code", "ConditionNotMet"}, {"ETag", "\"etagB\""}}),
                     ""});
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectStatus(client, out.Path, Parallel(6U, 2U), callbackCount, 412U, "ConditionNotMet");
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "If-Match"), "\"etagA\"");
}

TEST(T118_DownloadVersionValidatorTests, FirstResponseWithoutETagRejectsFurtherChunks) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content.substr(0, 6), content.size(), ""));
    EnqueueForRange(httpClient, RangeHeader(6, 6), RangeResponse(6, content.substr(6), content.size(), "\"other\""));
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    client.DownloadToAsync(out.Path, Parallel(6U, 2U), [&callbackCount](DownloadToResult result) {
        ++callbackCount;
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
    });
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(T118_DownloadVersionValidatorTests, CallerIfMatchPermitsChunksWhenTheResponseHasNoETag) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content.substr(0, 6), content.size(), ""));
    EnqueueForRange(httpClient, RangeHeader(6, 6), RangeResponse(6, content.substr(6), content.size(), ""));
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    auto options = Parallel(6U, 2U);
    options.Conditions.IfMatch = "\"caller\"";
    StartDownloadToAsyncAndExpectSuccess(client, out.Path, options, callbackCount, content.size());
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(ReadFile(out.Path), content);
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "If-Match"), "\"caller\"");
}

TEST(T118_DownloadVersionValidatorTests, SingleResponseWithoutETagStillSucceeds) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(6);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content, content.size(), ""));
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, out.Path, Parallel(6U, 2U), callbackCount, content.size());
    httpClient.CompleteAll();
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(ReadFile(out.Path), content);
}

TEST(T11_ParallelDownloadAcceptanceTests, ChunkFailureCancelsInFlightChunksAndReportsOnce) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    EnqueueForRange(httpClient, RangeHeader(0, 4), RangeResponse(0, content.substr(0, 4), content.size()));
    EnqueueForRange(httpClient, RangeHeader(4, 4), RangeResponse(4, content.substr(4, 4), content.size()));
    EnqueueForRange(httpClient, RangeHeader(8, 4),
                    HttpResponse{500, MakeCanonicalSuccessHeaders({{"x-ms-error-code", "InternalError"}}), ""});
    EnqueueForRange(httpClient, RangeHeader(12, 4), RangeResponse(12, content.substr(12, 4), content.size()));
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectStatus(client, out.Path, Parallel(4U, 3U), callbackCount, 500U);

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);
    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(8, 4))));
    httpClient.Poll();

    // The other in-flight chunks were cancelled through their cancellation slots and drained.
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 4U);
}

TEST(T11_ParallelDownloadAcceptanceTests, ParentCancellationCancelsAllInFlightChunks) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(40);
    EnqueueChunks(httpClient, content, 4U);
    BlobClient client{httpClient, BuildOptions()};

    boost::asio::cancellation_signal cancel;
    HttpRequestOptions requestOptions;
    requestOptions.SetCancellationSlot(cancel.slot());
    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectCancellation(client, out.Path, Parallel(4U, 4U), callbackCount, requestOptions);

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 4U);
    cancel.emit(boost::asio::cancellation_type::all);
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(httpClient.RequestCount(), 5U);
}

TEST(T11_ParallelDownloadAcceptanceTests, ExplicitSlotEmittedAfterCompletionAndClientDestructionTouchesNothing) {
    boost::asio::io_context context;
    boost::asio::cancellation_signal cancel;
    const std::string content = MakeContent(8);
    TempFile out;
    int callbackCount = 0;
    {
        FakeHttpClient httpClient{context};
        EnqueueChunks(httpClient, content, 4U);
        BlobClient client{httpClient, BuildOptions()};
        HttpRequestOptions requestOptions;
        requestOptions.SetCancellationSlot(cancel.slot());
        client.DownloadToAsync(
            out.Path, Parallel(4U, 2U),
            [&](DownloadToResult result) {
                ++callbackCount;
                EXPECT_TRUE(result.has_value());
            },
            requestOptions);
        ASSERT_TRUE(httpClient.RunUntil([&] {
            while (CompleteNewest(httpClient)) {
            }
            return callbackCount > 0;
        }));
        context.restart();
        context.poll();
    }

    // The operation is gone, so the leftover handler must not post anything (nor read the destroyed transport).
    cancel.emit(boost::asio::cancellation_type::terminal);
    context.restart();
    EXPECT_EQ(context.poll(), 0U);
    EXPECT_EQ(callbackCount, 1);
}

TEST(T11_ParallelDownloadAcceptanceTests, PartialCancellationDoesNotAbortTheDownload) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U);
    BlobClient client{httpClient, BuildOptions()};

    boost::asio::cancellation_signal cancel;
    HttpRequestOptions requestOptions;
    requestOptions.SetCancellationSlot(cancel.slot());
    TempFile out;
    int callbackCount = 0;
    client.DownloadToAsync(
        out.Path, Parallel(3U, 3U),
        [&](DownloadToResult result) {
            ++callbackCount;
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->Value().BytesWritten, content.size());
        },
        requestOptions);

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);
    cancel.emit(boost::asio::cancellation_type::partial);
    httpClient.Poll();
    ASSERT_EQ(callbackCount, 0);
    ASSERT_EQ(httpClient.PendingCount(), 3U);

    static_cast<void>(CompleteNewestUntilCallback(httpClient, 3U, callbackCount));

    ASSERT_EQ(callbackCount, 1);
    EXPECT_EQ(ReadFile(out.Path), content);
}

TEST(T11_ParallelDownloadAcceptanceTests, TerminalCancellationAfterPartialStillAbortsTheDownload) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(40);
    EnqueueChunks(httpClient, content, 4U);
    BlobClient client{httpClient, BuildOptions()};

    boost::asio::cancellation_signal cancel;
    HttpRequestOptions requestOptions;
    requestOptions.SetCancellationSlot(cancel.slot());
    TempFile out;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectCancellation(client, out.Path, Parallel(4U, 4U), callbackCount, requestOptions);

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 4U);
    cancel.emit(boost::asio::cancellation_type::partial);
    httpClient.Poll();
    ASSERT_EQ(callbackCount, 0);
    cancel.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
}

TEST(T11_ParallelDownloadAcceptanceTests, ParallelFileDownloadReplacesTheTargetOnSuccess) {
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

TEST(T11_ParallelDownloadAcceptanceTests, ParallelFileDownloadFailureLeavesTheTargetUntouched) {
    const std::filesystem::path target =
        std::filesystem::temp_directory_path() / "azure-client-t11-parallel-failure.bin";
    CreateFileWithContents(target, "previous");

    FakeHttpClient httpClient;
    const std::string content = MakeContent(16);
    EnqueueForRange(httpClient, RangeHeader(0, 4), RangeResponse(0, content.substr(0, 4), content.size()));
    EnqueueForRange(httpClient, RangeHeader(4, 4), RangeResponse(4, content.substr(4, 4), content.size()));
    EnqueueForRange(httpClient, RangeHeader(8, 4),
                    HttpResponse{412, MakeCanonicalSuccessHeaders({{"x-ms-error-code", "ConditionNotMet"}}), ""});
    EnqueueForRange(httpClient, RangeHeader(12, 4), RangeResponse(12, content.substr(12, 4), content.size()));
    BlobClient client{httpClient, BuildOptions()};

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

namespace {
// Answers every request with a 206 whose Content-Range is `contentRange` (omitted when empty).
[[nodiscard]] std::optional<DownloadToResult> DownloadAgainstFixed206(const std::string& body,
                                                                      const std::string& contentRange) {
    FakeHttpClient httpClient;
    auto headers = MakeCanonicalSuccessHeaders();
    if (!contentRange.empty()) {
        headers.emplace_back("Content-Range", contentRange);
    }
    httpClient.DefaultResponse() = HttpResponse{206, std::move(headers), body};
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out.Path, DownloadToOptions{}, [&](DownloadToResult value) { result = std::move(value); });
    (void)httpClient.RunUntil([&] { return result.has_value(); });
    EXPECT_FALSE(std::filesystem::exists(out.Path));
    return result;
}
} // namespace

TEST(T08_DownloadContentRangeTests, PartialContentWithoutContentRangeIsInvalidResponse) {
    const auto result = DownloadAgainstFixed206(MakeContent(10), "");
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
}

TEST(T08_DownloadContentRangeTests, ContentRangeEndThatDisagreesWithTheBodyIsInvalidResponse) {
    const auto result = DownloadAgainstFixed206(MakeContent(10), "bytes 0-4/10");
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
}

TEST(AC_R01_DownloadRangeOverflowTests, OffsetWhoseProbeRangeWouldWrapIsRejectedWithoutSendingARequest) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = std::numeric_limits<std::uint64_t>::max() - 1U};
    TempFile out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out.Path, options, [&](DownloadToResult value) { result = std::move(value); });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));

    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
    EXPECT_FALSE(std::filesystem::exists(out.Path));
}

TEST(AC_R01_DownloadRangeOverflowTests, LengthThatOverflowsTheRangeEndIsRejected) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range =
        AVEVA::AzureClient::Models::BlobByteRange{.Offset = 10U, .Length = std::numeric_limits<std::uint64_t>::max()};
    TempFile out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out.Path, options, [&](DownloadToResult value) { result = std::move(value); });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));

    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::invalid_argument));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}
namespace {
struct NonStandardThrow {};

// Throws a type that does not derive from std::exception, from Reserve or Write.
template <bool InlineWrites> class ThrowingSink final {
  public:
    explicit ThrowingSink(bool throwOnReserve) : m_throwOnReserve(throwOnReserve) {}

    void Reserve(std::uint64_t /*size*/) {
        if (m_throwOnReserve) {
            throw NonStandardThrow{};
        }
    }

    static constexpr bool WritesInline = InlineWrites;

    [[nodiscard]] bool Write(std::string /*data*/) { throw NonStandardThrow{}; }

  private:
    bool m_throwOnReserve;
};

template <bool InlineWrites>
[[nodiscard]] std::optional<AVEVA::AzureClient::Private::DownloadDetail::SummaryResult>
DownloadIntoThrowingSink(bool throwOnReserve) {
    using AVEVA::AzureClient::NoTokenCredential;
    using AVEVA::AzureClient::Private::BasicBlobTarget;
    FakeHttpClient httpClient;
    const std::string content = MakeContent(10);
    httpClient.DefaultResponse() =
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-9/10"}}), content};
    const auto target = std::make_shared<const BasicBlobTarget<NoTokenCredential>>(
        BasicBlobTarget<NoTokenCredential>{AVEVA::AzureClient::Private::MakeBlobTarget(BuildOptions()), nullptr});

    DownloadToOptions options;
    std::optional<AVEVA::AzureClient::Private::DownloadDetail::SummaryResult> result;
    AVEVA::AzureClient::Private::DownloadDetail::StartDownload(
        httpClient, target, options, ThrowingSink<InlineWrites>{throwOnReserve},
        [&](AVEVA::AzureClient::Private::DownloadDetail::SummaryResult value) { result = std::move(value); },
        AVEVA::AzureClient::AzureRequestOptions{});
    (void)httpClient.RunUntil([&] { return result.has_value(); });
    return result;
}
} // namespace

class DiscardingSink final {
  public:
    static constexpr bool WritesInline = true;

    void Reserve(std::uint64_t /*size*/) {}

    [[nodiscard]] bool Write(std::string /*data*/) { return true; }
};

TEST(AC_R15_DownloadBufferCapTests, ChunksInFlightAreCappedByBytesNotJustByCount) {
    using AVEVA::AzureClient::NoTokenCredential;
    using AVEVA::AzureClient::Private::BasicBlobTarget;
    constexpr std::size_t ChunkSize = std::size_t{64} * 1024U * 1024U;

    FakeHttpClient httpClient;
    const auto target = std::make_shared<const BasicBlobTarget<NoTokenCredential>>(
        BasicBlobTarget<NoTokenCredential>{AVEVA::AzureClient::Private::MakeBlobTarget(BuildOptions()), nullptr});
    DownloadToOptions options;
    options.ChunkSize = ChunkSize;
    options.Concurrency = 16;
    httpClient.EnqueueDeferredResponse(
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-67108863/1073741824"}}),
                     std::string(ChunkSize, 'x')});
    for (int i = 0; i < 16; ++i) {
        httpClient.EnqueueDeferredResponse(
            HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-0/1073741824"}}), "x"});
    }
    AVEVA::AzureClient::Private::DownloadDetail::StartDownload(
        httpClient, target, options, DiscardingSink{},
        [](AVEVA::AzureClient::Private::DownloadDetail::SummaryResult) {}, AVEVA::AzureClient::AzureRequestOptions{});
    ASSERT_TRUE(httpClient.RunUntil([&] { return httpClient.PendingCount() == 1U; }));
    ASSERT_TRUE(httpClient.CompleteNext());
    // 256 MiB of buffering at 64 MiB per chunk allows 4 chunk requests, not the 16 that Concurrency alone allows.
    ASSERT_TRUE(httpClient.RunUntil([&] { return httpClient.PendingCount() >= 4U; }));
    EXPECT_EQ(httpClient.PendingCount(), 4U);
}

TEST(AC_R05_DownloadSinkExceptionTests, NonStandardExceptionFromAnInlineWriteIsAnIoError) {
    const auto result = DownloadIntoThrowingSink<true>(false);
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::io_error));
}

TEST(AC_R05_DownloadSinkExceptionTests, NonStandardExceptionFromAScheduledWriteIsAnIoError) {
    const auto result = DownloadIntoThrowingSink<false>(false);
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::io_error));
}

TEST(AC_R05_DownloadSinkExceptionTests, NonStandardExceptionFromReserveIsAnIoError) {
    const auto result = DownloadIntoThrowingSink<true>(true);
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::io_error));
}

TEST(B05_DownloadIgnoredRangeTests, OffsetRangeAnswered200IsInvalidResponse) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), content};
    BlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 10U};
    TempFile out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out.Path, options, [&](DownloadToResult value) { result = std::move(value); });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));

    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
    EXPECT_FALSE(std::filesystem::exists(out.Path));
}

TEST(B05_DownloadIgnoredRangeTests, LengthRangeAnswered200IsTruncatedToTheRequestedLength) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), content};
    BlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 5U};
    TempFile out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out.Path, options, [&](DownloadToResult value) { result = std::move(value); });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));

    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(result->value().Value().BytesWritten, 5U);
    EXPECT_EQ(ReadFile(out.Path), content.substr(0, 5U));
}

TEST(B05_DownloadIgnoredRangeTests, FullBodyAnswered200IsRejectedWhenOverMaxSize) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), content};
    BlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.MaxSize = 10U;
    TempFile out;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(out.Path, options, [&](DownloadToResult value) { result = std::move(value); });
    ASSERT_TRUE(httpClient.RunUntil([&] { return result.has_value(); }));

    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::file_too_large));
}

TEST(A3_DownloadIntoSpanTests, FullAndPartialRangesAreWrittenByteForByte) {
    const std::string content = MakeContent(18);
    for (const auto [offset, length] : {std::pair{0U, 18U}, std::pair{4U, 7U}}) {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(RangeResponse(offset, content.substr(offset, length), content.size()), {}, true);
        PageBlobClient client{httpClient, BuildOptions()};

        std::string destination(length, '\0');
        AVEVA::AzureClient::DownloadBlobOptions options;
        options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = offset, .Length = length};
        std::optional<DownloadToResult> result;
        client.DownloadToAsync(std::span<char>(destination), options,
                               [&](DownloadToResult r) { result = std::move(r); });
        ASSERT_TRUE(httpClient.CompleteRequest(0U));

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result->has_value());
        EXPECT_EQ(result->value().Value().BytesWritten, length);
        EXPECT_EQ(destination, content.substr(offset, length));
    }
}

TEST(A3_DownloadIntoSpanTests, BlobShorterThanTheRequestedRangeReportsTheBytesWritten) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    httpClient.EnqueueResponse(RangeResponse(0, content, content.size()), {}, true);
    PageBlobClient client{httpClient, BuildOptions()};

    std::string destination(20, '#');
    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0, .Length = 20};
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(std::span<char>(destination), options, [&](DownloadToResult r) { result = std::move(r); });
    ASSERT_TRUE(httpClient.CompleteRequest(0U));

    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(result->value().Value().BytesWritten, 18U);
    EXPECT_EQ(destination.substr(0, 18), content);
    EXPECT_EQ(destination.substr(18), "##");
}

TEST(A3_DownloadIntoSpanTests, ResponseLargerThanTheDestinationFailsWithoutWritingPastIt) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    httpClient.EnqueueResponse(RangeResponse(0, content, content.size()), {}, true);
    PageBlobClient client{httpClient, BuildOptions()};

    std::string storage(18, '#');
    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0, .Length = 18};
    std::optional<DownloadToResult> result;
    // Only the first 10 bytes are offered; the rest of `storage` acts as a guard region.
    client.DownloadToAsync(std::span<char>(storage).first(10), options,
                           [&](DownloadToResult r) { result = std::move(r); });
    ASSERT_TRUE(httpClient.CompleteRequest(0U));

    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
    EXPECT_EQ(storage.substr(10), std::string(8, '#'));
}

TEST(T11_ParallelDownloadAcceptanceTests, DownloadLargerThanMaxSizeFailsBeforeFetchingAnyChunk) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    DownloadToOptions options = Parallel(3U, 3U);
    options.MaxSize = 17U;
    int callbackCount = 0;
    client.DownloadToAsync(out.Path, options, [&callbackCount](DownloadToResult result) {
        ++callbackCount;
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::file_too_large));
    });
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    httpClient.Poll();

    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_FALSE(std::filesystem::exists(out.Path));
}

TEST(T11_ParallelDownloadAcceptanceTests, DownloadExactlyAtMaxSizeSucceeds) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    DownloadToOptions options = Parallel(3U, 3U);
    options.MaxSize = 18U;
    int callbackCount = 0;
    StartDownloadToAsyncAndExpectSuccess(client, out.Path, options, callbackCount, content.size());
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    static_cast<void>(CompleteNewestUntilCallback(httpClient, 3U, callbackCount));

    ASSERT_EQ(callbackCount, 1);
    EXPECT_EQ(ReadFile(out.Path), content);
}

TEST(T11_ParallelDownloadAcceptanceTests, SequentialChunksShareOneAggregateDeadline) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(9);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{200});
    TempFile out;
    int callbackCount = 0;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(
        out.Path, Parallel(3U, 1U),
        [&](DownloadToResult value) {
            ++callbackCount;
            result = std::move(value);
        },
        requestOptions);

    // Each request fits the 200 ms budget on its own, but together they overrun it.
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    std::this_thread::sleep_for(std::chrono::milliseconds{110});
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    std::this_thread::sleep_for(std::chrono::milliseconds{110});
    static_cast<void>(httpClient.CompleteRequest(1U));
    ASSERT_TRUE(httpClient.RunUntil([&] { return callbackCount > 0 || httpClient.RequestCount() > 2U; }));

    EXPECT_EQ(httpClient.RequestCount(), 2U);
    ASSERT_EQ(callbackCount, 1);
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::timed_out));
}

TEST(T11_ParallelDownloadAcceptanceTests, FileDownloadDelayedBeforeTheFirstGetStartsNoRequestAfterTheDeadline) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(9);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    boost::asio::thread_pool pool{1U};
    std::promise<void> release;
    boost::asio::post(pool, [gate = release.get_future()]() mutable { gate.wait(); });
    auto options = Parallel(3U, 1U);
    options.WriteScheduler = pool.get_executor();
    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{50});

    TempFile out;
    int callbackCount = 0;
    std::optional<DownloadToResult> result;
    client.DownloadToAsync(
        out.Path, options,
        [&](DownloadToResult value) {
            ++callbackCount;
            result = std::move(value);
        },
        requestOptions);
    // The file open is queued behind the blocked writer, so the budget runs out before the first GET could start.
    std::this_thread::sleep_for(std::chrono::milliseconds{150});
    release.set_value();
    ASSERT_TRUE(httpClient.RunUntil([&] { return callbackCount > 0 || httpClient.RequestCount() > 0U; }));
    pool.join();

    EXPECT_EQ(httpClient.RequestCount(), 0U);
    ASSERT_EQ(callbackCount, 1);
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_FALSE(HasPartialFiles(out.Path));
}

namespace {
// Makes chosen posts throw, as an allocation failure would. Writer posts are counted across every thread; posts to
// the I/O executor are counted only when made off the test thread (that is, by a writer).
struct PostFaults {
    std::thread::id Main = std::this_thread::get_id();
    std::atomic<int> WriterPosts{0};
    std::atomic<int> FailWriterPost{0};
    std::atomic<int> WriterThreadPosts{0};
    std::atomic<int> FailWriterThreadPost{0};
};

class FlakyExecutor {
  public:
    FlakyExecutor(FakeHttpClient::executor_type inner, PostFaults* faults) : m_inner(inner), m_faults(faults) {}

    template <class F> void execute(F&& task) const {
        if (std::this_thread::get_id() != m_faults->Main &&
            m_faults->WriterThreadPosts.fetch_add(1) + 1 == m_faults->FailWriterThreadPost.load()) {
            throw std::bad_alloc{};
        }
        boost::asio::post(m_inner, std::forward<F>(task));
    }
    [[nodiscard]] FlakyExecutor require(boost::asio::execution::blocking_t::never_t) const noexcept { return *this; }
    // Lets the pipeline's deadline timer, which is a steady_timer, be built on this executor.
    [[nodiscard]] boost::asio::execution_context&
    query(boost::asio::execution::context_as_t<boost::asio::execution_context&> /*property*/) const noexcept {
        return boost::asio::query(m_inner, boost::asio::execution::context);
    }
    [[nodiscard]] FakeHttpClient::executor_type Inner() const noexcept { return m_inner; }
    bool operator==(const FlakyExecutor&) const noexcept = default;

  private:
    FakeHttpClient::executor_type m_inner;
    PostFaults* m_faults;
};

class FlakyWriterExecutor {
  public:
    explicit FlakyWriterExecutor(PostFaults* faults) : m_faults(faults) {}

    template <class F> void execute(F&& task) const {
        if (m_faults->WriterPosts.fetch_add(1) + 1 == m_faults->FailWriterPost.load()) {
            throw std::bad_alloc{};
        }
        FakeHttpClient::GetDownloadWriter().execute(std::forward<F>(task));
    }
    [[nodiscard]] FlakyWriterExecutor require(boost::asio::execution::blocking_t::never_t) const noexcept {
        return *this;
    }
    bool operator==(const FlakyWriterExecutor&) const noexcept = default;

  private:
    PostFaults* m_faults;
};

class FlakyRetryTimer : public FakeHttpClient::RetryTimer {
  public:
    explicit FlakyRetryTimer(const FlakyExecutor& executor) : RetryTimer(executor.Inner()) {}
};

// Delegates to a FakeHttpClient but hands out executors that can fail on demand.
class FlakyHttpClient {
  public:
    using executor_type = FlakyExecutor;
    using retry_timer_type = FlakyRetryTimer;

    FlakyHttpClient(FakeHttpClient& fake, PostFaults& faults) : m_fake(fake), m_faults(faults) {}

    [[nodiscard]] executor_type get_executor() const noexcept { return {m_fake.get_executor(), &m_faults}; }
    [[nodiscard]] FlakyWriterExecutor GetDownloadWriter() const noexcept { return FlakyWriterExecutor{&m_faults}; }

    template <class TToken> auto SendAsync(AVEVA::HttpRequest request, HttpRequestOptions options, TToken&& token) {
        return m_fake.SendAsync(std::move(request), options, std::forward<TToken>(token));
    }

  private:
    FakeHttpClient& m_fake;
    PostFaults& m_faults;
};

// Runs a one-chunk file download over an existing destination with the given post failures armed, and checks what
// every such failure must leave behind.
void ExpectFaultedFileDownloadFailsCleanly(int failWriterPost, int failWriterThreadPost) {
    using AVEVA::AzureClient::NoTokenCredential;
    using AVEVA::AzureClient::Private::BasicBlobTarget;

    FakeHttpClient fake;
    const std::string content = MakeContent(4);
    EnqueueChunks(fake, content, 8U);
    PostFaults faults;
    faults.FailWriterPost = failWriterPost;
    faults.FailWriterThreadPost = failWriterThreadPost;
    FlakyHttpClient httpClient{fake, faults};
    const auto target = std::make_shared<const BasicBlobTarget<NoTokenCredential>>(
        BasicBlobTarget<NoTokenCredential>{AVEVA::AzureClient::Private::MakeBlobTarget(BuildOptions()), nullptr});

    TempFile out;
    CreateFileWithContents(out.Path, "old");
    int callbackCount = 0;
    std::optional<DownloadToResult> result;
    EXPECT_NO_THROW({
        AVEVA::AzureClient::Private::DownloadBlobToFileAsync(
            httpClient, target, DownloadToOptions{}, out.Path,
            [&](DownloadToResult value) {
                ++callbackCount;
                result = std::move(value);
            },
            AVEVA::AzureClient::AzureRequestOptions{});
        ASSERT_TRUE(fake.RunUntil([&] { return callbackCount > 0 || fake.RequestCount() > 0U; }));
        if (callbackCount == 0) {
            ASSERT_TRUE(fake.CompleteRequest(0U));
        }
        ASSERT_TRUE(fake.RunUntil([&] { return callbackCount > 0; }));
    });
    fake.SettleWriters();

    ASSERT_EQ(callbackCount, 1);
    ASSERT_TRUE(result.has_value());
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(ReadFile(out.Path), "old");
    EXPECT_FALSE(HasPartialFiles(out.Path));
}
} // namespace

TEST(T114_DownloadWriterHandoffTests, FailureOfAWritersReturnPostFailsTheDownloadOnceAndCleansUp) {
    ExpectFaultedFileDownloadFailsCleanly(0, 2);
}

TEST(T114_DownloadWriterHandoffTests, FailureOfTheFileStartHandoffFailsTheDownloadOnceAndCleansUp) {
    ExpectFaultedFileDownloadFailsCleanly(0, 1);
}

TEST(T114_DownloadWriterHandoffTests, FailureToScheduleFinalizationFailsTheDownloadOnceAndCleansUp) {
    ExpectFaultedFileDownloadFailsCleanly(3, 0);
}
