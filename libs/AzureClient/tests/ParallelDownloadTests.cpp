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
#include <boost/asio/cancellation_type.hpp>
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
using AVEVA::AzureClient::Tests::Operation;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::RangeHeader;
using AVEVA::AzureClient::Tests::RangeResponse;
using AVEVA::AzureClient::Tests::RunAwaitable;
using AVEVA::AzureClient::Tests::StartAwaitable;

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

void ExpectSuccess(const DownloadToResult& result, std::size_t expectedBytes) {
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().BytesWritten, expectedBytes);
}

void ExpectStatus(const DownloadToResult& result, unsigned expectedStatus, std::string_view expectedErrorCode = {}) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().StatusCode, expectedStatus);
    if (!expectedErrorCode.empty()) {
        EXPECT_EQ(result.error().ErrorCode, expectedErrorCode);
    }
}

void ExpectCancellation(const DownloadToResult& result) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::operation_canceled));
}

[[nodiscard]] std::size_t CompleteNewestUntilDone(FakeHttpClient& httpClient, std::size_t maxPending,
                                                  const Operation<DownloadToResult>& operation) {
    std::size_t peakPending = 0;
    for (int guard = 0; guard < 100 && !operation.Done(); ++guard) {
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(3U, 3U)));

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);

    const std::size_t peakPending = CompleteNewestUntilDone(httpClient, 3U, operation);

    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options));
    // The injected pool is not observed by Poll(), so wait for its work explicitly.
    ASSERT_TRUE(httpClient.WaitForRequest(1U));
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    for (int guard = 0; guard < 100 && !operation.Done(); ++guard) {
        ASSERT_TRUE(httpClient.RunUntil([&] { return httpClient.PendingCount() > 0U || operation.Done(); }));
        if (!operation.Done()) {
            ASSERT_TRUE(CompleteNewest(httpClient));
        }
    }

    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(3U, 3U)));

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    httpClient.FailNextInitiation(std::make_exception_ptr(std::bad_alloc{}));
    EXPECT_NO_THROW({
        EXPECT_TRUE(httpClient.CompleteRequest(0U));
        httpClient.CompleteAll();
        httpClient.Poll();
    });

    ASSERT_TRUE(operation.Done());
    EXPECT_FALSE(operation.Result().has_value());
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(3U, 3U)));

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
    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(6U, 2U)));
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ExpectStatus(operation.Result(), 412U, "ConditionNotMet");
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(6U, 2U)));
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(T118_DownloadVersionValidatorTests, CallerIfMatchPermitsChunksWhenTheResponseHasNoETag) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content.substr(0, 6), content.size(), ""));
    EnqueueForRange(httpClient, RangeHeader(6, 6), RangeResponse(6, content.substr(6), content.size(), ""));
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto options = Parallel(6U, 2U);
    options.Conditions.IfMatch = "\"caller\"";
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options));
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
    EXPECT_EQ(ReadFile(out.Path), content);
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "If-Match"), "\"caller\"");
}

TEST(T118_DownloadVersionValidatorTests, WildcardIfMatchIsReplacedByTheResponseETagForLaterChunks) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content.substr(0, 6), content.size(), "\"etagA\""));
    EnqueueForRange(
        httpClient, RangeHeader(6, 6),
        HttpResponse{412, MakeCanonicalSuccessHeaders({{"x-ms-error-code", "ConditionNotMet"}, {"ETag", "\"etagB\""}}),
                     ""});
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto options = Parallel(6U, 2U);
    options.Conditions.IfMatch = "*";
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options));
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ExpectStatus(operation.Result(), 412U, "ConditionNotMet");
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "If-Match"), "*");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "If-Match"), "\"etagA\"");
}

TEST(T118_DownloadVersionValidatorTests, WildcardIfMatchWithoutResponseETagRejectsFurtherChunks) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(12);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content.substr(0, 6), content.size(), ""));
    EnqueueForRange(httpClient, RangeHeader(6, 6), RangeResponse(6, content.substr(6), content.size(), ""));
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto options = Parallel(6U, 2U);
    options.Conditions.IfMatch = "*";
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options));
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(T118_DownloadVersionValidatorTests, SingleResponseWithoutETagStillSucceeds) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(6);
    EnqueueForRange(httpClient, RangeHeader(0, 6), RangeResponse(0, content, content.size(), ""));
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(6U, 2U)));
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(4U, 3U)));

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);
    ASSERT_TRUE(httpClient.CompleteRequest(FindRequestByRange(httpClient, RangeHeader(8, 4))));
    httpClient.Poll();

    // The other in-flight chunks were cancelled through their cancellation slots and drained.
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    ASSERT_TRUE(operation.Done());
    ExpectStatus(operation.Result(), 500U);
    EXPECT_EQ(httpClient.RequestCount(), 4U);
}

TEST(T11_ParallelDownloadAcceptanceTests, ParentCancellationCancelsAllInFlightChunks) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(40);
    EnqueueChunks(httpClient, content, 4U);
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(4U, 4U)));

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 4U);
    operation.Cancel(boost::asio::cancellation_type::all);
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ExpectCancellation(operation.Result());
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(httpClient.RequestCount(), 5U);
}

TEST(T11_ParallelDownloadAcceptanceTests, PartialCancellationDoesNotAbortTheDownload) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    EnqueueChunks(httpClient, content, 3U);
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(3U, 3U)));

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 3U);
    operation.Cancel(boost::asio::cancellation_type::partial);
    httpClient.Poll();
    ASSERT_FALSE(operation.Done());
    ASSERT_EQ(httpClient.PendingCount(), 3U);

    static_cast<void>(CompleteNewestUntilDone(httpClient, 3U, operation));

    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
    EXPECT_EQ(ReadFile(out.Path), content);
}

TEST(T11_ParallelDownloadAcceptanceTests, TerminalCancellationAfterPartialStillAbortsTheDownload) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(40);
    EnqueueChunks(httpClient, content, 4U);
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(4U, 4U)));

    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    ASSERT_EQ(httpClient.PendingCount(), 4U);
    operation.Cancel(boost::asio::cancellation_type::partial);
    httpClient.Poll();
    ASSERT_FALSE(operation.Done());
    operation.Cancel(boost::asio::cancellation_type::terminal);
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ExpectCancellation(operation.Result());
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

    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(target, Parallel(4U, 3U)));
    const std::size_t peakPending = CompleteNewestUntilDone(httpClient, 3U, operation);

    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
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

    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(target, Parallel(4U, 2U)));
    httpClient.CompleteAll();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ExpectStatus(operation.Result(), 412U);
    EXPECT_EQ(httpClient.PendingCount(), 0U);
    EXPECT_EQ(ReadFile(target), "previous");
    EXPECT_FALSE(HasPartialFiles(target));
    std::error_code ignored;
    std::filesystem::remove(target, ignored);
}

namespace {
// Answers every request with a 206 whose Content-Range is `contentRange` (omitted when empty).
[[nodiscard]] DownloadToResult DownloadAgainstFixed206(const std::string& body, const std::string& contentRange) {
    FakeHttpClient httpClient;
    auto headers = MakeCanonicalSuccessHeaders();
    if (!contentRange.empty()) {
        headers.emplace_back("Content-Range", contentRange);
    }
    httpClient.DefaultResponse() = HttpResponse{206, std::move(headers), body};
    BlobClient client{httpClient, BuildOptions()};

    TempFile out;
    auto result = RunAwaitable(httpClient, client.DownloadToAsync(out.Path, DownloadToOptions{}));
    EXPECT_FALSE(std::filesystem::exists(out.Path));
    return result;
}
} // namespace

TEST(T08_DownloadContentRangeTests, PartialContentWithoutContentRangeIsInvalidResponse) {
    const auto result = DownloadAgainstFixed206(MakeContent(10), "");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
}

TEST(T08_DownloadContentRangeTests, ContentRangeEndThatDisagreesWithTheBodyIsInvalidResponse) {
    const auto result = DownloadAgainstFixed206(MakeContent(10), "bytes 0-4/10");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
}

TEST(AC_R01_DownloadRangeOverflowTests, OffsetWhoseProbeRangeWouldWrapIsRejectedWithoutSendingARequest) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = std::numeric_limits<std::uint64_t>::max() - 1U};
    TempFile out;
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(out.Path, options));

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
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
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(out.Path, options));

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
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
[[nodiscard]] AVEVA::AzureClient::Private::DownloadDetail::SummaryResult DownloadIntoThrowingSink(bool throwOnReserve) {
    using AVEVA::AzureClient::NoTokenCredential;
    using AVEVA::AzureClient::Private::BasicBlobTarget;
    FakeHttpClient httpClient;
    const std::string content = MakeContent(10);
    httpClient.DefaultResponse() =
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-9/10"}}), content};
    const auto target = std::make_shared<const BasicBlobTarget<NoTokenCredential>>(
        BasicBlobTarget<NoTokenCredential>{AVEVA::AzureClient::Private::MakeBlobTarget(BuildOptions()), nullptr});

    // The outcome carries the sink, which is not default constructible as co_spawn requires; hand out the summary.
    const auto run = [&]() -> boost::asio::awaitable<AVEVA::AzureClient::Private::DownloadDetail::SummaryResult> {
        auto outcome = co_await AVEVA::AzureClient::Private::DownloadDetail::RunDownload(
            httpClient, target, DownloadToOptions{}, ThrowingSink<InlineWrites>{throwOnReserve},
            AVEVA::AzureClient::AzureRequestOptions{});
        co_return std::move(outcome.Result);
    };
    return RunAwaitable(httpClient, run());
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
    auto operation = StartAwaitable(
        httpClient, AVEVA::AzureClient::Private::DownloadDetail::RunDownload(
                        httpClient, target, options, DiscardingSink{}, AVEVA::AzureClient::AzureRequestOptions{}));
    ASSERT_TRUE(httpClient.RunUntil([&] { return httpClient.PendingCount() == 1U; }));
    ASSERT_TRUE(httpClient.CompleteNext());
    // 256 MiB of buffering at 64 MiB per chunk allows 4 chunk requests, not the 16 that Concurrency alone allows.
    ASSERT_TRUE(httpClient.RunUntil([&] { return httpClient.PendingCount() >= 4U; }));
    EXPECT_EQ(httpClient.PendingCount(), 4U);

    // Drain the abandoned download so no coroutine outlives the client it uses.
    operation.Cancel();
    httpClient.Poll();
    EXPECT_TRUE(operation.Done());
}

TEST(AC_R05_DownloadSinkExceptionTests, NonStandardExceptionFromAnInlineWriteIsAnIoError) {
    const auto result = DownloadIntoThrowingSink<true>(false);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::io_error));
}

TEST(AC_R05_DownloadSinkExceptionTests, NonStandardExceptionFromAScheduledWriteIsAnIoError) {
    const auto result = DownloadIntoThrowingSink<false>(false);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::io_error));
}

TEST(AC_R05_DownloadSinkExceptionTests, NonStandardExceptionFromReserveIsAnIoError) {
    const auto result = DownloadIntoThrowingSink<true>(true);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::io_error));
}

TEST(B05_DownloadIgnoredRangeTests, OffsetRangeAnswered200IsInvalidResponse) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(20);
    httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), content};
    BlobClient client{httpClient, BuildOptions()};

    DownloadToOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 10U};
    TempFile out;
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(out.Path, options));

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, AVEVA::AzureClient::BlobStorageErrorCode::InvalidResponse);
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
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(out.Path, options));

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().BytesWritten, 5U);
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
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(out.Path, options));

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::file_too_large));
}

TEST(A3_DownloadIntoSpanTests, FullAndPartialRangesAreWrittenByteForByte) {
    const std::string content = MakeContent(18);
    for (const auto [offset, length] : {std::pair{0U, 18U}, std::pair{4U, 7U}}) {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(RangeResponse(offset, content.substr(offset, length), content.size()));
        PageBlobClient client{httpClient, BuildOptions()};

        std::string destination(length, '\0');
        AVEVA::AzureClient::DownloadBlobOptions options;
        options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = offset, .Length = length};
        const auto result = RunAwaitable(httpClient, client.DownloadToAsync(std::span<char>(destination), options));

        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().BytesWritten, length);
        EXPECT_EQ(destination, content.substr(offset, length));
    }
}

TEST(A3_DownloadIntoSpanTests, BlobShorterThanTheRequestedRangeReportsTheBytesWritten) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    httpClient.EnqueueResponse(RangeResponse(0, content, content.size()));
    PageBlobClient client{httpClient, BuildOptions()};

    std::string destination(20, '#');
    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0, .Length = 20};
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(std::span<char>(destination), options));

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().BytesWritten, 18U);
    EXPECT_EQ(destination.substr(0, 18), content);
    EXPECT_EQ(destination.substr(18), "##");
}

TEST(A3_DownloadIntoSpanTests, ResponseLargerThanTheDestinationFailsWithoutWritingPastIt) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(18);
    httpClient.EnqueueResponse(RangeResponse(0, content, content.size()));
    PageBlobClient client{httpClient, BuildOptions()};

    std::string storage(18, '#');
    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0, .Length = 18};
    // Only the first 10 bytes are offered; the rest of `storage` acts as a guard region.
    const auto result = RunAwaitable(httpClient, client.DownloadToAsync(std::span<char>(storage).first(10), options));

    EXPECT_FALSE(result.has_value());
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options));
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::file_too_large));
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options));
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    static_cast<void>(CompleteNewestUntilDone(httpClient, 3U, operation));

    ASSERT_TRUE(operation.Done());
    ExpectSuccess(operation.Result(), content.size());
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, Parallel(3U, 1U), requestOptions));

    // Each request fits the 200 ms budget on its own, but together they overrun it.
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    std::this_thread::sleep_for(std::chrono::milliseconds{110});
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    std::this_thread::sleep_for(std::chrono::milliseconds{110});
    static_cast<void>(httpClient.CompleteRequest(1U));
    ASSERT_TRUE(httpClient.RunUntil([&] { return operation.Done() || httpClient.RequestCount() > 2U; }));

    EXPECT_EQ(httpClient.RequestCount(), 2U);
    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::timed_out));
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
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options, requestOptions));
    // The file open is queued behind the blocked writer, so the budget runs out before the first GET could start.
    std::this_thread::sleep_for(std::chrono::milliseconds{150});
    release.set_value();
    ASSERT_TRUE(httpClient.RunUntil([&] { return operation.Done() || httpClient.RequestCount() > 0U; }));
    pool.join();

    EXPECT_EQ(httpClient.RequestCount(), 0U);
    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_FALSE(HasPartialFiles(out.Path));
}

namespace {
// Starts a file download whose preparation is queued behind a blocked writer over an existing destination.
struct GatedFileDownload {
    FakeHttpClient HttpClient;
    boost::asio::thread_pool Pool{1U};
    std::promise<void> Release;
    TempFile Out;
    std::optional<Operation<DownloadToResult>> Download;

    void Start(const std::string& content) {
        EnqueueChunks(HttpClient, content, 3U, "\"etag-a\"");
        BlobClient client{HttpClient, BuildOptions()};
        boost::asio::post(Pool, [gate = Release.get_future()]() mutable { gate.wait(); });
        auto options = Parallel(3U, 1U);
        options.WriteScheduler = Pool.get_executor();
        CreateFileWithContents(Out.Path, "old");
        Download.emplace(StartAwaitable(HttpClient, client.DownloadToAsync(Out.Path, options)));
    }
};
} // namespace

TEST(T11_ParallelDownloadAcceptanceTests, CancellationWhileFilePreparationIsQueuedCompletesCanceledWithoutAGet) {
    GatedFileDownload download;
    download.Start(MakeContent(9));
    download.Download->Cancel();
    download.Release.set_value();
    ASSERT_TRUE(download.HttpClient.RunUntil([&] { return download.Download->Done(); }));
    download.Pool.join();

    EXPECT_EQ(download.HttpClient.RequestCount(), 0U);
    ExpectCancellation(download.Download->Result());
    EXPECT_EQ(ReadFile(download.Out.Path), "old");
    EXPECT_FALSE(HasPartialFiles(download.Out.Path));
}

TEST(T11_ParallelDownloadAcceptanceTests, CancellationAfterTheLastChunkBeforeFinalizationKeepsTheOriginalFile) {
    FakeHttpClient httpClient;
    const std::string content = MakeContent(3);
    EnqueueChunks(httpClient, content, 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    boost::asio::thread_pool pool{1U};
    auto options = Parallel(3U, 1U);
    options.WriteScheduler = pool.get_executor();
    TempFile out;
    CreateFileWithContents(out.Path, "old");
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options));
    ASSERT_TRUE(httpClient.WaitForRequest(1U));
    ASSERT_TRUE(httpClient.CompleteRequest(0U));

    // The gate is queued behind the final write, so finalization (posted once that write is reported) waits behind it.
    std::promise<void> started;
    std::promise<void> release;
    boost::asio::post(pool, [&started, gate = release.get_future()]() mutable {
        started.set_value();
        gate.wait();
    });
    started.get_future().wait();
    static_cast<void>(httpClient.RunUntil([] { return false; }, std::chrono::milliseconds{300}));
    operation.Cancel();
    release.set_value();
    ASSERT_TRUE(httpClient.RunUntil([&] { return operation.Done(); }));
    pool.join();

    ExpectCancellation(operation.Result());
    EXPECT_EQ(ReadFile(out.Path), "old");
    EXPECT_FALSE(HasPartialFiles(out.Path));
}

namespace {
// A one-chunk file download over an existing destination with a 400 ms budget whose last write is held up past it.
void ExpectDeadlinePassingInLastDiskWorkTimesOut() {
    FakeHttpClient httpClient;
    EnqueueChunks(httpClient, MakeContent(3), 3U, "\"etag-a\"");
    BlobClient client{httpClient, BuildOptions()};

    boost::asio::thread_pool pool{1U};
    auto options = Parallel(3U, 1U);
    options.WriteScheduler = pool.get_executor();
    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{400});
    TempFile out;
    CreateFileWithContents(out.Path, "old");
    auto operation = StartAwaitable(httpClient, client.DownloadToAsync(out.Path, options, requestOptions));
    ASSERT_TRUE(httpClient.WaitForRequest(1U));

    std::promise<void> started;
    std::promise<void> release;
    const auto block = [&] {
        boost::asio::post(pool, [&started, gate = release.get_future()]() mutable {
            started.set_value();
            gate.wait();
        });
        started.get_future().wait();
    };
    block();
    ASSERT_TRUE(httpClient.CompleteRequest(0U));
    // The last response arrived within budget; the paused disk work is what outlasts it.
    std::this_thread::sleep_for(std::chrono::milliseconds{450});
    release.set_value();
    ASSERT_TRUE(httpClient.RunUntil([&] { return operation.Done(); }));
    pool.join();

    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_EQ(ReadFile(out.Path), "old");
    EXPECT_FALSE(HasPartialFiles(out.Path));
}
} // namespace

TEST(T11_ParallelDownloadAcceptanceTests, DeadlinePassingDuringTheLastWriteTimesOutAndKeepsTheDestination) {
    ExpectDeadlinePassingInLastDiskWorkTimesOut();
}
