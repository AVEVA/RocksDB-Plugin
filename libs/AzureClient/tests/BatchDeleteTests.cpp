// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/BlobBatch.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/cancellation_type.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace {
using AVEVA::HttpMethod;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::DeleteBlobsOptions;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::Models::DeleteBlobsResult;
using AVEVA::AzureClient::Tests::BlobContainerClient;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::MakeToken;
using AVEVA::AzureClient::Tests::RunAwaitable;
using AVEVA::AzureClient::Tests::ScriptedTokenCredential;
using AVEVA::AzureClient::Tests::StartAwaitable;

using BatchOutcome = std::expected<Response<DeleteBlobsResult>, BlobStorageError>;

struct PartOutcome {
    unsigned int Status = 202;
    std::string ErrorCode;
};

// Answers a batch request the way the service does: one multipart part per Content-ID, each carrying the outcome that
// `decide` picks for that sub-request's URL.
[[nodiscard]] FakeHttpClient::ScriptedResult
BuildBatchResponse(const FakeHttpClient::RequestRecord& record,
                   const std::function<PartOutcome(const std::string&)>& decide) {
    static const std::regex part{R"(Content-ID: (\d+)\r\n\r\nDELETE (\S+) HTTP/1.1)"};
    std::string body;
    for (std::sregex_iterator it{record.Body.begin(), record.Body.end(), part}, end; it != end; ++it) {
        const PartOutcome outcome = decide((*it)[2].str());
        body += "--batchresponse_test\r\nContent-Type: application/http\r\nContent-ID: " + (*it)[1].str() + "\r\n\r\n";
        body +=
            "HTTP/1.1 " + std::to_string(outcome.Status) + " Status\r\nx-ms-request-id: sub-" + (*it)[1].str() + "\r\n";
        if (!outcome.ErrorCode.empty()) {
            body += "x-ms-error-code: " + outcome.ErrorCode + "\r\nContent-Type: application/xml\r\n\r\n<Error><Code>" +
                    outcome.ErrorCode + "</Code><Message>sub failure</Message></Error>";
        }
        body += "\r\n\r\n";
    }
    body += "--batchresponse_test--\r\n";
    return {.Response = HttpResponse{
                202,
                {{"Content-Type", "multipart/mixed; boundary=batchresponse_test"}, {"x-ms-request-id", "outer-1"}},
                body}};
}

void RespondToBatches(FakeHttpClient& http, std::function<PartOutcome(const std::string&)> decide = {}) {
    http.SetResponder([decide = std::move(decide)](const FakeHttpClient::RequestRecord& record)
                          -> std::optional<FakeHttpClient::ScriptedResult> {
        return BuildBatchResponse(record, decide ? decide : [](const std::string&) { return PartOutcome{}; });
    });
}

[[nodiscard]] std::vector<std::string> MakeNames(std::size_t count) {
    std::vector<std::string> names;
    for (std::size_t i = 0; i < count; ++i) {
        names.push_back("dir/file-" + std::to_string(i));
    }
    return names;
}

[[nodiscard]] BatchOutcome DeleteAll(FakeHttpClient& http, BlobContainerClient& client, std::vector<std::string> names,
                                     DeleteBlobsOptions options = {}) {
    return RunAwaitable(http, client.DeleteBlobsAsync(std::move(names), options));
}

// Answers batch requests only when the test completes them.
void DeferBatchResponses(FakeHttpClient& http) {
    http.SetResponder([](const FakeHttpClient::RequestRecord& record) -> std::optional<FakeHttpClient::ScriptedResult> {
        FakeHttpClient::ScriptedResult result =
            BuildBatchResponse(record, [](const std::string&) { return PartOutcome{}; });
        result.Deferred = true;
        return result;
    });
}

[[nodiscard]] std::size_t CountOccurrences(std::string_view text, std::string_view needle) {
    std::size_t count = 0;
    for (std::size_t pos = text.find(needle); pos != std::string_view::npos; pos = text.find(needle, pos + 1U)) {
        ++count;
    }
    return count;
}
} // namespace

TEST(BatchDeleteTests, BuildsMultipartPostWithOneDeletePerBlob) {
    FakeHttpClient http;
    RespondToBatches(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, {"a.txt", "folder name/b?.txt", "c"});

    ASSERT_TRUE(outcome.has_value());
    ASSERT_EQ(http.RequestCount(), 1U);
    const auto& record = http.RequestAt(0);
    EXPECT_EQ(record.Request.GetMethod(), HttpMethod::Post);
    EXPECT_EQ(
        record.Request.GetUrl(),
        "https://storageaccount.blob.core.windows.net/images?restype=container&comp=batch&sv=2025-01-05&sig=fakesig");

    const std::string contentType = FakeHttpClient::FindHeaderValue(record.Request, "Content-Type");
    const std::smatch boundaryMatch = [&] {
        std::smatch match;
        std::regex_match(contentType, match, std::regex{R"(multipart/mixed; boundary=(batch_[0-9a-f-]+))"});
        return match;
    }();
    ASSERT_EQ(boundaryMatch.size(), 2U) << contentType;
    const std::string delimiter = "--" + boundaryMatch[1].str();

    EXPECT_EQ(CountOccurrences(record.Body, delimiter + "\r\n"), 3U);
    EXPECT_TRUE(record.Body.ends_with(delimiter + "--\r\n"));
    for (std::size_t i = 0; i < 3U; ++i) {
        EXPECT_NE(record.Body.find("Content-ID: " + std::to_string(i) + "\r\n"), std::string::npos);
    }
    EXPECT_NE(record.Body.find("Content-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n"),
              std::string::npos);
    EXPECT_NE(record.Body.find("DELETE /images/a.txt?sv=2025-01-05&sig=fakesig HTTP/1.1\r\n"), std::string::npos);
    EXPECT_NE(record.Body.find("/images/folder%20name/b%3F.txt?sv=2025-01-05&sig=fakesig HTTP/1.1\r\n"),
              std::string::npos);
    EXPECT_EQ(CountOccurrences(record.Body, "x-ms-date: "), 3U);
    EXPECT_EQ(CountOccurrences(record.Body, "Content-Length: 0\r\n"), 3U);
    // A SAS travels in each sub-request URL; there is nothing to authorize with.
    EXPECT_EQ(record.Body.find("Authorization"), std::string::npos);

    const DeleteBlobsResult& result = outcome->Value();
    ASSERT_EQ(result.Items.size(), 3U);
    EXPECT_EQ(result.Items[0].Name, "a.txt");
    EXPECT_EQ(result.Items[2].Name, "c");
    EXPECT_TRUE(result.AllSucceeded());
}

TEST(BatchDeleteTests, SharedKeyAuthorizesTheRequestAndEverySubrequest) {
    FakeHttpClient http;
    RespondToBatches(http);
    auto options = MakeBlobContainerClientOptions();
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "MDEyMzQ1Njc4OWFiY2RlZg=="};
    BlobContainerClient client{http, options};

    const BatchOutcome outcome = DeleteAll(http, client, {"a", "b"});

    ASSERT_TRUE(outcome.has_value());
    const auto& record = http.RequestAt(0);
    EXPECT_EQ(record.Request.GetUrl(),
              "https://storageaccount.blob.core.windows.net/images?restype=container&comp=batch");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(record.Request, "Authorization").rfind("SharedKey storageaccount:", 0),
              0U);
    EXPECT_EQ(CountOccurrences(record.Body, "Authorization: SharedKey storageaccount:"), 2U);
}

TEST(BatchDeleteTests, SplitsMoreThan256NamesIntoSeveralRequestsAndKeepsOrder) {
    FakeHttpClient http;
    RespondToBatches(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const std::vector<std::string> names = MakeNames(600);
    const BatchOutcome outcome = DeleteAll(http, client, names);

    ASSERT_TRUE(outcome.has_value());
    ASSERT_EQ(http.RequestCount(), 3U);
    std::vector<std::size_t> partCounts;
    for (std::size_t i = 0; i < 3U; ++i) {
        partCounts.push_back(CountOccurrences(http.RequestAt(i).Body, "DELETE /"));
    }
    EXPECT_EQ(std::ranges::count(partCounts, 256U), 2);
    EXPECT_EQ(std::ranges::count(partCounts, 88U), 1);

    const DeleteBlobsResult& result = outcome->Value();
    ASSERT_EQ(result.Items.size(), names.size());
    for (std::size_t i = 0; i < names.size(); ++i) {
        EXPECT_EQ(result.Items[i].Name, names[i]);
        EXPECT_TRUE(result.Items[i].Succeeded());
    }
}

TEST(BatchDeleteTests, ExactlyOneFullBatchIsASingleRequest) {
    FakeHttpClient http;
    RespondToBatches(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, MakeNames(256));

    ASSERT_TRUE(outcome.has_value());
    EXPECT_EQ(http.RequestCount(), 1U);
    EXPECT_EQ(outcome->Value().Items.size(), 256U);
}

TEST(BatchDeleteTests, ReportsEachBlobOutcomeAndTreatsNotFoundAsAlreadyDeleted) {
    FakeHttpClient http;
    RespondToBatches(http, [](const std::string& url) {
        if (url.contains("missing")) {
            return PartOutcome{404, "BlobNotFound"};
        }
        if (url.contains("leased")) {
            return PartOutcome{412, "LeaseIdMissing"};
        }
        return PartOutcome{};
    });
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, {"ok", "missing", "leased", "ok2"});

    ASSERT_TRUE(outcome.has_value());
    const DeleteBlobsResult& result = outcome->Value();
    ASSERT_EQ(result.Items.size(), 4U);
    EXPECT_TRUE(result.Items[0].Succeeded());
    EXPECT_FALSE(result.Items[0].AlreadyAbsent);
    EXPECT_TRUE(result.Items[1].Succeeded());
    EXPECT_TRUE(result.Items[1].AlreadyAbsent);
    ASSERT_FALSE(result.Items[2].Succeeded());
    EXPECT_EQ(result.Items[2].Error->StatusCode, 412U);
    EXPECT_EQ(result.Items[2].Error->ErrorCode, "LeaseIdMissing");
    EXPECT_EQ(result.Items[2].Error->Code, BlobStorageErrorCode::LeaseIdMissing);
    EXPECT_EQ(result.Items[2].Error->RequestId, "sub-2");
    EXPECT_TRUE(result.Items[3].Succeeded());
    EXPECT_FALSE(result.AllSucceeded());
}

TEST(BatchDeleteTests, WholeRequestFailureCompletesAsError) {
    FakeHttpClient http;
    http.DefaultResponse() =
        HttpResponse{403, {{"x-ms-error-code", "AuthenticationFailed"}, {"x-ms-request-id", "req-9"}}, ""};
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, {"a", "b"});

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().StatusCode, 403U);
    EXPECT_EQ(outcome.error().Code, BlobStorageErrorCode::AuthenticationFailed);
    EXPECT_EQ(outcome.error().RequestId, "req-9");
}

TEST(BatchDeleteTests, TransportFailureCompletesAsError) {
    FakeHttpClient http;
    http.EnqueueResponse(HttpResponse{}, std::make_error_code(std::errc::connection_reset));
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, {"a"});

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().Code, std::errc::connection_reset);
}

TEST(BatchDeleteTests, FailedRequestStopsLaterChunksAndReportsTheFirstFailure) {
    FakeHttpClient http;
    http.DefaultResponse() = HttpResponse{500, {{"x-ms-error-code", "InternalError"}}, ""};
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome =
        DeleteAll(http, client, MakeNames(600), DeleteBlobsOptions{.MaxConcurrentRequests = 1});

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().StatusCode, 500U);
    EXPECT_EQ(http.RequestCount(), 1U);
}

TEST(BatchDeleteTests, LimitsRequestsInFlight) {
    FakeHttpClient http;
    DeferBatchResponses(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    auto op = StartAwaitable(
        http, client.DeleteBlobsAsync(MakeNames(256U * 3U), DeleteBlobsOptions{.MaxConcurrentRequests = 2}));
    ASSERT_EQ(http.PendingCount(), 2U);

    ASSERT_TRUE(http.CompleteNext());
    http.Poll();
    EXPECT_EQ(http.RequestCount(), 3U);
    EXPECT_FALSE(op.Done());

    http.CompleteAll();
    ASSERT_TRUE(http.RunUntil([&] { return op.Done(); }));
    ASSERT_TRUE(op.Result().has_value());
    EXPECT_EQ(op.Result().value().Value().Items.size(), 768U);
}

TEST(BatchDeleteTests, CancelAbortsEveryChunkInFlightAndStartsNoMore) {
    FakeHttpClient http;
    DeferBatchResponses(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    auto op = StartAwaitable(
        http, client.DeleteBlobsAsync(MakeNames(256U * 5U), DeleteBlobsOptions{.MaxConcurrentRequests = 2}));
    ASSERT_EQ(http.PendingCount(), 2U);

    ASSERT_TRUE(http.CompleteNext());
    http.Poll();
    ASSERT_EQ(http.RequestCount(), 3U);
    ASSERT_EQ(http.PendingCount(), 2U);

    op.Cancel();
    ASSERT_TRUE(http.RunUntil([&] { return op.Done(); }));

    const auto& outcome = op.Result();
    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(http.PendingCount(), 0U);
    EXPECT_EQ(http.RequestCount(), 3U);
}

TEST(BatchDeleteTests, PartialCancellationDoesNotAbortTheBatchButTerminalDoes) {
    FakeHttpClient http;
    DeferBatchResponses(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    auto op = StartAwaitable(
        http, client.DeleteBlobsAsync(MakeNames(256U * 3U), DeleteBlobsOptions{.MaxConcurrentRequests = 1}));
    ASSERT_EQ(http.PendingCount(), 1U);

    op.Cancel(boost::asio::cancellation_type::partial);
    http.Poll();
    ASSERT_FALSE(op.Done());
    ASSERT_EQ(http.PendingCount(), 1U);

    ASSERT_TRUE(http.CompleteNext());
    http.Poll();
    ASSERT_EQ(http.RequestCount(), 2U);
    op.Cancel();
    ASSERT_TRUE(http.RunUntil([&] { return op.Done(); }));

    const auto& outcome = op.Result();
    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(http.RequestCount(), 2U);
}

TEST(BatchDeleteTests, PartialCancellationLetsTheBatchSucceed) {
    FakeHttpClient http;
    DeferBatchResponses(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    auto op = StartAwaitable(http, client.DeleteBlobsAsync(MakeNames(10U)));
    ASSERT_EQ(http.PendingCount(), 1U);

    op.Cancel(boost::asio::cancellation_type::partial);
    http.Poll();
    ASSERT_TRUE(http.CompleteNext());
    ASSERT_TRUE(http.RunUntil([&] { return op.Done(); }));

    ASSERT_TRUE(op.Result().has_value());
    EXPECT_EQ(op.Result().value().Value().Items.size(), 10U);
}

TEST(BatchDeleteTests, CancelRightAfterInitiationOfManyChunksAlwaysCompletesCanceled) {
    for (int iteration = 0; iteration < 20; ++iteration) {
        FakeHttpClient http;
        DeferBatchResponses(http);
        BlobContainerClient client{http, MakeBlobContainerClientOptions()};

        auto op = StartAwaitable(
            http, client.DeleteBlobsAsync(MakeNames(256U * 5U), DeleteBlobsOptions{.MaxConcurrentRequests = 4}));
        op.Cancel();
        ASSERT_TRUE(http.RunUntil([&] { return op.Done(); }));
        const auto& outcome = op.Result();
        ASSERT_FALSE(outcome.has_value());
        EXPECT_EQ(outcome.error().Code, std::make_error_code(std::errc::operation_canceled));
    }
}

TEST(BatchDeleteTests, EmptyListSucceedsWithoutSendingAnything) {
    FakeHttpClient http;
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, {});

    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome->Value().Items.empty());
    EXPECT_TRUE(outcome->Value().AllSucceeded());
    EXPECT_EQ(http.RequestCount(), 0U);
}

TEST(BatchDeleteTests, RejectsInvalidNamesBeforeSendingAnything) {
    FakeHttpClient http;
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome emptyName = DeleteAll(http, client, {"fine", ""});
    ASSERT_FALSE(emptyName.has_value());
    EXPECT_EQ(emptyName.error().Code, std::errc::invalid_argument);

    const BatchOutcome longName = DeleteAll(http, client, {std::string(1025U, 'x')});
    ASSERT_FALSE(longName.has_value());
    EXPECT_EQ(longName.error().Code, std::errc::invalid_argument);

    EXPECT_EQ(http.RequestCount(), 0U);
}

TEST(BatchDeleteTests, MalformedBatchResponseIsAnInvalidResponse) {
    FakeHttpClient http;
    http.SetResponder([](const FakeHttpClient::RequestRecord&) -> std::optional<FakeHttpClient::ScriptedResult> {
        return FakeHttpClient::ScriptedResult{
            .Response = HttpResponse{202, {{"Content-Type", "multipart/mixed; boundary=b"}}, "not multipart"}};
    });
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, {"a"});

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().Code, BlobStorageErrorCode::InvalidResponse);
}

TEST(BatchDeleteTests, ResponseMissingASubrequestIsAnInvalidResponse) {
    FakeHttpClient http;
    http.SetResponder([](const FakeHttpClient::RequestRecord&) -> std::optional<FakeHttpClient::ScriptedResult> {
        return FakeHttpClient::ScriptedResult{.Response =
                                                  HttpResponse{202,
                                                               {{"Content-Type", "multipart/mixed; boundary=b"}},
                                                               "--b\r\nContent-Type: application/http\r\nContent-ID: "
                                                               "0\r\n\r\nHTTP/1.1 202 Accepted\r\n\r\n\r\n--b--\r\n"}};
    });
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    const BatchOutcome outcome = DeleteAll(http, client, {"a", "b"});

    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().Code, BlobStorageErrorCode::InvalidResponse);
}

// A retried batch must not replay the sub-requests of the failed attempt: their bearer tokens are re-acquired and the
// body is rebuilt for every attempt.
TEST(BatchDeleteTests, RetriedBatchRebuildsSubrequestsWithAFreshToken) {
    FakeHttpClient http;
    http.SetResponder([](const FakeHttpClient::RequestRecord& record) -> std::optional<FakeHttpClient::ScriptedResult> {
        if (record.Body.find("token-one") != std::string::npos) {
            return FakeHttpClient::ScriptedResult{.Response =
                                                      HttpResponse{503, {{"x-ms-error-code", "ServerBusy"}}, ""}};
        }
        return BuildBatchResponse(record, [](const std::string&) { return PartOutcome{}; });
    });
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueImmediate({}, MakeToken("token-one", -std::chrono::seconds(1)));
    credential->EnqueueImmediate({}, MakeToken("token-two", std::chrono::hours(1)));
    auto options = MakeBlobContainerClientOptions();
    options.SasToken.clear();
    options.Retry.MaxRetries = 1;
    options.Retry.InitialDelay = std::chrono::milliseconds{0};
    AVEVA::AzureClient::BasicBlobContainerClient<FakeHttpClient, ScriptedTokenCredential> client{http, options,
                                                                                                 credential};

    const BatchOutcome outcome = RunAwaitable(http, client.DeleteBlobsAsync({"a", "b"}));

    ASSERT_TRUE(outcome.has_value()) << outcome.error().Message << " status " << outcome.error().StatusCode
                                     << " requests " << http.RequestCount();
    const auto& first = http.RequestAt(0);
    const auto& second = http.RequestAt(1);
    EXPECT_EQ(CountOccurrences(first.Body, "Authorization: Bearer token-one"), 2U);
    EXPECT_EQ(CountOccurrences(first.Body, "token-two"), 0U);
    EXPECT_EQ(CountOccurrences(second.Body, "Authorization: Bearer token-two"), 2U);
    EXPECT_EQ(CountOccurrences(second.Body, "token-one"), 0U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(second.Request, "Authorization"), "Bearer token-two");
}
TEST(BatchDeleteTests, AppendBatchDeletePartRejectsUrlsWithoutSchemeOrPath) {
    for (const char* url : {"no-scheme/container/blob", "https://account.blob.core.windows.net"}) {
        AVEVA::HttpRequest subrequest;
        subrequest.SetMethod(HttpMethod::Delete);
        subrequest.SetUrl(url);
        std::string body;
        EXPECT_THROW(AVEVA::AzureClient::Private::AppendBatchDeletePart(body, "b", 0, subrequest),
                     std::invalid_argument)
            << url;
        EXPECT_TRUE(body.empty()) << "nothing is appended for a rejected URL";
    }

    AVEVA::HttpRequest valid;
    valid.SetMethod(HttpMethod::Delete);
    valid.SetUrl("https://account.blob.core.windows.net/container/blob?sig=x");
    std::string body;
    AVEVA::AzureClient::Private::AppendBatchDeletePart(body, "b", 0, valid);
    EXPECT_NE(body.find("DELETE /container/blob?sig=x HTTP/1.1"), std::string::npos);
}

namespace {
std::string OnePart(const std::string& status, const std::string& closing = "--b--\r\n") {
    return "--b\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\n" + status +
           "\r\nx-ms-request-id: r0\r\n\r\n\r\n" + closing;
}

void ExpectBatchRejected(const std::string& body, const std::string& contentType = "multipart/mixed; boundary=b") {
    const std::vector<std::string> names{"a"};
    const HttpResponse response{202, {{"Content-Type", contentType}}, body};
    EXPECT_THROW(static_cast<void>(AVEVA::AzureClient::Private::ParseBatchDeleteResponse(response, names)),
                 std::invalid_argument)
        << body << " / " << contentType;
}
} // namespace

TEST(AS_R01_BatchStatusLineTests, RejectsTrailingGarbageWrongPrefixAndOutOfRangeCodes) {
    ExpectBatchRejected(OnePart("HTTP/1.1 202junk"));
    ExpectBatchRejected(OnePart("HTTP/1.1 20"));
    ExpectBatchRejected(OnePart("HTTP/1.1 2020 Accepted"));
    ExpectBatchRejected(OnePart("HTTP/1.1 099 Low"));
    ExpectBatchRejected(OnePart("HTTP/1.1 600 High"));
    ExpectBatchRejected(OnePart("HTTP/2 202 Accepted"));
    ExpectBatchRejected(OnePart("HTTP/1.1 +20 Accepted"));
}

TEST(AS_R02_BatchBoundaryTests, ParsesMediaTypeParametersStrictly) {
    const std::string body = OnePart("HTTP/1.1 202 Accepted");
    const std::vector<std::string> names{"a"};
    const auto parse = [&](const std::string& contentType) {
        return AVEVA::AzureClient::Private::ParseBatchDeleteResponse(
            HttpResponse{202, {{"Content-Type", contentType}}, body}, names);
    };
    EXPECT_EQ(parse("multipart/mixed; boundary=\"b\"; charset=utf-8").size(), 1U);
    EXPECT_EQ(parse("multipart/mixed; charset=utf-8; BOUNDARY=b").size(), 1U);
    ExpectBatchRejected(body, "multipart/mixed; xboundary=b");
    ExpectBatchRejected(body, "multipart/mixed; charset=\"boundary=b\"");
    ExpectBatchRejected(body, "multipart/mixed; boundary=");
    ExpectBatchRejected(body, "multipart/mixed; boundary=\"b\"\"");
    ExpectBatchRejected(body, "multipart/mixed; boundary=a@b");
    ExpectBatchRejected(body, "multipart/mixed; boundary=" + std::string(71, 'b'));
}

TEST(AS_R03_BatchClosingDelimiterTests, RequiresALineEndAfterTheClosingDelimiter) {
    ExpectBatchRejected(OnePart("HTTP/1.1 202 Accepted", "--b--garbage"));
    ExpectBatchRejected(OnePart("HTTP/1.1 202 Accepted", "--b--  x\r\n"));
    const std::vector<std::string> names{"a"};
    for (const char* closing : {"--b--", "--b--\r\n", "--b--  \t\r\n"}) {
        const HttpResponse response{
            202, {{"Content-Type", "multipart/mixed; boundary=b"}}, OnePart("HTTP/1.1 202 Accepted", closing)};
        EXPECT_EQ(AVEVA::AzureClient::Private::ParseBatchDeleteResponse(response, names).size(), 1U) << closing;
    }
}

TEST(AS_R05_BatchSubrequestHeaderTests, RejectsLineBreaksInSubrequestHeaders) {
    for (const auto& [name, value] : std::vector<std::pair<std::string, std::string>>{
             {"x-ms-a", "v\r\nInjected: 1"}, {"x-ms-a", "v\nx"}, {"x-ms-a\r\nb", "v"}, {"", "v"}, {"a:b", "v"}}) {
        AVEVA::HttpRequest subrequest;
        subrequest.SetMethod(HttpMethod::Delete);
        subrequest.SetUrl("https://account.blob.core.windows.net/container/blob");
        subrequest.AddHeader(AVEVA::HttpHeader{name, value});
        std::string body;
        EXPECT_THROW(AVEVA::AzureClient::Private::AppendBatchDeletePart(body, "b", 0, subrequest),
                     std::invalid_argument);
        EXPECT_TRUE(body.empty());
    }
}

TEST(AS_R12_BatchBoundaryLineTests, ABoundaryLikeLineInsideAPartBodyIsNotASplitPoint) {
    const std::vector<std::string> names{"a"};
    const std::string body = "--b\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 404 Not Found\r\n"
                             "x-ms-error-code: BlobNotFound\r\n\r\nnote --b inline and --bb\r\n--b--\r\n";
    const HttpResponse response{202, {{"Content-Type", "multipart/mixed; boundary=b"}}, body};
    const auto items = AVEVA::AzureClient::Private::ParseBatchDeleteResponse(response, names);
    ASSERT_EQ(items.size(), 1U);
    EXPECT_TRUE(items[0].AlreadyAbsent);
}

TEST(BatchDeleteTests, SuccessfulPartsNeedNoHeaderParsingAndFailedPartsKeepTheirErrorDetails) {
    const std::vector<std::string> names{"ok", "gone", "denied"};
    const std::string body =
        "--b\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\nHTTP/1.1 202 Accepted\r\nx-ms-request-id: "
        "r0\r\n\r\n\r\n"
        "--b\r\nContent-Type: application/http\r\nContent-ID: 1\r\n\r\nHTTP/1.1 404 Not Found\r\nx-ms-error-code: "
        "BlobNotFound\r\n\r\n\r\n"
        "--b\r\nContent-Type: application/http\r\nContent-ID: 2\r\n\r\nHTTP/1.1 403 Forbidden\r\nx-ms-error-code: "
        "AuthorizationFailure\r\nx-ms-request-id: r2\r\n\r\n\r\n--b--\r\n";
    const HttpResponse response{202, {{"Content-Type", "multipart/mixed; boundary=b"}}, body};

    const auto items = AVEVA::AzureClient::Private::ParseBatchDeleteResponse(response, names);

    ASSERT_EQ(items.size(), 3U);
    EXPECT_FALSE(items[0].Error.has_value());
    EXPECT_FALSE(items[0].AlreadyAbsent);
    EXPECT_TRUE(items[1].AlreadyAbsent);
    EXPECT_FALSE(items[1].Error.has_value());
    ASSERT_TRUE(items[2].Error.has_value());
    EXPECT_EQ(items[2].Error->StatusCode, 403);
    EXPECT_EQ(items[2].Error->RequestId, "r2");
}

TEST(BatchDeleteTests, SequentialBatchesShareOneAggregateDeadline) {
    FakeHttpClient http;
    DeferBatchResponses(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{200});
    auto op =
        StartAwaitable(http, client.DeleteBlobsAsync(MakeNames(256U * 3U),
                                                     DeleteBlobsOptions{.MaxConcurrentRequests = 1}, requestOptions));

    // Each batch fits the 200 ms budget on its own, but together they overrun it.
    ASSERT_EQ(http.PendingCount(), 1U);
    std::this_thread::sleep_for(std::chrono::milliseconds{110});
    ASSERT_TRUE(http.CompleteNext());
    std::this_thread::sleep_for(std::chrono::milliseconds{110});
    static_cast<void>(http.CompleteNext());
    ASSERT_TRUE(http.RunUntil([&] { return op.Done() || http.RequestCount() > 2U; }));

    EXPECT_EQ(http.RequestCount(), 2U);
    ASSERT_TRUE(op.Done());
    const auto& outcome = op.Result();
    ASSERT_FALSE(outcome.has_value());
    EXPECT_EQ(outcome.error().Code, std::make_error_code(std::errc::timed_out));
}

TEST(BatchDeleteTests, ChildInitiationFailureAfterACompletedChunkCompletesOnceWithTheFailure) {
    FakeHttpClient http;
    DeferBatchResponses(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    auto op = StartAwaitable(
        http, client.DeleteBlobsAsync(MakeNames(256U * 3U), DeleteBlobsOptions{.MaxConcurrentRequests = 1}));
    ASSERT_EQ(http.PendingCount(), 1U);

    http.FailNextInitiation(std::make_exception_ptr(std::bad_alloc{}));
    // The completion of the first chunk starts the next one; its failure must not escape into the event loop.
    EXPECT_NO_THROW(static_cast<void>(http.CompleteNext()));
    ASSERT_TRUE(http.RunUntil([&] { return op.Done(); }));

    ASSERT_FALSE(op.Result().has_value());
    EXPECT_EQ(http.RequestCount(), 1U);
    EXPECT_EQ(http.PendingCount(), 0U);
}

TEST(BatchDeleteTests, ChildInitiationFailureCancelsAndDrainsTheChunksAlreadyAdmitted) {
    FakeHttpClient http;
    DeferBatchResponses(http);
    BlobContainerClient client{http, MakeBlobContainerClientOptions()};

    auto op = StartAwaitable(
        http, client.DeleteBlobsAsync(MakeNames(256U * 4U), DeleteBlobsOptions{.MaxConcurrentRequests = 2}));
    ASSERT_EQ(http.PendingCount(), 2U);

    http.FailNextInitiation(std::make_exception_ptr(std::bad_alloc{}));
    EXPECT_NO_THROW(static_cast<void>(http.CompleteNext()));
    http.Poll();
    // The first failure cancels the chunk still in flight, and the aggregate returns once it has drained.
    ASSERT_TRUE(http.RunUntil([&] { return op.Done(); }));
    EXPECT_EQ(http.PendingCount(), 0U);

    ASSERT_FALSE(op.Result().has_value());
    EXPECT_EQ(http.RequestCount(), 2U);
}
