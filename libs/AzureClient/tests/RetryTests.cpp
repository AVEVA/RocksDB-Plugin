// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"
#include "ValueOrFail.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/WithRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <algorithm>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <cstddef>
#include <expected>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <limits>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>

namespace AVEVA::AzureClient::Tests {
using AVEVA::HttpResponse;

TEST(RetryTests, NoRetries_DoesNotRetryOnTransientFailure) {
    FakeHttpClient httpClient;
    // Single immediate 503 response
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = 0; // no retries

    BlobClient client{httpClient, options};

    CallbackExpectation cb;
    std::optional<BlobStorageError> observedError;
    client.DeleteAsync([&](std::expected<Response<Models::DeleteBlobResult>, BlobStorageError> result) mutable {
        if (!result.has_value()) {
            observedError = std::move(result).error();
        }
        cb.MarkInvoked();
    });

    // Ensure any posted completions run
    httpClient.Poll();

    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(observedError.has_value());
}

TEST(RetryTests, MaxRetriesAtIntMaxDoesNotOverflow) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{200, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = std::numeric_limits<int>::max();
    options.Retry.InitialDelay = std::chrono::milliseconds{0};

    BlobClient client{httpClient, options};

    CallbackExpectation cb;
    std::optional<BlobStorageError> observedError;
    client.DeleteAsync([&](std::expected<Response<Models::DeleteBlobResult>, BlobStorageError> result) mutable {
        if (!result.has_value()) {
            observedError = std::move(result).error();
        }
        cb.MarkInvoked();
    });

    EXPECT_TRUE(httpClient.WaitForRequest(2));
    httpClient.Poll();

    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_FALSE(observedError.has_value());
}

TEST(RetryTests, Retries_OnTransientThenSucceeds) {
    FakeHttpClient httpClient;
    // First respond 503, then 200
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{200, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = 1; // allow one retry
    options.Retry.InitialDelay = std::chrono::milliseconds{0};

    BlobClient client{httpClient, options};

    CallbackExpectation cb;
    std::optional<Response<Models::DeleteBlobResult>> success;
    std::optional<BlobStorageError> observedError;

    client.DeleteAsync([&](std::expected<Response<Models::DeleteBlobResult>, BlobStorageError> result) mutable {
        if (result.has_value()) {
            success = std::move(result).value();
        } else {
            observedError = std::move(result).error();
        }
        cb.MarkInvoked();
    });

    // Wait for the retry to occur without sleeping.
    EXPECT_TRUE(httpClient.WaitForRequest(2));
    // Run remaining handlers
    httpClient.Poll();

    EXPECT_GE(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(success.has_value());
    EXPECT_FALSE(observedError.has_value());
}

namespace {
using DeleteResult = std::expected<Response<Models::DeleteBlobResult>, BlobStorageError>;

void StartDeleteAsync(BlobClient& client, boost::asio::cancellation_signal& signal,
                      std::optional<DeleteResult>& observed) {
    client.DeleteAsync(
        boost::asio::bind_cancellation_slot(signal.slot(), [&](DeleteResult result) { observed = std::move(result); }));
}

// Drives the fake transport's io_context (which also runs retry backoff timers) until `done`
// holds or the deadline expires.
bool PollUntil(FakeHttpClient& httpClient, const std::function<bool()>& done,
               std::chrono::milliseconds timeout = std::chrono::seconds{5}) {
    const bool completed = httpClient.RunUntil(done, timeout);
    httpClient.Poll();
    return completed;
}

BlobClientOptions MakeRetryOptions(int maxRetries,
                                   std::chrono::milliseconds initialDelay = std::chrono::milliseconds{0}) {
    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = maxRetries;
    options.Retry.InitialDelay = initialDelay;
    return options;
}

struct UploadState {
    bool Done = false;
    bool Succeeded = false;
};

void StartUploadAsync(PageBlobClient& client, const std::string& payload, UploadState& state) {
    client.UploadPagesAsync(0, payload,
                            [&](std::expected<Response<Models::UploadPagesResult>, BlobStorageError> result) {
                                state.Succeeded = result.has_value();
                                state.Done = true;
                            });
}

[[nodiscard]] std::size_t CountAuthorizationHeaders(const AVEVA::HttpRequest& request) {
    return static_cast<std::size_t>(std::ranges::count_if(
        request.GetHeaders(), [](const AVEVA::HttpHeader& header) { return header.GetName() == "Authorization"; }));
}
} // namespace

TEST(RetryTests, RetriesEveryTransientStatus) {
    for (const unsigned status : {408U, 429U, 500U, 502U, 503U, 504U}) {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{status, {}, ""});
        httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
        BlobClient client{httpClient, MakeRetryOptions(1)};

        std::optional<DeleteResult> observed;
        client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

        ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); })) << "status " << status;
        EXPECT_EQ(httpClient.RequestCount(), 2U) << "status " << status;
        EXPECT_TRUE(ValueOrFail(observed).has_value()) << "status " << status;
    }
}

TEST(RetryTests, DoesNotRetryNonTransientStatus) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().StatusCode, 404U);
}

TEST(RetryTests, RetriesTransientTransportErrors) {
    // The upload is a plain overwrite, so it is safe to replay after any transport failure.
    for (const std::error_code error :
         {std::error_code{AVEVA::HttpClientError::TimedOut}, std::error_code{AVEVA::HttpClientError::ConnectFailed},
          std::make_error_code(std::errc::connection_reset)}) {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{}, error);
        httpClient.EnqueueResponse(HttpResponse{201, {}, ""});
        PageBlobClient client{httpClient, MakeRetryOptions(1)};

        UploadState state;
        StartUploadAsync(client, std::string(512, 'p'), state);

        ASSERT_TRUE(PollUntil(httpClient, [&] { return state.Done; })) << error.message();
        EXPECT_EQ(httpClient.RequestCount(), 2U) << error.message();
        EXPECT_TRUE(state.Succeeded) << error.message();
    }
}

TEST(RetryTests, DeleteIsRetriedAfterFailuresThatHappenedBeforeAnythingWasSent) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ConnectFailed);
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(1)};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, DeleteIsNotReplayedAfterAnAmbiguousTransportErrorBecauseItsOutcomeIsUnknown) {
    for (const std::error_code error :
         {std::error_code{AVEVA::HttpClientError::TimedOut}, std::error_code{AVEVA::HttpClientError::ReadFailed},
          std::make_error_code(std::errc::connection_reset)}) {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{}, error);
        // A replay would find the blob already gone and answer 404.
        httpClient.EnqueueResponse(HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""});
        BlobClient client{httpClient, MakeRetryOptions(3)};

        std::optional<DeleteResult> observed;
        client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

        ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); })) << error.message();
        EXPECT_EQ(httpClient.RequestCount(), 1U) << error.message();
        EXPECT_FALSE(ValueOrFail(observed).has_value()) << error.message();
    }
}

using CreateResult = std::expected<Response<Models::CreatePageBlobResult>, BlobStorageError>;

TEST(RetryTests, ConditionalCreateIfNotExistsIsReplayedAfterAnAmbiguousTransportErrorBecauseTheReplayIsAbsorbed) {
    for (const std::error_code error :
         {std::error_code{AVEVA::HttpClientError::TimedOut}, std::error_code{AVEVA::HttpClientError::ReadFailed},
          std::make_error_code(std::errc::connection_reset)}) {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{}, error);
        // The first attempt may have created the blob; the replay's 409 is suppressed by CreateIfNotExists.
        httpClient.EnqueueResponse(HttpResponse{409, {{"x-ms-error-code", "BlobAlreadyExists"}}, ""});
        PageBlobClient client{httpClient, MakeRetryOptions(3)};

        std::optional<CreateResult> observed;
        client.CreateIfNotExistsAsync(512, CreatePageBlobOptions{},
                                      [&](CreateResult result) { observed = std::move(result); });

        ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); })) << error.message();
        EXPECT_EQ(httpClient.RequestCount(), 2U) << error.message();
        EXPECT_TRUE(ValueOrFail(observed).has_value()) << error.message();
    }
}

TEST(RetryTests, DeleteIfExistsIsReplayedAfterAnAmbiguousTransportErrorBecauseTheReplayIsAbsorbed) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ReadFailed);
    httpClient.EnqueueResponse(HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<DeleteResult> observed;
    client.DeleteIfExistsAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, ConditionalCreateIsRetriedAfterFailuresThatHappenedBeforeAnythingWasSent) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ConnectFailed);
    httpClient.EnqueueResponse(HttpResponse{201, {}, ""});
    PageBlobClient client{httpClient, MakeRetryOptions(1)};

    std::optional<CreateResult> observed;
    client.CreateIfNotExistsAsync(512, CreatePageBlobOptions{},
                                  [&](CreateResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, UnconditionalCreateOverwriteIsReplayedAfterAnAmbiguousTransportError) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ReadFailed);
    httpClient.EnqueueResponse(HttpResponse{201, {}, ""});
    PageBlobClient client{httpClient, MakeRetryOptions(1)};

    std::optional<CreateResult> observed;
    client.CreateAsync(512, CreatePageBlobOptions{}, [&](CreateResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, UnauthorizedResponseIsRepeatedOnceWithAFreshToken) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{401, {{"x-ms-error-code", "InvalidAuthenticationInfo"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueImmediate({}, MakeToken("old-token", std::chrono::hours(1)));
    credential->EnqueueImmediate({}, MakeToken("new-token", std::chrono::hours(1)));

    BlobClientOptions options = MakeRetryOptions(0);
    options.SasToken.clear();
    BasicBlobClient<FakeHttpClient, ScriptedTokenCredential> client{httpClient, std::move(options), credential};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "Authorization"), "Bearer old-token");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "Authorization"), "Bearer new-token");
    EXPECT_EQ(credential->CallCount(), 2);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, UnauthorizedResponseIsOnlyRefreshedOnce) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{401, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{401, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueImmediate({}, MakeToken("old-token", std::chrono::hours(1)));
    credential->EnqueueImmediate({}, MakeToken("new-token", std::chrono::hours(1)));

    BlobClientOptions options = MakeRetryOptions(0);
    options.SasToken.clear();
    BasicBlobClient<FakeHttpClient, ScriptedTokenCredential> client{httpClient, std::move(options), credential};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().StatusCode, 401U);
}

TEST(RetryTests, GivesUpAfterMaxRetriesAndReportsLastResponse) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {{"x-ms-request-id", "first"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{503, {{"x-ms-request-id", "second"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{500, {{"x-ms-request-id", "last"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(2)};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 3U);
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().StatusCode, 500U);
    EXPECT_EQ(ValueOrFail(observed).error().RequestId, "last");
}

TEST(RetryTests, HonorsServerRetryAfterHeadersOverLongBackoff) {
    for (const std::string_view header : {"x-ms-retry-after-ms", "Retry-After"}) {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{503, {{std::string{header}, "0"}}, ""});
        httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
        // The computed backoff would be ~1 minute; the server hint of 0 must win.
        BlobClient client{httpClient, MakeRetryOptions(1, std::chrono::minutes{1})};

        std::optional<DeleteResult> observed;
        client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

        ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2})) << header;
        EXPECT_EQ(httpClient.RequestCount(), 2U) << header;
        EXPECT_TRUE(ValueOrFail(observed).has_value()) << header;
    }
}

TEST(RetryTests, RetryAfterHttpDateInThePastRetriesImmediately) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {{"Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(1, std::chrono::minutes{1})};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}

TEST(RetryTests, RetryAfterHttpDateIsRelativeToTheResponseDateHeader) {
    FakeHttpClient httpClient;
    // The server clock is an hour ahead of ours: against the local clock this would be a one hour wait.
    const auto serverNow = std::chrono::system_clock::now() + std::chrono::hours{1};
    const auto serverDate = AVEVA::AzureClient::Private::BuildDateHeaderValue(serverNow);
    httpClient.EnqueueResponse(HttpResponse{503, {{"Date", serverDate}, {"Retry-After", serverDate}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(1, std::chrono::milliseconds{0})};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}
TEST(RetryTests, RetryAfterFutureHttpDateDelaysTheRetry) {
    FakeHttpClient httpClient;
    FakeHttpClient::ClearRetryDelays();
    const FakeHttpClient::InstantRetryTimers instantTimers;
    const auto retryAt = std::chrono::system_clock::now() + std::chrono::seconds{2};
    httpClient.EnqueueResponse(
        HttpResponse{503, {{"Retry-After", AVEVA::AzureClient::Private::BuildDateHeaderValue(retryAt)}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(1, std::chrono::milliseconds{0})};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{4}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
    // The date has whole-second resolution, so the delay is the remaining 1-2 seconds, not the (zero) backoff.
    const auto delays = FakeHttpClient::RetryDelays();
    ASSERT_EQ(delays.size(), 1U);
    EXPECT_GT(delays[0], std::chrono::milliseconds{0});
    EXPECT_LE(delays[0], std::chrono::milliseconds{2000});
}

TEST(RetryTests, XMsRetryAfterMsTakesPrecedenceOverRetryAfter) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(
        HttpResponse{503, {{"x-ms-retry-after-ms", "0"}, {"Retry-After", "Wed, 21 Oct 2099 07:28:00 GMT"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(1, std::chrono::minutes{1})};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, ComputedBackoffIsCappedByMaxDelay) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClientOptions options = MakeRetryOptions(1, std::chrono::minutes{1});
    options.Retry.MaxDelay = std::chrono::milliseconds{0};
    BlobClient client{httpClient, options};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result) { observed = std::move(result); });

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, ServerRetryAfterHintIsNotClampedToMaxDelay) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {{"Retry-After", "3600"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClientOptions options = MakeRetryOptions(1);
    options.Retry.MaxDelay = std::chrono::milliseconds{0};
    BlobClient client{httpClient, options};

    std::optional<DeleteResult> observed;
    boost::asio::cancellation_signal signal;
    client.DeleteAsync(
        boost::asio::bind_cancellation_slot(signal.slot(), [&](DeleteResult result) { observed = std::move(result); }));

    EXPECT_FALSE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::milliseconds{300}));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    signal.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();
}

TEST(RetryTests, CancellationDuringBackoffCompletesWithOperationCanceledAndSendsNothingMore) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    StartDeleteAsync(client, signal, observed);

    httpClient.Poll(); // deliver the 503; the operation is now waiting on its backoff timer
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_FALSE(observed.has_value());

    signal.emit(boost::asio::cancellation_type::terminal);
    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_FALSE(signal.slot().has_handler());
}

TEST(RetryTests, CancellationFromAnotherThreadWhileRetryTimerIsPending) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    StartDeleteAsync(client, signal, observed);
    httpClient.Poll();
    ASSERT_EQ(httpClient.RequestCount(), 1U);

    std::thread canceller([&] { signal.emit(boost::asio::cancellation_type::terminal); });
    canceller.join();

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, CancellationOfInFlightAttemptIsNotRetried) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeRetryOptions(3)};

    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    StartDeleteAsync(client, signal, observed);
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    ASSERT_TRUE(signal.slot().has_handler());
    ASSERT_TRUE(httpClient.LastRequestOptions().GetCancellationSlot().has_handler());

    signal.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();
    ASSERT_TRUE(observed.has_value());
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, MaxRetriesOverrideReplacesClientPolicy) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetMaxRetries(0);
    std::optional<DeleteResult> observed;
    client.DeleteAsync(WithRequestOptions(requestOptions, [&](DeleteResult result) { observed = std::move(result); }));

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().StatusCode, 503U);
}

TEST(RetryTests, MaxRetriesOverrideCanEnableRetriesDisabledOnTheClient) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(0)};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetMaxRetries(1);
    std::optional<DeleteResult> observed;
    client.DeleteAsync(WithRequestOptions(requestOptions, [&](DeleteResult result) { observed = std::move(result); }));

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, TotalDeadlineEndsRetryBackoffWithTimedOut) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{50});
    std::optional<DeleteResult> observed;
    client.DeleteAsync(WithRequestOptions(requestOptions, [&](DeleteResult result) { observed = std::move(result); }));

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{5}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, TotalDeadlineFailsImmediatelyWhenTheBackoffWouldOutlastIt) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::seconds{10});
    std::optional<DeleteResult> observed;
    client.DeleteAsync(WithRequestOptions(requestOptions, [&](DeleteResult result) { observed = std::move(result); }));

    // Without the check the operation would idle until the 10 second deadline fired, which outlasts this poll.
    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{5}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, TotalDeadlineCancelsInFlightAttemptAndIsNotRetried) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeRetryOptions(3)};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{50});
    std::optional<DeleteResult> observed;
    client.DeleteAsync(WithRequestOptions(requestOptions, [&](DeleteResult result) { observed = std::move(result); }));
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{5}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, TotalDeadlineClampsAttemptTimeoutAndDoesNotFireAfterSuccess) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::seconds{30});
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{200});
    std::optional<DeleteResult> observed;
    int completions = 0;
    client.DeleteAsync(WithRequestOptions(requestOptions, [&](DeleteResult result) {
        ++completions;
        observed = std::move(result);
    }));

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_TRUE(ValueOrFail(observed).has_value());
    EXPECT_LE(httpClient.LastRequestOptions().GetTimeout(), std::chrono::milliseconds{200});

    // Let the (cancelled) deadline timer's would-be expiry pass; the operation must not complete twice.
    (void)PollUntil(httpClient, [] { return false; }, std::chrono::milliseconds{300});
    EXPECT_EQ(completions, 1);
}

TEST(RetryTests, RequestOptionsWithoutOverridesKeepClientPolicy) {
    const AVEVA::AzureClient::AzureRequestOptions defaults;
    EXPECT_FALSE(defaults.GetTotalDeadline().has_value());
    EXPECT_FALSE(defaults.GetMaxRetries().has_value());
}

TEST(RetryTests, NegativeRetryCountOverrideIsRejected) {
    AVEVA::AzureClient::AzureRequestOptions options;
    EXPECT_THROW(options.SetMaxRetries(-1), std::invalid_argument);
    EXPECT_FALSE(options.GetMaxRetries().has_value());
    options.SetMaxRetries(0);
    EXPECT_EQ(options.GetMaxRetries(), 0);
}

TEST(RetryTests, RetryReSignsSharedKeyAndRefreshesDateHeader) {
    FakeHttpClient httpClient;
    // A 1.1 s server hint guarantees the second attempt crosses an x-ms-date (1 s resolution) boundary.
    httpClient.EnqueueResponse(HttpResponse{503, {{"x-ms-retry-after-ms", "1100"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});
    BlobClientOptions options = MakeRetryOptions(1);
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "c2VjcmV0LWtleQ=="};
    PageBlobClient client{httpClient, options};

    const std::string payload(512, 'p');
    UploadState uploadState;
    StartUploadAsync(client, payload, uploadState);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return uploadState.Done; }));
    EXPECT_TRUE(uploadState.Succeeded);
    ASSERT_EQ(httpClient.RequestCount(), 2U);

    const auto& first = httpClient.RequestAt(0);
    const auto& second = httpClient.RequestAt(1);
    EXPECT_EQ(first.Body, payload);
    EXPECT_EQ(second.Body, payload);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(first.Request, "x-ms-client-request-id"),
              FakeHttpClient::FindHeaderValue(second.Request, "x-ms-client-request-id"));

    const std::string firstDate = FakeHttpClient::FindHeaderValue(first.Request, "x-ms-date");
    const std::string secondDate = FakeHttpClient::FindHeaderValue(second.Request, "x-ms-date");
    EXPECT_NE(firstDate, secondDate);

    EXPECT_EQ(CountAuthorizationHeaders(first.Request), 1);
    EXPECT_EQ(CountAuthorizationHeaders(second.Request), 1);
    EXPECT_NE(FakeHttpClient::FindHeaderValue(first.Request, "Authorization"),
              FakeHttpClient::FindHeaderValue(second.Request, "Authorization"));
}

TEST(RetryTests, EveryAttemptOfARetriedUploadCarriesTheFullBody) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});
    PageBlobClient client{httpClient, MakeRetryOptions(2)};

    const std::string payload(64 * 1024, 'x');
    UploadState uploadState;
    StartUploadAsync(client, payload, uploadState);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return uploadState.Done; }));
    EXPECT_TRUE(uploadState.Succeeded);
    ASSERT_EQ(httpClient.RequestCount(), 3U);
    for (std::size_t i = 0; i < 3U; ++i) {
        EXPECT_EQ(httpClient.RequestAt(i).Body, payload) << "attempt " << i;
    }
}
} // namespace AVEVA::AzureClient::Tests

namespace {
// A generator whose draws are fixed, so the jitter factor is known.
struct FixedGenerator {
    using result_type = std::uint64_t;
    static constexpr result_type min() { return 0; }
    static constexpr result_type max() { return std::numeric_limits<std::uint64_t>::max(); }
    result_type operator()() const { return 0; }
};
} // namespace

TEST(BackoffTests, InjectedGeneratorMakesTheJitterDeterministic) {
    AVEVA::AzureClient::RetryOptions retry;
    retry.InitialDelay = std::chrono::milliseconds{100};
    retry.MaxDelay = std::chrono::milliseconds{10000};
    FixedGenerator generator;

    const auto first = AVEVA::AzureClient::Private::RetryDetail::ComputeBackoff(retry, 1, generator);
    const auto second = AVEVA::AzureClient::Private::RetryDetail::ComputeBackoff(retry, 2, generator);

    EXPECT_EQ(first, std::chrono::milliseconds{80});   // 100 ms * lowest jitter (0.8)
    EXPECT_EQ(second, std::chrono::milliseconds{160}); // doubled per retry
}