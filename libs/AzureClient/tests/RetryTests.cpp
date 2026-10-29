// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"
#include "ValueOrFail.hpp"
#include <AVEVA/AzureClient/Detail/AggregateDeadline.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <algorithm>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <cstddef>
#include <expected>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Tests {
using AVEVA::HttpResponse;

TEST(RetryTests, NoRetries_DoesNotRetryOnTransientFailure) {
    FakeHttpClient httpClient;
    // Single immediate 503 response
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = 0; // no retries

    BlobClient client{httpClient, options};

    const auto result = RunAwaitable(httpClient, client.DeleteAsync());

    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_FALSE(result.has_value());
}

TEST(RetryTests, MaxRetriesAtIntMaxDoesNotOverflow) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{200, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = std::numeric_limits<int>::max();
    options.Retry.InitialDelay = std::chrono::milliseconds{0};

    BlobClient client{httpClient, options};

    const auto result = RunAwaitable(httpClient, client.DeleteAsync());

    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(result.has_value());
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

    const auto result = RunAwaitable(httpClient, client.DeleteAsync());

    EXPECT_GE(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(result.has_value());
}

TEST(RetryTests, FirstAttemptSuccessDoesNotConstructABackoffTimer) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{200, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = 3;

    BlobClient client{httpClient, options};

    const auto before = FakeHttpClient::RetryTimersConstructed();
    const auto result = RunAwaitable(httpClient, client.DeleteAsync());

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(FakeHttpClient::RetryTimersConstructed(), before);
}

TEST(RetryTests, EachRetryConstructsExactlyOneBackoffTimer) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{200, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = 2;
    options.Retry.InitialDelay = std::chrono::milliseconds{0};

    BlobClient client{httpClient, options};

    const auto before = FakeHttpClient::RetryTimersConstructed();
    const auto result = RunAwaitable(httpClient, client.DeleteAsync());

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(FakeHttpClient::RetryTimersConstructed(), before + 2U);
}

namespace {
using DeleteResult = std::expected<Response<Models::DeleteBlobResult>, BlobStorageError>;

// Starts `awaitable` without polling and stores its value in `observed` when it completes.
template <class T>
void StartInto(FakeHttpClient& httpClient, boost::asio::awaitable<T> awaitable, std::optional<T>& observed) {
    Spawn(httpClient, std::move(awaitable), [&observed](T result) { observed = std::move(result); });
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

void StartUploadAsync(FakeHttpClient& httpClient, PageBlobClient& client, const std::string& payload,
                      UploadState& state) {
    Spawn(httpClient, client.UploadPagesAsync(0, payload),
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
        StartInto(httpClient, client.DeleteAsync(), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
        StartUploadAsync(httpClient, client, std::string(512, 'p'), state);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
        StartInto(httpClient, client.DeleteAsync(), observed);

        ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); })) << error.message();
        EXPECT_EQ(httpClient.RequestCount(), 1U) << error.message();
        EXPECT_FALSE(ValueOrFail(observed).has_value()) << error.message();
    }
}

namespace {
// Reduces an operation's outcome to its error (a default one on success) so mutations of different result types can
// be driven by the same helper.
template <class TResult>
boost::asio::awaitable<BlobStorageError>
FailureOf(boost::asio::awaitable<std::expected<Response<TResult>, BlobStorageError>> operation) {
    auto result = co_await std::move(operation);
    co_return result.has_value() ? BlobStorageError{} : std::move(result).error();
}

// Runs a conditional mutation whose first attempt fails with `error`; a replay would answer 412 because the ETag moved.
template <class TStart>
void ExpectConditionalMutationSentOnceOrRetried(const std::error_code& error, bool expectReplay, TStart start) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, error);
    httpClient.EnqueueResponse(HttpResponse{412, {{"x-ms-error-code", "ConditionNotMet"}}, ""});
    PageBlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<BlobStorageError> failure;
    StartInto(httpClient, start(client), failure);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return failure.has_value(); })) << error.message();
    EXPECT_EQ(httpClient.RequestCount(), expectReplay ? 2U : 1U) << error.message();
    // The ambiguous transport outcome is preserved rather than reported as a precondition failure.
    EXPECT_EQ(ValueOrFail(failure).StatusCode == 412U, expectReplay) << error.message();
}

Models::BlobRequestConditions IfMatchConditions() {
    Models::BlobRequestConditions conditions;
    conditions.IfMatch = "\"0x8DC\"";
    return conditions;
}
} // namespace

TEST(RetryTests, ConditionalMutationsAreNotReplayedAfterAnAmbiguousTransportError) {
    const std::error_code error{AVEVA::HttpClientError::ReadFailed};
    const auto upload = [](PageBlobClient& client) {
        UploadPagesOptions options;
        options.Conditions = IfMatchConditions();
        return FailureOf(client.UploadPagesAsync(0, std::string(512, 'p'), options));
    };
    const auto resize = [](PageBlobClient& client) {
        ResizePageBlobOptions options;
        options.Conditions = IfMatchConditions();
        return FailureOf(client.ResizeAsync(1024, options));
    };
    const auto metadata = [](PageBlobClient& client) {
        SetBlobMetadataOptions options;
        options.Conditions = IfMatchConditions();
        return FailureOf(client.SetMetadataAsync(options));
    };
    const auto unmodifiedSince = [](PageBlobClient& client) {
        UploadPagesOptions options;
        options.Conditions.IfUnmodifiedSince = std::chrono::system_clock::now();
        return FailureOf(client.UploadPagesAsync(0, std::string(512, 'p'), options));
    };
    ExpectConditionalMutationSentOnceOrRetried(error, false, upload);
    ExpectConditionalMutationSentOnceOrRetried(error, false, resize);
    ExpectConditionalMutationSentOnceOrRetried(error, false, metadata);
    ExpectConditionalMutationSentOnceOrRetried(error, false, unmodifiedSince);
}

TEST(RetryTests, ConditionalMutationsAreStillRetriedWhenNothingWasSent) {
    ExpectConditionalMutationSentOnceOrRetried(
        std::error_code{AVEVA::HttpClientError::ConnectFailed}, true, [](PageBlobClient& client) {
            UploadPagesOptions options;
            options.Conditions = IfMatchConditions();
            return FailureOf(client.UploadPagesAsync(0, std::string(512, 'p'), options));
        });
}

TEST(RetryTests, WildcardIfMatchMutationIsStillReplayedAfterAnAmbiguousTransportError) {
    ExpectConditionalMutationSentOnceOrRetried(
        std::error_code{AVEVA::HttpClientError::ReadFailed}, true, [](PageBlobClient& client) {
            UploadPagesOptions options;
            options.Conditions.IfMatch = "*";
            return FailureOf(client.UploadPagesAsync(0, std::string(512, 'p'), options));
        });
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
        StartInto(httpClient, client.CreateIfNotExistsAsync(512), observed);

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
    StartInto(httpClient, client.DeleteIfExistsAsync(), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

using ReleaseResult = std::expected<Response<Models::ReleaseBlobLeaseResult>, BlobStorageError>;

TEST(RetryTests, ReleaseLeaseReplayedAfterAnAmbiguousErrorAbsorbsTheLeaseAlreadyReleased409) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ReadFailed);
    httpClient.EnqueueResponse(HttpResponse{409, {{"x-ms-error-code", "LeaseNotPresentWithLeaseOperation"}}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<ReleaseResult> observed;
    StartInto(httpClient, client.ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = "lease"}), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    const auto& value = ValueOrFail(observed);
    ASSERT_TRUE(value.has_value());
    ASSERT_TRUE(value->Error().has_value());
    EXPECT_EQ(value->Error()->StatusCode, 409U);
}

TEST(RetryTests, ReleaseLeaseFirstAttempt409StillFails) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{409, {{"x-ms-error-code", "LeaseNotPresentWithLeaseOperation"}}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<ReleaseResult> observed;
    StartInto(httpClient, client.ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = "lease"}), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_FALSE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, ReleaseLease409AfterATokenRefreshRepeatStillFails) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{401, {{"x-ms-error-code", "InvalidAuthenticationInfo"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{409, {{"x-ms-error-code", "LeaseNotPresentWithLeaseOperation"}}, ""});
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueImmediate({}, MakeToken("old-token", std::chrono::hours(1)));
    credential->EnqueueImmediate({}, MakeToken("new-token", std::chrono::hours(1)));

    BlobClientOptions options = MakeRetryOptions(3);
    options.SasToken.clear();
    BasicBlobClient<FakeHttpClient, ScriptedTokenCredential> client{httpClient, std::move(options), credential};

    std::optional<ReleaseResult> observed;
    StartInto(httpClient, client.ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = "lease"}), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().StatusCode, 409U);
}

TEST(RetryTests, RenewLeaseReplayedAfterAnAmbiguousErrorSurfacesTheSubsequent409) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ReadFailed);
    httpClient.EnqueueResponse(HttpResponse{409, {{"x-ms-error-code", "LeaseNotPresentWithLeaseOperation"}}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<std::expected<Response<Models::RenewBlobLeaseResult>, BlobStorageError>> observed;
    StartInto(httpClient, client.RenewLeaseAsync(RenewLeaseOptions{.LeaseId = "lease"}), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    // Only a release may treat the 409 as success; a renew of a lease that is gone must be seen as a failure.
    ASSERT_FALSE(observed->has_value());
    EXPECT_EQ(observed->error().StatusCode, 409U);
}

TEST(RetryTests, AcquireLeaseReplayedAfterAnAmbiguousErrorSurfacesTheSubsequent409) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ReadFailed);
    httpClient.EnqueueResponse(HttpResponse{409, {{"x-ms-error-code", "LeaseAlreadyPresent"}}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<std::expected<Response<Models::AcquireBlobLeaseResult>, BlobStorageError>> observed;
    StartInto(httpClient,
              client.AcquireLeaseAsync(
                  AcquireLeaseOptions{.ProposedLeaseId = "proposed", .Duration = std::chrono::seconds{15}}),
              observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    // The replay reuses the proposed lease id so that it can tell the caller's own lease apart.
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.LastRequest(), "x-ms-proposed-lease-id"), "proposed");
    ASSERT_FALSE(observed->has_value());
    EXPECT_EQ(observed->error().StatusCode, 409U);
}

TEST(RetryTests, ConditionalUploadPagesAfterAnAmbiguousErrorIsNotReplayedAndKeepsTheTransportError) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ReadFailed);
    httpClient.EnqueueResponse(HttpResponse{412, {{"x-ms-error-code", "ConditionNotMet"}}, ""});
    PageBlobClient client{httpClient, MakeRetryOptions(3)};

    UploadPagesOptions options;
    options.Conditions.IfMatch = "\"0x1\"";
    std::optional<std::expected<Response<Models::UploadPagesResult>, BlobStorageError>> observed;
    StartInto(httpClient, client.UploadPagesAsync(0, std::string(512, 'x'), std::move(options)), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_FALSE(observed->has_value());
    EXPECT_NE(observed->error().StatusCode, 412U);
}

TEST(RetryTests, ConditionalCreateIsRetriedAfterFailuresThatHappenedBeforeAnythingWasSent) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{}, AVEVA::HttpClientError::ConnectFailed);
    httpClient.EnqueueResponse(HttpResponse{201, {}, ""});
    PageBlobClient client{httpClient, MakeRetryOptions(1)};

    std::optional<CreateResult> observed;
    StartInto(httpClient, client.CreateIfNotExistsAsync(512), observed);

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
    StartInto(httpClient, client.CreateAsync(512), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }));
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(0).Request, "Authorization"), "Bearer old-token");
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(httpClient.RequestAt(1).Request, "Authorization"), "Bearer new-token");
    EXPECT_EQ(credential->CallCount(), 2);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, ThrowDuringSecondAttemptCompletesOnceWithAnErrorAndDoesNotEscape) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{401, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueImmediate({}, MakeToken("token", std::chrono::hours(1)));
    credential->EnqueueThrow();

    BlobClientOptions options = MakeRetryOptions(0);
    options.SasToken.clear();
    BasicBlobClient<FakeHttpClient, ScriptedTokenCredential> client{httpClient, std::move(options), credential};

    int completions = 0;
    std::optional<DeleteResult> observed;
    Spawn(httpClient, client.DeleteAsync(), [&](DeleteResult result) {
        ++completions;
        observed = std::move(result);
    });

    EXPECT_NO_THROW(EXPECT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2})));
    EXPECT_EQ(completions, 1);
    ASSERT_TRUE(observed.has_value());
    ASSERT_FALSE(observed->has_value());
    EXPECT_EQ(observed->error().Code, std::make_error_code(std::errc::not_enough_memory));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, UnauthorizedResponseAfterTheDeadlineDoesNotRefreshTheToken) {
    FakeHttpClient httpClient;
    httpClient.EnqueueDeferredResponse(HttpResponse{401, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueImmediate({}, MakeToken("old-token", std::chrono::hours(1)));
    credential->EnqueueImmediate({}, MakeToken("new-token", std::chrono::hours(1)));

    BlobClientOptions options = MakeRetryOptions(0);
    options.SasToken.clear();
    BasicBlobClient<FakeHttpClient, ScriptedTokenCredential> client{httpClient, std::move(options), credential};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{15});
    std::optional<DeleteResult> observed;
    StartInto(httpClient, client.DeleteAsync({}, requestOptions), observed);
    ASSERT_TRUE(PollUntil(httpClient, [&] { return httpClient.PendingCount() == 1U; }, std::chrono::seconds{2}));
    // The 401 arrives after the deadline but before its timer has been serviced.
    std::this_thread::sleep_for(std::chrono::milliseconds{40});
    ASSERT_TRUE(httpClient.CompleteNext());
    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));

    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_EQ(credential->CallCount(), 1);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, HungTokenAcquisitionIsBoundedByTheTotalDeadline) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueDeferred({}, MakeToken("never-delivered", std::chrono::hours(1)));

    BlobClientOptions options = MakeRetryOptions(0);
    options.SasToken.clear();
    BasicBlobClient<FakeHttpClient, ScriptedTokenCredential> client{httpClient, std::move(options), credential};

    AVEVA::AzureClient::AzureRequestOptions requestOptions;
    requestOptions.SetTotalDeadline(std::chrono::milliseconds{50});
    std::optional<DeleteResult> observed;
    StartInto(httpClient, client.DeleteAsync({}, requestOptions), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{5}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::timed_out));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
        StartInto(httpClient, client.DeleteAsync(), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

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
    StartInto(httpClient, client.DeleteAsync(), observed);

    ASSERT_TRUE(PollUntil(httpClient, [&] { return observed.has_value(); }, std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, MalformedOrNegativeRetryAfterHintsAreTreatedAsAbsent) {
    for (const std::string_view value : {"-5", "-0", "+3", "abc", "1.5", "5 seconds", "", "99999999999999999999"}) {
        const HttpResponse response{503, {{"Retry-After", std::string{value}}}, ""};
        const auto hint = AVEVA::AzureClient::Private::ParseRetryAfter(response);
        EXPECT_FALSE(hint.has_value()) << value;
    }
    const HttpResponse negativeMs{503, {{"x-ms-retry-after-ms", "-100"}}, ""};
    EXPECT_FALSE(AVEVA::AzureClient::Private::ParseRetryAfter(negativeMs).has_value());
}

TEST(RetryTests, RetryAfterTrimsOnlySpaceAndTab) {
    using AVEVA::AzureClient::Private::ParseRetryAfter;
    for (const std::string_view value : {" \t7\t ", "7", "\t7"}) {
        const HttpResponse response{503, {{"Retry-After", std::string{value}}}, ""};
        const auto hint = ParseRetryAfter(response);
        ASSERT_TRUE(hint.has_value()) << value;
        EXPECT_EQ(*hint, std::chrono::seconds{7});
    }
    for (const std::string_view value : {"\v7", "7\f", "\r7", "7\n", " \t ", "\t"}) {
        const HttpResponse response{503, {{"Retry-After", std::string{value}}}, ""};
        EXPECT_FALSE(ParseRetryAfter(response).has_value()) << value;
    }
}

TEST(RetryTests, RetryAfterHintsAreNeverNegative) {
    const HttpResponse pastDate{503, {{"Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT"}}, ""};
    const auto hint = AVEVA::AzureClient::Private::ParseRetryAfter(pastDate);
    ASSERT_TRUE(hint.has_value());
    EXPECT_GE(*hint, std::chrono::milliseconds{0});
}

TEST(RetryTests, NegativeRetryAfterFallsBackToTheComputedBackoff) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {{"Retry-After", "-30"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(1, std::chrono::milliseconds{0})};

    std::optional<DeleteResult> observed;
    StartInto(httpClient, client.DeleteAsync(), observed);

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

    auto operation = StartAwaitable(httpClient, client.DeleteAsync());

    EXPECT_FALSE(PollUntil(httpClient, [&] { return operation.Done(); }, std::chrono::milliseconds{300}));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    operation.Cancel();
    httpClient.Poll();
}

TEST(RetryTests, CancellationDuringBackoffCompletesWithOperationCanceledAndSendsNothingMore) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    // The 503 is delivered on the first poll; the operation is then waiting on its backoff timer.
    auto operation = StartAwaitable(httpClient, client.DeleteAsync());
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_FALSE(operation.Done());

    operation.Cancel();
    ASSERT_TRUE(PollUntil(httpClient, [&] { return operation.Done(); }, std::chrono::seconds{2}));
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, PartialCancellationDoesNotAbortTheOperationButTerminalDoes) {
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    auto operation = StartAwaitable(httpClient, client.DeleteAsync());
    ASSERT_EQ(httpClient.RequestCount(), 1U);

    operation.Cancel(boost::asio::cancellation_type::partial);
    EXPECT_FALSE(PollUntil(httpClient, [&] { return operation.Done(); }, std::chrono::milliseconds{200}));

    operation.Cancel(boost::asio::cancellation_type::terminal);
    ASSERT_TRUE(PollUntil(httpClient, [&] { return operation.Done(); }, std::chrono::seconds{2}));
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::operation_canceled));
}

TEST(RetryTests, CancellationOfInFlightAttemptIsNotRetried) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeRetryOptions(3)};

    auto operation = StartAwaitable(httpClient, client.DeleteAsync());
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    operation.Cancel();
    httpClient.Poll();
    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::operation_canceled));
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
    StartInto(httpClient, client.DeleteAsync({}, requestOptions), observed);

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
    StartInto(httpClient, client.DeleteAsync({}, requestOptions), observed);

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
    StartInto(httpClient, client.DeleteAsync({}, requestOptions), observed);

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
    StartInto(httpClient, client.DeleteAsync({}, requestOptions), observed);

    // Without the check the operation would idle until the 10 second deadline fired, which outlasts this poll.
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
    Spawn(httpClient, client.DeleteAsync({}, requestOptions), [&](DeleteResult result) {
        ++completions;
        observed = std::move(result);
    });

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

TEST(RetryTests, ExtremeTotalDeadlinesAreSaturated) {
    AVEVA::AzureClient::AzureRequestOptions options;
    options.SetTotalDeadline(std::chrono::milliseconds::max());
    EXPECT_EQ(options.GetTotalDeadline(), AVEVA::AzureClient::AzureRequestOptions::MaxTotalDeadline);
    options.SetTotalDeadline(std::chrono::milliseconds::min());
    EXPECT_EQ(options.GetTotalDeadline(), -AVEVA::AzureClient::AzureRequestOptions::MaxTotalDeadline);
    options.SetTotalDeadline(std::chrono::seconds{10});
    EXPECT_EQ(options.GetTotalDeadline(), std::chrono::milliseconds{10000});

    // The aggregate budget adds the saturated value to a clock reading without overflowing.
    options.SetTotalDeadline(std::chrono::milliseconds::max());
    const AVEVA::AzureClient::Private::AggregateDeadline aggregate{options};
    AVEVA::AzureClient::AzureRequestOptions child;
    EXPECT_TRUE(aggregate.Apply(child));
    EXPECT_FALSE(aggregate.Expired());
    options.SetTotalDeadline(std::chrono::milliseconds::min());
    const AVEVA::AzureClient::Private::AggregateDeadline spent{options};
    EXPECT_FALSE(spent.Apply(child));
    EXPECT_TRUE(spent.Expired());
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
    StartUploadAsync(httpClient, client, payload, uploadState);

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
    StartUploadAsync(httpClient, client, payload, uploadState);

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
TEST(BackoffTests, ExtremeDelaysAreClampedBeforeFloatingPointConversion) {
    AVEVA::AzureClient::RetryOptions retry;
    retry.InitialDelay = std::chrono::milliseconds::max();
    retry.MaxDelay = std::chrono::milliseconds::max();
    FixedGenerator generator;

    for (const int retryNumber : {1, 2, 64, std::numeric_limits<int>::max()}) {
        const auto delay = AVEVA::AzureClient::Private::RetryDetail::ComputeBackoff(retry, retryNumber, generator);
        EXPECT_GE(delay, std::chrono::milliseconds{0}) << retryNumber;
        EXPECT_LE(delay, AVEVA::AzureClient::AzureRequestOptions::MaxTotalDeadline) << retryNumber;
    }

    retry.InitialDelay = std::chrono::milliseconds::min();
    retry.MaxDelay = std::chrono::milliseconds::min();
    EXPECT_EQ(AVEVA::AzureClient::Private::RetryDetail::ComputeBackoff(retry, 3, generator),
              std::chrono::milliseconds{0});
}

TEST(RetryTests, WidelySeparatedRetryAfterAndServerDatesAreBounded) {
    constexpr std::chrono::milliseconds Day{std::chrono::hours{24}};
    const AVEVA::HttpResponse farFuture{
        503, {{"Retry-After", "Wed, 01 Jan 2200 00:00:00 GMT"}, {"Date", "Fri, 01 Jan 1700 00:00:00 GMT"}}, ""};
    const AVEVA::HttpResponse farPast{
        503, {{"Retry-After", "Fri, 01 Jan 1700 00:00:00 GMT"}, {"Date", "Wed, 01 Jan 2200 00:00:00 GMT"}}, ""};

    EXPECT_EQ(AVEVA::AzureClient::Private::ParseRetryAfter(farFuture), std::optional{Day});
    EXPECT_EQ(AVEVA::AzureClient::Private::ParseRetryAfter(farPast), std::optional{std::chrono::milliseconds{0}});
}
