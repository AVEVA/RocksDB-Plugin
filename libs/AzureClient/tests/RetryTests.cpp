#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "BlobRequestHelpers.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlockBlobClient.hpp>
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

using AVEVA::AzureClient::Tests::ValueOrFail;

using namespace AVEVA::AzureClient;
using namespace AVEVA::AzureClient::Tests;
using AVEVA::HttpResponse;

TEST(RetryTests, NoRetries_DoesNotRetryOnTransientFailure)
{
    FakeHttpClient httpClient;
    // Single immediate 503 response
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = 0; // no retries

    BlockBlobClient client{httpClient, options};

    CallbackExpectation cb;
    std::optional<BlobStorageError> observedError;
    client.DeleteAsync([&](std::expected<Response<Models::DeleteBlobResult>, BlobStorageError> result) mutable
    {
        if (!result.has_value())
        {
            observedError = std::move(result).error();
        }
        cb.MarkInvoked();
    });

    // Ensure any posted completions run
    httpClient.Poll();

    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(observedError.has_value());
}

TEST(RetryTests, MaxRetriesAtIntMaxDoesNotOverflow)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{200, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = std::numeric_limits<int>::max();
    options.Retry.InitialDelay = std::chrono::milliseconds{0};

    BlockBlobClient client{httpClient, options};

    CallbackExpectation cb;
    std::optional<BlobStorageError> observedError;
    client.DeleteAsync([&](std::expected<Response<Models::DeleteBlobResult>, BlobStorageError> result) mutable
    {
        if (!result.has_value())
        {
            observedError = std::move(result).error();
        }
        cb.MarkInvoked();
    });

    EXPECT_TRUE(httpClient.WaitForRequest(2));
    httpClient.Poll();

    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_FALSE(observedError.has_value());
}

TEST(RetryTests, Retries_OnTransientThenSucceeds)
{
    FakeHttpClient httpClient;
    // First respond 503, then 200
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{200, {}, ""});

    BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
    options.Retry.MaxRetries = 1; // allow one retry
    options.Retry.InitialDelay = std::chrono::milliseconds{0};

    BlockBlobClient client{httpClient, options};

    CallbackExpectation cb;
    std::optional<Response<Models::DeleteBlobResult>> success;
    std::optional<BlobStorageError> observedError;

    client.DeleteAsync([&](std::expected<Response<Models::DeleteBlobResult>, BlobStorageError> result) mutable
    {
        if (result.has_value())
        {
            success = std::move(result).value();
        }
        else
        {
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

namespace
{
    using DeleteResult = std::expected<Response<Models::DeleteBlobResult>, BlobStorageError>;

    void StartDeleteAsync(BlockBlobClient& client,
        boost::asio::cancellation_signal& signal,
        std::optional<DeleteResult>& observed)
    {
        client.DeleteAsync(boost::asio::bind_cancellation_slot(signal.slot(),
            [&](DeleteResult result)
        {
            observed = std::move(result);
        }));
    }

    // Drives the fake transport's io_context (which also runs retry backoff timers) until `done`
    // holds or the deadline expires.
    bool PollUntil(FakeHttpClient& httpClient,
        const std::function<bool()>& done,
        std::chrono::milliseconds timeout = std::chrono::seconds{5})
    {
        const bool completed = httpClient.RunUntil(done, timeout);
        httpClient.Poll();
        return completed;
    }

    BlobClientOptions MakeRetryOptions(int maxRetries,
        std::chrono::milliseconds initialDelay = std::chrono::milliseconds{0})
    {
        BlobClientOptions options = MakeBlobClientOptions("container", "blob.txt");
        options.Retry.MaxRetries = maxRetries;
        options.Retry.InitialDelay = initialDelay;
        return options;
    }

    struct UploadState
    {
        bool Done = false;
        bool Succeeded = false;
    };

    void StartUploadAsync(BlockBlobClient& client, const std::string& payload, UploadState& state)
    {
        client.UploadAsync(payload,
            [&](std::expected<Response<Models::UploadBlockBlobResult>, BlobStorageError> result)
        {
            state.Succeeded = result.has_value();
            state.Done = true;
        });
    }

    [[nodiscard]] std::size_t CountAuthorizationHeaders(const AVEVA::HttpRequest& request)
    {
        return static_cast<std::size_t>(std::ranges::count_if(request.GetHeaders(),
            [](const AVEVA::HttpHeader& header)
        {
            return header.GetName() == "Authorization";
        }));
    }
} // namespace

TEST(RetryTests, RetriesEveryTransientStatus)
{
    for (const unsigned status : {408U, 429U, 500U, 502U, 503U, 504U})
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{status, {}, ""});
        httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
        BlockBlobClient client{httpClient, MakeRetryOptions(1)};

        std::optional<DeleteResult> observed;
        client.DeleteAsync([&](DeleteResult result)
        {
            observed = std::move(result);
        });

        ASSERT_TRUE(PollUntil(httpClient,
            [&]
        {
            return observed.has_value();
        })) << "status "
            << status;
        EXPECT_EQ(httpClient.RequestCount(), 2U) << "status " << status;
        EXPECT_TRUE(ValueOrFail(observed).has_value()) << "status " << status;
    }
}

TEST(RetryTests, DoesNotRetryNonTransientStatus)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlockBlobClient client{httpClient, MakeRetryOptions(3)};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result)
    {
        observed = std::move(result);
    });

    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    }));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().StatusCode, 404U);
}

TEST(RetryTests, RetriesTransientTransportErrors)
{
    for (const std::error_code error : {std::error_code{AVEVA::HttpClientError::TimedOut},
             std::error_code{AVEVA::HttpClientError::ConnectFailed},
             std::make_error_code(std::errc::connection_reset)})
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{}, error);
        httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
        BlockBlobClient client{httpClient, MakeRetryOptions(1)};

        std::optional<DeleteResult> observed;
        client.DeleteAsync([&](DeleteResult result)
        {
            observed = std::move(result);
        });

        ASSERT_TRUE(PollUntil(httpClient,
            [&]
        {
            return observed.has_value();
        })) << error.message();
        EXPECT_EQ(httpClient.RequestCount(), 2U) << error.message();
        EXPECT_TRUE(ValueOrFail(observed).has_value()) << error.message();
    }
}

TEST(RetryTests, GivesUpAfterMaxRetriesAndReportsLastResponse)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {{"x-ms-request-id", "first"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{503, {{"x-ms-request-id", "second"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{500, {{"x-ms-request-id", "last"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlockBlobClient client{httpClient, MakeRetryOptions(2)};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result)
    {
        observed = std::move(result);
    });

    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    }));
    EXPECT_EQ(httpClient.RequestCount(), 3U);
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().StatusCode, 500U);
    EXPECT_EQ(ValueOrFail(observed).error().RequestId, "last");
}

TEST(RetryTests, HonorsServerRetryAfterHeadersOverLongBackoff)
{
    for (const std::string_view header : {"x-ms-retry-after-ms", "Retry-After"})
    {
        FakeHttpClient httpClient;
        httpClient.EnqueueResponse(HttpResponse{503, {{std::string{header}, "0"}}, ""});
        httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
        // The computed backoff would be ~1 minute; the server hint of 0 must win.
        BlockBlobClient client{httpClient, MakeRetryOptions(1, std::chrono::minutes{1})};

        std::optional<DeleteResult> observed;
        client.DeleteAsync([&](DeleteResult result)
        {
            observed = std::move(result);
        });

        ASSERT_TRUE(PollUntil(httpClient,
            [&]
        {
            return observed.has_value();
        },
            std::chrono::seconds{2}))
            << header;
        EXPECT_EQ(httpClient.RequestCount(), 2U) << header;
        EXPECT_TRUE(ValueOrFail(observed).has_value()) << header;
    }
}

TEST(RetryTests, RetryAfterHttpDateInThePastRetriesImmediately)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {{"Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlockBlobClient client{httpClient, MakeRetryOptions(1, std::chrono::minutes{1})};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result)
    {
        observed = std::move(result);
    });

    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    },
        std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}

TEST(RetryTests, RetryAfterFutureHttpDateDelaysTheRetry)
{
    FakeHttpClient httpClient;
    const auto retryAt = std::chrono::system_clock::now() + std::chrono::seconds{2};
    httpClient.EnqueueResponse(HttpResponse{503,
        {{"Retry-After", AVEVA::AzureClient::Private::BuildDateHeaderValue(retryAt)}},
        ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlockBlobClient client{httpClient, MakeRetryOptions(1, std::chrono::milliseconds{0})};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result)
    {
        observed = std::move(result);
    });

    httpClient.Poll();
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_FALSE(PollUntil(httpClient,
        [&]
    {
        return httpClient.RequestCount() >= 2U;
    },
        std::chrono::milliseconds{500}));
    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    },
        std::chrono::seconds{4}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, XMsRetryAfterMsTakesPrecedenceOverRetryAfter)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503,
        {{"x-ms-retry-after-ms", "0"}, {"Retry-After", "Wed, 21 Oct 2099 07:28:00 GMT"}},
        ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlockBlobClient client{httpClient, MakeRetryOptions(1, std::chrono::minutes{1})};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result)
    {
        observed = std::move(result);
    });

    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    },
        std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, ComputedBackoffIsCappedByMaxDelay)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClientOptions options = MakeRetryOptions(1, std::chrono::minutes{1});
    options.Retry.MaxDelay = std::chrono::milliseconds{0};
    BlockBlobClient client{httpClient, options};

    std::optional<DeleteResult> observed;
    client.DeleteAsync([&](DeleteResult result)
    {
        observed = std::move(result);
    });

    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    },
        std::chrono::seconds{2}));
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}

TEST(RetryTests, ServerRetryAfterHintIsNotClampedToMaxDelay)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {{"Retry-After", "3600"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlobClientOptions options = MakeRetryOptions(1);
    options.Retry.MaxDelay = std::chrono::milliseconds{0};
    BlockBlobClient client{httpClient, options};

    std::optional<DeleteResult> observed;
    boost::asio::cancellation_signal signal;
    client.DeleteAsync(boost::asio::bind_cancellation_slot(signal.slot(),
        [&](DeleteResult result)
    {
        observed = std::move(result);
    }));

    EXPECT_FALSE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    },
        std::chrono::milliseconds{300}));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    signal.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();
}

TEST(RetryTests, CancellationDuringBackoffCompletesWithOperationCanceledAndSendsNothingMore)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlockBlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    StartDeleteAsync(client, signal, observed);

    httpClient.Poll(); // deliver the 503; the operation is now waiting on its backoff timer
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    ASSERT_FALSE(observed.has_value());

    signal.emit(boost::asio::cancellation_type::terminal);
    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    },
        std::chrono::seconds{2}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_FALSE(signal.slot().has_handler());
}

TEST(RetryTests, CancellationFromAnotherThreadWhileRetryTimerIsPending)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    httpClient.EnqueueResponse(HttpResponse{202, {}, ""});
    BlockBlobClient client{httpClient, MakeRetryOptions(3, std::chrono::minutes{1})};

    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    StartDeleteAsync(client, signal, observed);
    httpClient.Poll();
    ASSERT_EQ(httpClient.RequestCount(), 1U);

    std::thread canceller([&]
    {
        signal.emit(boost::asio::cancellation_type::terminal);
    });
    canceller.join();

    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return observed.has_value();
    },
        std::chrono::seconds{2}));
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_EQ(ValueOrFail(observed).error().Code, std::make_error_code(std::errc::operation_canceled));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

TEST(RetryTests, CancellationOfInFlightAttemptIsNotRetried)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeRetryOptions(3)};

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

TEST(RetryTests, RetryReSignsSharedKeyAndRefreshesDateHeader)
{
    FakeHttpClient httpClient;
    // A 1.1 s server hint guarantees the second attempt crosses an x-ms-date (1 s resolution) boundary.
    httpClient.EnqueueResponse(HttpResponse{503, {{"x-ms-retry-after-ms", "1100"}}, ""});
    httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders(), ""});
    BlobClientOptions options = MakeRetryOptions(1);
    options.SasToken.clear();
    options.SharedKey = {.AccountName = "storageaccount", .AccountKey = "c2VjcmV0LWtleQ=="};
    BlockBlobClient client{httpClient, options};

    UploadState uploadState;
    StartUploadAsync(client, "payload", uploadState);

    ASSERT_TRUE(PollUntil(httpClient,
        [&]
    {
        return uploadState.Done;
    }));
    EXPECT_TRUE(uploadState.Succeeded);
    ASSERT_EQ(httpClient.RequestCount(), 2U);

    const auto& first = httpClient.RequestAt(0);
    const auto& second = httpClient.RequestAt(1);
    EXPECT_EQ(first.Body, "payload");
    EXPECT_EQ(second.Body, "payload");
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
