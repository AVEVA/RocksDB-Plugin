// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AsyncBehaviorSupport.hpp"

#include <boost/asio/post.hpp>

#include <chrono>
#include <memory>
#include <system_error>
#include <utility>

using namespace AsyncBehaviorSupport;
using AVEVA::AzureClient::Tests::StartAwaitableOn;

// Cancelling before the coroutine has run: the pipeline checks the awaiting coroutine's cancellation state itself, so
// a credential that would have completed immediately must not lead to a request.
TEST(AsyncBehaviorTests, CancellingAnAwaitedOperationCancelsTheInFlightRequest) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    auto operation = StartAwaitable(httpClient, client.DeleteAsync());
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    ASSERT_FALSE(operation.Done());

    operation.Cancel();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_TRUE(IsCanceled(operation.Result().error()));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

// Before send: with a token credential the request is not handed to the transport until the token arrives.
// Cancellation must complete the operation promptly rather than waiting for (possibly slow) credential I/O, and
// the late token must be ignored.
TEST(AsyncBehaviorTests, CancellationDuringTokenAcquisitionCompletesPromptlyAndNeverSends) {
    FakeHttpClient httpClient;
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueDeferred({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    ScriptedBlobClient client{httpClient, options, credential};

    auto operation = StartAwaitable(httpClient, client.DeleteAsync());
    ASSERT_EQ(credential->PendingCount(), 1U);
    ASSERT_FALSE(operation.Done());

    operation.Cancel();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done()) << "cancellation must not wait for the credential";
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_TRUE(IsCanceled(operation.Result().error()));

    EXPECT_FALSE(credential->CompleteNext());
    httpClient.Poll();
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

TEST(AsyncBehaviorTests, MidFlightCancellationWorksForContainerClients) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

    auto operation = StartAwaitable(httpClient, client.CreateIfNotExistsAsync());
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    operation.Cancel();
    httpClient.Poll();

    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_TRUE(IsCanceled(operation.Result().error()));
}

// Supported cancellation pattern: the signal is emitted from the coroutine's executor, so it cannot race with the
// library clearing the slot when the operation finishes.
TEST(AsyncBehaviorTests, CancellationEmittedFromTheExecutorCancelsTheOperation) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    auto operation = StartAwaitable(httpClient, client.DeleteAsync());
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    boost::asio::post(httpClient.get_executor(), [&operation] { operation.Cancel(); });
    httpClient.Poll();
    ASSERT_TRUE(operation.Done());
    ASSERT_FALSE(operation.Result().has_value());
    EXPECT_EQ(operation.Result().error().Code, std::make_error_code(std::errc::operation_canceled));
}
