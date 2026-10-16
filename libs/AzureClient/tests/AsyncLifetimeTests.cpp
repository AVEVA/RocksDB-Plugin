// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AsyncBehaviorSupport.hpp"

using namespace AsyncBehaviorSupport;

TEST(AsyncBehaviorTests, DeferredOperationRunsAfterItsClientWasMovedFromAndDestroyed) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto client = std::make_unique<BlobClient>(httpClient, MakeBlobClientOptions());
    auto operation = client->DeleteAsync(boost::asio::deferred);
    BlobClient moved{std::move(*client)};
    client.reset();

    std::optional<DeleteResult> observed;
    InvokeDeferredDeleteOperation(std::move(operation), observed);
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_TRUE(observed.has_value());
}

TEST(AsyncBehaviorTests, DeferredContainerOperationRunsAfterItsClientWasDestroyed) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto client = std::make_unique<BlobContainerClient>(httpClient, MakeBlobContainerClientOptions());
    auto operation = client->CreateIfNotExistsAsync(boost::asio::deferred);
    client.reset();

    int completions = 0;
    std::move(operation)([&](auto) { ++completions; });
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(completions, 1);
}

TEST(AsyncBehaviorTests, BlobClientCanBeDestroyedAfterRequestIsInFlight) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    CallbackExpectation callback;
    {
        auto client = std::make_unique<BlobClient>(httpClient, MakeBlobClientOptions());
        client->DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result) {
            ASSERT_TRUE(result.has_value());
            callback.MarkInvoked();
        });
    }

    EXPECT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, PageBlobClientCanBeDestroyedAfterRequestIsInFlight) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    CallbackExpectation callback;
    {
        auto client = std::make_unique<PageBlobClient>(httpClient, MakePageBlobOptions());
        client->DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result) {
            EXPECT_TRUE(result.has_value());
            callback.MarkInvoked();
        });
    }

    EXPECT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, TokenCredentialRequestCanDestroyClientAfterRequestIsSent) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueDeferred({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    CallbackExpectation callback;
    auto client = std::make_unique<ScriptedBlobClient>(httpClient, std::move(options), tokenCredential);
    StartDeleteAndExpectSuccess(*client, callback);

    EXPECT_EQ(tokenCredential->PendingCount(), 1U);
    EXPECT_EQ(httpClient.RequestCount(), 0U);

    EXPECT_TRUE(tokenCredential->CompleteNext());
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.PendingCount(), 1U);

    client.reset();

    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, CallbackCanDestroyClientAfterCompletionBegins) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto client = std::make_unique<BlobClient>(httpClient, MakeBlobClientOptions());
    CallbackExpectation callback;
    client->DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result) {
        ASSERT_TRUE(result.has_value());
        client.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(client, nullptr);
}

TEST(AsyncBehaviorTests, DownloadToAsyncAllowsDestroyingTheTargetStreamInTheCompletionHandler) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    httpClient.DefaultResponse() = HttpResponse{
        200, MakeCanonicalSuccessHeaders({{"Content-Length", "4"}, {"Content-Type", "text/plain"}}), "data"};
    BlobClient client{httpClient, MakeBlobClientOptions()};

    auto stream = std::make_shared<std::string>(4U, '\0');
    CallbackExpectation callback;
    StartDownloadToAndReleaseStream(client, stream, callback);

    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(stream, nullptr);
}

// Destroying the transport -- and with it the io_context that owns every pending handler, timer and
// posted completion -- must destroy the user's handler (releasing everything it owns) without ever
// invoking it. The sentinel's weak_ptr proves nothing is leaked by a reference cycle; ASan/LSan on CI
// catch use-after-free or leaks in the engines' teardown.
TEST(AsyncBehaviorTests, DestroyingTheIoContextWithPendingWorkReleasesTheHandlerWithoutInvokingIt) {
    VerifyPendingWorkHandlersAreReleased();
}
