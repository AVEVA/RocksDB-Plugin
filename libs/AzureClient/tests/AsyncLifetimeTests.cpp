// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AsyncBehaviorSupport.hpp"

#include <boost/asio/io_context.hpp>

#include <memory>
#include <string>
#include <utility>

using namespace AsyncBehaviorSupport;

namespace {
using AVEVA::AzureClient::Tests::Spawn;
} // namespace

TEST(AsyncBehaviorTests, AwaitableRunsAfterItsClientWasMovedFromAndDestroyed) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto client = std::make_unique<BlobClient>(httpClient, MakeBlobClientOptions());
    auto operation = client->DeleteAsync();
    BlobClient moved{std::move(*client)};
    client.reset();

    auto started = StartAwaitable(httpClient, std::move(operation));
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    ASSERT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
    ASSERT_TRUE(started.Done());
    EXPECT_TRUE(started.Result().has_value());
}

TEST(AsyncBehaviorTests, ContainerAwaitableRunsAfterItsClientWasDestroyed) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto client = std::make_unique<BlobContainerClient>(httpClient, MakeBlobContainerClientOptions());
    auto operation = client->CreateIfNotExistsAsync();
    client.reset();

    auto started = StartAwaitable(httpClient, std::move(operation));
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    ASSERT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
    EXPECT_TRUE(started.Done());
}

TEST(AsyncBehaviorTests, BlobClientCanBeDestroyedAfterRequestIsInFlight) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    std::optional<Operation<DeleteResult>> operation;
    {
        auto client = std::make_unique<BlobClient>(httpClient, MakeBlobClientOptions());
        operation = StartAwaitable(httpClient, client->DeleteAsync());
    }

    EXPECT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
    ASSERT_TRUE(operation->Done());
    EXPECT_TRUE(operation->Result().has_value());
}

TEST(AsyncBehaviorTests, PageBlobClientCanBeDestroyedAfterRequestIsInFlight) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    std::optional<Operation<DeleteResult>> operation;
    {
        auto client = std::make_unique<PageBlobClient>(httpClient, MakePageBlobOptions());
        operation = StartAwaitable(httpClient, client->DeleteAsync());
    }

    EXPECT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
    ASSERT_TRUE(operation->Done());
    EXPECT_TRUE(operation->Result().has_value());
}

TEST(AsyncBehaviorTests, TokenCredentialRequestCanDestroyClientAfterRequestIsSent) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueDeferred({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    auto client = std::make_unique<ScriptedBlobClient>(httpClient, std::move(options), tokenCredential);
    auto operation = StartAwaitable(httpClient, client->DeleteAsync());

    EXPECT_EQ(tokenCredential->PendingCount(), 1U);
    EXPECT_EQ(httpClient.RequestCount(), 0U);

    EXPECT_TRUE(tokenCredential->CompleteNext());
    httpClient.Poll();
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.PendingCount(), 1U);

    client.reset();

    EXPECT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
    ASSERT_TRUE(operation.Done());
    EXPECT_TRUE(operation.Result().has_value());
}

TEST(AsyncBehaviorTests, CompletionHandlerCanDestroyClientAfterCompletionBegins) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto client = std::make_unique<BlobClient>(httpClient, MakeBlobClientOptions());
    bool invoked = false;
    Spawn(httpClient, client->DeleteAsync(), [&](DeleteResult result) {
        EXPECT_TRUE(result.has_value());
        client.reset();
        invoked = true;
    });
    httpClient.Poll();

    EXPECT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
    EXPECT_TRUE(invoked);
    EXPECT_EQ(client, nullptr);
}

// Destroying the io_context that owns every suspended coroutine must destroy the coroutine frame (releasing everything
// it owns) without ever resuming it or invoking the completion handler. The sentinel's weak_ptr proves nothing is
// leaked by a reference cycle; ASan/LSan on CI catch use-after-free or leaks in the teardown.
TEST(AsyncBehaviorTests, DestroyingTheIoContextWithPendingWorkReleasesTheHandlerWithoutInvokingIt) {
    auto context = std::make_unique<boost::asio::io_context>();
    auto httpClient = std::make_unique<FakeHttpClient>(*context);
    httpClient->DeferByDefault() = true;
    auto client = std::make_unique<BlobClient>(*httpClient, MakeBlobClientOptions());

    auto sentinel = std::make_shared<int>(0);
    std::weak_ptr<int> weak = sentinel;
    bool invoked = false;
    Spawn(*httpClient, client->DeleteAsync(),
          [sentinel = std::move(sentinel), &invoked](DeleteResult) { invoked = true; });
    httpClient->Poll();
    ASSERT_EQ(httpClient->PendingCount(), 1U);
    ASSERT_FALSE(weak.expired());

    client.reset();
    httpClient.reset();
    context.reset();

    EXPECT_TRUE(weak.expired());
    EXPECT_FALSE(invoked);
}
