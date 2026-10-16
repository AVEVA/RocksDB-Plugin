// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AsyncBehaviorSupport.hpp"

using namespace AsyncBehaviorSupport;

TEST(AsyncBehaviorTests, CallbackCanStartAnotherRequestReentrantly) {
    FakeHttpClient httpClient;
    httpClient.EnqueueDeferredResponse(HttpResponse{202, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "0"}}), ""});

    BlobClient client{httpClient, MakeBlobClientOptions()};

    CallbackExpectation deleteCallback;
    CallbackExpectation propertiesCallback;
    StartDeleteAndVerifyReentrantPropertiesRequest(client, deleteCallback, propertiesCallback);

    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    httpClient.Poll(); // drive the posted (async) GetPropertiesAsync completion (T26)
}

TEST(AsyncBehaviorTests, ExceptionsFromUserCallbacksPropagateWithoutCorruptingLaterRequests) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    client.DeleteAsync(ThrowDeleteCallback);

    EXPECT_THROW(static_cast<void>(httpClient.CompleteNext()), std::runtime_error);
    EXPECT_EQ(httpClient.PendingCount(), 0U);

    CallbackExpectation callback;
    StartDeleteAndExpectSuccess(client, callback);
    EXPECT_TRUE(httpClient.CompleteNext());
}

// Task 8 verification: a completion handler that immediately re-issues the same failing
// operation must not recurse through the stack, since the posted completion always resumes on
// the executor's run/poll loop rather than being invoked inline from within the previous
// initiating call.
TEST(AsyncBehaviorTests, RecursivePostedEarlyExitFailuresDoNotGrowTheStack) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, MakePageBlobOptions()};

    constexpr int Iterations = 5000;
    int completedCount = 0;

    std::function<void()> issue = [&]() { IssueRecursiveInvalidPageUpload(client, completedCount, issue); };

    issue();
    DrainRecursiveInvalidPageUploadCompletions(httpClient, completedCount, Iterations);

    EXPECT_EQ(completedCount, Iterations);
}
