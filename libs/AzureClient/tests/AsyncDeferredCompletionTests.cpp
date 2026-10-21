// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AsyncBehaviorSupport.hpp"

using namespace AsyncBehaviorSupport;

namespace {
// T26 acceptance: table-driven guard asserting that no public operation, across every client
// type, ever invokes its completion handler before the initiating ...Async() call returns.
// Each row issues one representative call per named operation (overloads are not repeated,
// since they all funnel through the same SendAuthorizedRequestAsync/PostCompletion machinery)
// against a FakeHttpClient configured to resolve generically (so parsing correctness is not
// the concern here -- that's covered by each client's own test file). A single row that fired
// synchronously would make `invoked` true before Poll() runs and fail the EXPECT_FALSE below.
//
// Each client is owned by a shared_ptr captured (by value) inside its own completion handler,
// so it stays alive for exactly as long as the posted completion is outstanding, rather than
// relying on `[&]` into a local that could be destroyed before the (posted, not inline)
// completion runs.
struct DeferredCompletionCase {
    const char* Name;
    std::function<void(FakeHttpClient&, bool&)> Invoke;
};

[[nodiscard]] std::vector<DeferredCompletionCase> MakeDeferredCompletionCases() {
    using AVEVA::AzureClient::ReleaseLeaseOptions;
    using AVEVA::AzureClient::SetBlobMetadataOptions;

    std::vector<DeferredCompletionCase> cases;

    // --- BlobClient ---
    cases.push_back({.Name = "BlobClient.DownloadAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->DownloadAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.DownloadToAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         auto destination = std::make_shared<std::string>(4U, '\0');
                         client->DownloadToAsync(std::span<char>(*destination),
                                                 AVEVA::AzureClient::DownloadBlobOptions{},
                                                 [client, destination, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.DeleteAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->DeleteAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.GetPropertiesAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->GetPropertiesAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.SetMetadataAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->SetMetadataAsync(SetBlobMetadataOptions{},
                                                  [client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.AcquireLeaseAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->AcquireLeaseAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.ReleaseLeaseAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = "lease-1"},
                                                   [client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.ExistsAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->ExistsAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "BlobClient.DeleteIfExistsAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobClient>(http, MakeBlobClientOptions());
                         client->DeleteIfExistsAsync([client, &invoked](auto) { invoked = true; });
                     }});

    // --- PageBlobClient ---
    cases.push_back({.Name = "PageBlobClient.CreateAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->CreateAsync(512U, [client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.UploadPagesAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->UploadPagesAsync(0, std::string(512U, 'x'),
                                                  [client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.ResizeAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->ResizeAsync(512U, [client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.DownloadAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->DownloadAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.DownloadToAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         auto destination = std::make_shared<std::string>(4U, '\0');
                         client->DownloadToAsync(std::span<char>(*destination),
                                                 AVEVA::AzureClient::DownloadBlobOptions{},
                                                 [client, destination, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.DeleteAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->DeleteAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.GetPropertiesAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->GetPropertiesAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.SetMetadataAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->SetMetadataAsync(SetBlobMetadataOptions{},
                                                  [client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.AcquireLeaseAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->AcquireLeaseAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.ReleaseLeaseAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = "lease-1"},
                                                   [client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.ExistsAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->ExistsAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back({.Name = "PageBlobClient.DeleteIfExistsAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
                         client->DeleteIfExistsAsync([client, &invoked](auto) { invoked = true; });
                     }});

    // --- BlobContainerClient ---
    cases.push_back({.Name = "BlobContainerClient.ListBlobsAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
                         auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
                         client->ListBlobsAsync([client, &invoked](auto) { invoked = true; });
                     }});
    cases.push_back(
        {.Name = "BlobContainerClient.CreateIfNotExistsAsync", .Invoke = [](FakeHttpClient& http, bool& invoked) {
             auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
             client->CreateIfNotExistsAsync([client, &invoked](auto) { invoked = true; });
         }});

    return cases;
}
} // namespace

// ---------------------------------------------------------------------------------------------
// T29: async-behaviour matrix. Already covered elsewhere and not repeated here: use_future (each
// client's *Tests.cpp), co_await on the default deferred token (Task 14 tests), bind_executor(strand)
// and bind_allocator (above), move-only handlers (above), cancellation during retry backoff and of an
// in-flight attempt (RetryTests), and mid-transfer cancellation of UploadFrom/DownloadTo
// (T10_ConcurrentUploadTests, T04_ConcurrencyTests, T11_ParallelDownloadAcceptanceTests).
// ---------------------------------------------------------------------------------------------

TEST(AsyncBehaviorTests, HandlerRunsOnlyAfterExplicitDeferredCompletion) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    bool callReturned = false;
    bool callbackObservedReturnedCall = false;
    CallbackExpectation callback;
    StartDeleteAndVerifyExplicitDeferredCompletion(client, callReturned, callbackObservedReturnedCall, callback);

    EXPECT_FALSE(callbackObservedReturnedCall);
    EXPECT_EQ(httpClient.PendingCount(), 1U);

    callReturned = true;
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_TRUE(callbackObservedReturnedCall);
}

TEST(AsyncBehaviorTests, MoveOnlyCapturedStateSurvivesDeferredCompletion) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.DeleteAsync([state = std::make_unique<std::string>("move-only-state"), &observed,
                        &callback](std::expected<Response<DeleteBlobResult>, BlobStorageError>) mutable {
        ASSERT_TRUE(state);
        observed = *state;
        state.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(observed.empty());
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(observed, "move-only-state");
}

// Task 15 verification: a move-only completion token (a lambda capturing a std::unique_ptr) must
// compile and run correctly for at least one operation per client class, now that every
// ...Async call forwards `token` via Private::InitiateAsync instead of passing it unforwarded.
TEST(AsyncBehaviorTests, PageBlobClientMoveOnlyCompletionTokenSurvivesDeferredCompletion) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    PageBlobClient client{httpClient, MakePageBlobOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.DeleteAsync([state = std::make_unique<std::string>("page-move-only-state"), &observed,
                        &callback](std::expected<Response<DeleteBlobResult>, BlobStorageError>) mutable {
        ASSERT_TRUE(state);
        observed = *state;
        state.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(observed.empty());
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(observed, "page-move-only-state");
}

TEST(AsyncBehaviorTests, BlobContainerClientMoveOnlyCompletionTokenSurvivesDeferredCompletion) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.CreateIfNotExistsAsync(
        [state = std::make_unique<std::string>("container-move-only-state"), &observed, &callback](
            std::expected<Response<AVEVA::AzureClient::Models::CreateBlobContainerResult>, BlobStorageError>) mutable {
            ASSERT_TRUE(state);
            observed = *state;
            state.reset();
            callback.MarkInvoked();
        });

    EXPECT_TRUE(observed.empty());
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(observed, "container-move-only-state");
}

TEST(AsyncBehaviorTests, TimeoutSimulationCompletesExactlyOnceAndPreservesPlumbedTimeout) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::milliseconds{250});

    int callbackCount = 0;
    StartDeleteAndExpectTimeout(client, callbackCount, requestOptions);

    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), std::chrono::milliseconds{250});
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.TimeoutPending(0U));
    EXPECT_EQ(callbackCount, 1);
}

TEST(AsyncBehaviorTests, ConcurrentDeferredRequestsCanCompleteOutOfOrderExactlyOnce) {
    FakeHttpClient httpClient;
    httpClient.EnqueueDeferredResponse(
        HttpResponse{202, MakeCanonicalSuccessHeaders({{"x-ms-request-id", "first"}}), ""});
    httpClient.EnqueueDeferredResponse(
        HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}, {"x-ms-request-id", "second"}}, ""});
    httpClient.EnqueueDeferredResponse(
        HttpResponse{204, MakeCanonicalSuccessHeaders({{"x-ms-request-id", "third"}}), ""});

    BlobClient first{httpClient, MakeBlobClientOptions("images", "one.bin")};
    BlobClient second{httpClient, MakeBlobClientOptions("images", "two.bin")};
    BlobClient third{httpClient, MakeBlobClientOptions("images", "three.bin")};

    std::vector<std::string> completionOrder;
    int firstCount = 0;
    int secondCount = 0;
    int thirdCount = 0;

    StartSuccessfulDeleteAndRecord(first, firstCount, completionOrder, "first", "first");
    StartBlobNotFoundDeleteAndRecord(second, secondCount, completionOrder);
    StartSuccessfulDeleteAndRecord(third, thirdCount, completionOrder, "third", "third");

    ASSERT_EQ(httpClient.PendingCount(), 3U);
    EXPECT_TRUE(httpClient.CompleteRequest(2U));
    EXPECT_TRUE(httpClient.CompleteRequest(0U));
    EXPECT_TRUE(httpClient.CompleteRequest(1U));

    EXPECT_EQ(firstCount, 1);
    EXPECT_EQ(secondCount, 1);
    EXPECT_EQ(thirdCount, 1);
    EXPECT_EQ(completionOrder, (std::vector<std::string>{"third", "first", "second"}));
}

// Task 16 verification: a token credential that completes GetTokenAsync synchronously (the
// default for the built-in StaticTokenCredential/CachingTokenCredential when no executor is
// supplied, and for any third-party implementation that simply calls the handler inline) must
// not cause the overall ...Async() call to complete before it returns to its caller.
// SendAuthorizedRequestAsync detects same-stack completions of GetTokenAsync and defers them via
// post() so Boost.Asio's completion contract holds regardless of which credential is plugged in.
TEST(AsyncBehaviorTests, TokenCredentialSynchronousCompletionIsDeferredRatherThanInvokedInline) {
    FakeHttpClient httpClient;

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueImmediate({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    ScriptedBlobClient client{httpClient, options, tokenCredential};

    bool callbackInvoked = false;
    StartDeleteAndCaptureInvocation(client, callbackInvoked);

    // GetTokenAsync already ran synchronously (CallCount == 1), but its downstream continuation
    // (building headers and sending the request) must have been deferred rather than run inline.
    EXPECT_EQ(tokenCredential->CallCount(), 1);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
    EXPECT_FALSE(callbackInvoked);

    // Under T26's async-by-default FakeHttpClient contract, driving the executor now runs two
    // posted handlers in this scenario: the token continuation (which builds headers and sends
    // the request) and then the request's own completion (previously invoked inline when
    // CompleteInline defaulted to true). Both still ran strictly after this call returned, which
    // is what this test guards against.
    EXPECT_EQ(httpClient.Poll(), 2U);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(callbackInvoked);
}

// T50: a credential that finishes on a foreign thread (here with a failure, so the operation ends on that thread)
// must not run the handler there; it is dispatched to the client's executor.
TEST(AsyncBehaviorTests, CredentialCompletingOnForeignThreadRunsTheHandlerOnTheClientExecutor) {
    FakeHttpClient httpClient;

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueDeferred(std::make_error_code(std::errc::permission_denied),
                                     AVEVA::AzureClient::AccessToken{});

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    ScriptedBlobClient client{httpClient, options, tokenCredential};

    std::atomic<bool> invoked{false};
    std::atomic<bool> onClientExecutor{false};
    client.DeleteAsync([&](auto&&) {
        onClientExecutor = httpClient.get_executor().running_in_this_thread();
        invoked = true;
    });
    ASSERT_EQ(tokenCredential->PendingCount(), 1U);

    std::thread foreign{[&] { EXPECT_TRUE(tokenCredential->CompleteNext()); }};
    foreign.join();
    EXPECT_FALSE(invoked) << "the handler ran on the credential's thread";

    EXPECT_GE(httpClient.Poll(), 1U);
    EXPECT_TRUE(invoked);
    EXPECT_TRUE(onClientExecutor);
}

// Task 8 verification: an early-exit validation failure must not invoke the completion before
// the initiating call returns; it must run only once the executor it was posted to is driven.
TEST(AsyncBehaviorTests, EarlyExitValidationFailurePostsRatherThanInvokingSynchronously) {
    FakeHttpClient httpClient;
    PageBlobClient client{httpClient, MakePageBlobOptions()};

    bool callbackInvoked = false;
    StartInvalidPageUploadAndCaptureInvocation(client, callbackInvoked);

    EXPECT_FALSE(callbackInvoked);
    EXPECT_EQ(httpClient.Poll(), 1U);
    EXPECT_TRUE(callbackInvoked);
}

// Task 6 verification: a completion token with an explicit associated executor (via
// boost::asio::bind_executor) must have its completion dispatched onto that executor rather than
// invoked directly on FakeHttpClient's own default executor.
TEST(AsyncBehaviorTests, ExplicitAssociatedExecutorReceivesTheCompletionInsteadOfTheDefault) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::io_context strandContext;
    auto strand = boost::asio::make_strand(strandContext);

    bool completedOnStrand = false;
    CallbackExpectation callback;
    StartDeleteBoundToStrand(client, strand, completedOnStrand, callback);

    ASSERT_TRUE(httpClient.CompleteNext());
    // The handler is dispatched onto `strand`'s context, not invoked inline by CompleteNext().
    EXPECT_FALSE(completedOnStrand);

    strandContext.poll();
    EXPECT_TRUE(completedOnStrand);
}

// The handler's executor must be kept busy from initiation, so run() on a separate io_context does not return
// before the (still pending) operation completes.
TEST(AsyncBehaviorTests, HandlerExecutorWorkGuardIsTakenAtInitiation) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::io_context strandContext;
    auto strand = boost::asio::make_strand(strandContext);

    bool completedOnStrand = false;
    CallbackExpectation callback;
    StartDeleteBoundToStrand(client, strand, completedOnStrand, callback);

    strandContext.run_for(std::chrono::milliseconds{50});
    EXPECT_FALSE(strandContext.stopped());

    ASSERT_TRUE(httpClient.CompleteNext());
    strandContext.run();
    EXPECT_TRUE(completedOnStrand);
}

// A plain callback on the default executor takes the inline-completion path, but a credential still pending on
// another thread must keep the client's context running: run() may not return and strand the completion.
TEST(AsyncBehaviorTests, DefaultExecutorStaysBusyWhileCredentialIsPendingOffExecutor) {
    boost::asio::io_context context;
    FakeHttpClient httpClient{context};

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueDeferred(std::make_error_code(std::errc::permission_denied),
                                     AVEVA::AzureClient::AccessToken{});

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    ScriptedBlobClient client{httpClient, options, tokenCredential};

    int invocations = 0;
    client.DeleteAsync([&](auto&&) { ++invocations; });
    ASSERT_EQ(tokenCredential->PendingCount(), 1U);

    // Nothing is queued on the context; only outstanding work can keep run() from returning.
    context.run_for(std::chrono::milliseconds{50});
    EXPECT_FALSE(context.stopped());

    std::thread foreign{[&] { EXPECT_TRUE(tokenCredential->CompleteNext()); }};
    foreign.join();

    context.run();
    EXPECT_EQ(invocations, 1);
}

// Task 16 stretch verification: a completion token with an explicit associated allocator (via
// boost::asio::bind_allocator, layered under bind_executor so the non-skip branch of
// BindAssociationsAndAdoptCancellation is actually taken) must have that allocator genuinely used
// to allocate/deallocate the dispatched completion's internal state -- not merely be discoverable
// via get_associated_allocator without ever being invoked.
TEST(AsyncBehaviorTests, ExplicitAssociatedAllocatorIsActuallyUsedForTheDispatchedCompletion) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::io_context strandContext;
    auto strand = boost::asio::make_strand(strandContext);

    auto allocateCount = std::make_shared<int>(0);
    auto deallocateCount = std::make_shared<int>(0);

    bool callbackInvoked = false;
    StartDeleteWithAssociatedAllocator(client, strand, allocateCount, deallocateCount, callbackInvoked);

    ASSERT_TRUE(httpClient.CompleteNext());
    strandContext.poll();

    EXPECT_TRUE(callbackInvoked);
    EXPECT_GT(*allocateCount, 0);
    EXPECT_EQ(*allocateCount, *deallocateCount);
}

// Task 16 stretch verification: a boost::asio::cancel_after-wrapped token must compile and flow
// through InitiateAsync/BindAssociationsAndAdoptCancellation like any other completion token, and
// must not interfere with an otherwise-successful completion. (cancel_after manages cancellation
// internally between its timer and the operation rather than exposing a discoverable associated
// cancellation slot via get_associated_cancellation_slot, so unlike a directly
// bind_cancellation_slot-wrapped token it is not observable via HttpRequestOptions -- this test
// therefore focuses on end-to-end compileability/behavior rather than slot plumbing.)
//
// Note: this uses the basic_waitable_timer& overload of cancel_after rather than the plain
// cancel_after(timeout, token) overload -- the latter requires the *initiation* itself to expose
// get_executor() (an I/O-object-style requirement, e.g. a socket/stream), which this library's
// internal initiation lambdas intentionally do not model (they are plain callables, not I/O
// objects). Supplying an explicit timer sidesteps that requirement entirely.
TEST(AsyncBehaviorTests, CancelAfterWrappedTokenCompilesAndCompletesNormally) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::steady_timer timer{httpClient.get_executor()};

    CallbackExpectation callback;
    StartDeleteWithCancelAfter(client, timer, callback);

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
}

TEST(AsyncBehaviorTests, NoPublicOperationCompletesBeforeTheInitiatingCallReturns) {
    for (const DeferredCompletionCase& testCase : MakeDeferredCompletionCases()) {
        SCOPED_TRACE(testCase.Name);

        FakeHttpClient httpClient;
        httpClient.DefaultResponse() = HttpResponse{200, MakeCanonicalSuccessHeaders(), ""};

        bool invoked = false;
        testCase.Invoke(httpClient, invoked);

        EXPECT_FALSE(invoked) << testCase.Name << " invoked its completion before the initiating call returned";
        httpClient.Poll();
        EXPECT_TRUE(invoked) << testCase.Name << " never invoked its completion after Poll()";
    }
}

TEST(AsyncBehaviorTests, ExplicitDeferredTokenSendsNothingUntilTheOperationIsInvoked) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    auto operation = client.DeleteAsync(boost::asio::deferred);
    httpClient.Poll();
    EXPECT_EQ(httpClient.RequestCount(), 0U);

    std::optional<DeleteResult> observed;
    InvokeDeferredDeleteOperation(std::move(operation), observed);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_FALSE(observed.has_value());

    httpClient.Poll();
    ASSERT_TRUE(observed.has_value());
    EXPECT_TRUE(ValueOrFail(observed).has_value());
}
