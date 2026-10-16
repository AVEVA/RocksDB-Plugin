// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AsyncBehaviorSupport.hpp"

using namespace AsyncBehaviorSupport;

TEST(AsyncBehaviorTests, CancellationWinsIfASynchronousTokenCredentialCompletedBeforePolling) {
    FakeHttpClient httpClient;

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueImmediate({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    ScriptedBlobClient client{httpClient, options, tokenCredential};

    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    StartDeleteWithCancellationSlot(client, signal, observed);

    EXPECT_EQ(tokenCredential->CallCount(), 1);
    EXPECT_EQ(httpClient.RequestCount(), 0U);
    ASSERT_FALSE(observed.has_value());

    signal.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();

    ASSERT_TRUE(observed.has_value());
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_TRUE(IsCanceled(ValueOrFail(observed).error()));
    EXPECT_EQ(httpClient.RequestCount(), 0U);
}

// Task 6 verification: an explicitly-set HttpRequestOptions cancellation slot must win over a
// cancellation slot merely associated with the completion token (explicit wins, matching
// BindAssociationsAndAdoptCancellation's documented contract).
TEST(AsyncBehaviorTests, ExplicitRequestOptionsCancellationSlotWinsOverAssociatedSlot) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::cancellation_signal explicitSignal;
    boost::asio::cancellation_signal associatedSignal;

    HttpRequestOptions requestOptions;
    requestOptions.SetCancellationSlot(explicitSignal.slot());

    CallbackExpectation callback;
    StartDeleteWithAssociatedCancellationSlot(client, associatedSignal, callback, requestOptions);

    // The slot actually plumbed through to the request must be the explicit one, not the token's.
    EXPECT_TRUE(explicitSignal.slot().is_connected());
    EXPECT_TRUE(httpClient.LastRequestOptions().GetCancellationSlot().is_connected());

    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, CancellingACoSpawnedUseAwaitableOperationCancelsTheInFlightRequest) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    bool coroutineFinished = false;
    SpawnAwaitableDeleteWithCancellation(httpClient, client, signal, observed, coroutineFinished);

    httpClient.Poll();
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    signal.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();

    EXPECT_TRUE(coroutineFinished);
    ASSERT_TRUE(observed.has_value());
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_TRUE(IsCanceled(ValueOrFail(observed).error()));
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

// Asio semantics: a cancellation slot only observes signals emitted while an operation is attached
// to it, so a signal emitted before initiation has no effect on a later operation.
TEST(AsyncBehaviorTests, SignalEmittedBeforeInitiationDoesNotCancelALaterOperation) {
    FakeHttpClient httpClient;
    BlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::cancellation_signal signal;
    signal.emit(boost::asio::cancellation_type::terminal);

    std::optional<DeleteResult> observed;
    StartDeleteWithCancellationSlot(client, signal, observed);
    httpClient.Poll();

    ASSERT_TRUE(observed.has_value());
    EXPECT_TRUE(ValueOrFail(observed).has_value());
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

// Before send: with a token credential the request is not handed to the transport until the token
// arrives. Token credentials have no cancellation hook, so cancellation must complete the operation
// promptly rather than waiting for (possibly slow) credential I/O, and the late token must be ignored.
TEST(AsyncBehaviorTests, CancellationDuringTokenAcquisitionCompletesPromptlyAndNeverSends) {
    FakeHttpClient httpClient;
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueDeferred({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    ScriptedBlobClient client{httpClient, options, credential};

    boost::asio::cancellation_signal signal;
    int completions = 0;
    std::optional<DeleteResult> observed;
    StartDeleteDuringTokenAcquisition(client, signal, completions, observed);
    httpClient.Poll();
    ASSERT_EQ(credential->PendingCount(), 1U);
    ASSERT_FALSE(observed.has_value());

    signal.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();

    ASSERT_TRUE(observed.has_value()) << "cancellation must not wait for the credential";
    ASSERT_FALSE(ValueOrFail(observed).has_value());
    EXPECT_TRUE(IsCanceled(ValueOrFail(observed).error()));
    EXPECT_FALSE(signal.slot().has_handler());

    EXPECT_TRUE(credential->CompleteNext());
    httpClient.Poll();
    EXPECT_EQ(httpClient.RequestCount(), 0U);
    EXPECT_EQ(completions, 1);
}

TEST(AsyncBehaviorTests, MidFlightCancellationThroughAnAssociatedSlotWorksForContainerClients) {
    ExpectMidFlightCancellation(StartContainerDeleteWithCancellation);
}

// Supported cancellation pattern: the signal is emitted from the handler's executor, so it cannot race
// with the library clearing the slot when the operation finishes.
TEST(AsyncBehaviorTests, CancellationEmittedFromTheHandlerExecutorCancelsTheOperation) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobClient client{httpClient, MakeBlobClientOptions()};
    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    client.DeleteAsync(
        boost::asio::bind_cancellation_slot(signal.slot(), [&](DeleteResult result) { observed = std::move(result); }));
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    boost::asio::post(httpClient.get_executor(), [&signal] { signal.emit(boost::asio::cancellation_type::terminal); });
    httpClient.Poll();
    ASSERT_TRUE(observed.has_value());
    ASSERT_FALSE(observed->has_value());
    EXPECT_EQ(observed->error().Code, std::make_error_code(std::errc::operation_canceled));
}
