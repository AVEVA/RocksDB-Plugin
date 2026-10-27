// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Helpers shared by the AsyncBehavior*Tests.cpp files, which split the async-behaviour suite by theme.

#pragma once

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp> // IWYU pragma: keep
#include <boost/asio/deferred.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <gtest/gtest.h>
#include <ios>
#include <istream>
#include <new>
#include <stdexcept>

#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace AsyncBehaviorSupport {
using AVEVA::HttpRequestOptions;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::BlobClientOptions;
using AVEVA::AzureClient::BlobStorageError;
using AVEVA::AzureClient::BlobStorageErrorCode;
using AVEVA::AzureClient::Response;
using AVEVA::AzureClient::Models::BlobProperties;
using AVEVA::AzureClient::Models::DeleteBlobResult;
using AVEVA::AzureClient::Models::UploadPagesResult;
using AVEVA::AzureClient::Tests::BlobClient;
using AVEVA::AzureClient::Tests::BlobContainerClient;
using AVEVA::AzureClient::Tests::BlobServiceClient;
using AVEVA::AzureClient::Tests::CallbackExpectation;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::AzureClient::Tests::MakeToken;
using AVEVA::AzureClient::Tests::PageBlobClient;
using AVEVA::AzureClient::Tests::ScriptedTokenCredential;
using ScriptedBlobClient =
    AVEVA::AzureClient::BasicBlobClient<AVEVA::AzureClient::Tests::FakeHttpClient, ScriptedTokenCredential>;
using DeleteResult = std::expected<Response<DeleteBlobResult>, BlobStorageError>;
using DownloadBlobToResult =
    std::expected<Response<AVEVA::AzureClient::Models::DownloadBlobToResult>, BlobStorageError>;
using GetBlobPropertiesResult = std::expected<Response<BlobProperties>, BlobStorageError>;

// Minimal allocator that counts allocations/deallocations, used to prove
// BindAssociationsAndAdoptCancellation's non-skip branch genuinely uses an explicitly
// associated allocator rather than merely discovering it (Task 16 stretch test).
template <class T> class CountingAllocator {
  public:
    using value_type = T;

    CountingAllocator(std::shared_ptr<int> allocateCount, std::shared_ptr<int> deallocateCount)
        : m_allocateCount(std::move(allocateCount)), m_deallocateCount(std::move(deallocateCount)) {}

    template <class U>
    CountingAllocator(const CountingAllocator<U>& other)
        : m_allocateCount(other.m_allocateCount), m_deallocateCount(other.m_deallocateCount) {}

    [[nodiscard]] T* allocate(std::size_t n) {
        ++*m_allocateCount;
        return std::allocator<T>{}.allocate(n);
    }

    void deallocate(T* p, std::size_t n) {
        ++*m_deallocateCount;
        std::allocator<T>{}.deallocate(p, n);
    }

    [[nodiscard]] const std::shared_ptr<int>& AllocateCount() const noexcept { return m_allocateCount; }

    [[nodiscard]] const std::shared_ptr<int>& DeallocateCount() const noexcept { return m_deallocateCount; }

  private:
    template <class U> friend class CountingAllocator;

    std::shared_ptr<int> m_allocateCount;
    std::shared_ptr<int> m_deallocateCount;
};

// A strand executor whose next `Failures` execute() calls throw, to fail the final hand-off to a handler's executor.
struct FailingStrandExecutor {
    boost::asio::strand<boost::asio::io_context::executor_type> Strand;
    std::shared_ptr<std::atomic<int>> Failures;

    template <typename F> void execute(F&& function) const {
        if (Failures->fetch_sub(1) > 0) {
            throw std::bad_alloc();
        }
        Strand.execute(std::forward<F>(function));
    }
    [[nodiscard]] boost::asio::execution_context& query(boost::asio::execution::context_t) const noexcept {
        return boost::asio::query(Strand, boost::asio::execution::context);
    }
    [[nodiscard]] FailingStrandExecutor require(boost::asio::execution::blocking_t::never_t) const { return *this; }
    [[nodiscard]] bool running_in_this_thread() const noexcept { return Strand.running_in_this_thread(); }
    friend bool operator==(const FailingStrandExecutor& a, const FailingStrandExecutor& b) noexcept {
        return a.Strand == b.Strand;
    }
};

[[nodiscard]] inline BlobClientOptions MakePageBlobOptions() { return MakeBlobClientOptions("images", "page.bin"); }

[[nodiscard]] inline bool IsCanceled(const BlobStorageError& error) {
    return error.Code == std::make_error_code(std::errc::operation_canceled);
}

struct DeferredDeleteExpectation {
    bool* CallReturned;
    bool* CallbackObservedReturnedCall;
    CallbackExpectation* Callback;
};

inline void HandleDeferredDeleteCompletion(DeferredDeleteExpectation expectation, DeleteResult result) {
    EXPECT_TRUE(*expectation.CallReturned);
    ASSERT_TRUE(result.has_value());
    *expectation.CallbackObservedReturnedCall = true;
    expectation.Callback->MarkInvoked();
}

inline void StartDeleteAndVerifyExplicitDeferredCompletion(BlobClient& client, bool& callReturned,
                                                           bool& callbackObservedReturnedCall,
                                                           CallbackExpectation& callback) {
    client.DeleteAsync(
        std::bind_front(HandleDeferredDeleteCompletion,
                        DeferredDeleteExpectation{.CallReturned = &callReturned,
                                                  .CallbackObservedReturnedCall = &callbackObservedReturnedCall,
                                                  .Callback = &callback}));
}

inline void HandleSuccessfulDeleteCompletion(CallbackExpectation& callback, DeleteResult result) {
    ASSERT_TRUE(result.has_value());
    callback.MarkInvoked();
}

template <class TClient> void StartDeleteAndExpectSuccess(TClient& client, CallbackExpectation& callback) {
    client.DeleteAsync(std::bind_front(HandleSuccessfulDeleteCompletion, std::ref(callback)));
}

inline void HandleTimedOutDeleteCompletion(int& callbackCount, DeleteResult result) {
    ++callbackCount;
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::timed_out));
}

inline void StartDeleteAndExpectTimeout(BlobClient& client, int& callbackCount,
                                        const HttpRequestOptions& requestOptions) {
    client.DeleteAsync(std::bind_front(HandleTimedOutDeleteCompletion, std::ref(callbackCount)), requestOptions);
}

struct DeleteCompletionRecordExpectation {
    const char* ExpectedRequestId;
    const char* CompletionLabel;
};

inline void HandleSuccessfulDeleteCompletionAndRecord(int& completionCount, std::vector<std::string>& completionOrder,
                                                      DeleteCompletionRecordExpectation expectation,
                                                      DeleteResult result) {
    ++completionCount;
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->Value().RequestId, expectation.ExpectedRequestId);
    completionOrder.emplace_back(expectation.CompletionLabel);
}

inline void StartSuccessfulDeleteAndRecord(BlobClient& client, int& completionCount,
                                           std::vector<std::string>& completionOrder, const char* expectedRequestId,
                                           const char* completionLabel) {
    client.DeleteAsync(std::bind_front(
        HandleSuccessfulDeleteCompletionAndRecord, std::ref(completionCount), std::ref(completionOrder),
        DeleteCompletionRecordExpectation{.ExpectedRequestId = expectedRequestId, .CompletionLabel = completionLabel}));
}

inline void HandleBlobNotFoundDeleteCompletion(int& completionCount, std::vector<std::string>& completionOrder,
                                               DeleteResult result) {
    ++completionCount;
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobNotFound);
    EXPECT_EQ(result.error().RequestId, "second");
    completionOrder.emplace_back("second");
}

inline void StartBlobNotFoundDeleteAndRecord(BlobClient& client, int& completionCount,
                                             std::vector<std::string>& completionOrder) {
    client.DeleteAsync(
        std::bind_front(HandleBlobNotFoundDeleteCompletion, std::ref(completionCount), std::ref(completionOrder)));
}

inline void HandleGetPropertiesCompletion(CallbackExpectation& callback, GetBlobPropertiesResult result) {
    ASSERT_TRUE(result.has_value());
    const Response<BlobProperties>& response = *result;
    EXPECT_EQ(response.Value().ContentLength, 0U);
    callback.MarkInvoked();
}

inline void StartGetPropertiesAndVerify(BlobClient& client, CallbackExpectation& callback) {
    client.GetPropertiesAsync(std::bind_front(HandleGetPropertiesCompletion, std::ref(callback)));
}

struct ReentrantPropertiesExpectations {
    CallbackExpectation* DeleteCallback;
    CallbackExpectation* PropertiesCallback;
};

inline void HandleDeleteCompletionThenStartGetProperties(BlobClient& client,
                                                         ReentrantPropertiesExpectations expectations,
                                                         DeleteResult result) {
    ASSERT_TRUE(result.has_value());
    expectations.DeleteCallback->MarkInvoked();
    StartGetPropertiesAndVerify(client, *expectations.PropertiesCallback);
}

inline void StartDeleteAndVerifyReentrantPropertiesRequest(BlobClient& client, CallbackExpectation& deleteCallback,
                                                           CallbackExpectation& propertiesCallback) {
    client.DeleteAsync(std::bind_front(
        HandleDeleteCompletionThenStartGetProperties, std::ref(client),
        ReentrantPropertiesExpectations{.DeleteCallback = &deleteCallback, .PropertiesCallback = &propertiesCallback}));
}

inline void HandleDownloadToCompletionAndReleaseStream(std::shared_ptr<std::string>& stream,
                                                       CallbackExpectation& callback, DownloadBlobToResult result) {
    ASSERT_TRUE(result.has_value());
    const Response<AVEVA::AzureClient::Models::DownloadBlobToResult>& response = *result;
    EXPECT_EQ(response.Value().BytesWritten, 4U);
    EXPECT_EQ(*stream, "data");
    stream.reset();
    callback.MarkInvoked();
}

inline void StartDownloadToAndReleaseStream(BlobClient& client, std::shared_ptr<std::string>& stream,
                                            CallbackExpectation& callback) {
    client.DownloadToAsync(
        std::span<char>(*stream), AVEVA::AzureClient::DownloadBlobOptions{},
        std::bind_front(HandleDownloadToCompletionAndReleaseStream, std::ref(stream), std::ref(callback)));
}

inline void ThrowDeleteCallback(DeleteResult /*unused*/) { throw std::runtime_error("callback boom"); }

inline void HandleSuccessfulDeleteInvocation(bool& callbackInvoked, DeleteResult result) {
    ASSERT_TRUE(result.has_value());
    callbackInvoked = true;
}

template <class TClient> void StartDeleteAndCaptureInvocation(TClient& client, bool& callbackInvoked) {
    client.DeleteAsync(std::bind_front(HandleSuccessfulDeleteInvocation, std::ref(callbackInvoked)));
}

inline void HandleInvalidPageUploadCompletion(
    bool& callbackInvoked,
    std::expected<Response<AVEVA::AzureClient::Models::UploadPagesResult>, BlobStorageError> result) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
    callbackInvoked = true;
}

inline void StartInvalidPageUploadAndCaptureInvocation(PageBlobClient& client, bool& callbackInvoked) {
    client.UploadPagesAsync(0, std::string{},
                            std::bind_front(HandleInvalidPageUploadCompletion, std::ref(callbackInvoked)));
}

inline void HandleRecursiveInvalidPageUploadCompletion(
    int& completedCount, const std::function<void()>& issue,
    std::expected<Response<AVEVA::AzureClient::Models::UploadPagesResult>, BlobStorageError> result) {
    ASSERT_FALSE(result.has_value());
    ++completedCount;
    if (completedCount < 5000) {
        issue();
    }
}

inline void IssueRecursiveInvalidPageUpload(PageBlobClient& client, int& completedCount,
                                            const std::function<void()>& issue) {
    client.UploadPagesAsync(0, std::string{}, [&completedCount, capture0 = std::cref(issue)](auto&& result) {
        HandleRecursiveInvalidPageUploadCompletion(completedCount, capture0, std::forward<decltype(result)>(result));
    });
}

inline void DrainRecursiveInvalidPageUploadCompletions(FakeHttpClient& httpClient, int& completedCount,
                                                       int iterations) {
    while (completedCount < iterations) {
        ASSERT_GT(httpClient.Poll(), 0U);
    }
}

inline void HandleDeleteOnStrandCompletion(boost::asio::strand<boost::asio::io_context::executor_type>& strand,
                                           bool& completedOnStrand, CallbackExpectation& callback,
                                           DeleteResult result) {
    ASSERT_TRUE(result.has_value());
    completedOnStrand = strand.running_in_this_thread();
    callback.MarkInvoked();
}

inline void StartDeleteBoundToStrand(BlobClient& client,
                                     boost::asio::strand<boost::asio::io_context::executor_type>& strand,
                                     bool& completedOnStrand, CallbackExpectation& callback) {
    client.DeleteAsync(
        boost::asio::bind_executor(strand, std::bind_front(HandleDeleteOnStrandCompletion, std::ref(strand),
                                                           std::ref(completedOnStrand), std::ref(callback))));
}

inline void MarkCallbackInvoked(CallbackExpectation& callback, DeleteResult /*unused*/) { callback.MarkInvoked(); }

inline void StartDeleteWithAssociatedCancellationSlot(BlobClient& client,
                                                      boost::asio::cancellation_signal& associatedSignal,
                                                      CallbackExpectation& callback,
                                                      const HttpRequestOptions& requestOptions) {
    client.DeleteAsync(boost::asio::bind_cancellation_slot(associatedSignal.slot(),
                                                           std::bind_front(MarkCallbackInvoked, std::ref(callback))),
                       requestOptions);
}

inline void HandleAllocatorBoundDeleteCompletion(bool& callbackInvoked, DeleteResult result) {
    ASSERT_TRUE(result.has_value());
    callbackInvoked = true;
}

inline void StartDeleteWithAssociatedAllocator(BlobClient& client,
                                               boost::asio::strand<boost::asio::io_context::executor_type>& strand,
                                               const std::shared_ptr<int>& allocateCount,
                                               const std::shared_ptr<int>& deallocateCount, bool& callbackInvoked) {
    client.DeleteAsync(boost::asio::bind_executor(
        strand,
        boost::asio::bind_allocator(CountingAllocator<void>{allocateCount, deallocateCount},
                                    std::bind_front(HandleAllocatorBoundDeleteCompletion, std::ref(callbackInvoked)))));
}

inline void StartDeleteWithCancelAfter(BlobClient& client, boost::asio::steady_timer& timer,
                                       CallbackExpectation& callback) {
    client.DeleteAsync(boost::asio::cancel_after(timer, std::chrono::seconds(30),
                                                 std::bind_front(MarkCallbackInvoked, std::ref(callback))));
}

inline boost::asio::awaitable<void> AwaitDeleteAsync(BlobClient* client, std::optional<DeleteResult>* observed) {
    *observed = co_await client->DeleteAsync(boost::asio::use_awaitable);
}

inline void HandleAwaitableDeleteCompletion(bool& coroutineFinished, const std::exception_ptr& exception) {
    EXPECT_FALSE(exception);
    coroutineFinished = true;
}

inline void SpawnAwaitableDeleteWithCancellation(FakeHttpClient& httpClient, BlobClient& client,
                                                 boost::asio::cancellation_signal& signal,
                                                 std::optional<DeleteResult>& observed, bool& coroutineFinished) {
    boost::asio::co_spawn(
        httpClient.get_executor(), AwaitDeleteAsync(&client, &observed),
        boost::asio::bind_cancellation_slot(
            signal.slot(), std::bind_front(HandleAwaitableDeleteCompletion, std::ref(coroutineFinished))));
}

inline void HandleDeleteCompletionDuringTokenAcquisition(int& completions, std::optional<DeleteResult>& observed,
                                                         DeleteResult result) {
    ++completions;
    observed = std::move(result);
}

template <class TClient>
inline void StartDeleteDuringTokenAcquisition(TClient& client, boost::asio::cancellation_signal& signal,
                                              int& completions, std::optional<DeleteResult>& observed) {
    client.DeleteAsync(
        boost::asio::bind_cancellation_slot(signal.slot(), std::bind_front(HandleDeleteCompletionDuringTokenAcquisition,
                                                                           std::ref(completions), std::ref(observed))));
}

inline void CaptureDeleteResult(std::optional<DeleteResult>& observed, DeleteResult result) {
    observed = std::move(result);
}

template <class DeferredOperation>
inline void InvokeDeferredDeleteOperation(DeferredOperation&& operation, std::optional<DeleteResult>& observed) {
    std::forward<DeferredOperation>(operation)(std::bind_front(CaptureDeleteResult, std::ref(observed)));
}

template <class TClient>
inline void StartDeleteWithCancellationSlot(TClient& client, boost::asio::cancellation_signal& signal,
                                            std::optional<DeleteResult>& observed) {
    client.DeleteAsync(
        boost::asio::bind_cancellation_slot(signal.slot(), std::bind_front(CaptureDeleteResult, std::ref(observed))));
}

struct CancellationObservation {
    int* Completions;
    bool* Canceled;
};

template <class Result> void RecordCancellationResult(CancellationObservation observation, Result result) {
    ++*observation.Completions;
    *observation.Canceled = !result.has_value() && IsCanceled(result.error());
}

inline void StartContainerDeleteWithCancellation(FakeHttpClient& httpClient, boost::asio::cancellation_signal& signal,
                                                 int& completions, bool& canceled) {
    auto client = std::make_shared<BlobContainerClient>(httpClient, MakeBlobContainerClientOptions());
    client->CreateIfNotExistsAsync(
        boost::asio::bind_cancellation_slot(signal.slot(), [client, &completions, &canceled](auto result) {
            RecordCancellationResult(CancellationObservation{.Completions = &completions, .Canceled = &canceled},
                                     std::move(result));
        }));
}

// A plain function pointer rather than a template parameter: the two starters below have exactly
// this signature, and a non-dependent call keeps it obvious (to readers and to analysis tools)
// that `completions` and `canceled` are written through their reference parameters.
using CancellationStarter = void (*)(FakeHttpClient&, boost::asio::cancellation_signal&, int&, bool&);

inline void ExpectMidFlightCancellation(CancellationStarter start) {
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    boost::asio::cancellation_signal signal;
    int completions = 0;
    bool canceled = false;
    start(httpClient, signal, completions, canceled);
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    signal.emit(boost::asio::cancellation_type::terminal);
    httpClient.Poll();
    EXPECT_EQ(completions, 1);
    EXPECT_TRUE(canceled);
    EXPECT_EQ(httpClient.RequestCount(), 1U);
}

enum class PendingWork : std::uint8_t {
    TransportRequest,
    PostedCompletion,
    RetryBackoff,
    DownloadTo,
};

struct InvocationObserver {
    bool* Invoked;
    std::shared_ptr<int> KeepAlive;

    template <class T> void operator()(T&& /*unused*/) const { *Invoked = true; }
};

inline void ConfigurePendingWork(PendingWork pending, FakeHttpClient& httpClient, BlobClientOptions& options) {
    httpClient.DeferByDefault() = pending != PendingWork::PostedCompletion && pending != PendingWork::RetryBackoff;
    if (pending == PendingWork::RetryBackoff) {
        options.Retry.MaxRetries = 3;
        options.Retry.InitialDelay = std::chrono::minutes{1};
        httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
    }
    if (pending == PendingWork::DownloadTo) {
        httpClient.DefaultResponse() =
            HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "4"}}), "data"};
    }
}

inline void StartPendingWork(PendingWork pending, BlobClient& client, FakeHttpClient& httpClient,
                             std::string& downloadTarget, InvocationObserver handler) {
    switch (pending) {
    case PendingWork::TransportRequest:
    case PendingWork::PostedCompletion:
        client.DeleteAsync(std::move(handler));
        break;
    case PendingWork::RetryBackoff:
        client.DeleteAsync(std::move(handler));
        httpClient.Poll(); // deliver the 503; the operation now waits on its backoff timer
        break;
    case PendingWork::DownloadTo:
        client.DownloadToAsync(std::span<char>(downloadTarget), AVEVA::AzureClient::DownloadBlobOptions{},
                               std::move(handler));
        break;
    }
}

inline void VerifyPendingWorkHandlerIsReleased(PendingWork pending) {
    SCOPED_TRACE(static_cast<int>(pending));

    auto sentinel = std::make_shared<int>(0);
    const std::weak_ptr<int> watch = sentinel;
    bool invoked = false;
    std::string downloadTarget(4U, '\0');

    auto httpClient = std::make_unique<FakeHttpClient>();
    BlobClientOptions options = MakeBlobClientOptions();
    ConfigurePendingWork(pending, *httpClient, options);

    {
        BlobClient client{*httpClient, options};
        StartPendingWork(pending, client, *httpClient, downloadTarget,
                         InvocationObserver{.Invoked = &invoked, .KeepAlive = std::move(sentinel)});
        ASSERT_EQ(httpClient->RequestCount(), 1U);
    }

    httpClient.reset();
    EXPECT_FALSE(invoked);
    EXPECT_TRUE(watch.expired());
}

inline void VerifyPendingWorkHandlersAreReleased() {
    for (const PendingWork pending : {PendingWork::TransportRequest, PendingWork::PostedCompletion,
                                      PendingWork::RetryBackoff, PendingWork::DownloadTo}) {
        VerifyPendingWorkHandlerIsReleased(pending);
    }
}
} // namespace AsyncBehaviorSupport
