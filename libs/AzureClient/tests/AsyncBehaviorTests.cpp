#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"
#include "TestHelpers.hpp"
#include "ValueOrFail.hpp"

#include <AVEVA/AzureClient/AppendBlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
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

#include <cstddef>
#include <cstdint>
#include <exception>
#include <gtest/gtest.h>
#include <ios>
#include <istream>
#include <stdexcept>

#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

using AVEVA::AzureClient::Tests::ValueOrFail;

namespace
{
    using AVEVA::HttpRequestOptions;
    using AVEVA::HttpResponse;
    using AVEVA::AzureClient::AppendBlobClient;
    using AVEVA::AzureClient::BlobClientOptions;
    using AVEVA::AzureClient::BlobContainerClient;
    using AVEVA::AzureClient::BlobServiceClient;
    using AVEVA::AzureClient::BlobStorageError;
    using AVEVA::AzureClient::BlobStorageErrorCode;
    using AVEVA::AzureClient::BlockBlobClient;
    using AVEVA::AzureClient::PageBlobClient;
    using AVEVA::AzureClient::Response;
    using AVEVA::AzureClient::Models::BlobProperties;
    using AVEVA::AzureClient::Models::DeleteBlobResult;
    using AVEVA::AzureClient::Models::UploadBlockBlobResult;
    using AVEVA::AzureClient::Tests::CallbackExpectation;
    using AVEVA::AzureClient::Tests::FakeHttpClient;
    using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobContainerClientOptions;
    using AVEVA::AzureClient::Tests::MakeBlobServiceClientOptions;
    using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
    using AVEVA::AzureClient::Tests::MakeToken;
    using AVEVA::AzureClient::Tests::ScriptedTokenCredential;
    using DeleteResult = std::expected<Response<DeleteBlobResult>, BlobStorageError>;
    using DownloadBlobToResult =
        std::expected<Response<AVEVA::AzureClient::Models::DownloadBlobToResult>, BlobStorageError>;
    using GetBlobPropertiesResult = std::expected<Response<BlobProperties>, BlobStorageError>;
    using UploadBlockBlobAsyncResult = std::expected<Response<UploadBlockBlobResult>, BlobStorageError>;

    // Minimal allocator that counts allocations/deallocations, used to prove
    // BindAssociationsAndAdoptCancellation's non-skip branch genuinely uses an explicitly
    // associated allocator rather than merely discovering it (Task 16 stretch test).
    template <class T> class CountingAllocator
    {
      public:
        using value_type = T;

        CountingAllocator(std::shared_ptr<int> allocateCount, std::shared_ptr<int> deallocateCount)
            : m_allocateCount(std::move(allocateCount)), m_deallocateCount(std::move(deallocateCount))
        {
        }

        template <class U>
        CountingAllocator(const CountingAllocator<U>& other)
            : m_allocateCount(other.m_allocateCount), m_deallocateCount(other.m_deallocateCount)
        {
        }

        [[nodiscard]] T* allocate(std::size_t n)
        {
            ++*m_allocateCount;
            return std::allocator<T>{}.allocate(n);
        }

        void deallocate(T* p, std::size_t n)
        {
            ++*m_deallocateCount;
            std::allocator<T>{}.deallocate(p, n);
        }

        [[nodiscard]] const std::shared_ptr<int>& AllocateCount() const noexcept
        {
            return m_allocateCount;
        }

        [[nodiscard]] const std::shared_ptr<int>& DeallocateCount() const noexcept
        {
            return m_deallocateCount;
        }

      private:
        template <class U> friend class CountingAllocator;

        std::shared_ptr<int> m_allocateCount;
        std::shared_ptr<int> m_deallocateCount;
    };

    [[nodiscard]] BlobClientOptions MakePageBlobOptions()
    {
        return MakeBlobClientOptions("images", "page.bin");
    }

    [[nodiscard]] BlobClientOptions MakeAppendBlobOptions()
    {
        return MakeBlobClientOptions("images", "log.txt");
    }

    [[nodiscard]] bool IsCanceled(const BlobStorageError& error)
    {
        return error.Code == std::make_error_code(std::errc::operation_canceled);
    }

    struct DeferredDeleteExpectation
    {
        bool* CallReturned;
        bool* CallbackObservedReturnedCall;
        CallbackExpectation* Callback;
    };

    void HandleDeferredDeleteCompletion(DeferredDeleteExpectation expectation, DeleteResult result)
    {
        EXPECT_TRUE(*expectation.CallReturned);
        ASSERT_TRUE(result.has_value());
        *expectation.CallbackObservedReturnedCall = true;
        expectation.Callback->MarkInvoked();
    }

    void StartDeleteAndVerifyExplicitDeferredCompletion(BlockBlobClient& client,
        bool& callReturned,
        bool& callbackObservedReturnedCall,
        CallbackExpectation& callback)
    {
        client.DeleteAsync(std::bind_front(HandleDeferredDeleteCompletion,
            DeferredDeleteExpectation{.CallReturned = &callReturned,
                .CallbackObservedReturnedCall = &callbackObservedReturnedCall,
                .Callback = &callback}));
    }

    void HandleSuccessfulDeleteCompletion(CallbackExpectation& callback, DeleteResult result)
    {
        ASSERT_TRUE(result.has_value());
        callback.MarkInvoked();
    }

    void StartDeleteAndExpectSuccess(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.DeleteAsync(std::bind_front(HandleSuccessfulDeleteCompletion, std::ref(callback)));
    }

    void HandleTimedOutDeleteCompletion(int& callbackCount, DeleteResult result)
    {
        ++callbackCount;
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::timed_out));
    }

    void StartDeleteAndExpectTimeout(BlockBlobClient& client,
        int& callbackCount,
        const HttpRequestOptions& requestOptions)
    {
        client.DeleteAsync(std::bind_front(HandleTimedOutDeleteCompletion, std::ref(callbackCount)), requestOptions);
    }

    struct DeleteCompletionRecordExpectation
    {
        const char* ExpectedRequestId;
        const char* CompletionLabel;
    };

    void HandleSuccessfulDeleteCompletionAndRecord(int& completionCount,
        std::vector<std::string>& completionOrder,
        DeleteCompletionRecordExpectation expectation,
        DeleteResult result)
    {
        ++completionCount;
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->Value().RequestId, expectation.ExpectedRequestId);
        completionOrder.emplace_back(expectation.CompletionLabel);
    }

    void StartSuccessfulDeleteAndRecord(BlockBlobClient& client,
        int& completionCount,
        std::vector<std::string>& completionOrder,
        const char* expectedRequestId,
        const char* completionLabel)
    {
        client.DeleteAsync(std::bind_front(HandleSuccessfulDeleteCompletionAndRecord,
            std::ref(completionCount),
            std::ref(completionOrder),
            DeleteCompletionRecordExpectation{.ExpectedRequestId = expectedRequestId,
                .CompletionLabel = completionLabel}));
    }

    void HandleBlobNotFoundDeleteCompletion(int& completionCount,
        std::vector<std::string>& completionOrder,
        DeleteResult result)
    {
        ++completionCount;
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, BlobStorageErrorCode::BlobNotFound);
        EXPECT_EQ(result.error().RequestId, "second");
        completionOrder.emplace_back("second");
    }

    void StartBlobNotFoundDeleteAndRecord(BlockBlobClient& client,
        int& completionCount,
        std::vector<std::string>& completionOrder)
    {
        client.DeleteAsync(
            std::bind_front(HandleBlobNotFoundDeleteCompletion, std::ref(completionCount), std::ref(completionOrder)));
    }

    void HandleGetPropertiesCompletion(CallbackExpectation& callback, GetBlobPropertiesResult result)
    {
        ASSERT_TRUE(result.has_value());
        const Response<BlobProperties>& response = *result;
        EXPECT_EQ(response.Value().ContentLength, 0U);
        callback.MarkInvoked();
    }

    void StartGetPropertiesAndVerify(BlockBlobClient& client, CallbackExpectation& callback)
    {
        client.GetPropertiesAsync(std::bind_front(HandleGetPropertiesCompletion, std::ref(callback)));
    }

    struct ReentrantPropertiesExpectations
    {
        CallbackExpectation* DeleteCallback;
        CallbackExpectation* PropertiesCallback;
    };

    void HandleDeleteCompletionThenStartGetProperties(BlockBlobClient& client,
        ReentrantPropertiesExpectations expectations,
        DeleteResult result)
    {
        ASSERT_TRUE(result.has_value());
        expectations.DeleteCallback->MarkInvoked();
        StartGetPropertiesAndVerify(client, *expectations.PropertiesCallback);
    }

    void StartDeleteAndVerifyReentrantPropertiesRequest(BlockBlobClient& client,
        CallbackExpectation& deleteCallback,
        CallbackExpectation& propertiesCallback)
    {
        client.DeleteAsync(std::bind_front(HandleDeleteCompletionThenStartGetProperties,
            std::ref(client),
            ReentrantPropertiesExpectations{.DeleteCallback = &deleteCallback,
                .PropertiesCallback = &propertiesCallback}));
    }

    void HandleDownloadToCompletionAndReleaseStream(std::shared_ptr<std::ostringstream>& stream,
        CallbackExpectation& callback,
        DownloadBlobToResult result)
    {
        ASSERT_TRUE(result.has_value());
        const Response<AVEVA::AzureClient::Models::DownloadBlobToResult>& response = *result;
        EXPECT_EQ(response.Value().BytesWritten, 4U);
        EXPECT_EQ(stream->str(), "data");
        stream.reset();
        callback.MarkInvoked();
    }

    void StartDownloadToAndReleaseStream(BlockBlobClient& client,
        std::shared_ptr<std::ostringstream>& stream,
        CallbackExpectation& callback)
    {
        client.DownloadToAsync(*stream,
            std::bind_front(HandleDownloadToCompletionAndReleaseStream, std::ref(stream), std::ref(callback)));
    }

    void ThrowDeleteCallback(DeleteResult /*unused*/)
    {
        throw std::runtime_error("callback boom");
    }

    void HandleUploadFromPathCompletion(CallbackExpectation& callback, UploadBlockBlobAsyncResult result)
    {
        ASSERT_TRUE(result.has_value());
        const Response<UploadBlockBlobResult>& response = *result;
        EXPECT_EQ(response.Value().ETag, AVEVA::AzureClient::Tests::DefaultETag);
        callback.MarkInvoked();
    }

    void StartUploadFromPathAndVerifySuccess(BlockBlobClient& client,
        const std::filesystem::path& path,
        const AVEVA::AzureClient::UploadFromOptions& options,
        CallbackExpectation& callback)
    {
        client.UploadFromAsync(path, options, std::bind_front(HandleUploadFromPathCompletion, std::ref(callback)));
    }

    void HandleUploadFromFailure(int& callbackCount, UploadBlockBlobAsyncResult result)
    {
        ++callbackCount;
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::connection_reset));
    }

    void StartUploadFromStreamAndExpectFailure(BlockBlobClient& client,
        std::istream& stream,
        const AVEVA::AzureClient::UploadFromOptions& options,
        int& callbackCount)
    {
        client.UploadFromAsync(stream, options, std::bind_front(HandleUploadFromFailure, std::ref(callbackCount)));
    }

    void HandleSuccessfulDeleteInvocation(bool& callbackInvoked, DeleteResult result)
    {
        ASSERT_TRUE(result.has_value());
        callbackInvoked = true;
    }

    void StartDeleteAndCaptureInvocation(BlockBlobClient& client, bool& callbackInvoked)
    {
        client.DeleteAsync(std::bind_front(HandleSuccessfulDeleteInvocation, std::ref(callbackInvoked)));
    }

    void HandleInvalidStageBlockCompletion(bool& callbackInvoked,
        std::expected<Response<AVEVA::AzureClient::Models::StageBlockResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().Code, std::make_error_code(std::errc::invalid_argument));
        callbackInvoked = true;
    }

    void StartInvalidStageBlockAndCaptureInvocation(BlockBlobClient& client, bool& callbackInvoked)
    {
        client.StageBlockAsync("not-base64!",
            "data",
            std::bind_front(HandleInvalidStageBlockCompletion, std::ref(callbackInvoked)));
    }

    void HandleRecursiveInvalidStageBlockCompletion(int& completedCount,
        const std::function<void()>& issue,
        std::expected<Response<AVEVA::AzureClient::Models::StageBlockResult>, BlobStorageError> result)
    {
        ASSERT_FALSE(result.has_value());
        ++completedCount;
        if (completedCount < 5000)
        {
            issue();
        }
    }

    void IssueRecursiveInvalidStageBlock(BlockBlobClient& client,
        int& completedCount,
        const std::function<void()>& issue)
    {
        client.StageBlockAsync("not-base64!",
            "data",
            [&completedCount, capture0 = std::cref(issue)](auto&& result)
        {
            HandleRecursiveInvalidStageBlockCompletion(completedCount,
                capture0,
                std::forward<decltype(result)>(result));
        });
    }

    void DrainRecursiveInvalidStageBlockCompletions(FakeHttpClient& httpClient, int& completedCount, int iterations)
    {
        while (completedCount < iterations)
        {
            ASSERT_GT(httpClient.Poll(), 0U);
        }
    }

    void HandleDeleteOnStrandCompletion(boost::asio::strand<boost::asio::io_context::executor_type>& strand,
        bool& completedOnStrand,
        CallbackExpectation& callback,
        DeleteResult result)
    {
        ASSERT_TRUE(result.has_value());
        completedOnStrand = strand.running_in_this_thread();
        callback.MarkInvoked();
    }

    void StartDeleteBoundToStrand(BlockBlobClient& client,
        boost::asio::strand<boost::asio::io_context::executor_type>& strand,
        bool& completedOnStrand,
        CallbackExpectation& callback)
    {
        client.DeleteAsync(boost::asio::bind_executor(strand,
            std::bind_front(HandleDeleteOnStrandCompletion,
                std::ref(strand),
                std::ref(completedOnStrand),
                std::ref(callback))));
    }

    void MarkCallbackInvoked(CallbackExpectation& callback, DeleteResult /*unused*/)
    {
        callback.MarkInvoked();
    }

    void StartDeleteWithAssociatedCancellationSlot(BlockBlobClient& client,
        boost::asio::cancellation_signal& associatedSignal,
        CallbackExpectation& callback,
        const HttpRequestOptions& requestOptions)
    {
        client.DeleteAsync(boost::asio::bind_cancellation_slot(associatedSignal.slot(),
                               std::bind_front(MarkCallbackInvoked, std::ref(callback))),
            requestOptions);
    }

    void HandleAllocatorBoundDeleteCompletion(bool& callbackInvoked, DeleteResult result)
    {
        ASSERT_TRUE(result.has_value());
        callbackInvoked = true;
    }

    void StartDeleteWithAssociatedAllocator(BlockBlobClient& client,
        boost::asio::strand<boost::asio::io_context::executor_type>& strand,
        const std::shared_ptr<int>& allocateCount,
        const std::shared_ptr<int>& deallocateCount,
        bool& callbackInvoked)
    {
        client.DeleteAsync(boost::asio::bind_executor(strand,
            boost::asio::bind_allocator(CountingAllocator<void>{allocateCount, deallocateCount},
                std::bind_front(HandleAllocatorBoundDeleteCompletion, std::ref(callbackInvoked)))));
    }

    void StartDeleteWithCancelAfter(BlockBlobClient& client,
        boost::asio::steady_timer& timer,
        CallbackExpectation& callback)
    {
        client.DeleteAsync(boost::asio::cancel_after(timer,
            std::chrono::seconds(30),
            std::bind_front(MarkCallbackInvoked, std::ref(callback))));
    }

    boost::asio::awaitable<void> AwaitDeleteAsync(BlockBlobClient* client, std::optional<DeleteResult>* observed)
    {
        *observed = co_await client->DeleteAsync(boost::asio::use_awaitable);
    }

    void HandleAwaitableDeleteCompletion(bool& coroutineFinished, const std::exception_ptr& exception)
    {
        EXPECT_FALSE(exception);
        coroutineFinished = true;
    }

    void SpawnAwaitableDeleteWithCancellation(FakeHttpClient& httpClient,
        BlockBlobClient& client,
        boost::asio::cancellation_signal& signal,
        std::optional<DeleteResult>& observed,
        bool& coroutineFinished)
    {
        boost::asio::co_spawn(httpClient.get_executor(),
            AwaitDeleteAsync(&client, &observed),
            boost::asio::bind_cancellation_slot(signal.slot(),
                std::bind_front(HandleAwaitableDeleteCompletion, std::ref(coroutineFinished))));
    }

    void HandleDeleteCompletionDuringTokenAcquisition(int& completions,
        std::optional<DeleteResult>& observed,
        DeleteResult result)
    {
        ++completions;
        observed = std::move(result);
    }

    void StartDeleteDuringTokenAcquisition(BlockBlobClient& client,
        boost::asio::cancellation_signal& signal,
        int& completions,
        std::optional<DeleteResult>& observed)
    {
        client.DeleteAsync(boost::asio::bind_cancellation_slot(signal.slot(),
            std::bind_front(HandleDeleteCompletionDuringTokenAcquisition, std::ref(completions), std::ref(observed))));
    }

    void CaptureDeleteResult(std::optional<DeleteResult>& observed, DeleteResult result)
    {
        observed = std::move(result);
    }

    template <class DeferredOperation>
    void InvokeDeferredDeleteOperation(DeferredOperation&& operation, std::optional<DeleteResult>& observed)
    {
        std::forward<DeferredOperation>(operation)(std::bind_front(CaptureDeleteResult, std::ref(observed)));
    }

    void StartDeleteWithCancellationSlot(BlockBlobClient& client,
        boost::asio::cancellation_signal& signal,
        std::optional<DeleteResult>& observed)
    {
        client.DeleteAsync(boost::asio::bind_cancellation_slot(signal.slot(),
            std::bind_front(CaptureDeleteResult, std::ref(observed))));
    }

    struct CancellationObservation
    {
        int* Completions;
        bool* Canceled;
    };

    template <class Result> void RecordCancellationResult(CancellationObservation observation, Result result)
    {
        ++*observation.Completions;
        *observation.Canceled = !result.has_value() && IsCanceled(result.error());
    }

    void StartContainerDeleteWithCancellation(FakeHttpClient& httpClient,
        boost::asio::cancellation_signal& signal,
        int& completions,
        bool& canceled)
    {
        auto client = std::make_shared<BlobContainerClient>(httpClient, MakeBlobContainerClientOptions());
        client->DeleteAsync(boost::asio::bind_cancellation_slot(signal.slot(),
            [client, &completions, &canceled](auto result)
        {
            RecordCancellationResult(CancellationObservation{.Completions = &completions, .Canceled = &canceled},
                std::move(result));
        }));
    }

    void StartServiceGetPropertiesWithCancellation(FakeHttpClient& httpClient,
        boost::asio::cancellation_signal& signal,
        int& completions,
        bool& canceled)
    {
        auto client = std::make_shared<BlobServiceClient>(httpClient, MakeBlobServiceClientOptions());
        client->GetPropertiesAsync(boost::asio::bind_cancellation_slot(signal.slot(),
            [client, &completions, &canceled](auto result)
        {
            RecordCancellationResult(CancellationObservation{.Completions = &completions, .Canceled = &canceled},
                std::move(result));
        }));
    }

    // A plain function pointer rather than a template parameter: the two starters below have exactly
    // this signature, and a non-dependent call keeps it obvious (to readers and to analysis tools)
    // that `completions` and `canceled` are written through their reference parameters.
    using CancellationStarter = void (*)(FakeHttpClient&, boost::asio::cancellation_signal&, int&, bool&);

    void ExpectMidFlightCancellation(CancellationStarter start)
    {
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

    enum class PendingWork : std::uint8_t
    {
        TransportRequest,
        PostedCompletion,
        RetryBackoff,
        MultiBlockUpload,
        DownloadTo,
    };

    struct InvocationObserver
    {
        bool* Invoked;
        std::shared_ptr<int> KeepAlive;

        template <class T> void operator()(T&& /*unused*/) const
        {
            *Invoked = true;
        }
    };

    void ConfigurePendingWork(PendingWork pending, FakeHttpClient& httpClient, BlobClientOptions& options)
    {
        httpClient.DeferByDefault() = pending != PendingWork::PostedCompletion && pending != PendingWork::RetryBackoff;
        if (pending == PendingWork::RetryBackoff)
        {
            options.Retry.MaxRetries = 3;
            options.Retry.InitialDelay = std::chrono::minutes{1};
            httpClient.EnqueueResponse(HttpResponse{503, {}, ""});
        }
        if (pending == PendingWork::DownloadTo)
        {
            httpClient.DefaultResponse() =
                HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "4"}}), "data"};
        }
    }

    void StartPendingWork(PendingWork pending,
        BlockBlobClient& client,
        FakeHttpClient& httpClient,
        const std::filesystem::path& uploadPath,
        std::ostringstream& downloadTarget,
        InvocationObserver handler)
    {
        switch (pending)
        {
        case PendingWork::TransportRequest:
        case PendingWork::PostedCompletion:
            client.DeleteAsync(std::move(handler));
            break;
        case PendingWork::RetryBackoff:
            client.DeleteAsync(std::move(handler));
            httpClient.Poll(); // deliver the 503; the operation now waits on its backoff timer
            break;
        case PendingWork::MultiBlockUpload: {
            AVEVA::AzureClient::UploadFromOptions uploadOptions;
            uploadOptions.BlockSize = 4U;
            client.UploadFromAsync(uploadPath, uploadOptions, std::move(handler));
            break;
        }
        case PendingWork::DownloadTo:
            client.DownloadToAsync(downloadTarget, std::move(handler));
            break;
        }
    }

    void VerifyPendingWorkHandlerIsReleased(PendingWork pending, const std::filesystem::path& uploadPath)
    {
        SCOPED_TRACE(static_cast<int>(pending));

        auto sentinel = std::make_shared<int>(0);
        const std::weak_ptr<int> watch = sentinel;
        bool invoked = false;
        std::ostringstream downloadTarget;

        auto httpClient = std::make_unique<FakeHttpClient>();
        BlobClientOptions options = MakeBlobClientOptions();
        ConfigurePendingWork(pending, *httpClient, options);

        {
            BlockBlobClient client{*httpClient, options};
            StartPendingWork(pending,
                client,
                *httpClient,
                uploadPath,
                downloadTarget,
                InvocationObserver{.Invoked = &invoked, .KeepAlive = std::move(sentinel)});
            ASSERT_EQ(httpClient->RequestCount(), 1U);
        }

        httpClient.reset();
        EXPECT_FALSE(invoked);
        EXPECT_TRUE(watch.expired());
    }

    void VerifyPendingWorkHandlersAreReleased(const std::filesystem::path& uploadPath)
    {
        for (const PendingWork pending : {PendingWork::TransportRequest,
                 PendingWork::PostedCompletion,
                 PendingWork::RetryBackoff,
                 PendingWork::MultiBlockUpload,
                 PendingWork::DownloadTo})
        {
            VerifyPendingWorkHandlerIsReleased(pending, uploadPath);
        }
    }
} // namespace

TEST(AsyncBehaviorTests, HandlerRunsOnlyAfterExplicitDeferredCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

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

TEST(AsyncBehaviorTests, MoveOnlyCapturedStateSurvivesDeferredCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.DeleteAsync([state = std::make_unique<std::string>("move-only-state"), &observed, &callback](
                           std::expected<Response<DeleteBlobResult>, BlobStorageError>) mutable
    {
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
TEST(AsyncBehaviorTests, PageBlobClientMoveOnlyCompletionTokenSurvivesDeferredCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    PageBlobClient client{httpClient, MakePageBlobOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.DeleteAsync([state = std::make_unique<std::string>("page-move-only-state"), &observed, &callback](
                           std::expected<Response<DeleteBlobResult>, BlobStorageError>) mutable
    {
        ASSERT_TRUE(state);
        observed = *state;
        state.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(observed.empty());
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(observed, "page-move-only-state");
}

TEST(AsyncBehaviorTests, AppendBlobClientMoveOnlyCompletionTokenSurvivesDeferredCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    AppendBlobClient client{httpClient, MakeAppendBlobOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.DeleteAsync([state = std::make_unique<std::string>("append-move-only-state"), &observed, &callback](
                           std::expected<Response<DeleteBlobResult>, BlobStorageError>) mutable
    {
        ASSERT_TRUE(state);
        observed = *state;
        state.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(observed.empty());
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(observed, "append-move-only-state");
}

TEST(AsyncBehaviorTests, BlobContainerClientMoveOnlyCompletionTokenSurvivesDeferredCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobContainerClient client{httpClient, MakeBlobContainerClientOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.ExistsAsync([state = std::make_unique<std::string>("container-move-only-state"), &observed, &callback](
                           std::expected<Response<bool>, BlobStorageError>) mutable
    {
        ASSERT_TRUE(state);
        observed = *state;
        state.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(observed.empty());
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(observed, "container-move-only-state");
}

TEST(AsyncBehaviorTests, BlobServiceClientMoveOnlyCompletionTokenSurvivesDeferredCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlobServiceClient client{httpClient, MakeBlobServiceClientOptions()};

    std::string observed;
    CallbackExpectation callback;
    client.ListBlobContainersAsync(
        [state = std::make_unique<std::string>("service-move-only-state"), &observed, &callback](
            std::expected<Response<AVEVA::AzureClient::Models::ListBlobContainersResult>, BlobStorageError>) mutable
    {
        ASSERT_TRUE(state);
        observed = *state;
        state.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(observed.empty());
    ASSERT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(observed, "service-move-only-state");
}

TEST(AsyncBehaviorTests, BlockBlobClientCanBeDestroyedAfterRequestIsInFlight)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    CallbackExpectation callback;
    {
        auto client = std::make_unique<BlockBlobClient>(httpClient, MakeBlobClientOptions());
        client->DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            ASSERT_TRUE(result.has_value());
            callback.MarkInvoked();
        });
    }

    EXPECT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, PageBlobClientCanBeDestroyedAfterRequestIsInFlight)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    CallbackExpectation callback;
    {
        auto client = std::make_unique<PageBlobClient>(httpClient, MakePageBlobOptions());
        client->DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
        {
            EXPECT_TRUE(result.has_value());
            callback.MarkInvoked();
        });
    }

    EXPECT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, TokenCredentialRequestCanDestroyClientAfterRequestIsSent)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueDeferred({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    options.TokenCredential = tokenCredential;

    CallbackExpectation callback;
    auto client = std::make_unique<BlockBlobClient>(httpClient, std::move(options));
    StartDeleteAndExpectSuccess(*client, callback);

    EXPECT_EQ(tokenCredential->PendingCount(), 1U);
    EXPECT_EQ(httpClient.RequestCount(), 0U);

    EXPECT_TRUE(tokenCredential->CompleteNext());
    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.PendingCount(), 1U);

    client.reset();

    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, TimeoutSimulationCompletesExactlyOnceAndPreservesPlumbedTimeout)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    HttpRequestOptions requestOptions;
    requestOptions.SetTimeout(std::chrono::milliseconds{250});

    int callbackCount = 0;
    StartDeleteAndExpectTimeout(client, callbackCount, requestOptions);

    EXPECT_EQ(httpClient.LastRequestOptions().GetTimeout(), std::chrono::milliseconds{250});
    ASSERT_EQ(httpClient.PendingCount(), 1U);
    EXPECT_TRUE(httpClient.TimeoutPending(0U));
    EXPECT_EQ(callbackCount, 1);
}

TEST(AsyncBehaviorTests, ConcurrentDeferredRequestsCanCompleteOutOfOrderExactlyOnce)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueDeferredResponse(
        HttpResponse{202, MakeCanonicalSuccessHeaders({{"x-ms-request-id", "first"}}), ""});
    httpClient.EnqueueDeferredResponse(
        HttpResponse{404, {{"x-ms-error-code", "BlobNotFound"}, {"x-ms-request-id", "second"}}, ""});
    httpClient.EnqueueDeferredResponse(
        HttpResponse{204, MakeCanonicalSuccessHeaders({{"x-ms-request-id", "third"}}), ""});

    BlockBlobClient first{httpClient, MakeBlobClientOptions("images", "one.bin")};
    BlockBlobClient second{httpClient, MakeBlobClientOptions("images", "two.bin")};
    BlockBlobClient third{httpClient, MakeBlobClientOptions("images", "three.bin")};

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

TEST(AsyncBehaviorTests, CallbackCanStartAnotherRequestReentrantly)
{
    FakeHttpClient httpClient;
    httpClient.EnqueueDeferredResponse(HttpResponse{202, MakeCanonicalSuccessHeaders(), ""});
    httpClient.EnqueueResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({{"Content-Length", "0"}}), ""});

    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    CallbackExpectation deleteCallback;
    CallbackExpectation propertiesCallback;
    StartDeleteAndVerifyReentrantPropertiesRequest(client, deleteCallback, propertiesCallback);

    EXPECT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(httpClient.RequestCount(), 2U);
    httpClient.Poll(); // drive the posted (async) GetPropertiesAsync completion (T26)
}

TEST(AsyncBehaviorTests, CallbackCanDestroyClientAfterCompletionBegins)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;

    auto client = std::make_unique<BlockBlobClient>(httpClient, MakeBlobClientOptions());
    CallbackExpectation callback;
    client->DeleteAsync([&](std::expected<Response<DeleteBlobResult>, BlobStorageError> result)
    {
        ASSERT_TRUE(result.has_value());
        client.reset();
        callback.MarkInvoked();
    });

    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(client, nullptr);
}

TEST(AsyncBehaviorTests, DownloadToAsyncAllowsDestroyingTheTargetStreamInTheCompletionHandler)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    httpClient.DefaultResponse() = HttpResponse{200,
        MakeCanonicalSuccessHeaders({{"Content-Length", "4"}, {"Content-Type", "text/plain"}}),
        "data"};
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    auto stream = std::make_shared<std::ostringstream>();
    CallbackExpectation callback;
    StartDownloadToAndReleaseStream(client, stream, callback);

    EXPECT_TRUE(httpClient.CompleteNext());
    EXPECT_EQ(stream, nullptr);
}

TEST(AsyncBehaviorTests, ExceptionsFromUserCallbacksPropagateWithoutCorruptingLaterRequests)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    client.DeleteAsync(ThrowDeleteCallback);

    EXPECT_THROW(static_cast<void>(httpClient.CompleteNext()), std::runtime_error);
    EXPECT_EQ(httpClient.PendingCount(), 0U);

    CallbackExpectation callback;
    StartDeleteAndExpectSuccess(client, callback);
    EXPECT_TRUE(httpClient.CompleteNext());
}

TEST(AsyncBehaviorTests, UploadFromAsyncPathKeepsOwnedStateAliveAcrossDeferredMultiBlockCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    httpClient.DefaultResponse() = HttpResponse{201, MakeCanonicalSuccessHeaders(), ""};
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    const std::filesystem::path tempPath =
        std::filesystem::temp_directory_path() / "azure-client-upload-from-async-behavior.bin";
    {
        std::ofstream output(tempPath, std::ios::binary | std::ios::trunc);
        output << "ABCDEFGHI";
    }

    AVEVA::AzureClient::UploadFromOptions options;
    options.BlockSize = 4U;

    CallbackExpectation callback;
    StartUploadFromPathAndVerifySuccess(client, tempPath, options, callback);

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_EQ(httpClient.RequestAt(0).Body, "ABCD");
    EXPECT_TRUE(httpClient.CompleteRequest(0U));
    httpClient.Poll();

    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_EQ(httpClient.RequestAt(1).Body, "EFGH");
    EXPECT_TRUE(httpClient.CompleteRequest(1U));
    httpClient.Poll();

    ASSERT_EQ(httpClient.RequestCount(), 3U);
    EXPECT_EQ(httpClient.RequestAt(2).Body, "I");
    EXPECT_TRUE(httpClient.CompleteRequest(2U));
    httpClient.Poll();

    ASSERT_EQ(httpClient.RequestCount(), 4U);
    EXPECT_NE(httpClient.RequestAt(3).Request.GetUrl().find("comp=blocklist"), std::string::npos);
    const std::string body = httpClient.RequestAt(3).Body;
    ASSERT_EQ(httpClient.StagedBlockIds().size(), 3U);
    std::string expectedList;
    for (const std::string& id : httpClient.StagedBlockIds())
    {
        expectedList += "<Latest>" + id + "</Latest>";
    }
    EXPECT_NE(body.find(expectedList), std::string::npos) << body;
    EXPECT_TRUE(httpClient.CompleteRequest(3U));
    httpClient.Poll();

    std::error_code ignored;
    std::filesystem::remove(tempPath, ignored);
}

TEST(AsyncBehaviorTests, UploadFromAsyncStopsAfterMidUploadFailureAndDoesNotCommit)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    std::istringstream stream("ABCDEFGHI");
    AVEVA::AzureClient::UploadFromOptions options;
    options.BlockSize = 4U;

    int callbackCount = 0;
    StartUploadFromStreamAndExpectFailure(client, stream, options, callbackCount);

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteRequest(0U));
    httpClient.Poll();
    ASSERT_EQ(httpClient.RequestCount(), 2U);
    EXPECT_TRUE(httpClient.FailPending(0U, std::make_error_code(std::errc::connection_reset)));
    httpClient.Poll();
    EXPECT_EQ(callbackCount, 1);
    EXPECT_EQ(httpClient.RequestCount(), 2U);
}

// Task 16 verification: an ITokenCredential that completes GetTokenAsync synchronously (the
// default for the built-in StaticTokenCredential/CachingTokenCredential when no executor is
// supplied, and for any third-party implementation that simply calls the handler inline) must
// not cause the overall ...Async() call to complete before it returns to its caller.
// SendAuthorizedRequestAsync detects same-stack completions of GetTokenAsync and defers them via
// post() so Boost.Asio's completion contract holds regardless of which credential is plugged in.
TEST(AsyncBehaviorTests, TokenCredentialSynchronousCompletionIsDeferredRatherThanInvokedInline)
{
    FakeHttpClient httpClient;

    auto tokenCredential = std::make_shared<ScriptedTokenCredential>();
    tokenCredential->EnqueueImmediate({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    options.TokenCredential = tokenCredential;
    BlockBlobClient client{httpClient, options};

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

// Task 8 verification: an early-exit validation failure must not invoke the completion before
// the initiating call returns; it must run only once the executor it was posted to is driven.
TEST(AsyncBehaviorTests, EarlyExitValidationFailurePostsRatherThanInvokingSynchronously)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    bool callbackInvoked = false;
    StartInvalidStageBlockAndCaptureInvocation(client, callbackInvoked);

    EXPECT_FALSE(callbackInvoked);
    EXPECT_EQ(httpClient.Poll(), 1U);
    EXPECT_TRUE(callbackInvoked);
}

// Task 8 verification: a completion handler that immediately re-issues the same failing
// operation must not recurse through the stack, since the posted completion always resumes on
// the executor's run/poll loop rather than being invoked inline from within the previous
// initiating call.
TEST(AsyncBehaviorTests, RecursivePostedEarlyExitFailuresDoNotGrowTheStack)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    constexpr int Iterations = 5000;
    int completedCount = 0;

    std::function<void()> issue = [&]()
    {
        IssueRecursiveInvalidStageBlock(client, completedCount, issue);
    };

    issue();
    DrainRecursiveInvalidStageBlockCompletions(httpClient, completedCount, Iterations);

    EXPECT_EQ(completedCount, Iterations);
}

// Task 6 verification: a completion token with an explicit associated executor (via
// boost::asio::bind_executor) must have its completion dispatched onto that executor rather than
// invoked directly on FakeHttpClient's own default executor.
TEST(AsyncBehaviorTests, ExplicitAssociatedExecutorReceivesTheCompletionInsteadOfTheDefault)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

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

// Task 6 verification: an explicitly-set HttpRequestOptions cancellation slot must win over a
// cancellation slot merely associated with the completion token (explicit wins, matching
// BindAssociationsAndAdoptCancellation's documented contract).
TEST(AsyncBehaviorTests, ExplicitRequestOptionsCancellationSlotWinsOverAssociatedSlot)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

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

// Task 16 stretch verification: a completion token with an explicit associated allocator (via
// boost::asio::bind_allocator, layered under bind_executor so the non-skip branch of
// BindAssociationsAndAdoptCancellation is actually taken) must have that allocator genuinely used
// to allocate/deallocate the dispatched completion's internal state -- not merely be discoverable
// via get_associated_allocator without ever being invoked.
TEST(AsyncBehaviorTests, ExplicitAssociatedAllocatorIsActuallyUsedForTheDispatchedCompletion)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

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
TEST(AsyncBehaviorTests, CancelAfterWrappedTokenCompilesAndCompletesNormally)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

    boost::asio::steady_timer timer{httpClient.get_executor()};

    CallbackExpectation callback;
    StartDeleteWithCancelAfter(client, timer, callback);

    ASSERT_EQ(httpClient.RequestCount(), 1U);
    EXPECT_TRUE(httpClient.CompleteNext());
    httpClient.Poll();
}

namespace
{
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
    struct DeferredCompletionCase
    {
        const char* Name;
        std::function<void(FakeHttpClient&, bool&)> Invoke;
    };

    [[nodiscard]] std::vector<DeferredCompletionCase> MakeDeferredCompletionCases()
    {
        using AVEVA::AzureClient::ReleaseLeaseOptions;
        using AVEVA::AzureClient::SetBlobAccessTierOptions;
        using AVEVA::AzureClient::SetBlobHttpHeadersOptions;
        using AVEVA::AzureClient::SetBlobMetadataOptions;

        std::vector<DeferredCompletionCase> cases;

        // --- BlockBlobClient ---
        cases.push_back({.Name = "BlockBlobClient.UploadAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->UploadAsync("data",
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.StageBlockAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->StageBlockAsync("AAAA",
                "data",
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.CommitBlockListAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->CommitBlockListAsync({"AAAA"},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.GetBlockListAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->GetBlockListAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.DownloadAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->DownloadAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.DownloadToAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            auto stream = std::make_shared<std::ostringstream>();
            client->DownloadToAsync(*stream,
                [client, stream, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.DeleteAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->DeleteAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.GetPropertiesAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->GetPropertiesAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.SetMetadataAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->SetMetadataAsync(SetBlobMetadataOptions{},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.SetHttpHeadersAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->SetHttpHeadersAsync(SetBlobHttpHeadersOptions{},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.SetAccessTierAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->SetAccessTierAsync(SetBlobAccessTierOptions{},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.StartCopyFromUriAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->StartCopyFromUriAsync("https://source.example.com/b",
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.SnapshotAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->SnapshotAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.AcquireLeaseAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->AcquireLeaseAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.ReleaseLeaseAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = "lease-1"},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.BreakLeaseAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->BreakLeaseAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.ExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->ExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.CreateIfNotExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->CreateIfNotExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.DeleteIfExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            client->DeleteIfExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlockBlobClient.UploadFromAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
            auto stream = std::make_shared<std::istringstream>("data");
            client->UploadFromAsync(*stream,
                [client, stream, &invoked](auto)
            {
                invoked = true;
            });
        }});

        // --- AppendBlobClient ---
        cases.push_back({.Name = "AppendBlobClient.CreateAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<AppendBlobClient>(http, MakeAppendBlobOptions());
            client->CreateAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "AppendBlobClient.AppendBlockAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<AppendBlobClient>(http, MakeAppendBlobOptions());
            client->AppendBlockAsync("data",
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "AppendBlobClient.DownloadAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<AppendBlobClient>(http, MakeAppendBlobOptions());
            client->DownloadAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "AppendBlobClient.DownloadToAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<AppendBlobClient>(http, MakeAppendBlobOptions());
            auto stream = std::make_shared<std::ostringstream>();
            client->DownloadToAsync(*stream,
                [client, stream, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "AppendBlobClient.DeleteAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<AppendBlobClient>(http, MakeAppendBlobOptions());
            client->DeleteAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "AppendBlobClient.GetPropertiesAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<AppendBlobClient>(http, MakeAppendBlobOptions());
            client->GetPropertiesAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});

        // --- PageBlobClient ---
        cases.push_back({.Name = "PageBlobClient.CreateAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->CreateAsync(512U,
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.UploadPagesAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->UploadPagesAsync(0,
                std::string(512U, 'x'),
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.ClearPagesAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->ClearPagesAsync(0,
                512U,
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.ResizeAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->ResizeAsync(512U,
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.DownloadAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->DownloadAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.DownloadToAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            auto stream = std::make_shared<std::ostringstream>();
            client->DownloadToAsync(*stream,
                [client, stream, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.GetPageRangesAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->GetPageRangesAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.DeleteAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->DeleteAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.GetPropertiesAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->GetPropertiesAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.SetMetadataAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->SetMetadataAsync(SetBlobMetadataOptions{},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.SetHttpHeadersAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->SetHttpHeadersAsync(SetBlobHttpHeadersOptions{},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.SetAccessTierAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->SetAccessTierAsync(SetBlobAccessTierOptions{},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.StartCopyFromUriAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->StartCopyFromUriAsync("https://source.example.com/p",
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.SnapshotAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->SnapshotAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.AcquireLeaseAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->AcquireLeaseAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.ReleaseLeaseAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->ReleaseLeaseAsync(ReleaseLeaseOptions{.LeaseId = "lease-1"},
                [client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.BreakLeaseAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->BreakLeaseAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.ExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->ExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "PageBlobClient.DeleteIfExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<PageBlobClient>(http, MakePageBlobOptions());
            client->DeleteIfExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});

        // --- BlobContainerClient ---
        cases.push_back({.Name = "BlobContainerClient.CreateAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
            client->CreateAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlobContainerClient.DeleteAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
            client->DeleteAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlobContainerClient.GetPropertiesAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
            client->GetPropertiesAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlobContainerClient.ListBlobsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
            client->ListBlobsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlobContainerClient.ExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
            client->ExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlobContainerClient.CreateIfNotExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
            client->CreateIfNotExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});
        cases.push_back({.Name = "BlobContainerClient.DeleteIfExistsAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobContainerClient>(http, MakeBlobContainerClientOptions());
            client->DeleteIfExistsAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});

        // --- BlobServiceClient ---
        cases.push_back({.Name = "BlobServiceClient.ListBlobContainersAsync",
            .Invoke = [](FakeHttpClient& http, bool& invoked)
        {
            auto client = std::make_shared<BlobServiceClient>(http, MakeBlobServiceClientOptions());
            client->ListBlobContainersAsync([client, &invoked](auto)
            {
                invoked = true;
            });
        }});

        return cases;
    }
} // namespace

TEST(AsyncBehaviorTests, NoPublicOperationCompletesBeforeTheInitiatingCallReturns)
{
    for (const DeferredCompletionCase& testCase : MakeDeferredCompletionCases())
    {
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

// T26 guard, part 2: completions that do not come from a deferred transport response -- local
// validation/IO failures detected during initiation, and transports that complete inline inside
// SendAsync -- must still never run before the initiating call returns.
TEST(AsyncBehaviorTests, EarlyFailuresAndInlineTransportCompletionsAreNeverDeliveredBeforeTheCallReturns)
{
    const std::filesystem::path directory = std::filesystem::temp_directory_path();
    const std::filesystem::path missingFile = directory / "azure-client-t26-missing-source.bin";
    std::error_code ignored;
    std::filesystem::remove(missingFile, ignored);

    struct Case
    {
        const char* Name;
        bool InlineTransport;
        std::function<void(FakeHttpClient&, bool&)> Invoke;
    };

    std::vector<Case> cases;
    cases.push_back({.Name = "BlockBlobClient.DownloadToAsync(directory)",
        .InlineTransport = false,
        .Invoke = [&](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        client->DownloadToAsync(directory,
            [client, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "PageBlobClient.DownloadToAsync(directory)",
        .InlineTransport = false,
        .Invoke = [&](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<PageBlobClient>(http, MakeBlobClientOptions());
        client->DownloadToAsync(directory,
            [client, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "AppendBlobClient.DownloadToAsync(directory)",
        .InlineTransport = false,
        .Invoke = [&](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<AppendBlobClient>(http, MakeBlobClientOptions());
        client->DownloadToAsync(directory,
            [client, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "BlockBlobClient.DownloadToAsync(zero-length range)",
        .InlineTransport = false,
        .Invoke = [](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        auto stream = std::make_shared<std::ostringstream>();
        AVEVA::AzureClient::DownloadToOptions options;
        options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0U, .Length = 0U};
        client->DownloadToAsync(*stream,
            options,
            [client, stream, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "BlockBlobClient.UploadFromAsync(missing file)",
        .InlineTransport = false,
        .Invoke = [&](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        client->UploadFromAsync(missingFile,
            AVEVA::AzureClient::UploadFromOptions{},
            [client, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "BlockBlobClient.UploadFromAsync(BlockSize 0)",
        .InlineTransport = false,
        .Invoke = [](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        auto stream = std::make_shared<std::istringstream>("data");
        AVEVA::AzureClient::UploadFromOptions options;
        options.BlockSize = 0U;
        client->UploadFromAsync(*stream,
            options,
            [client, stream, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "inline transport: BlockBlobClient.DownloadAsync",
        .InlineTransport = true,
        .Invoke = [](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        client->DownloadAsync([client, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "inline transport: BlockBlobClient.DownloadToAsync(parallel)",
        .InlineTransport = true,
        .Invoke = [](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        auto stream = std::make_shared<std::ostringstream>();
        AVEVA::AzureClient::DownloadToOptions options;
        options.ChunkSize = 4U;
        options.Concurrency = 3U;
        client->DownloadToAsync(*stream,
            options,
            [client, stream, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "inline transport: BlockBlobClient.UploadFromAsync(multi-block)",
        .InlineTransport = true,
        .Invoke = [](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        auto stream = std::make_shared<std::istringstream>("ABCDEFGHIJ");
        AVEVA::AzureClient::UploadFromOptions options;
        options.BlockSize = 4U;
        options.Concurrency = 2U;
        client->UploadFromAsync(*stream,
            options,
            [client, stream, &invoked](auto)
        {
            invoked = true;
        });
    }});
    cases.push_back({.Name = "inline transport: BlockBlobClient.DeleteAsync",
        .InlineTransport = true,
        .Invoke = [](FakeHttpClient& http, bool& invoked)
    {
        auto client = std::make_shared<BlockBlobClient>(http, MakeBlobClientOptions());
        client->DeleteAsync([client, &invoked](auto)
        {
            invoked = true;
        });
    }});

    for (const Case& testCase : cases)
    {
        SCOPED_TRACE(testCase.Name);

        FakeHttpClient httpClient;
        httpClient.CompleteInline() = testCase.InlineTransport;
        // A 206 for the first 4 bytes of a 10-byte blob serves every ranged download request the
        // inline-transport download cases issue; other operations treat it as a plain success.
        httpClient.DefaultResponse() = HttpResponse{206,
            MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-3/4"}, {"Content-Length", "4"}}),
            "data"};

        bool invoked = false;
        testCase.Invoke(httpClient, invoked);

        EXPECT_FALSE(invoked) << testCase.Name << " invoked its completion before the initiating call returned";
        for (int i = 0; i < 10 && !invoked; ++i)
        {
            httpClient.Poll();
        }
        EXPECT_TRUE(invoked) << testCase.Name << " never invoked its completion after Poll()";
    }
}

// ---------------------------------------------------------------------------------------------
// T29: async-behaviour matrix. Already covered elsewhere and not repeated here: use_future (each
// client's *Tests.cpp), co_await on the default deferred token (Task 14 tests), bind_executor(strand)
// and bind_allocator (above), move-only handlers (above), cancellation during retry backoff and of an
// in-flight attempt (RetryTests), and mid-transfer cancellation of UploadFrom/DownloadTo
// (T10_ConcurrentUploadTests, T04_ConcurrencyTests, T11_ParallelDownloadAcceptanceTests).
// ---------------------------------------------------------------------------------------------
TEST(AsyncBehaviorTests, ExplicitDeferredTokenSendsNothingUntilTheOperationIsInvoked)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

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

TEST(AsyncBehaviorTests, CancellingACoSpawnedUseAwaitableOperationCancelsTheInFlightRequest)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

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
TEST(AsyncBehaviorTests, SignalEmittedBeforeInitiationDoesNotCancelALaterOperation)
{
    FakeHttpClient httpClient;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};

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
// arrives. ITokenCredential has no cancellation hook, so cancellation must complete the operation
// promptly rather than waiting for (possibly slow) credential I/O, and the late token must be ignored.
TEST(AsyncBehaviorTests, CancellationDuringTokenAcquisitionCompletesPromptlyAndNeverSends)
{
    FakeHttpClient httpClient;
    auto credential = std::make_shared<ScriptedTokenCredential>();
    credential->EnqueueDeferred({}, MakeToken("token-value", std::chrono::hours(1)));

    BlobClientOptions options = MakeBlobClientOptions();
    options.SasToken.clear();
    options.TokenCredential = credential;
    BlockBlobClient client{httpClient, options};

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

TEST(AsyncBehaviorTests, MidFlightCancellationThroughAnAssociatedSlotWorksForContainerAndServiceClients)
{
    {
        SCOPED_TRACE("BlobContainerClient::DeleteAsync");
        ExpectMidFlightCancellation(StartContainerDeleteWithCancellation);
    }
    {
        SCOPED_TRACE("BlobServiceClient::GetPropertiesAsync");
        ExpectMidFlightCancellation(StartServiceGetPropertiesWithCancellation);
    }
}

// Destroying the transport -- and with it the io_context that owns every pending handler, timer and
// posted completion -- must destroy the user's handler (releasing everything it owns) without ever
// invoking it. The sentinel's weak_ptr proves nothing is leaked by a reference cycle; ASan/LSan on CI
// catch use-after-free or leaks in the engines' teardown.
TEST(AsyncBehaviorTests, DestroyingTheIoContextWithPendingWorkReleasesTheHandlerWithoutInvokingIt)
{
    const std::filesystem::path uploadPath =
        std::filesystem::temp_directory_path() / "azure-client-io-context-teardown.bin";
    {
        std::ofstream output(uploadPath, std::ios::binary | std::ios::trunc);
        output << "ABCDEFGHI";
    }

    VerifyPendingWorkHandlersAreReleased(uploadPath);

    std::error_code ignored;
    std::filesystem::remove(uploadPath, ignored);
}

// Supported cancellation pattern: the signal is emitted from the handler's executor, so it cannot race
// with the library clearing the slot when the operation finishes.
TEST(AsyncBehaviorTests, CancellationEmittedFromTheHandlerExecutorCancelsTheOperation)
{
    FakeHttpClient httpClient;
    httpClient.DeferByDefault() = true;
    BlockBlobClient client{httpClient, MakeBlobClientOptions()};
    boost::asio::cancellation_signal signal;
    std::optional<DeleteResult> observed;
    client.DeleteAsync(boost::asio::bind_cancellation_slot(signal.slot(),
        [&](DeleteResult result)
    {
        observed = std::move(result);
    }));
    ASSERT_EQ(httpClient.PendingCount(), 1U);

    boost::asio::post(httpClient.get_executor(),
        [&signal]
    {
        signal.emit(boost::asio::cancellation_type::terminal);
    });
    httpClient.Poll();
    ASSERT_TRUE(observed.has_value());
    ASSERT_FALSE(observed->has_value());
    EXPECT_EQ(observed->error().Code, std::make_error_code(std::errc::operation_canceled));
}
