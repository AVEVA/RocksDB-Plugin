// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "AVEVA/RocksDB/Plugin/Core/Mocks/TempDirectory.hpp"
#include "FakeBlobEnvironment.hpp"
#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace BlobOperations = AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations;
using AVEVA::HttpResponse;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobServer;

namespace {
// Runs the real PageBlobClient against scripted HTTP responses.
class BlobOperationsTests : public ::testing::Test {
  protected:
    void SetUp() override {
        // Completions are posted to the fake's own io_context while the operation blocks on a future.
        m_httpClient.StartBackgroundPump();
        m_blob = std::make_unique<AVEVA::AzureClient::Tests::PageBlobClient>(
            m_httpClient, MakeBlobClientOptions("files", "000001.sst"));
    }

    FakeHttpClient m_httpClient;
    std::unique_ptr<AVEVA::AzureClient::Tests::PageBlobClient> m_blob;
};
} // namespace

TEST_F(BlobOperationsTests, DownloadToFileWithZeroLengthCreatesEmptyFileWithoutRequest) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("aveva-blobops-empty-test-" + std::to_string(std::random_device{}()) + ".bin");
    {
        std::ofstream stale(path, std::ios::binary);
        stale << "stale contents";
    }

    BlobOperations::DownloadToFile(*m_blob, path, 0, 0);

    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(0U, std::filesystem::file_size(path));
    EXPECT_TRUE(m_httpClient.NoRequestMade());
    std::filesystem::remove(path);
}

TEST_F(BlobOperationsTests, DownloadLargerThanTheBufferFailsWithoutWritingPastIt) {
    m_httpClient.EnqueueResponse(
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-15/16"}, {"Content-Length", "16"}}),
                     std::string(16, 'x')});

    std::vector<char> storage(16, '#');
    EXPECT_ANY_THROW((void)BlobOperations::DownloadToBuffer(*m_blob, std::span<char>(storage).first(8), 0, 16));
    EXPECT_EQ(std::string(8, '#'), std::string(storage.begin() + 8, storage.end()));
}

TEST_F(BlobOperationsTests, DownloadToBufferRejectsPartialContentWithoutContentRange) {
    m_httpClient.EnqueueResponse(HttpResponse{206, MakeCanonicalSuccessHeaders({}), std::string(4, 'y')});

    std::vector<char> buffer(8);
    EXPECT_ANY_THROW((void)BlobOperations::DownloadToBuffer(*m_blob, std::span<char>(buffer), 0, 4));
}

TEST_F(BlobOperationsTests, DownloadToBufferAcceptsPartialContentWithMatchingContentRange) {
    m_httpClient.EnqueueResponse(
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-3/8"}}), std::string(4, 'y')});

    std::vector<char> buffer(8);
    EXPECT_EQ(4, BlobOperations::DownloadToBuffer(*m_blob, std::span<char>(buffer), 0, 4));
}

TEST_F(BlobOperationsTests, DownloadAsyncRejectsBodyShorterThanContentRange) {
    m_httpClient.EnqueueResponse(
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-7/8"}}), std::string(4, 'y')});

    std::promise<std::exception_ptr> done;
    BlobOperations::DownloadAsync(*m_blob, 0, 8, std::string(), std::chrono::milliseconds(0),
                                  [&](std::exception_ptr error, unsigned int, std::string_view,
                                      const std::shared_ptr<const void>&) { done.set_value(error); });

    auto result = done.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(10)));
    EXPECT_TRUE(result.get() != nullptr);
}

TEST_F(BlobOperationsTests, BlockingFromTheIoContextThreadThrowsInsteadOfDeadlocking) {
    std::promise<bool> threw;
    boost::asio::post(m_httpClient.get_executor(), [&] {
        try {
            std::vector<char> buffer(8);
            (void)BlobOperations::DownloadToBuffer(*m_blob, std::span<char>(buffer), 0, 8);
            threw.set_value(false);
        } catch (const std::logic_error&) {
            threw.set_value(true);
        }
    });

    auto result = threw.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(10)));
    EXPECT_TRUE(result.get());
}

TEST_F(BlobOperationsTests, StopBridgeEmitsAStopRequestedWhileTheHandlerIsBeingInstalled) {
    using Bridge = BlobOperations::Detail::StopBridge<boost::asio::io_context::executor_type>;
    auto bridge = std::make_shared<Bridge>(m_httpClient.get_executor());
    std::atomic<int> emitted{0};
    std::thread requester;

    bridge->Install(bridge, [&] {
        requester = std::thread([&] { Bridge::Request(bridge); });
        // The request is made while the handler is not installed yet; it must not be lost or race the install.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        bridge->Slot().assign([&](boost::asio::cancellation_type_t) { ++emitted; });
        return 0;
    });
    requester.join();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (emitted.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    bridge->Retire();

    EXPECT_EQ(1, emitted.load());
}

TEST_F(BlobOperationsTests, StopBridgeEmitsAfterInstallationExactlyOnceOnTheExecutorThread) {
    using Bridge = BlobOperations::Detail::StopBridge<boost::asio::io_context::executor_type>;
    auto bridge = std::make_shared<Bridge>(m_httpClient.get_executor());
    std::promise<std::thread::id> fired;
    std::atomic<int> emitted{0};
    bridge->Install(bridge, [&] {
        bridge->Slot().assign([&](boost::asio::cancellation_type_t) {
            if (emitted.fetch_add(1) == 0) {
                fired.set_value(std::this_thread::get_id());
            }
        });
        return 0;
    });

    const auto requesterId = std::this_thread::get_id();
    std::thread first([&] { Bridge::Request(bridge); });
    std::thread second([&] { Bridge::Request(bridge); });
    first.join();
    second.join();

    auto firedOn = fired.get_future();
    ASSERT_EQ(std::future_status::ready, firedOn.wait_for(std::chrono::seconds(10)));
    EXPECT_NE(requesterId, firedOn.get());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(1, emitted.load());
    bridge->Retire();
}

TEST_F(BlobOperationsTests, RetiredStopBridgeDoesNotEmitAndReleasesItsWait) {
    using Bridge = BlobOperations::Detail::StopBridge<boost::asio::io_context::executor_type>;
    auto bridge = std::make_shared<Bridge>(m_httpClient.get_executor());
    std::atomic<int> emitted{0};
    bridge->Install(bridge, [&] {
        bridge->Slot().assign([&](boost::asio::cancellation_type_t) { ++emitted; });
        return 0;
    });
    const std::weak_ptr<Bridge> weak = bridge;

    bridge->Retire();
    Bridge::Request(bridge);
    bridge.reset();

    // The wait that Install left outstanding owns the bridge until the executor completes it.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!weak.expired() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(0, emitted.load());
}

TEST_F(BlobOperationsTests, StopRequestCancelsAnAcceptedStalledCall) {
    m_httpClient.EnqueueDeferredResponse(HttpResponse{200, MakeCanonicalSuccessHeaders({}), std::string()});
    std::stop_source stop;
    std::promise<void> finished;
    std::thread caller([&] {
        EXPECT_ANY_THROW((void)BlobOperations::GetInfo(*m_blob, stop.get_token()));
        finished.set_value();
    });
    auto done = finished.get_future();
    // The call is stalled on the deferred response until the stop request reaches it.
    EXPECT_EQ(std::future_status::timeout, done.wait_for(std::chrono::milliseconds(200)));
    stop.request_stop();
    EXPECT_EQ(std::future_status::ready, done.wait_for(std::chrono::seconds(10)));
    if (done.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        EXPECT_TRUE(m_httpClient.CompleteNext());
    }
    caller.join();
}

TEST_F(BlobOperationsTests, AlreadyStoppedTokenCancelsWithoutStartingARequest) {
    std::stop_source stop;
    stop.request_stop();

    EXPECT_THROW((void)BlobOperations::GetInfo(*m_blob, stop.get_token()), std::system_error);
    EXPECT_TRUE(m_httpClient.NoRequestMade());
}

TEST_F(BlobOperationsTests, DownloadToBufferWithZeroLengthReturnsZeroWithoutRequest) {
    std::vector<char> buffer(8);
    EXPECT_EQ(0, BlobOperations::DownloadToBuffer(*m_blob, std::span<char>(buffer), 0, 0));
    EXPECT_TRUE(m_httpClient.NoRequestMade());
}

namespace {
struct ThrowingContainer {
    void push_back(std::future<int>&&) { throw std::bad_alloc(); }
};
} // namespace

TEST(BlobOperationsPushAcceptedTests, DrainsAnAcceptedOperationWhenTheContainerCannotGrow) {
    std::promise<int> promise;
    std::future<int> pending = promise.get_future();
    ThrowingContainer container;
    bool drained = false;

    EXPECT_THROW(BlobOperations::PushAccepted(container, std::move(pending),
                                              [&](std::future<int>& item) {
                                                  EXPECT_TRUE(item.valid());
                                                  drained = true;
                                                  promise.set_value(1);
                                                  item.wait();
                                              }),
                 std::bad_alloc);
    EXPECT_TRUE(drained);
}

TEST(BlobOperationsPushAcceptedTests, KeepsTheOperationInTheContainerOnSuccess) {
    std::promise<int> promise;
    std::vector<std::future<int>> container;
    BlobOperations::PushAccepted(container, promise.get_future(), [](std::future<int>&) { FAIL(); });
    EXPECT_EQ(1U, container.size());
    promise.set_value(1);
}

namespace {
// Runs the operations against the in-memory blob, which answers without HTTP.
class BlobOperationsFakeBlobTests : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_env;
};
} // namespace

TEST_F(BlobOperationsFakeBlobTests, GetEtagReadsThePropertiesOfTheBlob) {
    m_env.Blob->ETag = "\"fake\"";

    EXPECT_EQ("\"fake\"", BlobOperations::GetInfo(*m_env.Client).ETag);
}

TEST_F(BlobOperationsFakeBlobTests, SetCapacityResizesTheBlobToTheRequestedSize) {
    BlobOperations::SetCapacity(*m_env.Runtime, *m_env.Client, 1024);

    EXPECT_EQ((std::vector<int64_t>{1024}), m_env.Blob->CapacityWrites);
    EXPECT_EQ(1024, BlobOperations::GetInfo(*m_env.Client).Capacity);
}

TEST_F(BlobOperationsFakeBlobTests, SetSizeRecordsTheSizeInTheBlobMetadata) {
    BlobOperations::SetSize(*m_env.Runtime, *m_env.Client, 77);

    EXPECT_EQ(77, BlobOperations::GetInfo(*m_env.Client).Size);
    EXPECT_EQ((std::vector<int64_t>{77}), m_env.Blob->SizeWrites);
}

TEST_F(BlobOperationsFakeBlobTests, GetMetadataReturnsTheSizeAndETagFromOneRequest) {
    m_env.Fill(512);

    const auto metadata = BlobOperations::GetMetadata(*m_env.Client);

    EXPECT_EQ(512, metadata.Size);
    EXPECT_EQ(m_env.Blob->ETag, metadata.ETag);
    EXPECT_EQ(1, m_env.Blob->PropertiesRequests);
}

TEST_F(BlobOperationsFakeBlobTests, StorageFailuresSurfaceAsExceptions) {
    m_env.Blob->Fault =
        [](const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::GetProperties) {
            return FakeBlobServer::Error(500, "InternalError");
        }
        return std::nullopt;
    };

    EXPECT_THROW((void)BlobOperations::GetInfo(*m_env.Client).ETag, RequestFailedException);
}

TEST_F(BlobOperationsFakeBlobTests, UploadPagesStoresTheBytesAtTheOffset) {
    const std::vector<char> page(512, 'u');
    // A real page blob rejects writes past its capacity (the old hand-written fake grew silently).
    m_env.Blob->SetCapacity(1024);

    BlobOperations::UploadPages(*m_env.Runtime, *m_env.Client, page, 512);

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(512, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(page, m_env.Blob->Uploads[0].Data);
}
namespace {
// A second, independent blob (with its own transport) for the copy target.
std::unique_ptr<FakeBlobEnvironment> MakeDestination(int64_t capacity) {
    auto destination = std::make_unique<FakeBlobEnvironment>();
    destination->Blob->SetCapacity(capacity);
    return destination;
}
} // namespace

TEST_F(BlobOperationsFakeBlobTests, CopyPagesTransfersOnlyThePopulatedRanges) {
    m_env.Fill(4096, '\0');
    std::fill_n(m_env.Blob->Data.begin(), 512, 'a');
    std::fill_n(m_env.Blob->Data.begin() + 2048, 1024, 'b');
    const auto destinationBlob = MakeDestination(4096);
    auto& destination = *destinationBlob;

    BlobOperations::CopyPages(*m_env.Runtime, *m_env.Client, *destination.Client, 4096, m_env.Blob->ETag);

    EXPECT_EQ(m_env.Blob->Data, destination.Blob->Data);
    ASSERT_EQ(2U, destination.Blob->Uploads.size());
    EXPECT_EQ(0, destination.Blob->Uploads[0].Offset);
    EXPECT_EQ(2048, destination.Blob->Uploads[1].Offset);
    EXPECT_EQ(1024U, destination.Blob->Uploads[1].Data.size());
    EXPECT_EQ(2U, m_env.Blob->Downloads.size());
}

TEST_F(BlobOperationsFakeBlobTests, CopyPagesKeepsMoreThanOneUploadInFlight) {
    m_env.Fill(4096, '\0');
    std::fill_n(m_env.Blob->Data.begin(), 512, 'a');
    std::fill_n(m_env.Blob->Data.begin() + 2048, 1024, 'b');
    const auto destinationBlob = MakeDestination(4096);
    auto& destination = *destinationBlob;
    destination.Blob->DeferUploads = true;

    auto copy = std::async(std::launch::async, [&] {
        BlobOperations::CopyPages(*m_env.Runtime, *m_env.Client, *destination.Client, 4096, m_env.Blob->ETag);
    });

    // Both uploads are held at once: the second started before the first finished.
    ASSERT_TRUE(destination.Blob->WaitForPendingUploads(2));
    destination.Blob->CompleteUpload();
    destination.Blob->CompleteUpload();
    copy.get();

    EXPECT_EQ(m_env.Blob->Data, destination.Blob->Data);
}

TEST_F(BlobOperationsFakeBlobTests, CopyPagesOfAnEmptyBlobTransfersNothing) {
    m_env.Fill(2048, '\0');
    const auto destinationBlob = MakeDestination(2048);
    auto& destination = *destinationBlob;

    BlobOperations::CopyPages(*m_env.Runtime, *m_env.Client, *destination.Client, 0, m_env.Blob->ETag);

    EXPECT_TRUE(destination.Blob->Uploads.empty());
    EXPECT_TRUE(m_env.Blob->Downloads.empty());
}

TEST_F(BlobOperationsFakeBlobTests, CopyPagesClipsToTheLogicalSizeAndPadsTheLastPage) {
    m_env.Fill(2048, 'z');
    m_env.Blob->Size = 700;
    const auto destinationBlob = MakeDestination(2048);
    auto& destination = *destinationBlob;

    BlobOperations::CopyPages(*m_env.Runtime, *m_env.Client, *destination.Client, 700, m_env.Blob->ETag);

    ASSERT_EQ(1U, destination.Blob->Uploads.size());
    ASSERT_EQ(1024U, destination.Blob->Uploads[0].Data.size());
    EXPECT_EQ('z', destination.Blob->Uploads[0].Data[699]);
    EXPECT_EQ('\0', destination.Blob->Uploads[0].Data[700]);
    EXPECT_EQ(1, m_env.Blob->Downloads.size());
    EXPECT_EQ(700, m_env.Blob->Downloads[0].Length);
}

TEST_F(BlobOperationsFakeBlobTests, CopyPagesSplitsRangesLargerThanTheCopyChunk) {
    const auto chunk = AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::Transfer::MaxCopyChunkSize;
    m_env.Fill(chunk + 1024, 'c');
    const auto destinationBlob = MakeDestination(chunk + 1024);
    auto& destination = *destinationBlob;

    BlobOperations::CopyPages(*m_env.Runtime, *m_env.Client, *destination.Client, chunk + 1024, m_env.Blob->ETag);

    ASSERT_EQ(2U, destination.Blob->Uploads.size());
    EXPECT_EQ(chunk, static_cast<int64_t>(destination.Blob->Uploads[0].Data.size()));
    EXPECT_EQ(chunk, destination.Blob->Uploads[1].Offset);
    EXPECT_EQ(m_env.Blob->Data, destination.Blob->Data);
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyTransfersOnlyThePopulatedRangesWithoutDownloading) {
    m_env.Fill(4096, '\0');
    std::fill_n(m_env.Blob->Data.begin(), 512, 'a');
    std::fill_n(m_env.Blob->Data.begin() + 2048, 1024, 'b');
    const auto destination = m_env.Blob->AddBlob("copy.sst", 4096);

    EXPECT_TRUE(
        BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, 4096, m_env.Blob->ETag));

    EXPECT_EQ(m_env.Blob->Data, m_env.Blob->BlobBytes("copy.sst"));
    ASSERT_EQ(2U, m_env.Blob->PageCopies.size());
    EXPECT_EQ(0, m_env.Blob->PageCopies[0].Offset);
    EXPECT_EQ(2048, m_env.Blob->PageCopies[1].Offset);
    EXPECT_EQ(1024, m_env.Blob->PageCopies[1].Length);
    EXPECT_EQ(m_env.Blob->ETag, m_env.Blob->PageCopies[1].SourceIfMatch);
    EXPECT_TRUE(m_env.Blob->Downloads.empty());
    EXPECT_TRUE(m_env.Blob->Uploads.empty());
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyOfAnEmptyBlobSendsNothing) {
    m_env.Fill(2048, '\0');
    const auto destination = m_env.Blob->AddBlob("copy.sst", 2048);

    EXPECT_TRUE(BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, 0, m_env.Blob->ETag));

    EXPECT_TRUE(m_env.Blob->PageCopies.empty());
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyRoundsTheLastChunkUpToAWholePage) {
    m_env.Fill(2048, 'z');
    const auto destination = m_env.Blob->AddBlob("copy.sst", 2048);

    EXPECT_TRUE(
        BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, 700, m_env.Blob->ETag));

    ASSERT_EQ(1U, m_env.Blob->PageCopies.size());
    EXPECT_EQ(1024, m_env.Blob->PageCopies[0].Length);
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopySplitsRangesLargerThanTheCopyChunkAndKeepsSeveralInFlight) {
    const auto chunk = AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::Transfer::MaxCopyChunkSize;
    const auto total = chunk * 6 + 1024;
    m_env.Fill(total, 'c');
    const auto destination = m_env.Blob->AddBlob("copy.sst", total);

    EXPECT_TRUE(
        BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, total, m_env.Blob->ETag));

    EXPECT_EQ(7U, m_env.Blob->PageCopies.size());
    EXPECT_EQ(m_env.Blob->Data, m_env.Blob->BlobBytes("copy.sst"));
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyReportsAnUnauthorizedSourceSoTheCallerCanFallBack) {
    m_env.Fill(1024, 'q');
    m_env.Blob->RequireCopySourceAuthorization = true;
    const auto destination = m_env.Blob->AddBlob("copy.sst", 1024);

    EXPECT_FALSE(
        BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, 1024, m_env.Blob->ETag));

    EXPECT_EQ(1U, m_env.Blob->PageCopies.size());
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyIsSkippedWhenNeitherSideCanAuthorizeTheSource) {
    m_env.Fill(1024, 'q');
    // Shared Key clients have no bearer token to forward and the source URL carries no SAS.
    auto sharedKeyOptions = MakeBlobClientOptions("files", "000001.sst");
    sharedKeyOptions.SasToken.clear();
    sharedKeyOptions.SharedKey = {.AccountName = "account", .AccountKey = "a2V5"};
    FakeBlobServer::BlobClientType source(m_env.Http, sharedKeyOptions);
    sharedKeyOptions.BlobName = "copy.sst";
    FakeBlobServer::BlobClientType destination(m_env.Http, sharedKeyOptions);
    const auto requestsBefore = m_env.Blob->PageCopies.size();

    EXPECT_FALSE(BlobOperations::CopyPagesServerSide(*m_env.Runtime, source, destination, 1024, m_env.Blob->ETag));

    EXPECT_EQ(requestsBefore, m_env.Blob->PageCopies.size());
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopySendsNothingWhenPlanningExhaustsTheWriteBudget) {
    m_env.Fill(4096, 'q');
    const auto destination = m_env.Blob->AddBlob("copy.sst", 4096);
    m_env.Blob->Fault =
        [this](const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::GetPageRanges) {
            m_env.Runtime->SetLeaseDeadline(this, std::chrono::steady_clock::now() - std::chrono::seconds(1));
        }
        return std::nullopt;
    };
    m_env.Runtime->SetLeaseDeadline(this, std::chrono::steady_clock::now() + std::chrono::minutes(5));

    EXPECT_THROW(
        (void)BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, 4096, m_env.Blob->ETag),
        AVEVA::RocksDB::Plugin::Azure::WritesFencedException);

    EXPECT_TRUE(m_env.Blob->PageCopies.empty());
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyBoundsLaterRequestsByTheRemainingWriteBudget) {
    const auto chunk = AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::Transfer::MaxCopyChunkSize;
    m_env.Fill(chunk * 3, 'q');
    const auto destination = m_env.Blob->AddBlob("copy.sst", chunk * 3);
    m_env.Runtime->SetLeaseDeadline(this, std::chrono::steady_clock::now() + std::chrono::minutes(5));
    // The first copy shrinks the lease budget, as a slow earlier request would.
    int copies = 0;
    m_env.Blob->Fault =
        [&](const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::UploadPagesFromUrl && ++copies == 1) {
            m_env.Runtime->SetLeaseDeadline(this, std::chrono::steady_clock::now() + std::chrono::seconds(2));
        }
        return std::nullopt;
    };

    EXPECT_TRUE(
        BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, chunk * 3, m_env.Blob->ETag));

    ASSERT_EQ(3U, m_env.Blob->PageCopies.size());
    EXPECT_GT(m_env.Blob->PageCopies[0].Timeout, std::chrono::seconds(2));
    EXPECT_LE(m_env.Blob->PageCopies[1].Timeout, std::chrono::seconds(2));
    EXPECT_LE(m_env.Blob->PageCopies[2].Timeout, std::chrono::seconds(2));
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyFailsWhenTheSourceChangedSincePlanning) {
    m_env.Fill(1024, 'q');
    const auto destination = m_env.Blob->AddBlob("copy.sst", 1024);

    EXPECT_THROW(
        (void)BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, 1024, "\"stale\""),
        RequestFailedException);
}

TEST_F(BlobOperationsFakeBlobTests, ServerSideCopyFailsWhenTheSourceChangesMidCopy) {
    const auto chunk = AVEVA::RocksDB::Plugin::Azure::Impl::Configuration::Transfer::MaxCopyChunkSize;
    m_env.Fill(chunk * 2, 'q');
    int copies = 0;
    m_env.Blob->Fault =
        [&copies](const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::UploadPagesFromUrl && ++copies == 2) {
            return FakeBlobServer::Error(412, "SourceConditionNotMet");
        }
        return std::nullopt;
    };
    const auto destination = m_env.Blob->AddBlob("copy.sst", chunk * 2);

    EXPECT_THROW((void)BlobOperations::CopyPagesServerSide(*m_env.Runtime, *m_env.Client, *destination, chunk * 2,
                                                           m_env.Blob->ETag),
                 RequestFailedException);
}

TEST_F(BlobOperationsFakeBlobTests, CopyPagesFailsWhenTheSourceChangedSincePlanning) {
    m_env.Fill(1024, 'q');
    const auto destinationBlob = MakeDestination(1024);
    auto& destination = *destinationBlob;

    EXPECT_THROW(BlobOperations::CopyPages(*m_env.Runtime, *m_env.Client, *destination.Client, 1024, "\"stale\""),
                 RequestFailedException);
    EXPECT_TRUE(destination.Blob->Uploads.empty());
}

TEST_F(BlobOperationsFakeBlobTests, CopyPagesFailsWhenTheSourceChangesMidCopy) {
    m_env.Fill(1024, 'q');
    m_env.Blob->PageRangeOverride = std::vector<AVEVA::AzureClient::Models::PageRange>{{0, 512}, {512, 512}};
    int downloads = 0;
    m_env.Blob->Fault =
        [&downloads](const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::DownloadToBuffer && ++downloads == 2) {
            return FakeBlobServer::Error(412, "ConditionNotMet");
        }
        return std::nullopt;
    };
    const auto destinationBlob = MakeDestination(1024);
    auto& destination = *destinationBlob;

    EXPECT_THROW(BlobOperations::CopyPages(*m_env.Runtime, *m_env.Client, *destination.Client, 1024, m_env.Blob->ETag),
                 RequestFailedException);
}

namespace {
class DownloadToFileTests : public BlobOperationsFakeBlobTests {
  protected:
    static std::string ReadAll(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    // Anything the client leaves next to the destination (partial files) counts as a leak.
    size_t FilesInDirectory() const {
        size_t count = 0;
        for ([[maybe_unused]] const auto& entry : std::filesystem::directory_iterator(m_dir.Path())) {
            ++count;
        }
        return count;
    }

    AVEVA::RocksDB::Plugin::Core::Mocks::TempDirectory m_dir{"download-to-file"};
};
} // namespace

TEST_F(DownloadToFileTests, FullBlobIsWrittenToTheFile) {
    m_env.Fill(4096, 'f');
    const auto path = m_dir.Path() / "full.sst";

    BlobOperations::DownloadToFile(*m_env.Client, path, 0, 4096);

    EXPECT_EQ(std::string(4096, 'f'), ReadAll(path));
}

TEST_F(DownloadToFileTests, OnlyTheRequestedRangeIsWritten) {
    m_env.Fill(4096, 'a');
    std::fill(m_env.Blob->Data.begin() + 1024, m_env.Blob->Data.begin() + 2048, 'b');
    const auto path = m_dir.Path() / "range.sst";

    BlobOperations::DownloadToFile(*m_env.Client, path, 1024, 1024);

    EXPECT_EQ(std::string(1024, 'b'), ReadAll(path));
}

TEST_F(DownloadToFileTests, MissingBlobThrowsAndLeavesNoFile) {
    m_env.Fill(1024);
    m_env.Blob->Fault =
        [](const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::Download) {
            return FakeBlobServer::Error(404, "BlobNotFound");
        }
        return std::nullopt;
    };
    const auto path = m_dir.Path() / "missing.sst";

    EXPECT_THROW(BlobOperations::DownloadToFile(*m_env.Client, path, 0, 1024), RequestFailedException);
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_EQ(0U, FilesInDirectory());
}

TEST_F(DownloadToFileTests, ShortBodyThrowsAndDoesNotLeaveATruncatedFile) {
    m_env.Fill(4096);
    m_env.Blob->DownloadOverride = [](const FakeBlobServer::DownloadRequest&) { return std::string(100, 'x'); };
    const auto path = m_dir.Path() / "short.sst";

    EXPECT_ANY_THROW(BlobOperations::DownloadToFile(*m_env.Client, path, 0, 4096));
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_EQ(0U, FilesInDirectory());
}

TEST_F(DownloadToFileTests, TransientServerErrorIsRetriedAndTheDownloadSucceeds) {
    m_env.Fill(2048, 'r');
    std::atomic<int> attempts{0};
    m_env.Blob->Fault =
        [&attempts](const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::Download && attempts.fetch_add(1) == 0) {
            return FakeBlobServer::Error(503, "ServerBusy");
        }
        return std::nullopt;
    };
    const auto path = m_dir.Path() / "retried.sst";
    auto options = MakeBlobClientOptions("files", "test.sst");
    options.Retry.MaxRetries = 2;
    options.Retry.InitialDelay = std::chrono::milliseconds{0};
    FakeBlobServer::BlobClientType retrying(m_env.Http, options);

    BlobOperations::DownloadToFile(retrying, path, 0, 2048);

    EXPECT_GE(attempts.load(), 2);
    EXPECT_EQ(std::string(2048, 'r'), ReadAll(path));
}

TEST(DownloadBufferPoolTests, ReleasedBufferIsReusedWithoutAllocating) {
    const auto pool = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool>();
    const char* first = nullptr;
    {
        const auto lease = pool->Acquire(4096);
        first = lease->Data();
        EXPECT_EQ(4096U, lease->Size());
    }
    EXPECT_EQ(1U, pool->PooledCount());

    const auto again = pool->Acquire(3000);

    EXPECT_EQ(first, again->Data());
    EXPECT_EQ(3000U, again->Size());
    EXPECT_EQ(0U, pool->PooledCount());
}

TEST(DownloadBufferPoolTests, BuffersOfADifferentSizeClassAreNotShared) {
    const auto pool = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool>();
    {
        const auto big = pool->Acquire(1U << 20);
    }
    {
        const auto small = pool->Acquire(100);
    }
    EXPECT_EQ(2U, pool->PooledCount());

    const auto small = pool->Acquire(512);
    EXPECT_EQ(512U, small->Size());
    EXPECT_EQ(1U, pool->PooledCount());
    const auto big = pool->Acquire((1U << 19) + 1);
    EXPECT_EQ(0U, pool->PooledCount());
}

TEST(DownloadBufferPoolTests, ClassKeepsAtMostTheConfiguredNumberOfBuffers) {
    using Pool = AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool;
    const auto pool = std::make_shared<Pool>();
    {
        std::vector<std::shared_ptr<Pool::Lease>> leases;
        for (size_t i = 0; i < Pool::MaxPooledBuffersPerClass + 4; ++i) {
            leases.push_back(pool->Acquire(1024));
        }
    }

    EXPECT_EQ(Pool::MaxPooledBuffersPerClass, pool->PooledCount());
}

TEST(DownloadBufferPoolTests, RetainedBytesAreCappedAndTheLargestBuffersGoFirst) {
    using Pool = AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool;
    const auto pool = std::make_shared<Pool>();
    {
        std::vector<std::shared_ptr<Pool::Lease>> leases;
        for (size_t i = 0; i < Pool::MaxPooledBuffersPerClass; ++i) {
            leases.push_back(pool->Acquire(Pool::MaxPooledBufferBytes));
        }
        leases.push_back(pool->Acquire(1024));
    }

    EXPECT_LE(pool->PooledBytes(), Pool::MaxPooledTotalBytes);
    // The small buffer survives while the big ones were evicted to make room.
    EXPECT_GE(pool->PooledBytes(), size_t{1024});
    auto small = pool->Acquire(1024);
    EXPECT_LT(pool->PooledBytes(), Pool::MaxPooledTotalBytes);
}

TEST(DownloadBufferPoolTests, ConcurrentChurnNeverWrapsTheByteCounter) {
    using Pool = AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool;
    const auto pool = std::make_shared<Pool>();
    std::atomic<bool> stop = false;
    std::atomic<std::size_t> maxObserved = 0;
    std::thread observer([&] {
        while (!stop.load()) {
            const std::size_t bytes = pool->PooledBytes();
            std::size_t previous = maxObserved.load();
            while (bytes > previous && !maxObserved.compare_exchange_weak(previous, bytes)) {
            }
        }
    });

    std::vector<std::thread> workers;
    for (int worker = 0; worker < 8; ++worker) {
        workers.emplace_back([&, worker] {
            for (int i = 0; i < 20000; ++i) {
                // Workers share two classes so returns and acquires of the same class keep interleaving.
                const auto lease = pool->Acquire(((i + worker) % 2 == 0) ? 1024 : 2048);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    stop = true;
    observer.join();

    EXPECT_LE(maxObserved.load(), Pool::MaxPooledTotalBytes);
    // Draining every retained buffer must leave the counter at exactly zero.
    std::vector<std::shared_ptr<Pool::Lease>> held;
    for (std::size_t n = 0; n < Pool::MaxPooledBuffersPerClass; ++n) {
        held.push_back(pool->Acquire(1024));
        held.push_back(pool->Acquire(2048));
    }
    EXPECT_EQ(0U, pool->PooledBytes());
    EXPECT_EQ(0U, pool->PooledCount());
}

namespace {
struct PausingPoolHooks {
    static inline std::atomic<bool> Armed = false;
    static inline std::promise<void> Entered;
    static inline std::shared_future<void> Resume;
    static void AfterPublish() noexcept {
        if (Armed.exchange(false)) {
            Entered.set_value();
            Resume.wait();
        }
    }
};
} // namespace

TEST(DownloadBufferPoolTests, AcquireDuringAReturnNeverSeesUnaccountedBytes) {
    using Pool = AVEVA::RocksDB::Plugin::Azure::Impl::BasicDownloadBufferPool<PausingPoolHooks>;
    const auto pool = std::make_shared<Pool>();
    PausingPoolHooks::Entered = std::promise<void>();
    std::promise<void> resume;
    PausingPoolHooks::Resume = resume.get_future().share();
    auto entered = PausingPoolHooks::Entered.get_future();

    auto lease = pool->Acquire(1024);
    PausingPoolHooks::Armed = true;
    // The return pauses just after its buffer became visible to Acquire.
    std::thread returning([&] { lease.reset(); });
    entered.wait();

    const auto reused = pool->Acquire(1024);
    EXPECT_EQ(0U, pool->PooledBytes());

    resume.set_value();
    returning.join();
    EXPECT_EQ(0U, pool->PooledBytes());
}

TEST(DownloadBufferPoolTests, OversizedBuffersAreNotRetained) {
    const auto pool = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool>();
    {
        const auto lease =
            pool->Acquire(AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool::MaxPooledBufferBytes + 1);
    }

    EXPECT_EQ(0U, pool->PooledCount());
}
