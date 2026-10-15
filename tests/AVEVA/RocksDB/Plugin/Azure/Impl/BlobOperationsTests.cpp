// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeBlobEnvironment.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

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

TEST_F(BlobOperationsTests, DownloadToBufferWithZeroLengthReturnsZeroWithoutRequest) {
    std::vector<char> buffer(8);
    EXPECT_EQ(0, BlobOperations::DownloadToBuffer(*m_blob, std::span<char>(buffer), 0, 0));
    EXPECT_TRUE(m_httpClient.NoRequestMade());
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

    EXPECT_EQ("\"fake\"", BlobOperations::GetEtag(*m_env.Client));
}

TEST_F(BlobOperationsFakeBlobTests, SetCapacityResizesTheBlobToTheRequestedSize) {
    BlobOperations::SetCapacity(*m_env.Runtime, *m_env.Client, 1024);

    EXPECT_EQ((std::vector<int64_t>{1024}), m_env.Blob->CapacityWrites);
    EXPECT_EQ(1024, BlobOperations::GetCapacity(*m_env.Client));
}

TEST_F(BlobOperationsFakeBlobTests, SetSizeRecordsTheSizeInTheBlobMetadata) {
    BlobOperations::SetSize(*m_env.Runtime, *m_env.Client, 77);

    EXPECT_EQ(77, BlobOperations::GetSize(*m_env.Client));
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

    EXPECT_THROW((void)BlobOperations::GetEtag(*m_env.Client), RequestFailedException);
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

TEST(DownloadBufferPoolTests, OversizedBuffersAreNotRetained) {
    const auto pool = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool>();
    {
        const auto lease =
            pool->Acquire(AVEVA::RocksDB::Plugin::Azure::Impl::DownloadBufferPool::MaxPooledBufferBytes + 1);
    }

    EXPECT_EQ(0U, pool->PooledCount());
}
