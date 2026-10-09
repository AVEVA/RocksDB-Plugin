// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeBlobEnvironment.hpp"
#include "FakeHttpClient.hpp"
#include "FakeHttpPump.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/AzureClient/PageBlobClient.hpp>

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
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakePageBlobClient;

namespace {
// Runs the real PageBlobClient against scripted HTTP responses.
class BlobOperationsTests : public ::testing::Test {
  protected:
    void SetUp() override {
        // Completions are posted to the fake's own io_context while the operation blocks on a future.
        m_pump = AVEVA::RocksDB::Plugin::Azure::Impl::Tests::StartFakeHttpPump(m_httpClient);
        m_blob = std::make_unique<AVEVA::AzureClient::PageBlobClient>(m_httpClient,
                                                                      MakeBlobClientOptions("files", "000001.sst"));
    }

    FakeHttpClient m_httpClient;
    std::jthread m_pump;
    std::unique_ptr<AVEVA::AzureClient::PageBlobClient> m_blob;
};
} // namespace

TEST_F(BlobOperationsTests, DownloadToFileWithZeroLengthCreatesEmptyFileWithoutRequest) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("aveva-blobops-empty-test-" + std::to_string(std::random_device{}()) + ".bin");
    {
        std::ofstream stale(path, std::ios::binary);
        stale << "stale contents";
    }

    BlobOperations::DownloadToFile(*m_blob, path.string(), 0, 0);

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

TEST_F(BlobOperationsTests, DownloadToBufferAcceptsResponseWithoutContentRange) {
    m_httpClient.EnqueueResponse(HttpResponse{206, MakeCanonicalSuccessHeaders({}), std::string(4, 'y')});

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

    EXPECT_EQ("\"fake\"", BlobOperations::GetEtag(*m_env.Blob));
    EXPECT_TRUE(m_env.Http.NoRequestMade());
}

TEST_F(BlobOperationsFakeBlobTests, SetCapacityResizesTheBlobToTheRequestedSize) {
    BlobOperations::SetCapacity(*m_env.Runtime, *m_env.Blob, 1024);

    EXPECT_EQ((std::vector<int64_t>{1024}), m_env.Blob->CapacityWrites);
    EXPECT_EQ(1024, BlobOperations::GetCapacity(*m_env.Blob));
    EXPECT_TRUE(m_env.Http.NoRequestMade());
}

TEST_F(BlobOperationsFakeBlobTests, SetSizeRecordsTheSizeInTheBlobMetadata) {
    BlobOperations::SetSize(*m_env.Runtime, *m_env.Blob, 77);

    EXPECT_EQ(77, BlobOperations::GetSize(*m_env.Blob));
    EXPECT_EQ((std::vector<int64_t>{77}), m_env.Blob->SizeWrites);
}

TEST_F(BlobOperationsFakeBlobTests, GetMetadataReturnsTheSizeAndETagFromOneRequest) {
    m_env.Fill(512);

    const auto metadata = BlobOperations::GetMetadata(*m_env.Blob);

    EXPECT_EQ(512, metadata.Size);
    EXPECT_EQ(m_env.Blob->ETag, metadata.ETag);
    EXPECT_EQ(1, m_env.Blob->PropertiesRequests);
}

TEST_F(BlobOperationsFakeBlobTests, StorageFailuresSurfaceAsExceptions) {
    m_env.Blob->Fault =
        [](const FakePageBlobClient::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakePageBlobClient::Operation::GetProperties) {
            return FakePageBlobClient::Error(500, "InternalError");
        }
        return std::nullopt;
    };

    EXPECT_THROW((void)BlobOperations::GetEtag(*m_env.Blob), RequestFailedException);
}

TEST_F(BlobOperationsFakeBlobTests, UploadPagesStoresTheBytesAtTheOffset) {
    const std::vector<char> page(512, 'u');

    BlobOperations::UploadPages(*m_env.Runtime, *m_env.Blob, page, 512);

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(512, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(page, m_env.Blob->Uploads[0].Data);
}
