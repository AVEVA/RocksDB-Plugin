// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"

#include "PageBlobClientMock.hpp"
#include "FakeHttpPump.hpp"
#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <random>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using AVEVA::HttpResponse;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::PageBlob;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::PageBlobClientMock;

namespace {
class PageBlobTests : public ::testing::Test {
  protected:
    void SetUp() override {
        // Completions are posted to the fake's own io_context while the blob blocks on a future.
        m_pump = AVEVA::RocksDB::Plugin::Azure::Impl::Tests::StartFakeHttpPump(m_httpClient);
        m_blob = std::make_unique<PageBlob>(
            std::make_shared<ClientRuntime>(m_context),
            AVEVA::AzureClient::PageBlobClient(m_httpClient, MakeBlobClientOptions("files", "000001.sst")));
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    std::jthread m_pump;
    std::unique_ptr<PageBlob> m_blob;
};
} // namespace

TEST_F(PageBlobTests, DownloadToFileWithZeroLengthCreatesEmptyFileWithoutRequest) {
    const auto path = std::filesystem::temp_directory_path() / ("aveva-pageblob-empty-test-" + std::to_string(std::random_device{}()) + ".bin");
    {
        std::ofstream stale(path, std::ios::binary);
        stale << "stale contents";
    }

    m_blob->DownloadTo(path.string(), 0, 0);

    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(0U, std::filesystem::file_size(path));
    EXPECT_TRUE(m_httpClient.NoRequestMade());
    std::filesystem::remove(path);
}

TEST_F(PageBlobTests, DownloadLargerThanTheBufferFailsWithoutWritingPastIt) {
    m_httpClient.EnqueueResponse(
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-15/16"}, {"Content-Length", "16"}}),
                     std::string(16, 'x')});

    std::vector<char> storage(16, '#');
    EXPECT_ANY_THROW((void)m_blob->DownloadTo(std::span<char>(storage).first(8), 0, 16));
    EXPECT_EQ(std::string(8, '#'), std::string(storage.begin() + 8, storage.end()));
}

TEST_F(PageBlobTests, DownloadToBufferAcceptsResponseWithoutContentRange) {
    m_httpClient.EnqueueResponse(HttpResponse{206, MakeCanonicalSuccessHeaders({}), std::string(4, 'y')});

    std::vector<char> buffer(8);
    EXPECT_EQ(4, m_blob->DownloadTo(std::span<char>(buffer), 0, 4));
}

TEST_F(PageBlobTests, BlockingFromTheIoContextThreadThrowsInsteadOfDeadlocking) {
    std::promise<bool> threw;
    boost::asio::post(m_httpClient.get_executor(), [&] {
        try {
            std::vector<char> buffer(8);
            m_blob->DownloadTo(std::span<char>(buffer), 0, 8);
            threw.set_value(false);
        } catch (const std::logic_error&) {
            threw.set_value(true);
        }
    });

    auto result = threw.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(10)));
    EXPECT_TRUE(result.get());
}

TEST_F(PageBlobTests, DownloadToBufferWithZeroLengthReturnsZeroWithoutRequest) {
    std::vector<char> buffer(8);
    EXPECT_EQ(0, m_blob->DownloadTo(std::span<char>(buffer), 0, 0));
    EXPECT_TRUE(m_httpClient.NoRequestMade());
}

namespace {
class PageBlobMockedClientTests : public ::testing::Test {
  protected:
    void SetUp() override {
        auto mock = std::make_unique<::testing::StrictMock<PageBlobClientMock>>(
            m_httpClient, MakeBlobClientOptions("files", "000001.sst"));
        m_mock = mock.get();
        m_blob = std::make_unique<PageBlob>(std::make_shared<ClientRuntime>(m_context), std::move(mock));
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    ::testing::StrictMock<PageBlobClientMock>* m_mock = nullptr;
    std::unique_ptr<PageBlob> m_blob;
};
} // namespace

TEST_F(PageBlobMockedClientTests, GetEtagReadsThePropertiesReturnedByTheMock) {
    EXPECT_CALL(*m_mock, GetPropertiesAsyncImpl)
        .WillOnce([](const auto&, auto completion, auto) {
            AVEVA::AzureClient::Models::BlobProperties properties;
            properties.ETag = "\"mocked\"";
            completion(AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::BlobProperties>(
                std::move(properties), HttpResponse{200, {}, {}}));
        });

    EXPECT_EQ("\"mocked\"", m_blob->GetEtag());
    EXPECT_TRUE(m_httpClient.NoRequestMade());
}

TEST_F(PageBlobMockedClientTests, SetCapacityResizesTheBlobToTheRequestedSize) {
    EXPECT_CALL(*m_mock, ResizeAsyncImpl(1024U, ::testing::_, ::testing::_, ::testing::_))
        .WillOnce([](auto, const auto&, auto completion, auto) {
            completion(AVEVA::AzureClient::Response<AVEVA::AzureClient::Models::ResizePageBlobResult>(
                {}, HttpResponse{200, {}, {}}));
        });

    m_blob->SetCapacity(1024);
    EXPECT_TRUE(m_httpClient.NoRequestMade());
}

TEST_F(PageBlobMockedClientTests, StorageFailuresFromTheMockSurfaceAsExceptions) {
    EXPECT_CALL(*m_mock, GetPropertiesAsyncImpl)
        .WillOnce([](const auto&, auto completion, auto) {
            completion(std::unexpected(AVEVA::AzureClient::BlobStorageError{}));
        });

    EXPECT_ANY_THROW((void)m_blob->GetEtag());
}