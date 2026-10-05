// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"

#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <boost/asio/io_context.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
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

namespace {
class PageBlobTests : public ::testing::Test {
  protected:
    void SetUp() override {
        // Completions are posted to the fake's own io_context while the blob blocks on a future.
        m_pump = std::jthread([this](const std::stop_token& stop) {
            while (!stop.stop_requested()) {
                m_httpClient.Poll();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
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
    const auto path = std::filesystem::temp_directory_path() / "aveva-pageblob-empty-test.bin";
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

TEST_F(PageBlobTests, DownloadToBufferReturnsBytesCopiedNotRangeLength) {
    m_httpClient.EnqueueResponse(
        HttpResponse{206, MakeCanonicalSuccessHeaders({{"Content-Range", "bytes 0-15/16"}, {"Content-Length", "16"}}),
                     std::string(16, 'x')});

    std::vector<char> buffer(8);
    const auto bytes = m_blob->DownloadTo(std::span<char>(buffer), 0, 16);

    EXPECT_EQ(8, bytes);
    EXPECT_EQ(std::string(8, 'x'), std::string(buffer.begin(), buffer.end()));
}

TEST_F(PageBlobTests, DownloadToBufferWithZeroLengthReturnsZeroWithoutRequest) {
    std::vector<char> buffer(8);
    EXPECT_EQ(0, m_blob->DownloadTo(std::span<char>(buffer), 0, 0));
    EXPECT_TRUE(m_httpClient.NoRequestMade());
}