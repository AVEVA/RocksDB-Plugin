// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeBlobEnvironment.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakePageBlobClient;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;

class ReadableFileTests : public ::testing::Test {
  protected:
    static const constexpr int64_t DefaultBlobSize = Configuration::PageBlob::PageSize * 2;
    FakeBlobEnvironment m_env;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger =
        std::make_shared<severity_logger_mt<severity_level>>();

    ReadableFileImpl MakeFile() { return ReadableFileImpl{"test.sst", m_env.Runtime, m_env.Blob, nullptr, m_logger}; }
};

TEST_F(ReadableFileTests, Constructor_InitializesWithBlobSize) {
    constexpr int64_t expectedSize = Configuration::PageBlob::PageSize;
    m_env.Fill(expectedSize);

    const auto file = MakeFile();

    EXPECT_EQ(expectedSize, file.GetSize());
}

TEST_F(ReadableFileTests, RandomRead_WithoutCache_ReadsFromBlob) {
    constexpr int64_t offset = 50;
    constexpr int64_t bytesToRead = 100;
    m_env.Fill(DefaultBlobSize, 'B');
    std::vector<char> buffer(bytesToRead);
    const auto file = MakeFile();

    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    EXPECT_EQ(bytesToRead, bytesRead);
    EXPECT_EQ(std::vector<char>(bytesToRead, 'B'), buffer);
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(offset, m_env.Blob->Downloads[0].Offset);
    EXPECT_EQ(bytesToRead, m_env.Blob->Downloads[0].Length);
}

TEST_F(ReadableFileTests, RandomRead_RequestMoreThanAvailable_ReadsOnlyAvailableBytes) {
    constexpr int64_t blobSize = 200;
    constexpr int64_t offset = 150;
    constexpr int64_t bytesToRead = 100;
    constexpr int64_t expectedBytes = 50; // Only 50 bytes available from offset 150 in a 200 byte blob
    m_env.Fill(blobSize, 'C');
    std::vector<char> buffer(bytesToRead);
    const auto file = MakeFile();

    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    EXPECT_EQ(expectedBytes, bytesRead);
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(expectedBytes, m_env.Blob->Downloads[0].Length);
}

TEST_F(ReadableFileTests, RandomRead_AtEndOfFile_ReturnsZero) {
    constexpr int64_t blobSize = 100;
    m_env.Fill(blobSize);
    std::vector<char> buffer(50);
    const auto file = MakeFile();

    const auto bytesRead = file.RandomRead(blobSize, 50, buffer.data());

    EXPECT_EQ(0, bytesRead);
    EXPECT_TRUE(m_env.Blob->Downloads.empty());
}

TEST_F(ReadableFileTests, GetSize_ReturnsCorrectSize) {
    constexpr int64_t expectedSize = 5000;
    m_env.Fill(expectedSize);

    const auto file = MakeFile();

    EXPECT_EQ(expectedSize, file.GetSize());
}

TEST_F(ReadableFileTests, RandomRead_DownloadFails_PropagatesTheError) {
    m_env.Fill(DefaultBlobSize);
    m_env.Blob->Fault =
        [](const FakePageBlobClient::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakePageBlobClient::Operation::DownloadToBuffer) {
            return FakePageBlobClient::Error(500, "InternalError");
        }
        return std::nullopt;
    };
    const auto file = MakeFile();
    std::vector<char> buffer(100);

    EXPECT_THROW([[maybe_unused]] auto n = file.RandomRead(50, 100, buffer.data()), RequestFailedException);
}

TEST_F(ReadableFileTests, RandomRead_EmptyBlob_ReturnsZero) {
    m_env.Fill(0);
    std::vector<char> buffer(100);
    const auto file = MakeFile();

    const auto bytesRead = file.RandomRead(0, 100, buffer.data());

    EXPECT_EQ(0, bytesRead);
    EXPECT_TRUE(m_env.Blob->Downloads.empty());
}

TEST_F(ReadableFileTests, Constructor_FetchesSizeAndEtagWithOneMetadataCall) {
    m_env.Fill(1024);

    const auto file = MakeFile();

    EXPECT_EQ(1, m_env.Blob->PropertiesRequests);
    EXPECT_TRUE(file.HasETag(m_env.Blob->ETag));
}

TEST_F(ReadableFileTests, GetSize_RefreshesWithOneMetadataCall) {
    m_env.Fill(1024);
    const auto file = MakeFile();
    m_env.Blob->Size = 2048;

    EXPECT_EQ(2048, file.GetSize());
    EXPECT_EQ(2, m_env.Blob->PropertiesRequests);
}

TEST_F(ReadableFileTests, RandomRead_GivesUpWhenBlobKeepsChanging) {
    m_env.Fill(1024);
    m_env.Blob->Fault =
        [](const FakePageBlobClient::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakePageBlobClient::Operation::DownloadToBuffer) {
            return FakePageBlobClient::Error(412, "ConditionNotMet");
        }
        return std::nullopt;
    };
    const auto file = MakeFile();
    std::vector<char> buffer(16);

    EXPECT_THROW([[maybe_unused]] auto n = file.RandomRead(0, 16, buffer.data()), RequestFailedException);
}
