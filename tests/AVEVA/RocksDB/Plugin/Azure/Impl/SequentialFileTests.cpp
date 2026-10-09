// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeBlobEnvironment.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::SequentialFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakePageBlobClient;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;

class SequentialFileTests : public ::testing::Test {
  protected:
    static const constexpr int64_t DefaultBlobSize = Configuration::PageBlob::PageSize * 2;
    FakeBlobEnvironment m_env;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger =
        std::make_shared<severity_logger_mt<severity_level>>();

    void SetUp() override { m_env.Fill(DefaultBlobSize); }

    ReadableFileImpl MakeReadable() {
        return ReadableFileImpl{"test.sst", m_env.Runtime, m_env.Blob, nullptr, m_logger};
    }
    SequentialFileImpl MakeFile() { return SequentialFileImpl{MakeReadable()}; }
};

TEST_F(SequentialFileTests, SequentialRead_WithoutCache_ReadsFromBlob) {
    static const constexpr int64_t bytesToRead = 100;
    m_env.Fill(DefaultBlobSize, 'A');
    std::vector<char> buffer(bytesToRead);
    auto file = MakeFile();

    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    EXPECT_EQ(bytesToRead, bytesRead);
    EXPECT_EQ(std::vector<char>(bytesToRead, 'A'), buffer);
    EXPECT_EQ(bytesToRead, file.GetOffset());
    // The read is served from a readahead block that covers the whole (small) blob.
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(0, m_env.Blob->Downloads[0].Offset);
    EXPECT_EQ(DefaultBlobSize, m_env.Blob->Downloads[0].Length);
}

TEST_F(SequentialFileTests, SequentialRead_MultipleReads_IncrementsOffset) {
    const constexpr int64_t firstRead = 50;
    const constexpr int64_t secondRead = 75;
    std::ranges::fill(m_env.Blob->Data, 'Y');
    std::fill_n(m_env.Blob->Data.begin(), firstRead, 'X');
    std::vector<char> buffer1(firstRead);
    std::vector<char> buffer2(secondRead);
    auto file = MakeFile();

    const auto bytesRead1 = file.SequentialRead(firstRead, buffer1.data());
    const auto bytesRead2 = file.SequentialRead(secondRead, buffer2.data());

    EXPECT_EQ(firstRead, bytesRead1);
    EXPECT_EQ(secondRead, bytesRead2);
    EXPECT_EQ(std::vector<char>(firstRead, 'X'), buffer1);
    EXPECT_EQ(std::vector<char>(secondRead, 'Y'), buffer2);
    EXPECT_EQ(firstRead + secondRead, file.GetOffset());
    EXPECT_EQ(1U, m_env.Blob->Downloads.size());
}

TEST_F(SequentialFileTests, SequentialRead_SmallReads_ShareOneReadaheadAndRefillAcrossItsEnd) {
    constexpr int64_t megabyte = 1024 * 1024;
    constexpr int64_t blobSize = 2 * megabyte + megabyte / 2;
    constexpr int64_t readSize = 768 * 1024;
    const auto patternAt = [](int64_t position) { return static_cast<char>(position % 251); };
    m_env.Fill(blobSize);
    for (int64_t i = 0; i < blobSize; ++i) {
        m_env.Blob->Data[static_cast<size_t>(i)] = patternAt(i);
    }
    auto file = MakeFile();

    // 0-768K comes from the first block, 768K-1536K straddles it and the second, then the tail is short.
    std::vector<char> buffer(readSize);
    int64_t position = 0;
    for (int i = 0; i < 4; ++i) {
        const auto bytesRead = file.SequentialRead(readSize, buffer.data());
        ASSERT_EQ(std::min(readSize, blobSize - position), bytesRead);
        for (int64_t j = 0; j < bytesRead; ++j) {
            ASSERT_EQ(patternAt(position + j), buffer[static_cast<size_t>(j)]) << "at " << position + j;
        }
        position += bytesRead;
    }

    // 2.5 MB is read in three GETs instead of four.
    std::vector<int64_t> downloads;
    for (const auto& download : m_env.Blob->Downloads) {
        downloads.push_back(download.Offset);
    }
    EXPECT_EQ((std::vector<int64_t>{0, megabyte, 2 * megabyte}), downloads);
    EXPECT_EQ(blobSize, file.GetOffset());
}

TEST_F(SequentialFileTests, HasETag_ComparesTheCachedETagWithoutCopying) {
    m_env.Blob->ETag = "etag-1";
    const auto file = MakeReadable();

    EXPECT_TRUE(file.HasETag("etag-1"));
    EXPECT_FALSE(file.HasETag("etag-2"));
    EXPECT_FALSE(file.HasETag(""));
}

TEST_F(SequentialFileTests, SequentialRead_LargeReadBypassesReadahead) {
    constexpr int64_t megabyte = 1024 * 1024;
    m_env.Fill(4 * megabyte);
    std::vector<char> buffer(2 * megabyte);
    auto file = MakeFile();

    const auto bytesRead = file.SequentialRead(2 * megabyte, buffer.data());

    EXPECT_EQ(2 * megabyte, bytesRead);
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(0, m_env.Blob->Downloads[0].Offset);
    EXPECT_EQ(2 * megabyte, m_env.Blob->Downloads[0].Length);
}

TEST_F(SequentialFileTests, SequentialRead_RequestMoreThanAvailable_ReadsOnlyAvailableBytes) {
    constexpr int64_t blobSize = 100;
    constexpr int64_t bytesToRead = 150;
    m_env.Fill(blobSize);
    std::vector<char> buffer(bytesToRead);
    auto file = MakeFile();

    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    EXPECT_EQ(blobSize, bytesRead);
    EXPECT_EQ(blobSize, file.GetOffset());
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(blobSize, m_env.Blob->Downloads[0].Length);
}

TEST_F(SequentialFileTests, SequentialRead_AtEndOfFile_ReturnsZero) {
    constexpr int64_t blobSize = 100;
    m_env.Fill(blobSize);
    std::vector<char> buffer(50);
    auto file = MakeFile();
    file.Skip(blobSize); // Move to end of file

    const auto bytesRead = file.SequentialRead(50, buffer.data());

    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(blobSize, file.GetOffset());
}

TEST_F(SequentialFileTests, Skip_IncrementsOffset) {
    constexpr int64_t skipAmount = 100;
    auto file = MakeFile();

    file.Skip(skipAmount);

    EXPECT_EQ(skipAmount, file.GetOffset());
}

TEST_F(SequentialFileTests, Skip_Multiple_AccumulatesOffset) {
    constexpr int64_t skip1 = 50;
    constexpr int64_t skip2 = 75;
    constexpr int64_t skip3 = 25;
    auto file = MakeFile();

    file.Skip(skip1);
    file.Skip(skip2);
    file.Skip(skip3);

    EXPECT_EQ(skip1 + skip2 + skip3, file.GetOffset());
}

TEST_F(SequentialFileTests, GetOffset_InitiallyZero) {
    const auto file = MakeFile();

    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(SequentialFileTests, SequentialRead_DownloadFails_DoesNotAdvanceOffset) {
    constexpr int64_t bytesToRead = 100;
    m_env.Blob->Fault =
        [](const FakePageBlobClient::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakePageBlobClient::Operation::DownloadToBuffer) {
            return FakePageBlobClient::Error(500, "InternalError");
        }
        return std::nullopt;
    };
    std::vector<char> buffer(bytesToRead);
    auto file = MakeFile();

    EXPECT_THROW([[maybe_unused]] auto n = file.SequentialRead(bytesToRead, buffer.data()), RequestFailedException);
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(SequentialFileTests, SequentialRead_EmptyBlob_ReturnsZero) {
    m_env.Fill(0);
    std::vector<char> buffer(100);
    auto file = MakeFile();

    const auto bytesRead = file.SequentialRead(100, buffer.data());

    EXPECT_EQ(0, bytesRead);
}

TEST_F(SequentialFileTests, SequentialRead_InterleavedWithSkip_MaintainsCorrectOffset) {
    constexpr int64_t readSize = 50;
    constexpr int64_t skipAmount = 25;
    std::vector<char> buffer(readSize);
    auto file = MakeFile();

    [[maybe_unused]] const auto bytesRead1 = file.SequentialRead(readSize, buffer.data());
    file.Skip(skipAmount);
    [[maybe_unused]] const auto bytesRead2 = file.SequentialRead(readSize, buffer.data());

    EXPECT_EQ(readSize + skipAmount + readSize, file.GetOffset());
}
