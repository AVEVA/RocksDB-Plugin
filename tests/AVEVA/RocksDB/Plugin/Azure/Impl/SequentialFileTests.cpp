// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::SequentialFileImpl;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;
using ::testing::_;

using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArrayArgument;

class SequentialFileTests : public ::testing::Test {
  protected:
    std::shared_ptr<BlobClientMock> m_blobClient;
    static const constexpr uint64_t DefaultBlobSize = Configuration::PageBlob::PageSize * 2;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger;

    void TearDown() override { ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_blobClient.get())); }

    void SetUp() override {
        m_blobClient = std::make_shared<BlobClientMock>();

        // Default behavior: return a blob size
        ON_CALL(*m_blobClient, GetSize()).WillByDefault(Return(DefaultBlobSize));

        m_logger = std::make_shared<severity_logger_mt<severity_level>>();
    }
};

TEST_F(SequentialFileTests, SequentialRead_WithoutCache_ReadsFromBlob) {
    // Arrange
    static const constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);

    // The read is served from a readahead block that covers the whole (small) blob.
    EXPECT_CALL(*m_blobClient,
                Download(::testing::A<std::span<char>>(), 0, static_cast<int64_t>(DefaultBlobSize), ::testing::_))
        .WillOnce([](std::span<char> downloadBuffer, int64_t /*offset*/, int64_t /*length*/,
                     const std::string& /*ifMatch*/) {
            std::ranges::fill(downloadBuffer, 'A');
            return static_cast<int64_t>(downloadBuffer.size());
        });

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(bytesToRead, bytesRead);
    EXPECT_EQ(std::vector<char>(bytesToRead, 'A'), buffer);
    EXPECT_EQ(bytesToRead, file.GetOffset());
}

TEST_F(SequentialFileTests, SequentialRead_MultipleReads_IncrementsOffset) {
    // Arrange
    const constexpr int64_t firstRead = 50;
    const constexpr int64_t secondRead = 75;
    std::vector<char> buffer1(firstRead);
    std::vector<char> buffer2(secondRead);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, ::testing::_, ::testing::_))
        .WillOnce([](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/, const std::string& /*ifMatch*/) {
            std::fill_n(buffer.begin(), firstRead, 'X');
            std::fill(buffer.begin() + firstRead, buffer.end(), 'Y');
            return static_cast<int64_t>(buffer.size());
        });

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    const auto bytesRead1 = file.SequentialRead(firstRead, buffer1.data());
    const auto bytesRead2 = file.SequentialRead(secondRead, buffer2.data());

    // Assert
    EXPECT_EQ(firstRead, bytesRead1);
    EXPECT_EQ(secondRead, bytesRead2);
    EXPECT_EQ(std::vector<char>(firstRead, 'X'), buffer1);
    EXPECT_EQ(std::vector<char>(secondRead, 'Y'), buffer2);
    EXPECT_EQ(firstRead + secondRead, file.GetOffset());
}

TEST_F(SequentialFileTests, SequentialRead_SmallReads_ShareOneReadaheadAndRefillAcrossItsEnd) {
    // Arrange
    constexpr int64_t megabyte = 1024 * 1024;
    constexpr int64_t blobSize = 2 * megabyte + megabyte / 2;
    constexpr int64_t readSize = 768 * 1024;
    const auto patternAt = [](int64_t position) { return static_cast<char>(position % 251); };
    ON_CALL(*m_blobClient, GetSize()).WillByDefault(Return(blobSize));
    ON_CALL(*m_blobClient, GetEtag()).WillByDefault(Return(std::string{"etag"}));

    std::vector<int64_t> downloads;
    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), ::testing::_, ::testing::_, ::testing::_))
        .WillRepeatedly([&](std::span<char> buffer, int64_t offset, int64_t /*length*/,
                            const std::string& /*ifMatch*/) {
            downloads.push_back(offset);
            for (size_t i = 0; i < buffer.size(); ++i) {
                buffer[i] = patternAt(offset + static_cast<int64_t>(i));
            }
            return static_cast<int64_t>(buffer.size());
        });

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act: 0-768K comes from the first block, 768K-1536K straddles it and the second, then the tail is short.
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

    // Assert: 2.5 MB is read in three GETs instead of four.
    EXPECT_EQ((std::vector<int64_t>{0, megabyte, 2 * megabyte}), downloads);
    EXPECT_EQ(blobSize, file.GetOffset());
}

TEST_F(SequentialFileTests, HasETag_ComparesTheCachedETagWithoutCopying) {
    ON_CALL(*m_blobClient, GetEtag()).WillByDefault(Return(std::string{"etag-1"}));
    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    EXPECT_TRUE(file.HasETag("etag-1"));
    EXPECT_FALSE(file.HasETag("etag-2"));
    EXPECT_FALSE(file.HasETag(""));
}

TEST_F(SequentialFileTests, SequentialRead_LargeReadBypassesReadahead) {    // Arrange
    constexpr int64_t megabyte = 1024 * 1024;
    constexpr int64_t blobSize = 4 * megabyte;
    ON_CALL(*m_blobClient, GetSize()).WillByDefault(Return(blobSize));
    ON_CALL(*m_blobClient, GetEtag()).WillByDefault(Return(std::string{"etag"}));
    std::vector<char> buffer(2 * megabyte);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, 2 * megabyte, ::testing::_))
        .WillOnce(Return(2 * megabyte));

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    const auto bytesRead = file.SequentialRead(2 * megabyte, buffer.data());

    // Assert
    EXPECT_EQ(2 * megabyte, bytesRead);
}

TEST_F(SequentialFileTests, SequentialRead_RequestMoreThanAvailable_ReadsOnlyAvailableBytes) {
    // Arrange
    constexpr uint64_t blobSize = 100;
    constexpr int64_t bytesToRead = 150;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, GetSize()).WillOnce(Return(blobSize));

    EXPECT_CALL(*m_blobClient,
                Download(::testing::A<std::span<char>>(), 0, static_cast<int64_t>(blobSize), ::testing::_))
        .WillOnce([blobSize](std::span<char> /*buffer*/, int64_t /*offset*/, int64_t /*length*/,
                             const std::string& /*ifMatch*/) { return static_cast<int64_t>(blobSize); });

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(static_cast<int64_t>(blobSize), bytesRead);
    EXPECT_EQ(static_cast<int64_t>(blobSize), file.GetOffset());
}

TEST_F(SequentialFileTests, SequentialRead_AtEndOfFile_ReturnsZero) {
    // Arrange
    constexpr uint64_t blobSize = 100;
    std::vector<char> buffer(50);

    EXPECT_CALL(*m_blobClient, GetSize()).WillOnce(Return(blobSize));

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};
    file.Skip(static_cast<int64_t>(blobSize)); // Move to end of file

    // Act
    const auto bytesRead = file.SequentialRead(50, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(static_cast<int64_t>(blobSize), file.GetOffset());
}

TEST_F(SequentialFileTests, Skip_IncrementsOffset) {
    // Arrange
    constexpr int64_t skipAmount = 100;
    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    file.Skip(skipAmount);

    // Assert
    EXPECT_EQ(skipAmount, file.GetOffset());
}

TEST_F(SequentialFileTests, Skip_Multiple_AccumulatesOffset) {
    // Arrange
    constexpr int64_t skip1 = 50;
    constexpr int64_t skip2 = 75;
    constexpr int64_t skip3 = 25;
    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    file.Skip(skip1);
    file.Skip(skip2);
    file.Skip(skip3);

    // Assert
    EXPECT_EQ(skip1 + skip2 + skip3, file.GetOffset());
}

TEST_F(SequentialFileTests, GetOffset_InitiallyZero) {
    // Arrange & Act
    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Assert
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(SequentialFileTests, SequentialRead_DownloadReturnsNegative_ReturnsZero) {
    // Arrange
    constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, ::testing::_, ::testing::_))
        .WillOnce(Return(-1)); // Simulate error

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(0, file.GetOffset()); // Offset should not advance on error
}

TEST_F(SequentialFileTests, SequentialRead_EmptyBlob_ReturnsZero) {
    // Arrange
    constexpr uint64_t blobSize = 0;
    std::vector<char> buffer(100);

    EXPECT_CALL(*m_blobClient, GetSize()).WillOnce(Return(blobSize));

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    const auto bytesRead = file.SequentialRead(100, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(SequentialFileTests, SequentialRead_InterleavedWithSkip_MaintainsCorrectOffset) {
    // Arrange
    constexpr int64_t readSize = 50;
    constexpr int64_t skipAmount = 25;
    std::vector<char> buffer(readSize);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, ::testing::_, ::testing::_))
        .WillOnce(Return(static_cast<int64_t>(DefaultBlobSize)));

    SequentialFileImpl file{ReadableFileImpl{"test.sst", m_blobClient, nullptr, m_logger}};

    // Act
    [[maybe_unused]] const auto bytesRead1 = file.SequentialRead(readSize, buffer.data());
    file.Skip(skipAmount);
    [[maybe_unused]] const auto bytesRead2 = file.SequentialRead(readSize, buffer.data());

    // Assert
    EXPECT_EQ(readSize + skipAmount + readSize, file.GetOffset());
}

