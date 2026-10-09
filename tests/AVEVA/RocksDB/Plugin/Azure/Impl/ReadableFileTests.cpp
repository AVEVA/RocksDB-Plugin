// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;
using ::testing::_;

using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArrayArgument;

class ReadableFileTests : public ::testing::Test {
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

TEST_F(ReadableFileTests, Constructor_InitializesWithBlobSize) {
    // Arrange
    static constexpr uint64_t expectedSize = Configuration::PageBlob::PageSize;

    ON_CALL(*m_blobClient, GetEtag()).WillByDefault(Return(std::string{"etag"}));

    EXPECT_CALL(*m_blobClient, GetSize()).WillRepeatedly(Return(expectedSize));

    // Act
    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    // Assert
    EXPECT_EQ(static_cast<int64_t>(expectedSize), file.GetSize());
}

TEST_F(ReadableFileTests, RandomRead_WithoutCache_ReadsFromBlob) {
    // Arrange
    constexpr int64_t offset = 50;
    constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);
    std::vector<char> expectedData(bytesToRead, 'B');

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), offset, bytesToRead, ::testing::_))
        .WillOnce([&expectedData](std::span<char> downloadBuffer, int64_t /*offset*/, int64_t /*length*/,
                                  const std::string& /*ifMatch*/) {
            std::copy(expectedData.begin(), expectedData.end(), downloadBuffer.begin());
            return static_cast<int64_t>(expectedData.size());
        });

    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    // Act
    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(bytesToRead, bytesRead);
    EXPECT_EQ(expectedData, buffer);
}

TEST_F(ReadableFileTests, RandomRead_RequestMoreThanAvailable_ReadsOnlyAvailableBytes) {
    // Arrange
    constexpr uint64_t blobSize = 200;
    constexpr int64_t offset = 150;
    constexpr int64_t bytesToRead = 100;
    constexpr int64_t expectedBytes = 50; // Only 50 bytes available from offset 150 in a 200 byte blob
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, GetSize()).WillOnce(Return(blobSize));

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), offset, expectedBytes, ::testing::_))
        .WillOnce([expectedBytes](std::span<char> downloadBuffer, int64_t /*offset*/, int64_t /*length*/,
                                  const std::string& /*ifMatch*/) {
            std::fill_n(downloadBuffer.begin(), expectedBytes, 'C');
            return static_cast<int64_t>(expectedBytes);
        });

    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    // Act
    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(expectedBytes, bytesRead);
}

TEST_F(ReadableFileTests, RandomRead_AtEndOfFile_ReturnsZero) {
    // Arrange
    constexpr uint64_t blobSize = 100;
    std::vector<char> buffer(50);

    EXPECT_CALL(*m_blobClient, GetSize()).WillOnce(Return(blobSize));

    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    // Act
    const auto bytesRead = file.RandomRead(static_cast<int64_t>(blobSize), 50, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, GetSize_ReturnsCorrectSize) {
    // Arrange
    constexpr uint64_t expectedSize = 5000;

    EXPECT_CALL(*m_blobClient, GetSize()).WillRepeatedly(Return(expectedSize));
    ON_CALL(*m_blobClient, GetEtag()).WillByDefault(Return(std::string{"etag"}));

    // Act
    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    // Assert
    EXPECT_EQ(static_cast<int64_t>(expectedSize), file.GetSize());
}

TEST_F(ReadableFileTests, RandomRead_DownloadReturnsNegative_ReturnsZero) {
    // Arrange
    constexpr int64_t offset = 50;
    constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), offset, bytesToRead, ::testing::_))
        .WillOnce(Return(-1)); // Simulate error

    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    // Act
    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, RandomRead_EmptyBlob_ReturnsZero) {
    // Arrange
    constexpr uint64_t blobSize = 0;
    std::vector<char> buffer(100);

    EXPECT_CALL(*m_blobClient, GetSize()).WillOnce(Return(blobSize));

    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    // Act
    const auto bytesRead = file.RandomRead(0, 100, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, Constructor_FetchesSizeAndEtagWithOneMetadataCall) {
    using AVEVA::RocksDB::Plugin::Core::BlobMetadata;
    EXPECT_CALL(*m_blobClient, GetMetadata()).Times(1).WillOnce(Return(BlobMetadata{1024, "etag-1"}));
    EXPECT_CALL(*m_blobClient, GetSize()).Times(0);
    EXPECT_CALL(*m_blobClient, GetEtag()).Times(0);

    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

}

TEST_F(ReadableFileTests, GetSize_RefreshesWithOneMetadataCall) {
    using AVEVA::RocksDB::Plugin::Core::BlobMetadata;
    EXPECT_CALL(*m_blobClient, GetMetadata())
        .Times(2)
        .WillOnce(Return(BlobMetadata{1024, "etag-1"}))
        .WillOnce(Return(BlobMetadata{2048, "etag-2"}));
    EXPECT_CALL(*m_blobClient, GetSize()).Times(0);
    EXPECT_CALL(*m_blobClient, GetEtag()).Times(0);

    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};

    EXPECT_EQ(2048, file.GetSize());
}
TEST_F(ReadableFileTests, RandomRead_GivesUpWhenBlobKeepsChanging) {
    using AVEVA::RocksDB::Plugin::Core::BlobMetadata;
    ON_CALL(*m_blobClient, GetMetadata()).WillByDefault(Return(BlobMetadata{1024, "etag"}));
    ON_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), _, _, _))
        .WillByDefault(::testing::Throw(RequestFailedException(412, "ConditionNotMet", "changed", "", {})));
    ReadableFileImpl file{"test.sst", m_blobClient, nullptr, m_logger};
    std::vector<char> buffer(16);

    EXPECT_THROW([[maybe_unused]] auto n = file.RandomRead(0, 16, buffer.data()), RequestFailedException);
}
