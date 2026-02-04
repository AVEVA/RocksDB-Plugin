// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;
using ::testing::_;

using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArrayArgument;

class ReadableFileTests : public ::testing::Test
{
protected:
    std::shared_ptr<BlobClientMock> m_blobClient;
    static const constexpr uint64_t DefaultBlobSize = Configuration::PageBlob::PageSize * 2;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger;

    void TearDown() override
    {
        ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_blobClient.get()));
    }

    void SetUp() override
    {
        m_blobClient = std::make_shared<BlobClientMock>();

        // Default behavior: return a blob size
        ON_CALL(*m_blobClient, GetSize())
            .WillByDefault(Return(DefaultBlobSize));

        m_logger = std::make_shared<severity_logger_mt<severity_level>>();
    }
};

TEST_F(ReadableFileTests, Constructor_InitializesWithBlobSize)
{
    // Arrange
    static constexpr uint64_t expectedSize = Configuration::PageBlob::PageSize;
    
    ON_CALL(*m_blobClient, GetEtag())
        .WillByDefault(Return(::Azure::ETag{ "etag" }));

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(Return(expectedSize));

    // Act
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Assert
    EXPECT_EQ(static_cast<int64_t>(expectedSize), file.GetSize());
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(ReadableFileTests, SequentialRead_WithoutCache_ReadsFromBlob)
{
    // Arrange
    static const constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);
    std::vector<char> expectedData(bytesToRead, 'A');

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, bytesToRead, ::testing::_))
        .WillOnce([&expectedData](std::span<char> downloadBuffer, int64_t /*offset*/, int64_t /*length*/, const ::Azure::ETag& /*ifMatch*/)
            {
                std::copy(expectedData.begin(), expectedData.end(), downloadBuffer.begin());
                return static_cast<int64_t>(expectedData.size());
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(bytesToRead, bytesRead);
    EXPECT_EQ(expectedData, buffer);
    EXPECT_EQ(bytesToRead, file.GetOffset());
}

TEST_F(ReadableFileTests, SequentialRead_MultipleReads_IncrementsOffset)
{
    // Arrange
    const constexpr int64_t firstRead = 50;
    const constexpr int64_t secondRead = 75;
    std::vector<char> buffer1(firstRead);
    std::vector<char> buffer2(secondRead);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, firstRead, ::testing::_))
        .WillOnce([firstRead](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/, const ::Azure::ETag& /*ifMatch*/)
            {
                std::fill_n(buffer.begin(), firstRead, 'X');
                return static_cast<int64_t>(firstRead);
            });

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), firstRead, secondRead, ::testing::_))
        .WillOnce([secondRead](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/, const ::Azure::ETag& /*ifMatch*/)
            {
                std::fill_n(buffer.begin(), secondRead, 'Y');
                return static_cast<int64_t>(secondRead);
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead1 = file.SequentialRead(firstRead, buffer1.data());
    const auto bytesRead2 = file.SequentialRead(secondRead, buffer2.data());

    // Assert
    EXPECT_EQ(firstRead, bytesRead1);
    EXPECT_EQ(secondRead, bytesRead2);
    EXPECT_EQ(firstRead + secondRead, file.GetOffset());
}

TEST_F(ReadableFileTests, SequentialRead_RequestMoreThanAvailable_ReadsOnlyAvailableBytes)
{
    // Arrange
    constexpr uint64_t blobSize = 100;
    constexpr int64_t bytesToRead = 150;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, static_cast<int64_t>(blobSize), ::testing::_))
        .WillOnce([blobSize](std::span<char> /*buffer*/, int64_t /*offset*/, int64_t /*length*/, const ::Azure::ETag& /*ifMatch*/)
            {
                return static_cast<int64_t>(blobSize);
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(static_cast<int64_t>(blobSize), bytesRead);
    EXPECT_EQ(static_cast<int64_t>(blobSize), file.GetOffset());
}

TEST_F(ReadableFileTests, SequentialRead_AtEndOfFile_ReturnsZero)
{
    // Arrange
    constexpr uint64_t blobSize = 100;
    std::vector<char> buffer(50);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };
    file.Skip(static_cast<int64_t>(blobSize)); // Move to end of file

    // Act
    const auto bytesRead = file.SequentialRead(50, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(static_cast<int64_t>(blobSize), file.GetOffset());
}

TEST_F(ReadableFileTests, RandomRead_WithoutCache_ReadsFromBlob)
{
    // Arrange
    constexpr int64_t offset = 50;
    constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);
    std::vector<char> expectedData(bytesToRead, 'B');

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), offset, bytesToRead, ::testing::_))
        .WillOnce([&expectedData](std::span<char> downloadBuffer, int64_t /*offset*/, int64_t /*length*/, const ::Azure::ETag& /*ifMatch*/)
            {
                std::copy(expectedData.begin(), expectedData.end(), downloadBuffer.begin());
                return static_cast<int64_t>(expectedData.size());
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(bytesToRead, bytesRead);
    EXPECT_EQ(expectedData, buffer);
    EXPECT_EQ(0, file.GetOffset()); // Random read shouldn't affect sequential offset
}

TEST_F(ReadableFileTests, RandomRead_DoesNotAffectSequentialOffset)
{
    // Arrange
    constexpr int64_t sequentialBytes = 50;
    constexpr int64_t randomOffset = 200;
    constexpr int64_t randomBytes = 75;
    std::vector<char> seqBuffer(sequentialBytes);
    std::vector<char> randomBuffer(randomBytes);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, sequentialBytes, ::testing::_))
        .WillOnce(Return(static_cast<int64_t>(sequentialBytes)));

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), randomOffset, randomBytes, ::testing::_))
        .WillOnce(Return(static_cast<int64_t>(randomBytes)));

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), sequentialBytes, sequentialBytes, ::testing::_))
        .WillOnce(Return(static_cast<int64_t>(sequentialBytes)));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    [[maybe_unused]] const auto bytesRead1 = file.SequentialRead(sequentialBytes, seqBuffer.data());
    [[maybe_unused]] const auto bytesRead2 = file.RandomRead(randomOffset, randomBytes, randomBuffer.data());
    [[maybe_unused]] const auto bytesRead3 = file.SequentialRead(sequentialBytes, seqBuffer.data());

    // Assert
    EXPECT_EQ(sequentialBytes * 2, file.GetOffset());
}

TEST_F(ReadableFileTests, RandomRead_RequestMoreThanAvailable_ReadsOnlyAvailableBytes)
{
    // Arrange
    constexpr uint64_t blobSize = 200;
    constexpr int64_t offset = 150;
    constexpr int64_t bytesToRead = 100;
    constexpr int64_t expectedBytes = 50; // Only 50 bytes available from offset 150 in a 200 byte blob
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), offset, expectedBytes, ::testing::_))
        .WillOnce([expectedBytes](std::span<char> downloadBuffer, int64_t /*offset*/, int64_t /*length*/, const ::Azure::ETag& /*ifMatch*/)
            {
                std::fill_n(downloadBuffer.begin(), expectedBytes, 'C');
                return static_cast<int64_t>(expectedBytes);
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(expectedBytes, bytesRead);
}

TEST_F(ReadableFileTests, RandomRead_AtEndOfFile_ReturnsZero)
{
    // Arrange
    constexpr uint64_t blobSize = 100;
    std::vector<char> buffer(50);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.RandomRead(static_cast<int64_t>(blobSize), 50, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, Skip_IncrementsOffset)
{
    // Arrange
    constexpr int64_t skipAmount = 100;
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    file.Skip(skipAmount);

    // Assert
    EXPECT_EQ(skipAmount, file.GetOffset());
}

TEST_F(ReadableFileTests, Skip_Multiple_AccumulatesOffset)
{
    // Arrange
    constexpr int64_t skip1 = 50;
    constexpr int64_t skip2 = 75;
    constexpr int64_t skip3 = 25;
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    file.Skip(skip1);
    file.Skip(skip2);
    file.Skip(skip3);

    // Assert
    EXPECT_EQ(skip1 + skip2 + skip3, file.GetOffset());
}

TEST_F(ReadableFileTests, GetOffset_InitiallyZero)
{
    // Arrange & Act
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Assert
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(ReadableFileTests, GetSize_ReturnsCorrectSize)
{
    // Arrange
    constexpr uint64_t expectedSize = 5000;

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(Return(expectedSize));
    ON_CALL(*m_blobClient, GetEtag())
        .WillByDefault(Return(::Azure::ETag{ "etag" }));

    // Act
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Assert
    EXPECT_EQ(static_cast<int64_t>(expectedSize), file.GetSize());
}

TEST_F(ReadableFileTests, SequentialRead_DownloadReturnsNegative_ReturnsZero)
{
    // Arrange
    constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, bytesToRead, ::testing::_))
        .WillOnce(Return(-1)); // Simulate error

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(0, file.GetOffset()); // Offset should not advance on error
}

TEST_F(ReadableFileTests, RandomRead_DownloadReturnsNegative_ReturnsZero)
{
    // Arrange
    constexpr int64_t offset = 50;
    constexpr int64_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), offset, bytesToRead, ::testing::_))
        .WillOnce(Return(-1)); // Simulate error

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.RandomRead(offset, bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, SequentialRead_EmptyBlob_ReturnsZero)
{
    // Arrange
    constexpr uint64_t blobSize = 0;
    std::vector<char> buffer(100);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.SequentialRead(100, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, RandomRead_EmptyBlob_ReturnsZero)
{
    // Arrange
    constexpr uint64_t blobSize = 0;
    std::vector<char> buffer(100);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    const auto bytesRead = file.RandomRead(0, 100, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, SequentialRead_InterleavedWithSkip_MaintainsCorrectOffset)
{
    // Arrange
    constexpr int64_t readSize = 50;
    constexpr int64_t skipAmount = 25;
    std::vector<char> buffer(readSize);

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), 0, readSize, ::testing::_))
        .WillOnce(Return(static_cast<int64_t>(readSize)));

    EXPECT_CALL(*m_blobClient, Download(::testing::A<std::span<char>>(), readSize + skipAmount, readSize, ::testing::_))
        .WillOnce(Return(static_cast<int64_t>(readSize)));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr, m_logger };

    // Act
    [[maybe_unused]] const auto bytesRead1 = file.SequentialRead(readSize, buffer.data());
    file.Skip(skipAmount);
    [[maybe_unused]] const auto bytesRead2 = file.SequentialRead(readSize, buffer.data());

    // Assert
    EXPECT_EQ(readSize + skipAmount + readSize, file.GetOffset());
}
