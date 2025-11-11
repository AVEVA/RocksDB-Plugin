#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArrayArgument;

class ReadableFileTests : public ::testing::Test
{
protected:
    std::shared_ptr<BlobClientMock> m_blobClient;
    static constexpr uint64_t DefaultBlobSize = 1024;

    void SetUp() override
    {
        m_blobClient = std::make_shared<BlobClientMock>();

        // Default behavior: return a blob size
        ON_CALL(*m_blobClient, GetSize())
            .WillByDefault(Return(DefaultBlobSize));
    }
};

TEST_F(ReadableFileTests, Constructor_InitializesWithBlobSize)
{
    // Arrange
    constexpr uint64_t expectedSize = 2048;
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(expectedSize));

    // Act
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Assert
    EXPECT_EQ(expectedSize, file.GetSize());
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(ReadableFileTests, SequentialRead_WithoutCache_ReadsFromBlob)
{
    // Arrange
    constexpr size_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);
    std::vector<char> expectedData(bytesToRead, 'A');

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), 0, bytesToRead))
        .WillOnce([&expectedData](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/)
            {
                std::copy(expectedData.begin(), expectedData.end(), buffer.begin());
                return static_cast<int64_t>(expectedData.size());
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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
    constexpr size_t firstRead = 50;
    constexpr size_t secondRead = 75;
    std::vector<char> buffer1(firstRead);
    std::vector<char> buffer2(secondRead);

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), 0, firstRead))
        .WillOnce([firstRead](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/)
            {
                std::fill_n(buffer.begin(), firstRead, 'X');
                return static_cast<int64_t>(firstRead);
            });

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), firstRead, secondRead))
        .WillOnce([secondRead](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/)
            {
                std::fill_n(buffer.begin(), secondRead, 'Y');
                return static_cast<int64_t>(secondRead);
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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
    constexpr size_t bytesToRead = 150;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), 0, blobSize))
        .WillOnce([blobSize](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/)
            {
                std::fill_n(buffer.begin(), blobSize, 'Z');
                return static_cast<int64_t>(blobSize);
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(blobSize, bytesRead);
    EXPECT_EQ(blobSize, file.GetOffset());
}

TEST_F(ReadableFileTests, SequentialRead_AtEndOfFile_ReturnsZero)
{
    // Arrange
    constexpr uint64_t blobSize = 100;
    std::vector<char> buffer(50);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };
    file.Skip(blobSize); // Move to end of file

    // Act
    const auto bytesRead = file.SequentialRead(50, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(blobSize, file.GetOffset());
}

TEST_F(ReadableFileTests, RandomRead_WithoutCache_ReadsFromBlob)
{
    // Arrange
    constexpr uint64_t offset = 50;
    constexpr size_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);
    std::vector<char> expectedData(bytesToRead, 'B');

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), offset, bytesToRead))
        .WillOnce([&expectedData](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/)
            {
                std::copy(expectedData.begin(), expectedData.end(), buffer.begin());
                return static_cast<int64_t>(expectedData.size());
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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
    constexpr size_t sequentialBytes = 50;
    constexpr uint64_t randomOffset = 200;
    constexpr size_t randomBytes = 75;
    std::vector<char> seqBuffer(sequentialBytes);
    std::vector<char> randomBuffer(randomBytes);

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), 0, sequentialBytes))
        .WillOnce(Return(static_cast<int64_t>(sequentialBytes)));

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), randomOffset, randomBytes))
        .WillOnce(Return(static_cast<int64_t>(randomBytes)));

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), sequentialBytes, sequentialBytes))
        .WillOnce(Return(static_cast<int64_t>(sequentialBytes)));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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
    constexpr uint64_t offset = 150;
    constexpr size_t bytesToRead = 100;
    constexpr size_t expectedBytes = 50; // Only 50 bytes available from offset 150 in a 200 byte blob
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(blobSize));

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), offset, expectedBytes))
        .WillOnce([expectedBytes](std::span<char> buffer, int64_t /*offset*/, int64_t /*length*/)
            {
                std::fill_n(buffer.begin(), expectedBytes, 'C');
                return static_cast<int64_t>(expectedBytes);
            });

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Act
    const auto bytesRead = file.RandomRead(blobSize, 50, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, Skip_IncrementsOffset)
{
    // Arrange
    constexpr uint64_t skipAmount = 100;
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Act
    file.Skip(skipAmount);

    // Assert
    EXPECT_EQ(skipAmount, file.GetOffset());
}

TEST_F(ReadableFileTests, Skip_Multiple_AccumulatesOffset)
{
    // Arrange
    constexpr uint64_t skip1 = 50;
    constexpr uint64_t skip2 = 75;
    constexpr uint64_t skip3 = 25;
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Assert
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(ReadableFileTests, GetSize_ReturnsCorrectSize)
{
    // Arrange
    constexpr uint64_t expectedSize = 5000;
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillOnce(Return(expectedSize));

    // Act
    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Assert
    EXPECT_EQ(expectedSize, file.GetSize());
}

TEST_F(ReadableFileTests, SequentialRead_DownloadReturnsNegative_ReturnsZero)
{
    // Arrange
    constexpr size_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), 0, bytesToRead))
        .WillOnce(Return(-1)); // Simulate error

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Act
    const auto bytesRead = file.SequentialRead(bytesToRead, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(0, file.GetOffset()); // Offset should not advance on error
}

TEST_F(ReadableFileTests, RandomRead_DownloadReturnsNegative_ReturnsZero)
{
    // Arrange
    constexpr uint64_t offset = 50;
    constexpr size_t bytesToRead = 100;
    std::vector<char> buffer(bytesToRead);

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), offset, bytesToRead))
        .WillOnce(Return(-1)); // Simulate error

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

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

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Act
    const auto bytesRead = file.RandomRead(0, 100, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadableFileTests, SequentialRead_InterleavedWithSkip_MaintainsCorrectOffset)
{
    // Arrange
    constexpr size_t readSize = 50;
    constexpr uint64_t skipAmount = 25;
    std::vector<char> buffer(readSize);

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), 0, readSize))
        .WillOnce(Return(static_cast<int64_t>(readSize)));

    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), readSize + skipAmount, readSize))
        .WillOnce(Return(static_cast<int64_t>(readSize)));

    ReadableFileImpl file{ "test.sst", m_blobClient, nullptr };

    // Act
    [[maybe_unused]] const auto bytesRead1 = file.SequentialRead(readSize, buffer.data());
    file.Skip(skipAmount);
    [[maybe_unused]] const auto bytesRead2 = file.SequentialRead(readSize, buffer.data());

    // Assert
    EXPECT_EQ(readSize + skipAmount + readSize, file.GetOffset());
}
