#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::Impl::WriteableFileImpl;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using boost::log::sources::logger_mt;
using ::testing::_;
using ::testing::Matcher;

class WriteableFileTests : public ::testing::Test
{
protected:
    std::shared_ptr<BlobClientMock> m_blobClient;
    std::shared_ptr<logger_mt> m_logger;

    void SetUp() override
    {
        m_blobClient = std::make_shared<BlobClientMock>();
        m_logger = std::make_shared<logger_mt>();
    }
};

TEST_F(WriteableFileTests, AppendBytes_LessThanAPage_PageWritten)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .WillOnce([this](std::span<char> expected, int64_t blobOffset)
            {
                ASSERT_EQ(0, blobOffset);
                ASSERT_EQ(512, expected.size());
            });
    WriteableFileImpl file{ "", 0, m_blobClient, nullptr, m_logger };

    // Act
    file.Append("1", 1);

    // Assert
    ASSERT_EQ(1, file.GetFileSize());
}

TEST_F(WriteableFileTests, AppendBytes_EqualToAPage_PageWritten)
{
    // Arrange
    std::vector<char> expected(512, 'a');
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .WillOnce([this, expected](std::span<char> toWrite, int64_t blobOffset)
            {
                ASSERT_EQ(0, blobOffset);
                ASSERT_EQ(expected, std::vector<char>(toWrite.begin(), toWrite.end()));
            });
    WriteableFileImpl file{ "", 0, m_blobClient, nullptr, m_logger };

    // Act
    file.Append(expected.data(), expected.size());

    // Assert
    ASSERT_EQ(expected.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, AppendBytes_MoreThanAPage_2PagesWritten)
{
    // Arrange
    std::vector<char> expected(515, 'b');
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .WillOnce([this, expected](std::span<char> toWrite, int64_t blobOffset)
            {
                ASSERT_EQ(0, blobOffset);
                ASSERT_EQ(expected, std::vector<char>(toWrite.begin(), toWrite.begin() + expected.size()));
            });
    WriteableFileImpl file{ "", 0, m_blobClient, nullptr, m_logger };

    // Act
    file.Append(expected.data(), expected.size());

    // Assert
    ASSERT_EQ(expected.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, Constructor_PartialPageInBlob_DataDownloaded)
{
    // Arrange
    std::vector<char> existingData(333, 'p');
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(333));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(512));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), _, _))
        .WillOnce([this, &existingData](std::span<char> buffer, int64_t blobOffset, int64_t length)
            {
                EXPECT_EQ(0, blobOffset);
                EXPECT_EQ(length, existingData.size());
                std::copy(existingData.begin(), existingData.end(), buffer.begin());
                return length;
            });

    // Act
    WriteableFileImpl file{ "", 0, std::move(m_blobClient), nullptr, m_logger };

    // Assert
    EXPECT_EQ(file.GetFileSize(), 333);
}

TEST_F(WriteableFileTests, Constructor_PartialLastPageInBlob_DataDownloaded)
{
    // Arrange
    std::vector<char> existingData(333, 'p');
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(512 + existingData.size()));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(1024));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), _, _))
        .WillOnce([this, existingData](std::span<char> buffer, int64_t blobOffset, int64_t length)
            {
                EXPECT_EQ(512, blobOffset);
                EXPECT_EQ(length, existingData.size());
                std::copy(existingData.begin(), existingData.end(), buffer.begin());
                return length;
            });

    // Act
    WriteableFileImpl file{ "", 0, std::move(m_blobClient), nullptr, m_logger };

    // Assert
    ASSERT_EQ(512 + existingData.size(), file.GetFileSize());
}
