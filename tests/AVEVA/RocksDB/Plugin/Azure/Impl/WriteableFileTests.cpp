#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gtest/gtest.h>

using AVEVA::RocksDB::Plugin::Azure::Impl::WriteableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using boost::log::sources::logger_mt;
using ::testing::_;

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
    WriteableFileImpl file{ "", m_blobClient, nullptr, m_logger };

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
    WriteableFileImpl file{ "",  m_blobClient, nullptr, m_logger };

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
    WriteableFileImpl file{ "", m_blobClient, nullptr, m_logger };

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
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), _, _))
        .WillOnce([this, &existingData](std::span<char> buffer, int64_t blobOffset, int64_t length)
            {
                EXPECT_EQ(0, blobOffset);
                EXPECT_EQ(length, existingData.size());
                std::copy(existingData.begin(), existingData.end(), buffer.begin());
                return length;
            });

    // Act
    WriteableFileImpl file{ "", std::move(m_blobClient), nullptr, m_logger };

    // Assert
    EXPECT_EQ(file.GetFileSize(), 333);
}

TEST_F(WriteableFileTests, Constructor_PartialLastPageInBlob_DataDownloaded)
{
    // Arrange
    std::vector<char> existingData(333, 'p');
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize + existingData.size()));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize * 2));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), _, _))
        .WillOnce([this, existingData](std::span<char> buffer, int64_t blobOffset, int64_t length)
            {
                EXPECT_EQ(Configuration::PageBlob::PageSize, blobOffset);
                EXPECT_EQ(length, existingData.size());
                std::copy(existingData.begin(), existingData.end(), buffer.begin());
                return length;
            });

    // Act
    WriteableFileImpl file{ "", std::move(m_blobClient), nullptr, m_logger };

    // Assert
    ASSERT_EQ(512 + existingData.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, Append_LessThanAPage_UploadPagesNotCalled)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(0);

    std::vector<char> dataToAppend(333, 'p');
    WriteableFileImpl file{ "", std::move(m_blobClient), nullptr, m_logger, Configuration::PageBlob::PageSize * 2 };

    // Act
    file.Append(dataToAppend);

    // Assert
    ASSERT_EQ(dataToAppend.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, Append_MultipleWritesLargerThanPage_UploadPagesCalled)
{
    // Arrange
    std::vector<char> dataToAppend1(Configuration::PageBlob::PageSize + 1, 'p');
    std::vector<char> dataToAppend2(Configuration::PageBlob::PageSize + 1, 'z');
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .WillOnce([this, &dataToAppend1](const std::span<char> buffer, int64_t blobOffset)
            {
                EXPECT_EQ(Configuration::PageBlob::PageSize * 2, buffer.size());
                const auto realData = buffer.subspan(0, dataToAppend1.size());
                EXPECT_EQ(dataToAppend1, std::vector(realData.begin(), realData.end()));
                EXPECT_EQ(0, blobOffset);
            })
        .WillOnce([this, &dataToAppend1, &dataToAppend2](const std::span<char> buffer, int64_t blobOffset)
            {
                EXPECT_EQ(Configuration::PageBlob::PageSize * 2, buffer.size());

                auto expected = std::vector(dataToAppend1.begin(), dataToAppend1.begin() + 1);
                expected.insert(expected.end(), dataToAppend2.begin(), dataToAppend2.end());
                const auto realData = buffer.subspan(0, expected.size());

                EXPECT_EQ(expected, std::vector(realData.begin(), realData.end()));
                EXPECT_EQ(Configuration::PageBlob::PageSize, blobOffset);
            });

    WriteableFileImpl file{ "", std::move(m_blobClient), nullptr, m_logger, Configuration::PageBlob::PageSize * 2 };

    // Act
    file.Append(dataToAppend1);
    file.Append(dataToAppend2);

    // Assert
    ASSERT_EQ(dataToAppend1.size() + dataToAppend2.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, Append_ExceedsCapacity_SetCapacityCalled)
{
    // Arrange
    const int64_t initialCapacity = Configuration::PageBlob::PageSize * 2;

    int64_t actualCapacity = 0;

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(initialCapacity));

    // Expect SetCapacity to be called when capacity is exceeded
    EXPECT_CALL(*m_blobClient, SetCapacity(::testing::_))
        .Times(1)
        .WillOnce(::testing::SaveArg<0>(&actualCapacity));

    // Expect UploadPages to be called when flushing
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(::testing::AtLeast(1));

    // Expect SetSize to be called during Sync/Close
    EXPECT_CALL(*m_blobClient, SetSize(::testing::_))
        .Times(::testing::AtLeast(0));

    {
        WriteableFileImpl file{ "", m_blobClient, nullptr, m_logger, Configuration::PageBlob::PageSize * 2 };

        // Act - Append enough data to exceed initial capacity
        // We need to write more than initialCapacity bytes
        std::vector<char> dataToAppend(initialCapacity + Configuration::PageBlob::PageSize, 'x');
        file.Append(dataToAppend);

        // Force a flush to trigger the capacity expansion
        file.Close();

        // Assert
        ASSERT_EQ(dataToAppend.size(), file.GetFileSize());
    } // File destructor called here

    // Verify SetCapacity was called with a value greater than initial capacity
    ASSERT_GT(actualCapacity, initialCapacity) << "SetCapacity should be called with a capacity greater than the initial capacity";
}



