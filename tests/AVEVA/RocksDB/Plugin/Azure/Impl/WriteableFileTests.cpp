// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

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

    void TearDown() override
    {
        ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(m_blobClient.get()));
    }

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
        .WillOnce([this](const std::span<char> expected, const int64_t blobOffset)
            {
                ASSERT_EQ(0, blobOffset);
                ASSERT_EQ(Configuration::PageBlob::PageSize, expected.size());
            });
    WriteableFileImpl file{ "", m_blobClient, nullptr, m_logger };

    // Act
    std::string_view data = "1";
    file.Append(data);

    // Assert
    ASSERT_EQ(data.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, AppendBytes_EqualToAPage_PageWritten)
{
    // Arrange
    const std::vector<char> expected(Configuration::PageBlob::PageSize, 'a');
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .WillOnce([this, &expected](const std::span<char> toWrite, const int64_t blobOffset)
            {
                ASSERT_EQ(0, blobOffset);
                ASSERT_EQ(expected, std::vector<char>(toWrite.begin(), toWrite.end()));
            });
    WriteableFileImpl file{ "",  m_blobClient, nullptr, m_logger };

    // Act
    file.Append(expected);

    // Assert
    ASSERT_EQ(expected.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, AppendBytes_MoreThanAPage_2PagesWritten)
{
    // Arrange
    constexpr size_t dataSize = Configuration::PageBlob::PageSize + 3; // 3 bytes over one page
    const std::vector<char> expected(dataSize, 'b');
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .WillOnce([this, &expected](const std::span<char> toWrite, const int64_t blobOffset)
            {
                ASSERT_EQ(0, blobOffset);
                ASSERT_EQ(expected, std::vector<char>(toWrite.data(), toWrite.data() + expected.size()));
            });
    WriteableFileImpl file{ "", m_blobClient, nullptr, m_logger };

    // Act
    file.Append(expected);

    // Assert
    ASSERT_EQ(expected.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, Constructor_PartialPageInBlob_DataDownloaded)
{
    // Arrange
    constexpr size_t partialPageSize = 333;
    const std::vector<char> existingData(partialPageSize, 'p');
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(partialPageSize));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), _, _))
        .WillOnce([this, &existingData](std::span<char> buffer, const int64_t blobOffset, const int64_t length)
            {
                EXPECT_EQ(0, blobOffset);
                EXPECT_EQ(length, existingData.size());
                std::copy(existingData.begin(), existingData.end(), buffer.begin());
                return length;
            });

    // Act
    WriteableFileImpl file{ "", std::move(m_blobClient), nullptr, m_logger };

    // Assert
    EXPECT_EQ(file.GetFileSize(), partialPageSize);
}

TEST_F(WriteableFileTests, Constructor_PartialLastPageInBlob_DataDownloaded)
{
    // Arrange
    constexpr size_t partialPageSize = 333;
    const std::vector<char> existingData(partialPageSize, 'p');
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize + existingData.size()));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize * 2));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), _, _))
        .WillOnce([this, &existingData](std::span<char> buffer, const int64_t blobOffset, const int64_t length)
            {
                EXPECT_EQ(Configuration::PageBlob::PageSize, blobOffset);
                EXPECT_EQ(length, existingData.size());
                std::copy(existingData.begin(), existingData.end(), buffer.begin());
                return length;
            });

    // Act
    WriteableFileImpl file{ "", std::move(m_blobClient), nullptr, m_logger };

    // Assert
    ASSERT_EQ(Configuration::PageBlob::PageSize + existingData.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, Append_LessThanAPage_UploadPagesNotCalled)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(0);

    constexpr size_t partialPageSize = 333;
    std::vector<char> dataToAppend(partialPageSize, 'p');
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
        .WillOnce([this, &dataToAppend1](const std::span<char> buffer, const int64_t blobOffset)
            {
                EXPECT_EQ(Configuration::PageBlob::PageSize * 2, buffer.size());
                const auto realData = buffer.subspan(0, dataToAppend1.size());
                EXPECT_EQ(dataToAppend1, std::vector(realData.begin(), realData.end()));
                EXPECT_EQ(0, blobOffset);
            })
        .WillOnce([this, &dataToAppend1, &dataToAppend2](const std::span<char> buffer, const int64_t blobOffset)
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
    constexpr int64_t initialCapacity = Configuration::PageBlob::PageSize * 2;
    bool setCapacityCalled = false;
    int64_t actualCapacity = 0;

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(initialCapacity));
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(::testing::AtLeast(1));
    EXPECT_CALL(*m_blobClient, SetCapacity(::testing::_))
        .Times(1)
        .WillOnce(::testing::DoAll(
            ::testing::SaveArg<0>(&actualCapacity),
            ::testing::Assign(&setCapacityCalled, true)
        ));
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(::testing::AtLeast(0));

    WriteableFileImpl file{ "", m_blobClient, nullptr, m_logger, Configuration::PageBlob::PageSize * 2 };

    // Act
    std::vector<char> dataToAppend(initialCapacity + Configuration::PageBlob::PageSize, 'x');
    file.Append(dataToAppend);
    file.Sync(); // Trigger flush which will call Expand

    // Assert
    ASSERT_TRUE(setCapacityCalled) << "SetCapacity should have been called";
    ASSERT_GT(actualCapacity, initialCapacity) << "SetCapacity should be called with a capacity greater than the initial capacity";
}

TEST_F(WriteableFileTests, Constructor_BufferSizeSmallerThanPage_ThrowsException)
{
    // Arrange
    constexpr size_t invalidBufferSize = Configuration::PageBlob::PageSize - 1;

    // Act & Assert
    EXPECT_THROW(
        WriteableFileImpl file("test.dat", m_blobClient, nullptr, m_logger, invalidBufferSize),
        std::invalid_argument
    );
}

TEST_F(WriteableFileTests, Constructor_EmptyBlob_InitializesCorrectly)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));

    // Act
    const WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };

    // Assert
    EXPECT_EQ(0, file.GetFileSize());
}

TEST_F(WriteableFileTests, Constructor_FullPagesInBlob_NoDataDownloaded)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize * 3));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize * 4));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(), _, _))
        .Times(0);

    // Act
    const WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };

    // Assert
    EXPECT_EQ(Configuration::PageBlob::PageSize * 3, file.GetFileSize());
}

TEST_F(WriteableFileTests, Close_CalledMultipleTimes_OnlySyncsOnce)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(1);
    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };

    // Act
    file.Close();
    file.Close();
    file.Close();

    // Assert
    // Expectations verified by mock
}

TEST_F(WriteableFileTests, Close_WithUnflushedData_DataIsSynced)
{
    // Arrange
    constexpr size_t testDataSize = 100;
    const std::vector<char> dataToWrite(testDataSize, 'x');
    int64_t setSizeValue = -1;

    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(1);
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .WillOnce(::testing::SaveArg<0>(&setSizeValue));

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };
    file.Append(dataToWrite);

    // Act
    file.Close();

    // Assert
    EXPECT_EQ(testDataSize, setSizeValue);
}

TEST_F(WriteableFileTests, Close_EmptyFile_NoErrors)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, SetSize(0))
        .Times(1);
    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };

    // Act & Assert
    EXPECT_NO_THROW(file.Close());
}

TEST_F(WriteableFileTests, Sync_WithoutFileCache_NoError)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(::testing::AtLeast(1));
    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };

    // Act & Assert
    EXPECT_NO_THROW(file.Sync());
}

TEST_F(WriteableFileTests, Sync_CalledMultipleTimes_SetsSizeCorrectly)
{
    // Arrange
    std::vector<int64_t> setSizeCalls;
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(::testing::AtLeast(3))
        .WillRepeatedly([&setSizeCalls](const int64_t size)
            {
                setSizeCalls.push_back(size);
            });

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };

    // Act
    file.Sync();
    static const constexpr std::string_view firstAppend = "test";
    file.Append(firstAppend);
    file.Sync();
    static const constexpr std::string_view secondAppend = "data";
    file.Append(secondAppend);
    file.Sync();

    // Assert
    ASSERT_EQ(3, setSizeCalls.size());
    EXPECT_EQ(0, setSizeCalls[0]);
    EXPECT_EQ(firstAppend.size(), setSizeCalls[1]);
    EXPECT_EQ(firstAppend.size() + secondAppend.size(), setSizeCalls[2]);
}

TEST_F(WriteableFileTests, Sync_WithPartialPage_FlushesAndSetsSizeCorrectly)
{
    // Arrange
    static const constexpr size_t partialPageSize = Configuration::PageBlob::PageSize / 2;
    const std::vector<char> dataToWrite(partialPageSize, 'y');
    int64_t setSizeValue = -1;

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(1);
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(::testing::AtLeast(1))
        .WillOnce(::testing::SaveArg<0>(&setSizeValue))
        .WillRepeatedly(::testing::DoDefault());

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };
    file.Append(dataToWrite);

    // Act
    file.Sync();

    // Assert
    EXPECT_EQ(partialPageSize, setSizeValue);
}

TEST_F(WriteableFileTests, Flush_EmptyBuffer_NoUploadCalled)
{
    // Arrange
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(0);

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };

    // Act
    file.Flush();

    // Assert
    // Expectations verified by mock
}

TEST_F(WriteableFileTests, Flush_ExactlyOnePage_OneUploadCall)
{
    // Arrange
    const std::vector<char> dataToWrite(Configuration::PageBlob::PageSize, 'z');

    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(1)
        .WillOnce([](const std::span<char> buffer, const int64_t offset)
            {
                EXPECT_EQ(Configuration::PageBlob::PageSize, buffer.size());
                EXPECT_EQ(0, offset);
            });

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };
    file.Append(dataToWrite);

    // Act
    file.Flush();

    // Assert
    // Expectations verified by mock
}

TEST_F(WriteableFileTests, Truncate_ToZero_FileEmptied)
{
    // Arrange
    static const constexpr size_t initialDataSize = 1000;
    const std::vector<char> initialData(initialDataSize, 'a');
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize * 2));
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(::testing::AtLeast(1));
    EXPECT_CALL(*m_blobClient, SetSize(0))
        .Times(2); // Once in Sync before truncate, once in Truncate
    EXPECT_CALL(*m_blobClient, SetCapacity(0))
        .Times(1);

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };
    file.Append(initialData);

    // Act
    file.Truncate(0);

    // Assert
    EXPECT_EQ(0, file.GetFileSize());
}

TEST_F(WriteableFileTests, Truncate_ToSmallerSize_DataReducedCorrectly)
{
    // Arrange
    static const constexpr int64_t initialDataSize = 1000;
    static const constexpr int64_t truncatedSize = 500;
    static const constexpr auto truncatedSizeRoundedUp = Configuration::PageBlob::PageSize; // Rounded up to page size
    static const constexpr auto partialPageSize = truncatedSize % Configuration::PageBlob::PageSize;
    std::vector<char> initialData(initialDataSize, 'b');
    const std::vector<char> expectedPartialData(partialPageSize, 'b');

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize * 2));
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(::testing::AtLeast(1));
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(::testing::AtLeast(1));
    EXPECT_CALL(*m_blobClient, SetCapacity(truncatedSizeRoundedUp))
        .Times(1);
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(),
        0,
        partialPageSize))
        .WillOnce([&expectedPartialData](std::span<char> buffer, int64_t /*offset*/, int64_t length)
            {
                std::copy(expectedPartialData.begin(), expectedPartialData.end(), buffer.begin());
                return length;
            });

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };
    file.Append(initialData);

    // Act
    file.Truncate(truncatedSize);

    // Assert
    EXPECT_EQ(truncatedSize, file.GetFileSize());
}

TEST_F(WriteableFileTests, Truncate_ToLargerSize_ThrowsException)
{
    // Arrange
    static const constexpr int64_t initialDataSize = 100;
    static const constexpr int64_t expandedSize = 2000;
    std::vector<char> initialData(initialDataSize, 'c');

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };
    file.Append(initialData);

    // Act & Assert
    EXPECT_THROW(file.Truncate(expandedSize), std::invalid_argument);
}

TEST_F(WriteableFileTests, Truncate_ToPartialPage_BufferOffsetSetCorrectly)
{
    // Arrange
    static const constexpr int64_t partialPageOffset = 123;
    static const constexpr auto initialSize = Configuration::PageBlob::PageSize * 2;
    static const constexpr auto truncateSize = Configuration::PageBlob::PageSize + partialPageOffset;
    std::vector<char> initialData(initialSize, 'd');
    const std::vector<char> expectedPartialData(partialPageOffset, 'd');

    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize * 4));
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(::testing::AtLeast(1));
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(::testing::AtLeast(1));
    EXPECT_CALL(*m_blobClient, SetCapacity(_))
        .Times(::testing::AtLeast(1));
    EXPECT_CALL(*m_blobClient, DownloadTo(::testing::A<std::span<char>>(),
        Configuration::PageBlob::PageSize,
        partialPageOffset))
        .WillOnce([&expectedPartialData](std::span<char> buffer, int64_t /*offset*/, int64_t length)
            {
                std::copy(expectedPartialData.begin(), expectedPartialData.end(), buffer.begin());
                return length;
            });

    WriteableFileImpl file{ "test.dat", m_blobClient, nullptr, m_logger };
    file.Append(initialData);

    // Act
    file.Truncate(truncateSize);

    // Assert
    EXPECT_EQ(truncateSize, file.GetFileSize());

    // Verify we can append after truncation
    std::vector<char> appendData(10, 'e');
    EXPECT_NO_THROW(file.Append(appendData));
    EXPECT_EQ(truncateSize + appendData.size(), file.GetFileSize());
}

TEST_F(WriteableFileTests, GetUniqueId_BufferLargerThanName_ReturnsFullName)
{
    // Arrange
    static const constexpr std::string_view filename = "test.dat";
    const WriteableFileImpl file{ filename, m_blobClient, nullptr, m_logger };
    constexpr size_t largeBufferSize = 100;
    std::vector<char> id(largeBufferSize, '\0');

    // Act
    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    // Assert
    EXPECT_EQ(filename.size(), length);
    EXPECT_EQ(filename, std::string(id.data(), static_cast<size_t>(length)));
}

TEST_F(WriteableFileTests, GetUniqueId_BufferSmallerThanName_ReturnsTruncatedName)
{
    // Arrange
    static const constexpr std::string_view filename = "very_long_filename_for_testing.dat";
    const WriteableFileImpl file{ filename, m_blobClient, nullptr, m_logger };
    static const constexpr size_t smallBufferSize = 10;
    std::vector<char> id(smallBufferSize, '\0');

    // Act
    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    // Assert
    EXPECT_EQ(smallBufferSize, length);
    EXPECT_EQ(filename.substr(0, smallBufferSize), std::string(id.data(), static_cast<size_t>(length)));
}

TEST_F(WriteableFileTests, GetUniqueId_EmptyName_ReturnsZero)
{
    // Arrange
    const WriteableFileImpl file{ "", m_blobClient, nullptr, m_logger };
    static const constexpr size_t bufferSize = 100;
    std::vector<char> id(bufferSize, '\0');

    // Act
    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    // Assert
    EXPECT_EQ(0, length);
}

TEST_F(WriteableFileTests, MoveConstructor_TransfersState_Correctly)
{
    // Arrange - create a file with some data
    EXPECT_CALL(*m_blobClient, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*m_blobClient, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));
    EXPECT_CALL(*m_blobClient, UploadPages(_, _))
        .Times(::testing::AtLeast(0));
    EXPECT_CALL(*m_blobClient, SetSize(_))
        .Times(::testing::AtLeast(0));

    const std::vector<char> testData(100, 'm');
    WriteableFileImpl file1{ "test.dat", m_blobClient, nullptr, m_logger };
    file1.Append(testData);

    // Act - move construct file2 from file1
    const WriteableFileImpl file2{ std::move(file1) };

    // Assert - file2 should have the data
    EXPECT_EQ(testData.size(), file2.GetFileSize());

    // file1 is now in a moved-from state and its destructor should not crash
}

TEST_F(WriteableFileTests, MoveAssignment_TransfersState_Correctly)
{
    // Arrange
    const auto blobClient1 = std::make_shared<BlobClientMock>();
    const auto blobClient2 = std::make_shared<BlobClientMock>();
    const auto logger1 = std::make_shared<logger_mt>();
    const auto logger2 = std::make_shared<logger_mt>();

    EXPECT_CALL(*blobClient1, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*blobClient1, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));
    EXPECT_CALL(*blobClient1, UploadPages(_, _))
        .Times(::testing::AtLeast(0));
    EXPECT_CALL(*blobClient1, SetSize(_))
        .Times(::testing::AtLeast(0));

    EXPECT_CALL(*blobClient2, GetSize())
        .WillRepeatedly(::testing::Return(0));
    EXPECT_CALL(*blobClient2, GetCapacity())
        .WillRepeatedly(::testing::Return(Configuration::PageBlob::PageSize));
    EXPECT_CALL(*blobClient2, UploadPages(_, _))
        .Times(::testing::AtLeast(0));
    EXPECT_CALL(*blobClient2, SetSize(_))
        .Times(::testing::AtLeast(0));

    std::vector<char> testData(200, 'n');
    WriteableFileImpl file1{ "test1.dat", blobClient1, nullptr, logger1 };
    WriteableFileImpl file2{ "test2.dat", blobClient2, nullptr, logger2 };
    file1.Append(testData);

    // Act - move assign file1 to file2
    file2 = std::move(file1);

    // Assert - file2 should have file1's data
    EXPECT_EQ(testData.size(), file2.GetFileSize());

    // file1 is now in a moved-from state and its destructor should not crash
}

































































































































































































































































