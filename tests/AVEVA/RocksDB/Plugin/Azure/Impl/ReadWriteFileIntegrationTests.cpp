#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "IntegrationTestHelpers.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>
#include <azure/core/http/http.hpp>
#include <boost/log/sources/logger.hpp>

#include <random>
#include <string>
#include <span>

using AVEVA::RocksDB::Plugin::Azure::Impl::ReadWriteFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::BlobHelpers;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::Testing::AzureIntegrationTestBase;

class ReadWriteFileIntegrationTests : public AzureIntegrationTestBase
{
protected:
    std::string GetBlobNamePrefix() const override
    {
        return "test-readwrite";
    }
};

TEST_F(ReadWriteFileIntegrationTests, Write_ThenRead_DataMatchesCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    std::vector<char> testData(500);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    // Read data back
    std::vector<char> readBuffer(testData.size());
    const auto bytesRead = file.Read(0, testData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, Write_AtDifferentOffsets_ReadsCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> data1(100, 'A');
    const std::vector<char> data2(150, 'B');
    const std::vector<char> data3(200, 'C');

    // Act - Write at different offsets
    file.Write(0, data1.data(), data1.size());
    file.Write(500, data2.data(), data2.size());
    file.Write(1000, data3.data(), data3.size());
    file.Sync();

    // Read back
    std::vector<char> readBuffer1(100);
    std::vector<char> readBuffer2(150);
    std::vector<char> readBuffer3(200);
    const auto read1 = file.Read(0, 100, readBuffer1.data());
    const auto read2 = file.Read(500, 150, readBuffer2.data());
    const auto read3 = file.Read(1000, 200, readBuffer3.data());

    // Assert
    EXPECT_EQ(100, read1);
    EXPECT_EQ(150, read2);
    EXPECT_EQ(200, read3);
    EXPECT_EQ(data1, readBuffer1);
    EXPECT_EQ(data2, readBuffer2);
    EXPECT_EQ(data3, readBuffer3);
}

TEST_F(ReadWriteFileIntegrationTests, Write_LargeData_HandlesCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Create large data spanning multiple pages
    std::vector<char> testData(Configuration::PageBlob::PageSize * 3);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    std::vector<char> readBuffer(testData.size());
    const auto bytesRead = file.Read(0, testData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, OverwriteExistingData_UpdatesCorrectly)
{
    // Arrange
    const std::vector<char> initialData(1000, 'X');
    auto blobClient = CreateBlobWithData(initialData);
    ReadWriteFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    const std::vector<char> newData(200, 'Y');

    // Act
    file.Write(100, newData.data(), newData.size());
    file.Sync();

    // Assert
    std::vector<char> expected = initialData;
    std::copy(newData.begin(), newData.end(), expected.begin() + 100);

    std::vector<char> readBuffer(expected.size());
    const auto bytesRead = file.Read(0, expected.size(), readBuffer.data());

    EXPECT_EQ(expected.size(), bytesRead);
    EXPECT_EQ(expected, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, Flush_PersistsChangesToBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };
    const std::vector<char> testData(750, 'F');

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Flush();

    // Assert - Data should be persisted, but size metadata may not be updated yet
    // Download the actual uploaded data (rounded to page size)
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    size_t downloadSize = testData.size();
    if (downloadSize % Configuration::PageBlob::PageSize != 0)
    {
        downloadSize = ((downloadSize / Configuration::PageBlob::PageSize) + 1) * Configuration::PageBlob::PageSize;
    }

    std::vector<char> downloadedData(downloadSize);
    Azure::Storage::Blobs::DownloadBlobToOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = 0;
    options.Range.Value().Length = static_cast<int64_t>(downloadSize);

    pageBlobClient.DownloadTo(
        reinterpret_cast<uint8_t*>(downloadedData.data()),
        downloadedData.size(),
        options
    );

    // Verify the test data is at the beginning
    EXPECT_TRUE(std::equal(testData.begin(), testData.end(), downloadedData.begin()));
}

TEST_F(ReadWriteFileIntegrationTests, Sync_UpdatesFileSizeInBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };
    const std::vector<char> testData(888, 'S');

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    // Assert
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    const auto actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(testData.size(), actualSize);
}

TEST_F(ReadWriteFileIntegrationTests, Read_FromExistingFile_ReadsCorrectly)
{
    // Arrange - Create file with data
    std::vector<char> initialData(Configuration::PageBlob::PageSize);
    for (size_t i = 0; i < initialData.size(); ++i)
    {
        initialData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateBlobWithData(initialData);
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Act
    std::vector<char> readBuffer(initialData.size());
    const auto bytesRead = file.Read(0, initialData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(initialData.size(), bytesRead);
    EXPECT_EQ(initialData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, NonPageAlignedWrites_HandleCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Write at non-page-aligned offset
    const int64_t offset = 137;
    const std::vector<char> testData(555, 'N');

    // Act
    file.Write(offset, testData.data(), testData.size());
    file.Sync();

    // Assert
    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(offset, testData.size(), readBuffer.data());

    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, GetFileSize_ReturnsCorrectSize)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> testData(1234, 'G');

    // Act
    file.Write(0, testData.data(), testData.size());
    file.Sync();

    // Assert - Verify size through blob client
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    const auto actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(testData.size(), actualSize);
}

TEST_F(ReadWriteFileIntegrationTests, Close_CanBeCalledMultipleTimes)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> testData(100, 'C');
    file.Write(0, testData.data(), testData.size());

    // Act & Assert - Should not throw
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
}

TEST_F(ReadWriteFileIntegrationTests, WriteAndReadAcrossPageBoundaries_HandlesCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Write data that spans across page boundaries
    const auto offset = Configuration::PageBlob::PageSize - 100;
    const std::vector<char> testData(200, 'B');

    // Act
    file.Write(offset, testData.data(), testData.size());
    file.Sync();

    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(offset, testData.size(), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, WriteExtendsBeyondInitialCapacity_ExpandsBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Write data beyond default size
    const auto offset = Configuration::PageBlob::DefaultSize + 1000;
    const std::vector<char> testData(1000, 'E');

    // Act
    file.Write(offset, testData.data(), testData.size());
    file.Sync();

    // Assert
    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = file.Read(offset, testData.size(), readBuffer.data());

    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, MultipleWritesAndReads_MaintainDataIntegrity)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Act - Perform multiple interleaved writes and reads
    const std::vector<char> data1(300, '1');
    const std::vector<char> data2(400, '2');
    const std::vector<char> data3(500, '3');

    file.Write(0, data1.data(), data1.size());
    file.Write(1000, data2.data(), data2.size());
    file.Sync();

    std::vector<char> read1(data1.size());
    const auto bytesRead1 = file.Read(0, 300, read1.data());
    EXPECT_EQ(data1, read1);

    file.Write(2000, data3.data(), data3.size());
    file.Sync();

    std::vector<char> read2(data2.size());
    std::vector<char> read3(data3.size());
    const auto bytesRead2 = file.Read(1000, 400, read2.data());
    const auto bytesRead3 = file.Read(2000, 500, read3.data());

    // Assert
    EXPECT_EQ(data1.size(), bytesRead1);
    EXPECT_EQ(data2.size(), bytesRead2);
    EXPECT_EQ(data3.size(), bytesRead3);
    EXPECT_EQ(data1, read1);
    EXPECT_EQ(data2, read2);
    EXPECT_EQ(data3, read3);
}
