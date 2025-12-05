// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "IntegrationTestHelpers.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>
#include <azure/core/http/http.hpp>
#include <boost/log/trivial.hpp>

#include <random>
#include <string>
#include <span>

using AVEVA::RocksDB::Plugin::Azure::Impl::WriteableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::BlobHelpers;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::Testing::AzureIntegrationTestBase;

class WriteableFileIntegrationTests : public AzureIntegrationTestBase
{
protected:
    std::string GetBlobNamePrefix() const override
    {
        return "test-writeable";
    }
};

TEST_F(WriteableFileIntegrationTests, Append_SmallData_WritesSuccessfully)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> testData(100, 'A');

    // Act
    file.Append(testData);
    file.Close();

    // Assert
    const auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, Append_MultipleWrites_AccumulatesData)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> data1(100, 'A');
    const std::vector<char> data2(150, 'B');
    const std::vector<char> data3(200, 'C');

    // Act
    file.Append(data1);
    file.Append(data2);
    file.Append(data3);
    file.Close();

    // Assert
    std::vector<char> expected;
    expected.insert(expected.end(), data1.begin(), data1.end());
    expected.insert(expected.end(), data2.begin(), data2.end());
    expected.insert(expected.end(), data3.begin(), data3.end());

    auto downloadedData = DownloadBlobData(expected.size());
    EXPECT_EQ(expected, downloadedData);
    EXPECT_EQ(expected.size(), file.GetFileSize());
}

TEST_F(WriteableFileIntegrationTests, Append_LargeData_WritesCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Create data larger than buffer size
    std::vector<char> testData(Configuration::PageBlob::DefaultBufferSize * 2);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    // Act
    file.Append(testData);
    file.Close();

    // Assert
    const auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData.size(), downloadedData.size());
    EXPECT_EQ(testData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, Flush_PersistsData_ToBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };
    const std::vector<char> testData(500, 'X');

    // Act
    file.Append(testData);
    file.Flush(); // Flush without closing

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

TEST_F(WriteableFileIntegrationTests, Sync_PersistsDataAndSize_ToBlob)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> testData(750, 'Y');

    // Act
    file.Append(testData);
    file.Sync();

    // Assert
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    const auto actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(testData.size(), actualSize);

    const auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, Truncate_ReducesFileSize)
{
    // Arrange
    std::vector<char> initialData(1000, 'Z');
    auto blobClient = CreateBlobWithData(initialData);
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Act
    file.Truncate(500);
    file.Close();

    // Assert
    auto pageBlobClient = m_containerClient->GetPageBlobClient(m_blobName);
    const auto actualSize = BlobHelpers::GetFileSize(pageBlobClient);
    EXPECT_EQ(500, actualSize);
    EXPECT_EQ(500, file.GetFileSize());
}

TEST_F(WriteableFileIntegrationTests, AppendToExistingFile_PreservesExistingData)
{
    // Arrange - Create file with initial data
    const std::vector<char> initialData(Configuration::PageBlob::PageSize - 100, 'I');
    const std::vector<char> newData(200, 'N');
    auto blobClient = CreateBlobWithData(initialData);
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Act
    file.Append(newData);
    file.Close();

    // Assert
    std::vector<char> expected;
    expected.insert(expected.end(), initialData.begin(), initialData.end());
    expected.insert(expected.end(), newData.begin(), newData.end());

    const auto downloadedData = DownloadBlobData(expected.size());
    EXPECT_EQ(expected, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, GetFileSize_ReturnsCorrectSize)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> data1(300, 'A');
    const std::vector<char> data2(200, 'B');

    // Act & Assert
    EXPECT_EQ(0, file.GetFileSize());

    file.Append(data1);
    EXPECT_EQ(300, file.GetFileSize());

    file.Append(data2);
    EXPECT_EQ(500, file.GetFileSize());
}

TEST_F(WriteableFileIntegrationTests, Close_CanBeCalledMultipleTimes)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    const std::vector<char> testData(100, 'M');
    file.Append(std::span<const char>(testData));

    // Act & Assert - Should not throw
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
}

TEST_F(WriteableFileIntegrationTests, PageAlignedWrites_HandleCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Write exactly one page
    const std::vector<char> pageData(Configuration::PageBlob::PageSize, 'P');

    // Act
    file.Append(pageData);
    file.Close();

    // Assert
    const auto downloadedData = DownloadBlobData(pageData.size());
    EXPECT_EQ(pageData, downloadedData);
}

TEST_F(WriteableFileIntegrationTests, NonPageAlignedWrites_HandleCorrectly)
{
    // Arrange
    auto blobClient = CreateEmptyBlob();
    WriteableFileImpl file{ m_blobName, blobClient, nullptr, m_logger };

    // Write non-page-aligned data
    const std::vector<char> testData(Configuration::PageBlob::PageSize + 100, 'Q');

    // Act
    file.Append(testData);
    file.Close();

    // Assert
    const auto downloadedData = DownloadBlobData(testData.size());
    EXPECT_EQ(testData, downloadedData);
}
