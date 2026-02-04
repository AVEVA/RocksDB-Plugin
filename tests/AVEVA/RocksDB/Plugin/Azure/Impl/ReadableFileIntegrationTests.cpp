// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "IntegrationTestHelpers.hpp"

#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/identity.hpp>
#include <azure/core/http/http.hpp>

#include <string>

using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::Testing::AzureIntegrationTestBase;

class ReadableFileIntegrationTests : public AzureIntegrationTestBase
{
protected:
    std::string GetBlobNamePrefix() const override
    {
        return "test-readable";
    }
};

TEST_F(ReadableFileIntegrationTests, SequentialRead_SmallFile_ReadsCorrectly)
{
    // Arrange
    std::vector<char> testData(1024);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateBlobWithData(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act
    std::vector<char> readBuffer(testData.size());
    const auto bytesRead = file.SequentialRead(static_cast<int64_t>(testData.size()), readBuffer.data());

    // Assert
    EXPECT_EQ(testData.size(), bytesRead);
    EXPECT_EQ(testData, readBuffer);
    EXPECT_EQ(testData.size(), file.GetOffset());
}

TEST_F(ReadableFileIntegrationTests, SequentialRead_MultipleChunks_ReadsInOrder)
{
    // Arrange
    std::vector<char> testData(2048);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateBlobWithData(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act - Read in chunks
    std::vector<char> chunk1(512);
    std::vector<char> chunk2(512);
    std::vector<char> chunk3(1024);

    int64_t read1 = file.SequentialRead(512, chunk1.data());
    int64_t read2 = file.SequentialRead(512, chunk2.data());
    int64_t read3 = file.SequentialRead(1024, chunk3.data());

    // Assert
    EXPECT_EQ(512, read1);
    EXPECT_EQ(512, read2);
    EXPECT_EQ(1024, read3);
    EXPECT_EQ(2048, file.GetOffset());

    // Verify data
    std::vector<char> combined;
    combined.insert(combined.end(), chunk1.begin(), chunk1.end());
    combined.insert(combined.end(), chunk2.begin(), chunk2.end());
    combined.insert(combined.end(), chunk3.begin(), chunk3.end());
    EXPECT_EQ(testData, combined);
}

TEST_F(ReadableFileIntegrationTests, RandomRead_DifferentOffsets_ReadsCorrectly)
{
    // Arrange
    std::vector<char> testData(Configuration::PageBlob::PageSize * 2);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateBlobWithData(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act - Random reads at different offsets
    std::vector<char> buffer1(100);
    std::vector<char> buffer2(200);
    std::vector<char> buffer3(512);

    int64_t read1 = file.RandomRead(0, 100, buffer1.data());
    int64_t read2 = file.RandomRead(500, 200, buffer2.data());
    int64_t read3 = file.RandomRead(Configuration::PageBlob::PageSize, 512, buffer3.data());

    // Assert
    EXPECT_EQ(100, read1);
    EXPECT_EQ(200, read2);
    EXPECT_EQ(512, read3);

    // Verify data
    EXPECT_TRUE(std::equal(buffer1.begin(), buffer1.end(), testData.begin()));
    EXPECT_TRUE(std::equal(buffer2.begin(), buffer2.end(), testData.begin() + 500));
    EXPECT_TRUE(std::equal(buffer3.begin(), buffer3.end(), testData.begin() + Configuration::PageBlob::PageSize));

    // Sequential offset should not be affected by random reads
    EXPECT_EQ(0, file.GetOffset());
}

TEST_F(ReadableFileIntegrationTests, Skip_AdvancesOffset_WithoutReading)
{
    // Arrange
    std::vector<char> testData(1024);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateBlobWithData(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act
    file.Skip(100);
    std::vector<char> buffer(200);
    int64_t bytesRead = file.SequentialRead(200, buffer.data());

    // Assert
    EXPECT_EQ(100, file.GetOffset() - bytesRead);
    EXPECT_EQ(200, bytesRead);
    EXPECT_TRUE(std::equal(buffer.begin(), buffer.end(), testData.begin() + 100));
}

TEST_F(ReadableFileIntegrationTests, GetSize_ReturnsCorrectSize)
{
    // Arrange
    std::vector<char> testData(12345);
    auto blobClient = CreateBlobWithData(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act
    int64_t size = file.GetSize();

    // Assert
    EXPECT_EQ(testData.size(), size);
}

TEST_F(ReadableFileIntegrationTests, SequentialRead_BeyondFileSize_ReturnsAvailableData)
{
    // Arrange
    std::vector<char> testData(500);
    for (size_t i = 0; i < testData.size(); ++i)
    {
        testData[i] = 'X';
    }

    auto blobClient = CreateBlobWithData(testData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act - Try to read more than available
    std::vector<char> buffer(1000, 0);
    int64_t bytesRead = file.SequentialRead(1000, buffer.data());

    // Assert
    EXPECT_EQ(500, bytesRead); // Should only read what's available
    EXPECT_TRUE(std::equal(buffer.begin(), buffer.begin() + 500, testData.begin()));
}

TEST_F(ReadableFileIntegrationTests, ReadEmptyFile_ReturnsZero)
{
    // Arrange - Create empty blob
    std::vector<char> emptyData;
    auto blobClient = CreateBlobWithData(emptyData);
    ReadableFileImpl file(m_blobName, blobClient, nullptr, m_logger);

    // Act
    std::vector<char> buffer(100);
    int64_t bytesRead = file.SequentialRead(100, buffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
    EXPECT_EQ(0, file.GetSize());
}
