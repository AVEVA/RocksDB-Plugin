// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "Fakes/SignedConfiguration.hpp"
#include "IntegrationTestHelpers.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <boost/log/trivial.hpp>

#include <random>
#include <span>
#include <string>

namespace BlobOperations = AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations;
namespace Configuration = AVEVA::RocksDB::Plugin::Azure::Impl::Tests::SignedConfiguration;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadWriteFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Testing::AzureIntegrationTestBase;
using AVEVA::RocksDB::Plugin::Core::ByteOffset;

class ReadWriteFileIntegrationTests : public AzureIntegrationTestBase {
  protected:
    std::string GetBlobNamePrefix() const override { return "test-readwrite"; }
};

TEST_F(ReadWriteFileIntegrationTests, Write_ThenRead_DataMatchesCorrectly) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    std::vector<char> testData(500);
    for (size_t i = 0; i < testData.size(); ++i) {
        testData[i] = static_cast<char>(i % 256);
    }

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Sync();

    // Read data back
    std::vector<char> readBuffer(testData.size());
    const auto bytesRead = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(0)},
                  std::span<char>(readBuffer.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))))
            .Value());

    // Assert
    EXPECT_EQ(static_cast<int64_t>(testData.size()), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, Write_AtDifferentOffsets_ReadsCorrectly) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    const std::vector<char> data1(100, 'A');
    const std::vector<char> data2(150, 'B');
    const std::vector<char> data3(200, 'C');

    // Act - Write at different offsets
    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(data1.data(), static_cast<size_t>(static_cast<int64_t>(data1.size()))));
    file.Write(ByteOffset{static_cast<uint64_t>(500)},
               std::span<const char>(data2.data(), static_cast<size_t>(static_cast<int64_t>(data2.size()))));
    file.Write(ByteOffset{static_cast<uint64_t>(1000)},
               std::span<const char>(data3.data(), static_cast<size_t>(static_cast<int64_t>(data3.size()))));
    file.Sync();

    // Read back
    std::vector<char> readBuffer1(100);
    std::vector<char> readBuffer2(150);
    std::vector<char> readBuffer3(200);
    const auto read1 = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(0)}, std::span<char>(readBuffer1.data(), static_cast<size_t>(100)))
            .Value());
    const auto read2 = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(500)}, std::span<char>(readBuffer2.data(), static_cast<size_t>(150)))
            .Value());
    const auto read3 = static_cast<int64_t>(file.Read(ByteOffset{static_cast<uint64_t>(1000)},
                                                      std::span<char>(readBuffer3.data(), static_cast<size_t>(200)))
                                                .Value());

    // Assert
    EXPECT_EQ(100, read1);
    EXPECT_EQ(150, read2);
    EXPECT_EQ(200, read3);
    EXPECT_EQ(data1, readBuffer1);
    EXPECT_EQ(data2, readBuffer2);
    EXPECT_EQ(data3, readBuffer3);
}

TEST_F(ReadWriteFileIntegrationTests, Write_LargeData_HandlesCorrectly) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    // Create large data spanning multiple pages
    std::vector<char> testData(Configuration::PageBlob::PageSize * 3);
    for (size_t i = 0; i < testData.size(); ++i) {
        testData[i] = static_cast<char>(i % 256);
    }

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Sync();

    std::vector<char> readBuffer(testData.size());
    const auto bytesRead = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(0)},
                  std::span<char>(readBuffer.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))))
            .Value());

    // Assert
    EXPECT_EQ(static_cast<int64_t>(testData.size()), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, OverwriteExistingData_UpdatesCorrectly) {
    // Arrange
    const std::vector<char> initialData(1000, 'X');
    auto blobClient = CreateBlobWithData(initialData);
    ReadWriteFileImpl file(m_blobName, m_runtime, blobClient, nullptr, m_logger);

    const std::vector<char> newData(200, 'Y');

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(100)},
               std::span<const char>(newData.data(), static_cast<size_t>(static_cast<int64_t>(newData.size()))));
    file.Sync();

    // Assert
    std::vector<char> expected = initialData;
    std::copy(newData.begin(), newData.end(), expected.begin() + 100);

    std::vector<char> readBuffer(expected.size());
    const auto bytesRead = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(0)},
                  std::span<char>(readBuffer.data(), static_cast<size_t>(static_cast<int64_t>(expected.size()))))
            .Value());

    EXPECT_EQ(static_cast<int64_t>(expected.size()), bytesRead);
    EXPECT_EQ(expected, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, Flush_PersistsChangesToBlob) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};
    const std::vector<char> testData(750, 'F');

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Flush();

    // Assert - Data should be persisted, but size metadata may not be updated yet
    // Download the actual uploaded data (rounded to page size)
    auto pageBlobClientPtr =
        std::make_unique<std::remove_cvref_t<decltype(m_containerClient->GetPageBlobClient(BlobPath()))>>(
            m_containerClient->GetPageBlobClient(BlobPath()));
    auto& pageBlobClient = *pageBlobClientPtr;
    size_t downloadSize = testData.size();
    if (downloadSize % Configuration::PageBlob::PageSize != 0) {
        downloadSize = ((downloadSize / Configuration::PageBlob::PageSize) + 1) * Configuration::PageBlob::PageSize;
    }

    std::vector<char> downloadedData(downloadSize);
    AVEVA::AzureClient::DownloadBlobOptions options;
    options.Range = AVEVA::AzureClient::Models::BlobByteRange{0, static_cast<uint64_t>(downloadSize)};

    const auto result =
        AVEVA::RocksDB::Plugin::Azure::Impl::Call(pageBlobClient, pageBlobClient.DownloadAsync(std::move(options)));
    std::copy_n(result.Content.begin(), std::min(downloadedData.size(), result.Content.size()), downloadedData.begin());

    // Verify the test data is at the beginning
    EXPECT_TRUE(std::equal(testData.begin(), testData.end(), downloadedData.begin()));
}

TEST_F(ReadWriteFileIntegrationTests, Sync_UpdatesFileSizeInBlob) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};
    const std::vector<char> testData(888, 'S');

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Sync();

    // Assert
    auto pageBlobClient = m_containerClient->GetPageBlobClient(BlobPath());
    const auto actualSize = BlobOperations::GetInfo(pageBlobClient).Size;
    EXPECT_EQ(static_cast<int64_t>(testData.size()), actualSize);
}

TEST_F(ReadWriteFileIntegrationTests, Read_FromExistingFile_ReadsCorrectly) {
    // Arrange - Create file with data
    std::vector<char> initialData(Configuration::PageBlob::PageSize);
    for (size_t i = 0; i < initialData.size(); ++i) {
        initialData[i] = static_cast<char>(i % 256);
    }

    auto blobClient = CreateBlobWithData(initialData);
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    // Act
    std::vector<char> readBuffer(initialData.size());
    const auto bytesRead = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(0)},
                  std::span<char>(readBuffer.data(), static_cast<size_t>(static_cast<int64_t>(initialData.size()))))
            .Value());

    // Assert
    EXPECT_EQ(static_cast<int64_t>(initialData.size()), bytesRead);
    EXPECT_EQ(initialData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, NonPageAlignedWrites_HandleCorrectly) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    // Write at non-page-aligned offset
    const int64_t offset = 137;
    const std::vector<char> testData(555, 'N');

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(offset)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Sync();

    // Assert
    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(offset)},
                  std::span<char>(readBuffer.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))))
            .Value());

    EXPECT_EQ(static_cast<int64_t>(testData.size()), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, GetFileSize_ReturnsCorrectSize) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    const std::vector<char> testData(1234, 'G');

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Sync();

    // Assert - Verify size through blob client
    auto pageBlobClient = m_containerClient->GetPageBlobClient(BlobPath());
    const auto actualSize = BlobOperations::GetInfo(pageBlobClient).Size;
    EXPECT_EQ(static_cast<int64_t>(testData.size()), actualSize);
}

TEST_F(ReadWriteFileIntegrationTests, Close_CanBeCalledMultipleTimes) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    const std::vector<char> testData(100, 'C');
    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));

    // Act & Assert - Should not throw
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
    EXPECT_NO_THROW(file.Close());
}

TEST_F(ReadWriteFileIntegrationTests, WriteAndReadAcrossPageBoundaries_HandlesCorrectly) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    // Write data that spans across page boundaries
    const auto offset = Configuration::PageBlob::PageSize - 100;
    const std::vector<char> testData(200, 'B');

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(offset)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Sync();

    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(offset)},
                  std::span<char>(readBuffer.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))))
            .Value());

    // Assert
    EXPECT_EQ(static_cast<int64_t>(testData.size()), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, WriteExtendsBeyondInitialCapacity_ExpandsBlob) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    // Write data beyond default size
    const auto offset = Configuration::PageBlob::DefaultSize + 1000;
    const std::vector<char> testData(1000, 'E');

    // Act
    file.Write(ByteOffset{static_cast<uint64_t>(offset)},
               std::span<const char>(testData.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))));
    file.Sync();

    // Assert
    std::vector<char> readBuffer(testData.size());
    int64_t bytesRead = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(offset)},
                  std::span<char>(readBuffer.data(), static_cast<size_t>(static_cast<int64_t>(testData.size()))))
            .Value());

    EXPECT_EQ(static_cast<int64_t>(testData.size()), bytesRead);
    EXPECT_EQ(testData, readBuffer);
}

TEST_F(ReadWriteFileIntegrationTests, MultipleWritesAndReads_MaintainDataIntegrity) {
    // Arrange
    auto blobClient = CreateEmptyBlob();
    ReadWriteFileImpl file{m_blobName, m_runtime, blobClient, nullptr, m_logger};

    // Act - Perform multiple interleaved writes and reads
    const std::vector<char> data1(300, '1');
    const std::vector<char> data2(400, '2');
    const std::vector<char> data3(500, '3');

    file.Write(ByteOffset{static_cast<uint64_t>(0)},
               std::span<const char>(data1.data(), static_cast<size_t>(static_cast<int64_t>(data1.size()))));
    file.Write(ByteOffset{static_cast<uint64_t>(1000)},
               std::span<const char>(data2.data(), static_cast<size_t>(static_cast<int64_t>(data2.size()))));
    file.Sync();

    std::vector<char> read1(data1.size());
    const auto bytesRead1 = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(0)}, std::span<char>(read1.data(), static_cast<size_t>(300)))
            .Value());
    EXPECT_EQ(data1, read1);

    file.Write(ByteOffset{static_cast<uint64_t>(2000)},
               std::span<const char>(data3.data(), static_cast<size_t>(static_cast<int64_t>(data3.size()))));
    file.Sync();

    std::vector<char> read2(data2.size());
    std::vector<char> read3(data3.size());
    const auto bytesRead2 = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(1000)}, std::span<char>(read2.data(), static_cast<size_t>(400)))
            .Value());
    const auto bytesRead3 = static_cast<int64_t>(
        file.Read(ByteOffset{static_cast<uint64_t>(2000)}, std::span<char>(read3.data(), static_cast<size_t>(500)))
            .Value());

    // Assert
    EXPECT_EQ(static_cast<int64_t>(data1.size()), bytesRead1);
    EXPECT_EQ(static_cast<int64_t>(data2.size()), bytesRead2);
    EXPECT_EQ(static_cast<int64_t>(data3.size()), bytesRead3);
    EXPECT_EQ(data1, read1);
    EXPECT_EQ(data2, read2);
    EXPECT_EQ(data3, read3);
}
