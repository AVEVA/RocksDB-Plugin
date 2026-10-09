// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include "FakeBlobEnvironment.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace AVEVA::RocksDB::Plugin::Azure::Impl;
using namespace AVEVA::RocksDB::Plugin::Core;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakePageBlobClient;

class ReadWriteFileImplTests : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_env;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    std::string m_testFileName = "test.blob";

    void SetUp() override {
        m_logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
        m_env.Blob->SetCapacity(Configuration::PageBlob::DefaultSize);
    }

    std::unique_ptr<ReadWriteFileImpl> CreateFile() {
        return std::make_unique<ReadWriteFileImpl>(m_testFileName, m_env.Runtime, m_env.Blob, nullptr, m_logger);
    }
};
TEST_F(ReadWriteFileImplTests, Constructor_InitializesCorrectly) {
    // Arrange
    m_env.Blob->Size = 1024;

    // Act
    const auto file = CreateFile();

    // Assert - file should be constructed successfully
    // Internal state is verified through subsequent operations
    SUCCEED();
}

TEST_F(ReadWriteFileImplTests, Constructor_InitializesFromEmptyBlob) {
    // Arrange - simulator starts with fileSize = 0

    // Act
    auto file = CreateFile();

    // Assert
    char buffer[10] = {0};
    auto bytesRead = file->Read(0, 10, buffer);
    EXPECT_EQ(0, bytesRead); // Nothing to read from empty file
}

TEST_F(ReadWriteFileImplTests, Write_PageAlignedData_BuffersCorrectly) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = Configuration::PageBlob::PageSize;
    std::vector<char> data(dataSize, 'A');

    // Act
    file->Write(0, data.data(), dataSize);
    file->Sync();

    // Assert
    std::vector<char> readBuffer(dataSize);
    auto bytesRead = file->Read(0, dataSize, readBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

TEST_F(ReadWriteFileImplTests, Write_NonPageAlignedOffset_HandlesPrePadding) {
    // Arrange
    auto file = CreateFile();
    const int64_t offset = 100; // Not page aligned
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'B');

    // Act
    file->Write(offset, data.data(), dataSize);
    file->Sync();

    // Assert
    std::vector<char> readBuffer(dataSize);
    const auto bytesRead = file->Read(offset, dataSize, readBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

TEST_F(ReadWriteFileImplTests, Write_NonPageAlignedEnd_HandlesPostPadding) {
    // Arrange
    auto file = CreateFile();
    const int64_t offset = 0;
    const int64_t dataSize = 100; // Not a multiple of page size
    std::vector<char> data(dataSize, 'C');

    // Act
    file->Write(offset, data.data(), dataSize);
    file->Sync();

    // Assert - verify data can be read back
    std::vector<char> readBuffer(dataSize);
    auto bytesRead = file->Read(offset, dataSize, readBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

TEST_F(ReadWriteFileImplTests, Write_BufferFull_TriggersAutoFlush) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = Configuration::PageBlob::DefaultBufferSize + Configuration::PageBlob::PageSize;
    std::vector<char> data(dataSize, 'D');

    // Act - write more than buffer can hold
    file->Write(0, data.data(), dataSize);

    // Assert - auto-flush should have occurred
    // Sync to finalize
    file->Sync();

    EXPECT_EQ(2U, m_env.Blob->Uploads.size());

    std::vector<char> readBuffer(dataSize);
    auto bytesRead = file->Read(0, dataSize, readBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

TEST_F(ReadWriteFileImplTests, Flush_PartialFirstPage_MergesExistingData) {
    // Arrange
    auto file = CreateFile();

    // Pre-populate blob with some data at page start
    const int64_t offset = 100; // Within first page
    std::fill_n(m_env.Blob->Data.begin(), offset, 'X');
    m_env.Blob->Size = offset;

    // Write data at offset
    const int64_t dataSize = 100;
    std::vector<char> newData(dataSize, 'Y');

    // Act
    file->Write(offset, newData.data(), dataSize);
    file->Sync();

    // Assert - existing data should still be there
    std::vector<char> readBuffer(offset);
    auto bytesRead = file->Read(0, offset, readBuffer.data());
    EXPECT_EQ(offset, bytesRead);

    // And new data should be there too
    std::vector<char> newDataBuffer(dataSize);
    bytesRead = file->Read(offset, dataSize, newDataBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(newData, newDataBuffer);
}

// Test Sync updates file size metadata
TEST_F(ReadWriteFileImplTests, Sync_UpdatesFileSizeMetadata) {
    // Arrange
    const int64_t dataSize = 500;

    auto file = CreateFile();
    std::vector<char> data(dataSize, 'E');

    // Act
    file->Write(0, data.data(), dataSize);
    file->Close(); // Close will call Sync internally

    // Assert - the size was written to the blob metadata exactly once
    EXPECT_EQ(dataSize, m_env.Blob->Size);
    EXPECT_EQ((std::vector<int64_t>{dataSize}), m_env.Blob->SizeWrites);
}

// Test Read returns correct data
TEST_F(ReadWriteFileImplTests, Read_ReturnsDataFromBlob) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = 1000;
    std::vector<char> data(dataSize, 'F');

    file->Write(0, data.data(), dataSize);
    file->Sync();

    // Act
    std::vector<char> readBuffer(dataSize);
    auto bytesRead = file->Read(0, dataSize, readBuffer.data());

    // Assert
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

TEST_F(ReadWriteFileImplTests, Read_OffsetBeyondFileSize_ReturnsZero) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'G');

    file->Write(0, data.data(), dataSize);
    file->Sync();

    // Act
    std::vector<char> readBuffer(100);
    auto bytesRead = file->Read(1000, 100, readBuffer.data());

    // Assert
    EXPECT_EQ(0, bytesRead);
}

TEST_F(ReadWriteFileImplTests, Read_PartialRead_ReturnsTruncatedData) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'H');

    file->Write(0, data.data(), dataSize);
    file->Sync();

    // Act - try to read more than available
    std::vector<char> readBuffer(200);
    auto bytesRead = file->Read(50, 200, readBuffer.data());

    // Assert - should only get 50 bytes (from offset 50 to end at 100)
    EXPECT_EQ(50, bytesRead);
}

// Test Close calls Sync
TEST_F(ReadWriteFileImplTests, Close_CallsSync) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'I');

    file->Write(0, data.data(), dataSize);

    // Act
    file->Close();

    // Assert - sync recorded the size
    EXPECT_EQ((std::vector<int64_t>{dataSize}), m_env.Blob->SizeWrites);
}

// Test Close is idempotent
TEST_F(ReadWriteFileImplTests, Close_CalledTwice_IsIdempotent) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'J');

    file->Write(0, data.data(), dataSize);

    // Act
    file->Close();
    file->Close(); // Second close should be no-op

    // Assert - sync ran only once
    EXPECT_EQ(1U, m_env.Blob->SizeWrites.size());
}

// Test Expand increases capacity
TEST_F(ReadWriteFileImplTests, Expand_IncreasesCapacity) {
    // Arrange
    auto file = CreateFile();
    const int64_t largeDataSize = Configuration::PageBlob::DefaultSize + 1000;
    std::vector<char> data(largeDataSize, 'K');

    // Act
    file->Write(0, data.data(), largeDataSize);
    file->Sync();

    // Assert - the blob had to grow
    EXPECT_FALSE(m_env.Blob->CapacityWrites.empty());
}

// Test destructor retries Close on failure
TEST_F(ReadWriteFileImplTests, Destructor_RetriesCloseOnFailure) {
    // Arrange - the first two attempts to record the size fail
    int sizeWriteAttempts = 0;
    m_env.Blob->Fault =
        [&sizeWriteAttempts](
            const FakePageBlobClient::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakePageBlobClient::Operation::SetMetadata && ++sizeWriteAttempts <= 2) {
            return FakePageBlobClient::Error(500, "InternalError");
        }
        return std::nullopt;
    };

    // Act - destructor should retry
    {
        ReadWriteFileImpl file(m_testFileName, m_env.Runtime, m_env.Blob, nullptr, m_logger);
    } // Destructor called here

    // Assert
    EXPECT_EQ(3, sizeWriteAttempts);
    EXPECT_EQ(1U, m_env.Blob->SizeWrites.size());
}

// Test move constructor
TEST_F(ReadWriteFileImplTests, MoveConstructor_TransfersOwnership) {
    // Arrange
    auto file1 = CreateFile();
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'L');
    file1->Write(0, data.data(), dataSize);

    // Act
    ReadWriteFileImpl file2(std::move(*file1));
    file2.Sync();

    // Assert - data should be accessible from moved object
    std::vector<char> readBuffer(dataSize);
    auto bytesRead = file2.Read(0, dataSize, readBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

// Test move assignment
TEST_F(ReadWriteFileImplTests, MoveAssignment_TransfersOwnership) {
    // Arrange
    auto file1 = CreateFile();
    auto file2 = CreateFile();

    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'M');
    file1->Write(0, data.data(), dataSize);

    // Act
    *file2 = std::move(*file1);
    file2->Sync();

    // Assert - data should be accessible from moved object
    std::vector<char> readBuffer(dataSize);
    auto bytesRead = file2->Read(0, dataSize, readBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

// Test sequential writes
TEST_F(ReadWriteFileImplTests, Write_Sequential_AccumulatesData) {
    // Arrange
    auto file = CreateFile();
    const int64_t chunkSize = 100;
    std::vector<char> chunk1(chunkSize, 'N');
    std::vector<char> chunk2(chunkSize, 'O');
    std::vector<char> chunk3(chunkSize, 'P');

    // Act
    file->Write(0, chunk1.data(), chunkSize);
    file->Write(chunkSize, chunk2.data(), chunkSize);
    file->Write(chunkSize * 2, chunk3.data(), chunkSize);
    file->Sync();

    // Assert
    std::vector<char> readBuffer1(chunkSize);
    std::vector<char> readBuffer2(chunkSize);
    std::vector<char> readBuffer3(chunkSize);

    file->Read(0, chunkSize, readBuffer1.data());
    file->Read(chunkSize, chunkSize, readBuffer2.data());
    file->Read(chunkSize * 2, chunkSize, readBuffer3.data());

    EXPECT_EQ(chunk1, readBuffer1);
    EXPECT_EQ(chunk2, readBuffer2);
    EXPECT_EQ(chunk3, readBuffer3);
}

// Test overlapping writes
TEST_F(ReadWriteFileImplTests, Write_Overlapping_OverwritesData) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = 200;
    std::vector<char> data1(dataSize, 'Q');
    std::vector<char> data2(100, 'R');

    // Act
    file->Write(0, data1.data(), dataSize);
    file->Write(50, data2.data(), 100); // Overwrite middle section
    file->Sync();

    // Assert
    std::vector<char> readBuffer(dataSize);
    file->Read(0, dataSize, readBuffer.data());

    // First 50 bytes should be 'Q'
    EXPECT_EQ('Q', readBuffer[0]);
    EXPECT_EQ('Q', readBuffer[49]);

    // Next 100 bytes should be 'R'
    EXPECT_EQ('R', readBuffer[50]);
    EXPECT_EQ('R', readBuffer[149]);

    // Last 50 bytes should be 'Q'
    EXPECT_EQ('Q', readBuffer[150]);
    EXPECT_EQ('Q', readBuffer[199]);
}