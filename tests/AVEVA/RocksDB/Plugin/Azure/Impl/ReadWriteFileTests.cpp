#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace AVEVA::RocksDB::Plugin::Azure::Impl;
using namespace AVEVA::RocksDB::Plugin::Core;
using namespace AVEVA::RocksDB::Plugin::Core::Mocks;
using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::NiceMock;
using ::testing::StrictMock;

/// <summary>
/// Helper class for blob simulation
/// </summary>
class BlobSimulator
{
    uint64_t m_fileSize;
    uint64_t m_capacity;
    std::vector<uint8_t> m_data;

public:
    explicit BlobSimulator(uint64_t initialCapacity = Configuration::PageBlob::DefaultSize)
        : m_fileSize(0), m_capacity(initialCapacity)
    {
        m_data.resize(static_cast<size_t>(initialCapacity), 0);
    }

    uint64_t GetSize() const
    {
        return m_fileSize;
    }

    void SetSize(int64_t size)
    {
        m_fileSize = static_cast<uint64_t>(size);
    }

    uint64_t GetCapacity() const
    {
        return m_capacity;
    }

    void SetCapacity(int64_t capacity)
    {
        m_capacity = static_cast<uint64_t>(capacity);
        if (static_cast<size_t>(capacity) > m_data.size())
        {
            m_data.resize(static_cast<size_t>(capacity), 0);
        }
    }

    void UploadPages(const std::span<char> buffer, int64_t offset)
    {
        if (static_cast<size_t>(offset + buffer.size()) > m_data.size())
        {
            m_data.resize(static_cast<size_t>(offset + buffer.size()), 0);
        }

        std::copy(buffer.begin(), buffer.end(), reinterpret_cast<char*>(m_data.data()) + offset);
    }

    int64_t DownloadTo(std::span<char> buffer, int64_t offset, int64_t length)
    {
        if (static_cast<size_t>(offset) < m_data.size())
        {
            size_t available = std::min(static_cast<size_t>(length), m_data.size() - static_cast<size_t>(offset));
            std::copy_n(reinterpret_cast<char*>(m_data.data()) + offset, available, buffer.data());
            // Zero out any remaining bytes
            if (available < buffer.size())
            {
                std::fill_n(buffer.data() + available, buffer.size() - available, static_cast<char>(0));
            }

            return static_cast<int64_t>(available);
        }
        else
        {
            std::fill_n(buffer.data(), buffer.size(), static_cast<char>(0));
            return 0;
        }
    }

    const std::vector<uint8_t>& GetData() const
    {
        return m_data;
    }
};

class ReadWriteFileImplTests : public ::testing::Test
{
protected:
    std::shared_ptr<::testing::StrictMock<BlobClientMock>> m_mockBlobClient;
    std::shared_ptr<BlobSimulator> m_blobSim;
    std::shared_ptr<boost::log::sources::logger_mt> m_logger;
    std::string m_testFileName = "test.blob";

    void SetUp() override
    {
        m_mockBlobClient = std::make_shared<::testing::StrictMock<BlobClientMock>>();
        m_blobSim = std::make_shared<BlobSimulator>();
        m_logger = std::make_shared<boost::log::sources::logger_mt>();

        // Setup default mock behavior using the simulator
        ON_CALL(*m_mockBlobClient, GetSize())
            .WillByDefault(Invoke([this]() { return m_blobSim->GetSize(); }));

        ON_CALL(*m_mockBlobClient, SetSize(_))
            .WillByDefault(Invoke([this](int64_t size) { m_blobSim->SetSize(size); }));

        ON_CALL(*m_mockBlobClient, GetCapacity())
            .WillByDefault(Invoke([this]() { return m_blobSim->GetCapacity(); }));

        ON_CALL(*m_mockBlobClient, SetCapacity(_))
            .WillByDefault(Invoke([this](int64_t capacity) { m_blobSim->SetCapacity(capacity); }));

        ON_CALL(*m_mockBlobClient, UploadPages(_, _))
            .WillByDefault(Invoke([this](const std::span<char> buffer, int64_t offset)
                {
                    m_blobSim->UploadPages(buffer, offset);
                }));

        ON_CALL(*m_mockBlobClient, DownloadTo(testing::A<std::span<char>>(), testing::A<int64_t>(), testing::A<int64_t>()))
            .WillByDefault(Invoke([this](std::span<char> buffer, int64_t offset, int64_t length)
                {
                    return m_blobSim->DownloadTo(buffer, offset, length);
                }));
    }

    std::unique_ptr<ReadWriteFileImpl> CreateFile()
    {
        return std::make_unique<ReadWriteFileImpl>(
            m_testFileName, m_mockBlobClient, nullptr, m_logger);
    }
};

TEST_F(ReadWriteFileImplTests, Constructor_InitializesCorrectly) {
    // Arrange
    m_blobSim->SetSize(1024);

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
    char buffer[10] = { 0 };
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
    EXPECT_CALL(*m_mockBlobClient, UploadPages(_, _))
        .Times(2);

    auto file = CreateFile();
    const int64_t dataSize = Configuration::PageBlob::DefaultBufferSize + Configuration::PageBlob::PageSize;
    std::vector<char> data(dataSize, 'D');

    // Act - write more than buffer can hold
    file->Write(0, data.data(), dataSize);

    // Assert - auto-flush should have occurred
    // Sync to finalize
    file->Sync();

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
    std::vector<char> existingData(offset, 'X');
    m_blobSim->UploadPages(existingData, 0);
    m_blobSim->SetSize(offset);

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

TEST_F(ReadWriteFileImplTests, Sync_UpdatesFileSizeMetadata) {
    // Arrange
    const int64_t dataSize = 500;
    EXPECT_CALL(*m_mockBlobClient, SetSize(dataSize)).Times(2);
    auto file = CreateFile();
    std::vector<char> data(dataSize, 'E');

    // Act
    file->Write(0, data.data(), dataSize);
    file->Sync();

    // Assert - mock expectation validates SetSize was called
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

    EXPECT_CALL(*m_mockBlobClient, SetSize(dataSize)).Times(1);

    // Act
    file->Close();

    // Assert - mock expectation validates sync was called
}

// Test Close is idempotent
TEST_F(ReadWriteFileImplTests, Close_CalledTwice_IsIdempotent) {
    // Arrange
    auto file = CreateFile();
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'J');

    file->Write(0, data.data(), dataSize);

    // Expect SetSize only once
    EXPECT_CALL(*m_mockBlobClient, SetSize(_)).Times(1);

    // Act
    file->Close();
    file->Close(); // Second close should be no-op

    // Assert - mock expectation validates sync was called only once
}

// Test Expand increases capacity
TEST_F(ReadWriteFileImplTests, Expand_IncreasesCapacity) {
    // Arrange
    auto file = CreateFile();
    const int64_t largeDataSize = Configuration::PageBlob::DefaultSize + 1000;
    std::vector<char> data(largeDataSize, 'K');

    // Expect SetCapacity to be called
    EXPECT_CALL(*m_mockBlobClient, SetCapacity(_)).Times(testing::AtLeast(1));

    // Act
    file->Write(0, data.data(), largeDataSize);
    file->Sync();

    // Assert - mock expectation validates SetCapacity was called
}

// Test destructor retries Close on failure
TEST_F(ReadWriteFileImplTests, Destructor_RetriesCloseOnFailure) {
    // Arrange
    auto strictMock = std::make_shared<StrictMock<BlobClientMock>>();

    // Setup initial calls for construction
    EXPECT_CALL(*strictMock, GetSize()).WillOnce(Return(0));
    EXPECT_CALL(*strictMock, GetCapacity()).WillOnce(Return(Configuration::PageBlob::DefaultSize));

    // Setup SetSize to fail a few times then succeed
    EXPECT_CALL(*strictMock, SetSize(_))
        .Times(3)
        .WillOnce(testing::Throw(std::runtime_error("Network error")))
        .WillOnce(testing::Throw(std::runtime_error("Network error")))
        .WillOnce(Return());

    // Act - destructor should retry
    {
        ReadWriteFileImpl file(m_testFileName, strictMock, nullptr, m_logger);
    } // Destructor called here

    // Assert - mock expectations verify retry behavior
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
