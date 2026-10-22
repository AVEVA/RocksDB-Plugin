// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlobWriter.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include "FakeBlobEnvironment.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <thread>

using namespace AVEVA::RocksDB::Plugin::Azure::Impl;
using namespace AVEVA::RocksDB::Plugin::Core;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobServer;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadableFile;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadWriteFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeSequentialFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeWriteableFileImpl;

class ReadWriteFileImplTests : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_env;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    std::string m_testFileName = "test.blob";

    void SetUp() override {
        m_logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
        m_env.Blob->SetCapacity(Configuration::PageBlob::DefaultSize);
    }

    std::unique_ptr<FakeReadWriteFileImpl> CreateFile() {
        return std::make_unique<FakeReadWriteFileImpl>(m_testFileName, m_env.Runtime, m_env.Client, nullptr, m_logger);
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

TEST_F(ReadWriteFileImplTests, WriteAndRead_RangeOverflowingInt64_ThrowInvalidArgument) {
    const auto file = CreateFile();
    char byte = 'x';

    EXPECT_THROW(file->Write(std::numeric_limits<int64_t>::max(), &byte, 1), std::invalid_argument);
    EXPECT_THROW(file->Write(-1, &byte, 1), std::invalid_argument);
    EXPECT_THROW([[maybe_unused]] auto n = file->Read(std::numeric_limits<int64_t>::max() - 1, 2, &byte),
                 std::invalid_argument);
}

TEST_F(ReadWriteFileImplTests, Flush_EndNearInt64Max_ThrowsInvalidArgumentWithoutResizing) {
    const auto file = CreateFile();
    char byte = 'x';
    const auto capacityWritesBefore = m_env.Blob->CapacityWrites.size();

    file->Write(std::numeric_limits<int64_t>::max() - 1, &byte, 1);

    EXPECT_THROW(file->Sync(), std::invalid_argument);
    EXPECT_EQ(capacityWritesBefore, m_env.Blob->CapacityWrites.size());
}
TEST_F(ReadWriteFileImplTests, Flush_UploadFails_RetryKeepsSizeAndPersistsContent) {
    auto file = CreateFile();
    const std::vector<char> data(100, 'Z');
    file->Write(0, data.data(), 100);
    auto failures = std::make_shared<std::atomic<int>>(1);
    m_env.Blob->Fault = [failures](const auto operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::UploadPages && failures->fetch_sub(1) > 0) {
            return FakeBlobServer::Error(500, "boom");
        }
        return std::nullopt;
    };

    EXPECT_ANY_THROW(file->Sync());
    std::vector<char> readBuffer(100, '\0');
    EXPECT_NO_THROW(file->Sync());

    EXPECT_EQ(100, file->Read(0, 100, readBuffer.data()));
    EXPECT_EQ(data, readBuffer);
    EXPECT_EQ(100, m_env.Blob->Size);
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

TEST_F(ReadWriteFileImplTests, Flush_ShortDownloadOfAnExistingGap_ThrowsAndUploadsNothing) {
    constexpr int64_t offset = 100;
    std::fill_n(m_env.Blob->Data.begin(), offset, 'X');
    m_env.Blob->Size = offset;
    auto file = CreateFile();
    // The blob ends before the size recorded when the file was opened, so the gap download is legitimately short.
    m_env.Blob->Data.resize(10);
    const std::vector<char> newData(100, 'Y');

    file->Write(offset, newData.data(), 100);

    EXPECT_THROW(file->Sync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
    EXPECT_TRUE(m_env.Blob->Uploads.empty());
}

TEST_F(ReadWriteFileImplTests, Flush_WritesOnDifferentPages_EachMergesItsOwnExistingData) {
    // Arrange - existing data on three separate pages
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    const int64_t total = 3 * page;
    std::fill_n(m_env.Blob->Data.begin(), total, 'X');
    m_env.Blob->Size = total;
    auto file = CreateFile();

    // Act - a small write in the middle of each page, so every chunk needs pre and post padding
    const std::string payload(10, 'Y');
    for (int64_t pageIndex = 0; pageIndex < 3; ++pageIndex) {
        file->Write(pageIndex * page + 100, payload.data(), static_cast<int64_t>(payload.size()));
    }
    file->Sync();

    // Assert
    std::vector<char> actual(static_cast<size_t>(total));
    ASSERT_EQ(total, file->Read(0, total, actual.data()));
    std::vector<char> expected(static_cast<size_t>(total), 'X');
    for (int64_t pageIndex = 0; pageIndex < 3; ++pageIndex) {
        std::fill_n(expected.begin() + pageIndex * page + 100, payload.size(), 'Y');
    }
    EXPECT_EQ(expected, actual);
}

TEST_F(ReadWriteFileImplTests, Flush_WritesSharingAPage_KeepEarlierWrites) {
    // Arrange
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    std::fill_n(m_env.Blob->Data.begin(), page, 'X');
    m_env.Blob->Size = page;
    auto file = CreateFile();

    // Act - two writes inside the same page: the second chunk's padding must include the first chunk's bytes
    const std::string first(10, 'A');
    const std::string second(10, 'B');
    file->Write(100, first.data(), static_cast<int64_t>(first.size()));
    file->Write(200, second.data(), static_cast<int64_t>(second.size()));
    file->Sync();

    // Assert
    std::vector<char> actual(static_cast<size_t>(page));
    ASSERT_EQ(page, file->Read(0, page, actual.data()));
    std::vector<char> expected(static_cast<size_t>(page), 'X');
    std::fill_n(expected.begin() + 100, first.size(), 'A');
    std::fill_n(expected.begin() + 200, second.size(), 'B');
    EXPECT_EQ(expected, actual);
}

TEST_F(ReadWriteFileImplTests, Flush_WritesSharingAPage_AreMergedIntoOneUploadAndOnlyTheGapsAreRead) {
    // Arrange
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    std::fill_n(m_env.Blob->Data.begin(), 2 * page, 'X');
    m_env.Blob->Size = 2 * page;
    auto file = CreateFile();

    // Act - two writes in page 0 and one in page 1: all three share one page-aligned extent
    const std::string payload(10, 'A');
    file->Write(100, payload.data(), static_cast<int64_t>(payload.size()));
    file->Write(200, payload.data(), static_cast<int64_t>(payload.size()));
    file->Write(page + 50, payload.data(), static_cast<int64_t>(payload.size()));
    file->Sync();

    // Assert
    ASSERT_EQ(1u, m_env.Blob->Uploads.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(static_cast<size_t>(2 * page), m_env.Blob->Uploads[0].Data.size());
    // The gaps are close together, so one read covers them; the writes are overlaid afterwards.
    EXPECT_EQ(1u, m_env.Blob->Downloads.size());
    std::vector<char> actual(static_cast<size_t>(2 * page));
    ASSERT_EQ(2 * page, file->Read(0, 2 * page, actual.data()));
    std::vector<char> expected(static_cast<size_t>(2 * page), 'X');
    std::fill_n(expected.begin() + 100, payload.size(), 'A');
    std::fill_n(expected.begin() + 200, payload.size(), 'A');
    std::fill_n(expected.begin() + page + 50, payload.size(), 'A');
    EXPECT_EQ(expected, actual);
}

// Many tiny writes scattered over one merged extent leave a gap between each pair; they must be read with a bounded
// number of requests, and the buffered writes must still win over the bytes the wider read returned.
TEST_F(ReadWriteFileImplTests, Flush_ManySmallWritesAcrossPages_CoalesceGapReadsAndKeepLastWriteWins) {
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    constexpr int64_t pages = 64;
    std::vector<char> original(static_cast<size_t>(pages * page));
    for (size_t i = 0; i < original.size(); ++i) {
        original[i] = static_cast<char>('a' + i % 23);
    }
    std::ranges::copy(original, m_env.Blob->Data.begin());
    m_env.Blob->Size = pages * page;
    auto file = CreateFile();

    auto expected = original;
    for (int64_t p = 0; p < pages; ++p) {
        const std::string first(10, 'A');
        const std::string second(4, 'B');
        file->Write(p * page + 100, first.data(), static_cast<int64_t>(first.size()));
        file->Write(p * page + 104, second.data(), static_cast<int64_t>(second.size())); // overlaps: later wins
        std::ranges::fill_n(expected.begin() + p * page + 100, first.size(), 'A');
        std::ranges::fill_n(expected.begin() + p * page + 104, second.size(), 'B');
    }
    file->Sync();

    EXPECT_LE(m_env.Blob->Downloads.size(), 2u);
    ASSERT_EQ(1u, m_env.Blob->Uploads.size());
    std::vector<char> actual(expected.size());
    ASSERT_EQ(pages * page, file->Read(0, pages * page, actual.data()));
    EXPECT_EQ(expected, actual);
}

TEST_F(ReadWriteFileImplTests, Flush_Coalesced_ReadsStillFailOnShortDownload) {
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    m_env.Blob->Size = 4 * page;
    auto file = CreateFile();
    m_env.Blob->Data.resize(static_cast<size_t>(page));
    const std::string payload(10, 'Y');
    file->Write(100, payload.data(), 10);
    file->Write(page + 100, payload.data(), 10);

    EXPECT_THROW(file->Sync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
    EXPECT_TRUE(m_env.Blob->Uploads.empty());
}

TEST_F(ReadWriteFileImplTests, Flush_ReusedBufferDoesNotLeakEarlierBytesIntoUnwrittenGaps) {
    // Arrange
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    auto file = CreateFile();
    const std::string first(static_cast<size_t>(page), 'A');
    file->Write(0, first.data(), page);
    file->Sync();

    // Act - a later flush past the old end leaves gaps that must read as zero, not as the first flush's 'A's
    const std::string payload(10, 'B');
    file->Write(2 * page + 10, payload.data(), static_cast<int64_t>(payload.size()));
    file->Sync();

    // Assert
    const auto length = page + 10 + static_cast<int64_t>(payload.size());
    std::vector<char> actual(static_cast<size_t>(length));
    ASSERT_EQ(length, file->Read(page, length, actual.data()));
    std::vector<char> expected(static_cast<size_t>(length), '\0');
    std::fill_n(expected.begin() + page + 10, payload.size(), 'B');
    EXPECT_EQ(expected, actual);
}

TEST_F(ReadWriteFileImplTests, Flush_OverlappingWritesToOnePage_LastWriteWins) {
    // Arrange
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    std::fill_n(m_env.Blob->Data.begin(), page, 'X');
    m_env.Blob->Size = page;
    auto file = CreateFile();

    // Act
    const std::string first(50, 'A');
    const std::string second(20, 'B');
    file->Write(100, first.data(), static_cast<int64_t>(first.size()));
    file->Write(120, second.data(), static_cast<int64_t>(second.size()));
    file->Sync();

    // Assert
    EXPECT_EQ(1u, m_env.Blob->Uploads.size());
    std::vector<char> actual(static_cast<size_t>(page));
    ASSERT_EQ(page, file->Read(0, page, actual.data()));
    std::vector<char> expected(static_cast<size_t>(page), 'X');
    std::fill_n(expected.begin() + 100, first.size(), 'A');
    std::fill_n(expected.begin() + 120, second.size(), 'B');
    EXPECT_EQ(expected, actual);
}

TEST_F(ReadWriteFileImplTests, Flush_AppendingPastTheEnd_ReadsNothing) {
    // Arrange
    auto file = CreateFile();

    // Act - the file is empty, so the unwritten bytes around the data hold nothing worth reading
    const std::string payload(300, 'A');
    file->Write(0, payload.data(), static_cast<int64_t>(payload.size()));
    file->Sync();

    // Assert
    EXPECT_TRUE(m_env.Blob->Downloads.empty());
    EXPECT_EQ(1u, m_env.Blob->Uploads.size());
}

TEST_F(ReadWriteFileImplTests, Flush_WritesOnDifferentPages_AreUploadedConcurrently) {
    // Arrange
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    std::fill_n(m_env.Blob->Data.begin(), 5 * page, 'X');
    m_env.Blob->Size = 5 * page;
    auto file = CreateFile();
    const std::string payload(10, 'A');
    // Every other page, so the extents are not adjacent and stay separate requests
    for (int64_t i = 0; i < 3; ++i) {
        file->Write(2 * i * page + 10, payload.data(), static_cast<int64_t>(payload.size()));
    }
    m_env.Blob->DeferUploads = true;

    // Act - the flush blocks until the uploads complete, so it runs on its own thread
    auto sync = std::async(std::launch::async, [&] { file->Sync(); });
    // Releases whatever is held however the test exits, so a failed assertion cannot leave Sync (and the future's
    // destructor) blocked forever.
    struct Release {
        FakeBlobServer& Blob;
        ~Release() {
            Blob.DeferUploads = false;
            while (Blob.PendingUploads() > 0) {
                Blob.CompleteUpload();
            }
        }
    } release{*m_env.Blob};
    const bool allHeld = m_env.Blob->WaitForPendingUploads(3);

    // Assert - all three were in flight at once, before any completed
    ASSERT_TRUE(allHeld);
    ASSERT_EQ(3u, m_env.Blob->PendingUploads());
    m_env.Blob->DeferUploads = false;
    for (size_t i = 0; i < 3; ++i) {
        m_env.Blob->CompleteUpload();
    }
    EXPECT_NO_THROW(sync.get());
    EXPECT_EQ(3u, m_env.Blob->Uploads.size());
}

TEST_F(ReadWriteFileImplTests, Flush_ManyExtents_ReusesBuffersAcrossWavesAndKeepsThePoolBounded) {
    // Arrange
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    constexpr auto extentCount = 3 * Configuration::Transfer::MaxConcurrentPageCopies;
    std::fill_n(m_env.Blob->Data.begin(), 2 * extentCount * page, 'X');
    m_env.Blob->Size = static_cast<int64_t>(2 * extentCount) * page;
    auto file = CreateFile();
    const std::string payload(10, 'A');
    const auto writeEveryOtherPage = [&] {
        for (int64_t i = 0; i < static_cast<int64_t>(extentCount); ++i) {
            file->Write(2 * i * page + 10, payload.data(), static_cast<int64_t>(payload.size()));
        }
    };

    // Act / Assert - later waves reuse the buffers of earlier ones, on a clean flush
    writeEveryOtherPage();
    file->Sync();
    EXPECT_LE(file->PooledExtentBufferCount(), Configuration::Transfer::MaxConcurrentPageCopies);
    writeEveryOtherPage();
    file->Sync();
    EXPECT_LE(file->PooledExtentBufferCount(), Configuration::Transfer::MaxConcurrentPageCopies);

    // and on a flush whose later wave fails
    bool fail = false;
    size_t uploads = 0;
    m_env.Blob->Fault =
        [&](FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::UploadPages &&
            ++uploads > Configuration::Transfer::MaxConcurrentPageCopies) {
            fail = true;
            return FakeBlobServer::Error(400, "InvalidInput");
        }
        return std::nullopt;
    };
    writeEveryOtherPage();
    EXPECT_ANY_THROW(file->Sync());
    EXPECT_TRUE(fail);
    EXPECT_LE(file->PooledExtentBufferCount(), Configuration::Transfer::MaxConcurrentPageCopies);

    m_env.Blob->Fault = nullptr;
    EXPECT_NO_THROW(file->Sync());
    EXPECT_LE(file->PooledExtentBufferCount(), Configuration::Transfer::MaxConcurrentPageCopies);
}

TEST_F(ReadWriteFileImplTests, Flush_UploadFailure_KeepsTheBufferedWritesForARetry) {
    // Arrange
    constexpr int64_t page = Configuration::PageBlob::PageSize;
    std::fill_n(m_env.Blob->Data.begin(), 2 * page, 'X');
    m_env.Blob->Size = 2 * page;
    auto file = CreateFile();
    const std::string payload(10, 'A');
    file->Write(10, payload.data(), static_cast<int64_t>(payload.size()));
    file->Write(page + 10, payload.data(), static_cast<int64_t>(payload.size()));
    bool fail = true;
    m_env.Blob->Fault =
        [&](FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (fail && operation == FakeBlobServer::Operation::UploadPages) {
            return FakeBlobServer::Error(400, "InvalidInput");
        }
        return std::nullopt;
    };

    // Act
    EXPECT_ANY_THROW(file->Sync());
    fail = false;
    file->Sync();

    // Assert
    std::vector<char> actual(static_cast<size_t>(2 * page));
    ASSERT_EQ(2 * page, file->Read(0, 2 * page, actual.data()));
    std::vector<char> expected(static_cast<size_t>(2 * page), 'X');
    std::fill_n(expected.begin() + 10, payload.size(), 'A');
    std::fill_n(expected.begin() + page + 10, payload.size(), 'A');
    EXPECT_EQ(expected, actual);
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

TEST_F(ReadWriteFileImplTests, Sync_WithNothingNewToPublish_SendsNoMetadataWriteAndKeepsEtag) {
    // Arrange
    auto file = CreateFile();
    std::vector<char> data(500, 'F');
    file->Write(0, data.data(), static_cast<int64_t>(data.size()));
    file->Sync();
    const auto etag = m_env.Blob->ETag;
    ASSERT_EQ(1U, m_env.Blob->SizeWrites.size());

    // Act
    file->Sync();
    file->Close();

    // Assert
    EXPECT_EQ(1U, m_env.Blob->SizeWrites.size());
    EXPECT_EQ(etag, m_env.Blob->ETag);
}

TEST_F(ReadWriteFileImplTests, Sync_AfterWriteThatChangesSize_PublishesNewSize) {
    // Arrange
    auto file = CreateFile();
    std::vector<char> data(500, 'G');
    file->Write(0, data.data(), static_cast<int64_t>(data.size()));
    file->Sync();

    // Act
    file->Write(500, data.data(), static_cast<int64_t>(data.size()));
    file->Sync();

    // Assert
    EXPECT_EQ((std::vector<int64_t>{500, 1000}), m_env.Blob->SizeWrites);
}

TEST_F(ReadWriteFileImplTests, Read_AfterWriteWithoutSync_ReturnsTheUnsyncedBytes) {
    // Arrange
    auto file = CreateFile();
    std::vector<char> data(1000, 'U');
    file->Write(0, data.data(), static_cast<int64_t>(data.size()));

    // Act
    std::vector<char> readBuffer(1000);
    const auto bytesRead = file->Read(0, 1000, readBuffer.data());

    // Assert
    EXPECT_EQ(1000, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

TEST_F(ReadWriteFileImplTests, Read_OverwriteWithoutSync_ReturnsTheNewBytes) {
    // Arrange
    auto file = CreateFile();
    std::vector<char> original(1000, 'A');
    file->Write(0, original.data(), static_cast<int64_t>(original.size()));
    file->Sync();
    std::vector<char> patch(100, 'B');
    file->Write(450, patch.data(), static_cast<int64_t>(patch.size()));

    // Act
    std::vector<char> readBuffer(1000);
    const auto bytesRead = file->Read(0, 1000, readBuffer.data());

    // Assert
    auto expected = original;
    std::ranges::copy(patch, expected.begin() + 450);
    EXPECT_EQ(1000, bytesRead);
    EXPECT_EQ(expected, readBuffer);
}

TEST_F(ReadWriteFileImplTests, Read_AfterUnsyncedReadThenSync_PublishesTheSize) {
    // Arrange
    auto file = CreateFile();
    std::vector<char> data(300, 'S');
    file->Write(0, data.data(), static_cast<int64_t>(data.size()));
    std::vector<char> readBuffer(300);
    ASSERT_EQ(300, file->Read(0, 300, readBuffer.data()));

    // Act
    file->Sync();

    // Assert
    EXPECT_EQ(300, m_env.Blob->Size);
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

// Test move constructor
TEST_F(ReadWriteFileImplTests, MoveConstructor_TransfersOwnership) {
    // Arrange
    auto file1 = CreateFile();
    const int64_t dataSize = 100;
    std::vector<char> data(dataSize, 'L');
    file1->Write(0, data.data(), dataSize);

    // Act
    FakeReadWriteFileImpl file2(std::move(*file1));
    file2.Sync();

    // Assert - data should be accessible from moved object
    std::vector<char> readBuffer(dataSize);
    auto bytesRead = file2.Read(0, dataSize, readBuffer.data());
    EXPECT_EQ(dataSize, bytesRead);
    EXPECT_EQ(data, readBuffer);
}

// Assignment would drop buffered data and in-flight uploads, so it is deleted.
TEST_F(ReadWriteFileImplTests, MoveAssignment_IsDeleted) {
    static_assert(!std::is_move_assignable_v<FakeReadWriteFileImpl>);
    static_assert(!std::is_copy_assignable_v<FakeReadWriteFileImpl>);
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
TEST_F(ReadWriteFileImplTests, DestructorWithoutCloseDoesNotRetryOrPersistBufferedData) {
    int sizeWriteAttempts = 0;
    m_env.Blob->Fault =
        [&sizeWriteAttempts](
            const FakeBlobServer::Operation operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::SetMetadata) {
            ++sizeWriteAttempts;
        }
        return std::nullopt;
    };

    const auto start = std::chrono::steady_clock::now();
    {
        FakeReadWriteFileImpl file(m_testFileName, m_env.Runtime, m_env.Client, nullptr, m_logger);
        file.Write(0, "data", 4);
    }

    EXPECT_EQ(0, sizeWriteAttempts);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(700));
}

TEST_F(ReadWriteFileImplTests, DestructorAfterExplicitCloseIsQuiet) {
    {
        auto file = CreateFile();
        file->Write(0, "data", 4);
        file->Close();
    }
    EXPECT_EQ(1U, m_env.Blob->SizeWrites.size());
}

TEST(PageBlobWriterTests, EnsureCapacityHonoursTheRequirementAndDoublesOtherwise) {
    FakeBlobEnvironment env;
    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    AVEVA::RocksDB::Plugin::Azure::Impl::PageBlobWriter<std::remove_reference_t<decltype(*env.Client)>> writer(
        "f", env.Runtime, env.Client, nullptr, logger, 0);
    EXPECT_EQ(writer.Capacity(), 0);
    const auto huge = Configuration::PageBlob::DefaultSize * 3 + 1;
    writer.EnsureCapacity(huge);
    EXPECT_GE(writer.Capacity(), huge);
    EXPECT_EQ(writer.Capacity() % 512, 0);
    const auto before = writer.Capacity();
    writer.EnsureCapacity(before - 1);
    EXPECT_EQ(writer.Capacity(), before);
    writer.EnsureCapacity(before + 1);
    EXPECT_GE(writer.Capacity(), before * 2);
}
TEST(PageBlobWriterTests, GrowthTarget_NearInt64Max_SaturatesInsteadOfOverflowing) {
    using Writer = AVEVA::RocksDB::Plugin::Azure::Impl::PageBlobWriter<
        std::remove_reference_t<decltype(*FakeBlobEnvironment{}.Client)>>;
    constexpr auto max = std::numeric_limits<int64_t>::max();

    EXPECT_EQ(max, Writer::GrowthTarget(1, max / 2 + 1));
    EXPECT_EQ(max, Writer::GrowthTarget(max, max));
    EXPECT_EQ(Configuration::PageBlob::DefaultSize * 4,
              Writer::GrowthTarget(1, Configuration::PageBlob::DefaultSize * 2));
    EXPECT_EQ(max - 5, Writer::GrowthTarget(max - 5, 10));
}
