// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"

#include "FakeBlobEnvironment.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <set>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::WriteableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakePageBlobClient;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;

namespace {
std::vector<char> Prefix(const std::vector<char>& data, const size_t size) {
    return {data.begin(), data.begin() + static_cast<std::ptrdiff_t>(size)};
}
} // namespace

class WriteableFileTests : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_env;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger =
        std::make_shared<severity_logger_mt<severity_level>>();

    WriteableFileImpl MakeFile(const int64_t bufferSize = Configuration::PageBlob::DefaultBufferSize) {
        return {"test.dat", m_env.Runtime, m_env.Blob, nullptr, m_logger, bufferSize};
    }

    WriteableFileImpl MakeKnownStateFile(const WriteableFileImpl::BlobState state,
                                         const int64_t bufferSize = Configuration::PageBlob::DefaultBufferSize) {
        return {"test.dat", m_env.Runtime, m_env.Blob, nullptr, m_logger, bufferSize, state};
    }
};

TEST_F(WriteableFileTests, Flush_ReusesBuffersAndKeepsTheTailAcrossTheSwap) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(64 * pageSize);

    std::vector<char> expected;
    for (int64_t i = 0; i < 3 * 4 * pageSize + 100; ++i) {
        expected.push_back(static_cast<char>('a' + (i * 7) % 26));
    }

    auto file = MakeFile(4 * pageSize);
    file.Append(expected);
    file.Sync();

    EXPECT_EQ(expected, Prefix(m_env.Blob->Data, expected.size()));
    ASSERT_EQ(4U, m_env.Blob->SharedUploadPayloadAddresses.size());
    const std::set<const char*> distinct(m_env.Blob->SharedUploadPayloadAddresses.begin(),
                                         m_env.Blob->SharedUploadPayloadAddresses.end());
    EXPECT_LE(distinct.size(), 2U) << "uploads should alternate between two pooled buffers";
}

TEST_F(WriteableFileTests, RangeSync_PartialPageThenMoreData_UploadsNeverOverlapAndLandInAnyOrder) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(16 * pageSize);
    m_env.Blob->DeferUploads = true;

    std::vector<char> expected;
    for (int64_t i = 0; i < pageSize + 200; ++i) {
        expected.push_back(static_cast<char>('a' + i % 26));
    }

    auto file = MakeFile();
    file.Append(std::span<const char>(expected.data(), static_cast<size_t>(pageSize + 100)));
    file.RangeSync();
    file.Append(std::span<const char>(expected.data() + pageSize + 100, 100));
    file.RangeSync();

    for (size_t i = 0; i < m_env.Blob->Uploads.size(); ++i) {
        for (size_t j = i + 1; j < m_env.Blob->Uploads.size(); ++j) {
            const auto& a = m_env.Blob->Uploads[i];
            const auto& b = m_env.Blob->Uploads[j];
            const auto aEnd = a.Offset + static_cast<int64_t>(a.Data.size());
            const auto bEnd = b.Offset + static_cast<int64_t>(b.Data.size());
            EXPECT_TRUE(aEnd <= b.Offset || bEnd <= a.Offset) << "uploads " << i << " and " << j << " overlap";
        }
    }

    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    m_env.Blob->CompleteUpload(0);
    m_env.Blob->DeferUploads = false;
    file.Sync();

    EXPECT_EQ(expected, Prefix(m_env.Blob->Data, expected.size()));
}

TEST_F(WriteableFileTests, Destructor_UploadsStillInFlightAfterFailure_WaitsForThem) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(16 * pageSize);
    m_env.Blob->DeferUploads = true;

    auto file = std::make_unique<WriteableFileImpl>("test.dat", m_env.Runtime, m_env.Blob, nullptr, m_logger);
    for (int i = 0; i < 4; ++i) {
        file->Append(std::vector<char>(pageSize, 'a'));
        file->RangeSync();
    }
    ASSERT_EQ(4U, m_env.Blob->PendingUploads());

    file->Append(std::vector<char>(pageSize, 'b'));
    m_env.Blob->FailUpload(FakePageBlobClient::Error(500, "upload failed"), 0);
    ASSERT_THROW(file->RangeSync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);

    std::atomic<bool> destroyed = false;
    std::thread destroyer([&] {
        file.reset();
        destroyed = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(destroyed);
    while (m_env.Blob->PendingUploads() > 0) {
        m_env.Blob->CompleteUpload(0);
    }
    destroyer.join();
    EXPECT_TRUE(destroyed);
}

TEST_F(WriteableFileTests, RangeSync_UploadInFlight_ReturnsWithoutWaitingAndSyncCompletesIt) {
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile();
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize, 'a'));
    file.RangeSync();

    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    m_env.Blob->CompleteUpload();
    EXPECT_NO_THROW(file.Sync());
}

TEST_F(WriteableFileTests, Sync_UploadFailed_ThrowsAndKeepsThrowing) {
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile();
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize, 'a'));
    file.RangeSync();
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    m_env.Blob->FailUpload(FakePageBlobClient::Error(500, "upload failed"));

    EXPECT_THROW(file.Sync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
    EXPECT_THROW(file.Sync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
}

TEST_F(WriteableFileTests, AppendBytes_LessThanAPage_PageWritten) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);

    {
        auto file = MakeFile();
        std::string_view data = "1";
        file.Append(data);
        ASSERT_EQ(data.size(), static_cast<size_t>(file.GetFileSize()));
    }

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(Configuration::PageBlob::PageSize, m_env.Blob->Uploads[0].Data.size());
}

TEST_F(WriteableFileTests, AppendBytes_EqualToAPage_PageWritten) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    const std::vector<char> expected(Configuration::PageBlob::PageSize, 'a');

    {
        auto file = MakeFile();
        file.Append(expected);
        ASSERT_EQ(expected.size(), static_cast<size_t>(file.GetFileSize()));
    }

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(expected, m_env.Blob->Uploads[0].Data);
}

TEST_F(WriteableFileTests, AppendBytes_MoreThanAPage_2PagesWritten) {
    constexpr size_t dataSize = Configuration::PageBlob::PageSize + 3;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize * 2);
    const std::vector<char> expected(dataSize, 'b');

    {
        auto file = MakeFile();
        file.Append(expected);
        ASSERT_EQ(expected.size(), static_cast<size_t>(file.GetFileSize()));
    }

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(Configuration::PageBlob::PageSize * 2, m_env.Blob->Uploads[0].Data.size());
    EXPECT_EQ(expected, Prefix(m_env.Blob->Uploads[0].Data, expected.size()));
}

TEST_F(WriteableFileTests, Constructor_PartialPageInBlob_DataDownloaded) {
    constexpr size_t partialPageSize = 333;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    std::fill_n(m_env.Blob->Data.begin(), partialPageSize, 'p');
    m_env.Blob->Size = partialPageSize;

    const auto file = MakeFile();

    EXPECT_EQ(partialPageSize, static_cast<size_t>(file.GetFileSize()));
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(0, m_env.Blob->Downloads[0].Offset);
    EXPECT_EQ(partialPageSize, m_env.Blob->Downloads[0].Length);
}

TEST_F(WriteableFileTests, Constructor_PartialLastPageInBlob_DataDownloaded) {
    constexpr size_t partialPageSize = 333;
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(pageSize * 2);
    std::fill_n(m_env.Blob->Data.begin() + pageSize, partialPageSize, 'p');
    m_env.Blob->Size = pageSize + partialPageSize;

    const auto file = MakeFile();

    ASSERT_EQ(pageSize + partialPageSize, file.GetFileSize());
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(pageSize, m_env.Blob->Downloads[0].Offset);
    EXPECT_EQ(partialPageSize, m_env.Blob->Downloads[0].Length);
}

TEST_F(WriteableFileTests, Append_LessThanAPage_UploadPagesNotCalled) {
    auto file = MakeFile(Configuration::PageBlob::PageSize * 2);

    constexpr size_t partialPageSize = 333;
    std::vector<char> dataToAppend(partialPageSize, 'p');
    file.Append(dataToAppend);

    ASSERT_EQ(dataToAppend.size(), static_cast<size_t>(file.GetFileSize()));
    EXPECT_TRUE(m_env.Blob->Uploads.empty());
}

TEST_F(WriteableFileTests, Append_MultipleWritesLargerThanPage_UploadPagesCalled) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(pageSize * 4);
    std::vector<char> dataToAppend1(pageSize + 1, 'p');
    std::vector<char> dataToAppend2(pageSize + 1, 'z');

    {
        auto file = MakeFile(pageSize * 2);
        file.Append(dataToAppend1);
        file.Append(dataToAppend2);
        ASSERT_EQ(dataToAppend1.size() + dataToAppend2.size(), static_cast<size_t>(file.GetFileSize()));
    }

    ASSERT_EQ(2U, m_env.Blob->Uploads.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(pageSize, static_cast<int64_t>(m_env.Blob->Uploads[0].Data.size()));
    EXPECT_EQ(Prefix(dataToAppend1, pageSize), m_env.Blob->Uploads[0].Data);

    EXPECT_EQ(pageSize, m_env.Blob->Uploads[1].Offset);
    EXPECT_EQ(pageSize * 2, static_cast<int64_t>(m_env.Blob->Uploads[1].Data.size()));
    auto expected = std::vector<char>{dataToAppend1.back()};
    expected.insert(expected.end(), dataToAppend2.begin(), dataToAppend2.end());
    EXPECT_EQ(expected, Prefix(m_env.Blob->Uploads[1].Data, expected.size()));
}

TEST_F(WriteableFileTests, Append_ExceedsCapacity_SetCapacityCalled) {
    constexpr int64_t initialCapacity = Configuration::PageBlob::PageSize * 2;
    m_env.Blob->SetCapacity(initialCapacity);

    auto file = MakeFile(Configuration::PageBlob::PageSize * 2);
    std::vector<char> dataToAppend(initialCapacity + Configuration::PageBlob::PageSize, 'x');
    file.Append(dataToAppend);
    file.Sync();

    ASSERT_EQ(1U, m_env.Blob->CapacityWrites.size());
    EXPECT_GT(m_env.Blob->CapacityWrites.back(), initialCapacity);
}

TEST_F(WriteableFileTests, Append_AfterTruncateToZero_CapacityCoversPendingWrite) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(pageSize);
    m_env.Blob->Size = pageSize;

    auto file = MakeFile();
    file.Truncate(0);
    file.Append(std::vector<char>(pageSize * 3, 'x'));
    file.Sync();

    EXPECT_EQ(pageSize * 3, file.GetFileSize());
    EXPECT_GE(m_env.Blob->Data.size(), static_cast<size_t>(pageSize * 3));
    EXPECT_GE(m_env.Blob->CapacityWrites.back(), pageSize * 3);
}

TEST_F(WriteableFileTests, Append_LargerThanTwiceCapacity_CapacityCoversPendingWrite) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(pageSize);

    auto file = MakeFile(pageSize * 64);
    file.Append(std::vector<char>(pageSize * 40, 'x'));
    file.Sync();

    EXPECT_EQ(pageSize * 40, file.GetFileSize());
    EXPECT_GE(m_env.Blob->CapacityWrites.back(), pageSize * 40);
}

TEST_F(WriteableFileTests, Constructor_KnownState_DoesNotQueryBlob) {
    const WriteableFileImpl file{"test.dat",
                                 m_env.Runtime,
                                 m_env.Blob,
                                 nullptr,
                                 m_logger,
                                 Configuration::PageBlob::DefaultBufferSize,
                                 WriteableFileImpl::BlobState{0, Configuration::PageBlob::DefaultSize}};

    EXPECT_EQ(0, file.GetFileSize());
    EXPECT_EQ(0, m_env.Blob->PropertiesRequests);
}

TEST_F(WriteableFileTests, Sync_NothingNewSinceLastSync_DoesNotWriteSizeAgain) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::DefaultSize);

    auto file = MakeFile();
    file.Append(std::vector<char>(100, 'x'));
    file.Sync();
    file.Sync();
    file.Close();

    EXPECT_EQ((std::vector<int64_t>{100}), m_env.Blob->SizeWrites);
}

TEST_F(WriteableFileTests, Constructor_BufferSizeSmallerThanPage_ThrowsException) {
    constexpr size_t invalidBufferSize = Configuration::PageBlob::PageSize - 1;

    EXPECT_THROW([[maybe_unused]] auto file =
                     WriteableFileImpl("test.dat", m_env.Runtime, m_env.Blob, nullptr, m_logger,
                                       static_cast<int64_t>(invalidBufferSize), {0, 0}),
                 std::invalid_argument);
}

TEST_F(WriteableFileTests, Constructor_EmptyBlob_InitializesCorrectly) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);

    const auto file = MakeFile();

    EXPECT_EQ(0, file.GetFileSize());
}

TEST_F(WriteableFileTests, Constructor_FullPagesInBlob_NoDataDownloaded) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(pageSize * 4);
    m_env.Blob->Size = pageSize * 3;

    const auto file = MakeFile();

    EXPECT_EQ(pageSize * 3, file.GetFileSize());
    EXPECT_TRUE(m_env.Blob->Downloads.empty());
}

TEST_F(WriteableFileTests, Close_CalledMultipleTimes_OnlySyncsOnce) {
    auto file = MakeFile();
    file.Append(std::vector<char>(10, 'x'));

    file.Close();
    file.Close();
    file.Close();

    EXPECT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ((std::vector<int64_t>{10}), m_env.Blob->SizeWrites);
}

TEST_F(WriteableFileTests, Close_WithUnflushedData_DataIsSynced) {
    constexpr size_t testDataSize = 100;
    const std::vector<char> dataToWrite(testDataSize, 'x');

    auto file = MakeFile();
    file.Append(dataToWrite);
    file.Close();

    EXPECT_EQ((std::vector<int64_t>{static_cast<int64_t>(testDataSize)}), m_env.Blob->SizeWrites);
}

TEST_F(WriteableFileTests, Close_EmptyFile_NoErrors) {
    auto file = MakeFile();

    EXPECT_NO_THROW(file.Close());
    EXPECT_TRUE(m_env.Blob->SizeWrites.empty());
}

TEST_F(WriteableFileTests, Sync_WithoutFileCache_NoError) {
    auto file = MakeFile();
    file.Append(std::vector<char>(10, 'x'));

    EXPECT_NO_THROW(file.Sync());
    EXPECT_EQ(10, file.GetFileSize());
}

TEST_F(WriteableFileTests, Sync_CalledMultipleTimes_SetsSizeCorrectly) {
    auto file = MakeFile();

    file.Sync();
    static constexpr std::string_view firstAppend = "test";
    file.Append(firstAppend);
    file.Sync();
    static constexpr std::string_view secondAppend = "data";
    file.Append(secondAppend);
    file.Sync();

    ASSERT_EQ(2U, m_env.Blob->SizeWrites.size());
    EXPECT_EQ(firstAppend.size(), static_cast<size_t>(m_env.Blob->SizeWrites[0]));
    EXPECT_EQ(firstAppend.size() + secondAppend.size(), static_cast<size_t>(m_env.Blob->SizeWrites[1]));
}

TEST_F(WriteableFileTests, Sync_WithPartialPage_FlushesAndSetsSizeCorrectly) {
    constexpr size_t partialPageSize = Configuration::PageBlob::PageSize / 2;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    const std::vector<char> dataToWrite(partialPageSize, 'y');

    auto file = MakeFile();
    file.Append(dataToWrite);
    file.Sync();

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(Configuration::PageBlob::PageSize, m_env.Blob->Uploads[0].Data.size());
    EXPECT_EQ(partialPageSize, static_cast<size_t>(m_env.Blob->SizeWrites[0]));
}

TEST_F(WriteableFileTests, Flush_EmptyBuffer_NoUploadCalled) {
    auto file = MakeFile();

    file.Flush();

    EXPECT_TRUE(m_env.Blob->Uploads.empty());
}

TEST_F(WriteableFileTests, Flush_ExactlyOnePage_OneUploadCall) {
    const std::vector<char> dataToWrite(Configuration::PageBlob::PageSize, 'z');
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);

    auto file = MakeFile();
    file.Append(dataToWrite);
    file.Flush();

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(Configuration::PageBlob::PageSize, m_env.Blob->Uploads[0].Data.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
}

TEST_F(WriteableFileTests, Truncate_ToZero_FileEmptied) {
    constexpr size_t initialDataSize = 1000;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize * 2);
    const std::vector<char> initialData(initialDataSize, 'a');

    auto file = MakeFile();
    file.Append(initialData);
    file.Truncate(0);

    EXPECT_EQ(0, file.GetFileSize());
    EXPECT_EQ((std::vector<int64_t>{static_cast<int64_t>(initialDataSize), 0}), m_env.Blob->SizeWrites);
    EXPECT_EQ(0, m_env.Blob->CapacityWrites.back());
}

TEST_F(WriteableFileTests, Truncate_ToSmallerSize_DataReducedCorrectly) {
    constexpr int64_t initialDataSize = 1000;
    constexpr int64_t truncatedSize = 500;
    constexpr int64_t truncatedSizeRoundedUp = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize * 2);
    std::vector<char> initialData(initialDataSize, 'b');

    auto file = MakeFile();
    file.Append(initialData);
    file.Truncate(truncatedSize);

    EXPECT_EQ(truncatedSize, file.GetFileSize());
    EXPECT_EQ(truncatedSizeRoundedUp, m_env.Blob->CapacityWrites.back());
    ASSERT_FALSE(m_env.Blob->Downloads.empty());
    EXPECT_EQ(0, m_env.Blob->Downloads.back().Offset);
    EXPECT_EQ(truncatedSize, m_env.Blob->Downloads.back().Length);
}

TEST_F(WriteableFileTests, Truncate_ToLargerSize_ThrowsException) {
    constexpr int64_t expandedSize = 2000;

    auto file = MakeFile();
    file.Append(std::vector<char>(100, 'c'));

    EXPECT_THROW(file.Truncate(expandedSize), std::invalid_argument);
}

TEST_F(WriteableFileTests, Truncate_ToPartialPage_BufferOffsetSetCorrectly) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    constexpr int64_t partialPageOffset = 123;
    constexpr int64_t initialSize = pageSize * 2;
    constexpr int64_t truncateSize = pageSize + partialPageOffset;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize * 4);
    std::vector<char> initialData(initialSize, 'd');

    auto file = MakeFile();
    file.Append(initialData);
    file.Truncate(truncateSize);

    EXPECT_EQ(truncateSize, file.GetFileSize());
    ASSERT_FALSE(m_env.Blob->Downloads.empty());
    EXPECT_EQ(pageSize, m_env.Blob->Downloads.back().Offset);
    EXPECT_EQ(partialPageOffset, m_env.Blob->Downloads.back().Length);

    std::vector<char> appendData(10, 'e');
    EXPECT_NO_THROW(file.Append(appendData));
    EXPECT_EQ(truncateSize + static_cast<int64_t>(appendData.size()), file.GetFileSize());
}

TEST_F(WriteableFileTests, GetUniqueId_BufferLargerThanName_ReturnsFullName) {
    const WriteableFileImpl file{
        "test.dat", m_env.Runtime, m_env.Blob, nullptr, m_logger, Configuration::PageBlob::DefaultBufferSize, {0, 0}};
    std::vector<char> id(100, '\0');

    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    EXPECT_EQ(8, length);
    EXPECT_EQ("test.dat", std::string(id.data(), static_cast<size_t>(length)));
}

TEST_F(WriteableFileTests, GetUniqueId_BufferSmallerThanName_ReturnsTruncatedName) {
    static constexpr std::string_view filename = "very_long_filename_for_testing.dat";
    const WriteableFileImpl file{
        filename, m_env.Runtime, m_env.Blob, nullptr, m_logger, Configuration::PageBlob::DefaultBufferSize, {0, 0}};
    std::vector<char> id(10, '\0');

    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    EXPECT_EQ(10, length);
    EXPECT_EQ(filename.substr(0, 10), std::string(id.data(), static_cast<size_t>(length)));
}

TEST_F(WriteableFileTests, GetUniqueId_EmptyName_ReturnsZero) {
    const WriteableFileImpl file{
        "", m_env.Runtime, m_env.Blob, nullptr, m_logger, Configuration::PageBlob::DefaultBufferSize, {0, 0}};
    std::vector<char> id(100, '\0');

    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    EXPECT_EQ(0, length);
}

TEST_F(WriteableFileTests, MoveConstructor_TransfersState_Correctly) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    const std::vector<char> testData(100, 'm');

    WriteableFileImpl file1{"test.dat", m_env.Runtime, m_env.Blob, nullptr, m_logger};
    file1.Append(testData);

    const WriteableFileImpl file2{std::move(file1)};

    EXPECT_EQ(testData.size(), static_cast<size_t>(file2.GetFileSize()));
}

TEST_F(WriteableFileTests, MoveAssignment_TransfersState_Correctly) {
    FakeBlobEnvironment env1;
    FakeBlobEnvironment env2;
    env1.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    env2.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    auto logger1 = std::make_shared<severity_logger_mt<severity_level>>();
    auto logger2 = std::make_shared<severity_logger_mt<severity_level>>();

    std::vector<char> testData(200, 'n');
    WriteableFileImpl file1{"test1.dat", env1.Runtime, env1.Blob, nullptr, logger1};
    WriteableFileImpl file2{"test2.dat", env2.Runtime, env2.Blob, nullptr, logger2};
    file1.Append(testData);

    file2 = std::move(file1);

    EXPECT_EQ(testData.size(), static_cast<size_t>(file2.GetFileSize()));
}
