// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/ContainerClientMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FileMock.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/FilesystemMock.hpp"

#include "FakeBlobEnvironment.hpp"

#include <boost/asio/post.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobServer;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadableFile;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadWriteFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeSequentialFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeWriteableFileImpl;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;

// Fails the Nth allocation of at least LargeAllocationMin bytes on this thread (1-based, 0 = disarmed). Shared with
// AsyncReadTests, which lives in the same executable and so the same replaced operator new.
thread_local size_t g_failLargeAllocationIn = 0;
constexpr size_t LargeAllocationMin = 64;
// Counts the allocations of at least LargeAllocationMin bytes made by this thread while armed (shared like the above).
thread_local bool g_countLargeAllocations = false;
thread_local size_t g_largeAllocationCount = 0;

namespace {
// Lets a test fail one small allocation on its own thread, which is how the shared payload owner in StartFlush is
// allocated. Inert unless armed.
thread_local bool g_failSmallAllocation = false;
constexpr size_t SmallAllocationMin = 40;
constexpr size_t SmallAllocationMax = 96;

// Fails the first allocation the size of one pooled buffer handle, i.e. the first growth of the free list.
thread_local bool g_failFreeListGrowth = false;
constexpr size_t FreeListGrowthSize = sizeof(std::vector<char>);

// Fails every allocation on this thread while set, to exercise cleanup that must not allocate.
thread_local bool g_failAllAllocations = false;

bool ShouldFailAllocation(const size_t size) {
    if (g_failAllAllocations) {
        return true;
    }
    if (g_countLargeAllocations && size >= LargeAllocationMin) {
        ++g_largeAllocationCount;
    }
    if (g_failLargeAllocationIn != 0 && size >= LargeAllocationMin && --g_failLargeAllocationIn == 0) {
        return true;
    }
    if (g_failFreeListGrowth && size == FreeListGrowthSize) {
        g_failFreeListGrowth = false;
        return true;
    }
    if (g_failSmallAllocation && size >= SmallAllocationMin && size <= SmallAllocationMax) {
        g_failSmallAllocation = false;
        return true;
    }
    return false;
}

} // namespace

// Shared with AsyncReadTests like g_failLargeAllocationIn.
// Counts the live allocations of exactly g_trackedAllocationSize bytes (0 = disarmed) and their peak, on any thread.
// Slots are a fixed array because this runs inside operator new and must not allocate.
std::atomic<size_t> g_trackedAllocationSize{0};
std::atomic<size_t> g_liveTrackedAllocations{0};
std::atomic<size_t> g_peakTrackedAllocations{0};
std::array<std::atomic<void*>, 512> g_trackedSlots{};

void TrackAllocation(void* p, const size_t size) {
    if (size == 0 || size != g_trackedAllocationSize.load(std::memory_order_relaxed)) {
        return;
    }
    for (auto& slot : g_trackedSlots) {
        void* expected = nullptr;
        if (slot.compare_exchange_strong(expected, p)) {
            const size_t live = g_liveTrackedAllocations.fetch_add(1) + 1;
            size_t peak = g_peakTrackedAllocations.load();
            while (live > peak && !g_peakTrackedAllocations.compare_exchange_weak(peak, live)) {
            }
            return;
        }
    }
}

void UntrackAllocation(void* p) {
    if (g_trackedAllocationSize.load(std::memory_order_relaxed) == 0 || p == nullptr) {
        return;
    }
    for (auto& slot : g_trackedSlots) {
        void* expected = p;
        if (slot.compare_exchange_strong(expected, nullptr)) {
            g_liveTrackedAllocations.fetch_sub(1);
            return;
        }
    }
}
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

    FakeWriteableFileImpl MakeFile(const int64_t bufferSize = Configuration::PageBlob::DefaultBufferSize) {
        return {"test.dat", m_env.Runtime, m_env.Client, nullptr, m_logger, bufferSize};
    }

    FakeWriteableFileImpl MakeKnownStateFile(const FakeWriteableFileImpl::BlobState state,
                                             const int64_t bufferSize = Configuration::PageBlob::DefaultBufferSize) {
        return {"test.dat", m_env.Runtime, m_env.Client, nullptr, m_logger, bufferSize, state};
    }
    void ExpectRetryAfterPayloadOwnerAllocationFailure(const int64_t tailSize) {
        constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
        m_env.Blob->SetCapacity(16 * pageSize);

        std::vector<char> expected;
        for (int64_t i = 0; i < 2 * pageSize + tailSize; ++i) {
            expected.push_back(static_cast<char>('a' + (i * 5) % 26));
        }

        auto file = MakeFile(3 * pageSize);
        file.Append(expected);

        // The payload owner is allocated by the coroutine on the client's executor, so the failure is armed there.
        const auto failOnTheExecutor = [&]() -> AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Task<void> {
            g_failSmallAllocation = true;
            try {
                co_await file.FlushAsync();
            } catch (...) {
                g_failSmallAllocation = false;
                throw;
            }
            g_failSmallAllocation = false;
        };
        EXPECT_THROW(AVEVA::RocksDB::Plugin::Azure::Impl::Block(*m_env.Client, failOnTheExecutor()), std::bad_alloc);

        file.Sync();
        EXPECT_EQ(expected, Prefix(m_env.Blob->Bytes(), expected.size()));
        EXPECT_EQ(static_cast<int64_t>(expected.size()), file.GetFileSize());
    }
};

TEST_F(WriteableFileTests, SinglePageBuffer_IsRejectedInsteadOfSpinningInAppend) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    EXPECT_THROW(MakeFile(pageSize), std::invalid_argument);
    EXPECT_THROW(MakeFile(pageSize / 2), std::invalid_argument);

    m_env.Blob->SetCapacity(16 * pageSize);
    const std::vector<char> data(100, 'x');
    auto file = MakeFile(2 * pageSize);
    file.Append(data);
    file.Append(data);
    file.Sync();
    EXPECT_EQ(std::vector<char>(200, 'x'), Prefix(m_env.Blob->Bytes(), 200));
}

TEST_F(WriteableFileTests, AwaitableOperations_AppendSyncAndClose_PersistTheData) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(64 * pageSize);
    // Several buffers' worth, so AppendAsync has to wait for uploads before it can accept more data.
    std::vector<char> expected;
    for (int64_t i = 0; i < 12 * pageSize + 100; ++i) {
        expected.push_back(static_cast<char>('a' + (i * 3) % 26));
    }

    auto file = MakeFile(2 * pageSize);
    const auto writeAll = [&]() -> AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Task<void> {
        co_await file.AppendAsync(expected);
        co_await file.FlushAsync();
        co_await file.CloseAsync();
    };
    AVEVA::RocksDB::Plugin::Azure::Impl::Block(*m_env.Client, writeAll());

    EXPECT_EQ(expected, Prefix(m_env.Blob->Bytes(), expected.size()));
    EXPECT_EQ(static_cast<int64_t>(expected.size()), file.GetFileSize());
}

TEST_F(WriteableFileTests, Flush_KeepsTheTailAcrossTheBufferSwap) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(64 * pageSize);

    std::vector<char> expected;
    for (int64_t i = 0; i < 3 * 4 * pageSize + 100; ++i) {
        expected.push_back(static_cast<char>('a' + (i * 7) % 26));
    }

    auto file = MakeFile(4 * pageSize);
    file.Append(expected);
    file.Sync();

    EXPECT_EQ(expected, Prefix(m_env.Blob->Bytes(), expected.size()));
}

TEST_F(WriteableFileTests, RangeSync_PartialPageThenMoreData_UploadsNeverOverlapAndLandInAnyOrder) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(16 * pageSize);
    m_env.Blob->DeferUploads = true;

    std::vector<char> expected;
    for (int64_t i = 0; i < pageSize + 200; ++i) {
        expected.push_back(static_cast<char>('a' + i % 26));
    }

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
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

    EXPECT_EQ(expected, Prefix(m_env.Blob->Bytes(), expected.size()));
}

TEST_F(WriteableFileTests, Destructor_WithoutClose_DoesNotUploadBufferedData) {
    auto file = std::make_unique<FakeWriteableFileImpl>("test.dat", m_env.Runtime, m_env.Client, nullptr, m_logger);
    file->Append(std::vector<char>(100, 'a'));

    const auto start = std::chrono::steady_clock::now();
    file.reset();

    EXPECT_TRUE(m_env.Blob->Uploads.empty());
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(700));
}

TEST_F(WriteableFileTests, Destructor_UploadsStillInFlight_DoesNotWaitAndTheyCompleteAfterwards) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(16 * pageSize);
    m_env.Blob->DeferUploads = true;

    auto file = std::make_unique<FakeWriteableFileImpl>("test.dat", m_env.Runtime, m_env.Client, nullptr, m_logger,
                                                        2 * pageSize);
    for (int i = 0; i < 2; ++i) {
        file->Append(std::vector<char>(pageSize, 'a'));
        file->RangeSync();
    }
    ASSERT_EQ(2U, m_env.Blob->PendingUploads());

    // Destroyed on the client's executor thread, where waiting for the uploads would deadlock. A regression
    // blocks here, so the uploads are released after a timeout to fail the test instead of hanging it.
    std::promise<void> destroyed;
    boost::asio::post(m_env.Http.get_executor(), [&] {
        file.reset();
        destroyed.set_value();
    });
    auto result = destroyed.get_future();
    const auto status = result.wait_for(std::chrono::seconds(5));
    while (m_env.Blob->PendingUploads() > 0) {
        m_env.Blob->CompleteUpload(0);
    }
    ASSERT_EQ(std::future_status::ready, status);
}

TEST_F(WriteableFileTests, RangeSync_UploadInFlight_ReturnsWithoutWaitingAndSyncCompletesIt) {
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize, 'a'));
    file.RangeSync();

    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    m_env.Blob->CompleteUpload();
    EXPECT_NO_THROW(file.Sync());
}

TEST_F(WriteableFileTests, Sync_FromTheClientExecutorWithAPendingUpload_ThrowsInsteadOfDeadlocking) {
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize, 'a'));
    file.RangeSync();
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());

    std::promise<bool> threw;
    boost::asio::post(m_env.Http.get_executor(), [&] {
        try {
            file.Sync();
            threw.set_value(false);
        } catch (const std::logic_error&) {
            threw.set_value(true);
        }
    });

    auto result = threw.get_future();
    const auto status = result.wait_for(std::chrono::seconds(5));
    // Releases the upload so a regression fails this test instead of hanging the pump and the destructor.
    m_env.Blob->CompleteUpload(0);
    ASSERT_EQ(std::future_status::ready, status);
    EXPECT_TRUE(result.get());
}

TEST_F(WriteableFileTests, GetFileSize_AfterUploadFailure_StillReportsTheTrackedSize) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(std::vector<char>(pageSize, 'a'));
    file.RangeSync();
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    m_env.Blob->FailUpload(FakeBlobServer::Error(503, "unavailable"), 0);

    EXPECT_EQ(pageSize, file.GetFileSize());
    EXPECT_THROW(file.Sync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
    EXPECT_EQ(pageSize, file.GetFileSize());
}

TEST_F(WriteableFileTests, Sync_WritesFencedWhileAnUploadWasInFlight_FailsEvenThoughTheUploadSucceeded) {
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize, 'a'));
    file.RangeSync();
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());

    m_env.Runtime->FenceWrites();
    m_env.Blob->CompleteUpload();

    EXPECT_THROW(file.Sync(), std::runtime_error);
    EXPECT_THROW(file.Close(), std::runtime_error);
    EXPECT_THROW(file.Flush(), std::runtime_error);
}

TEST_F(WriteableFileTests, Sync_UploadFailed_ThrowsAndKeepsThrowing) {
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize, 'a'));
    file.RangeSync();
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    m_env.Blob->FailUpload(FakeBlobServer::Error(500, "upload failed"));

    EXPECT_THROW(file.Sync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
    EXPECT_THROW(file.Sync(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
}

TEST_F(WriteableFileTests, Flush_PayloadOwnerAllocationFails_KeepsFullPagesAndTailForARetry) {
    ExpectRetryAfterPayloadOwnerAllocationFailure(100);
}

TEST_F(WriteableFileTests, Flush_PayloadOwnerAllocationFailsOnPageAlignedData_KeepsFullPagesForARetry) {
    ExpectRetryAfterPayloadOwnerAllocationFailure(0);
}

TEST_F(WriteableFileTests, AppendBytes_LessThanAPage_PageWritten) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);

    {
        auto file = MakeFile();
        std::string_view data = "1";
        file.Append(data);
        ASSERT_EQ(data.size(), static_cast<size_t>(file.GetFileSize()));
        file.Close();
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
        file.Close();
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
        file.Close();
    }

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
    EXPECT_EQ(Configuration::PageBlob::PageSize * 2, m_env.Blob->Uploads[0].Data.size());
    EXPECT_EQ(expected, Prefix(m_env.Blob->Uploads[0].Data, expected.size()));
}

TEST_F(WriteableFileTests, Constructor_PartialPageInBlob_DataDownloaded) {
    constexpr size_t partialPageSize = 333;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    m_env.Blob->WithPrimaryBlob([&](auto& b) { std::fill_n(b.Data.begin(), partialPageSize, 'p'); });
    m_env.Blob->SetSize(partialPageSize);

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
    m_env.Blob->WithPrimaryBlob([&](auto& b) { std::fill_n(b.Data.begin() + pageSize, partialPageSize, 'p'); });
    m_env.Blob->SetSize(pageSize + partialPageSize);

    const auto file = MakeFile();

    ASSERT_EQ(pageSize + partialPageSize, file.GetFileSize());
    ASSERT_EQ(1U, m_env.Blob->Downloads.size());
    EXPECT_EQ(pageSize, m_env.Blob->Downloads[0].Offset);
    EXPECT_EQ(partialPageSize, m_env.Blob->Downloads[0].Length);
}

TEST_F(WriteableFileTests, Constructor_ShortDownloadOfThePartialPage_ThrowsAndUploadsNothing) {
    constexpr size_t partialPageSize = 333;
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    m_env.Blob->WithPrimaryBlob([&](auto& b) { std::fill_n(b.Data.begin(), partialPageSize, 'p'); });
    m_env.Blob->SetSize(partialPageSize);
    m_env.Blob->DownloadOverride = [](const auto&) { return std::string(10, 'p'); };

    EXPECT_THROW(MakeFile(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
    EXPECT_EQ(0U, m_env.Blob->Uploads.size());
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
        file.Close();
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
    const FakeWriteableFileImpl file{"test.dat",
                                     m_env.Runtime,
                                     m_env.Client,
                                     nullptr,
                                     m_logger,
                                     Configuration::PageBlob::DefaultBufferSize,
                                     FakeWriteableFileImpl::BlobState{0, Configuration::PageBlob::DefaultSize}};

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
                     FakeWriteableFileImpl("test.dat", m_env.Runtime, m_env.Client, nullptr, m_logger,
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
    m_env.Blob->SetSize(pageSize * 3);

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

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(dataToWrite);
    file.Flush();
    AVEVA::RocksDB::Plugin::Azure::Impl::Tests::Settle(m_env.Http);

    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(Configuration::PageBlob::PageSize, m_env.Blob->Uploads[0].Data.size());
    EXPECT_EQ(0, m_env.Blob->Uploads[0].Offset);
}

TEST_F(WriteableFileTests, Flush_PartialPage_UploadsNothingAndLeavesTheTailToSync) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);

    auto file = MakeFile();
    file.Append(std::vector<char>(100, 'p'));
    file.Flush();
    file.Flush();

    EXPECT_TRUE(m_env.Blob->Uploads.empty());
    file.Sync();
    ASSERT_EQ(1U, m_env.Blob->Uploads.size());
    EXPECT_EQ(100, m_env.Blob->SizeWrites.back());
}

TEST_F(WriteableFileTests, Flush_BelowHalfTheBuffer_UploadsNothingUntilSync) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(16 * pageSize);

    auto file = MakeFile(16 * pageSize);
    for (int i = 0; i < 12; ++i) {
        file.Append(std::vector<char>(300, 'w'));
        file.Flush();
    }
    EXPECT_TRUE(m_env.Blob->Uploads.empty());

    file.Append(std::vector<char>(pageSize, 'w'));
    file.Flush();
    AVEVA::RocksDB::Plugin::Azure::Impl::Tests::Settle(m_env.Http);
    EXPECT_EQ(1U, m_env.Blob->Uploads.size());

    file.Sync();
    EXPECT_EQ(2U, m_env.Blob->Uploads.size());
}

TEST_F(WriteableFileTests, Flush_FullPage_ReturnsWithoutWaitingForTheUpload) {
    m_env.Blob->SetCapacity(4 * Configuration::PageBlob::PageSize);
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize + 10, 'f'));
    file.Flush();

    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    EXPECT_EQ(static_cast<size_t>(Configuration::PageBlob::PageSize), m_env.Blob->Uploads[0].Data.size());
    m_env.Blob->CompleteUpload();
    m_env.Blob->DeferUploads = false;
    EXPECT_NO_THROW(file.Sync());
}

TEST_F(WriteableFileTests, Flush_AfterAnEarlierUploadFailed_ReportsTheFailure) {
    m_env.Blob->SetCapacity(4 * Configuration::PageBlob::PageSize);
    m_env.Blob->DeferUploads = true;

    auto file = MakeFile(2 * Configuration::PageBlob::PageSize);
    file.Append(std::vector<char>(Configuration::PageBlob::PageSize, 'f'));
    file.Flush();
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());
    m_env.Blob->FailUpload(FakeBlobServer::Error(500, "upload failed"));

    EXPECT_THROW(file.Flush(), AVEVA::RocksDB::Plugin::Azure::RequestFailedException);
}

TEST_F(WriteableFileTests, GetUniqueId_BufferLargerThanName_ReturnsFullName) {
    const FakeWriteableFileImpl file{
        "test.dat", m_env.Runtime, m_env.Client, nullptr, m_logger, Configuration::PageBlob::DefaultBufferSize, {0, 0}};
    std::vector<char> id(100, '\0');

    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    EXPECT_EQ(8, length);
    EXPECT_EQ("test.dat", std::string(id.data(), static_cast<size_t>(length)));
}

TEST_F(WriteableFileTests, GetUniqueId_BufferSmallerThanName_ReturnsTruncatedName) {
    static constexpr std::string_view filename = "very_long_filename_for_testing.dat";
    const FakeWriteableFileImpl file{
        filename, m_env.Runtime, m_env.Client, nullptr, m_logger, Configuration::PageBlob::DefaultBufferSize, {0, 0}};
    std::vector<char> id(10, '\0');

    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    EXPECT_EQ(10, length);
    EXPECT_EQ(filename.substr(0, 10), std::string(id.data(), static_cast<size_t>(length)));
}

TEST_F(WriteableFileTests, GetUniqueId_EmptyName_ReturnsZero) {
    const FakeWriteableFileImpl file{
        "", m_env.Runtime, m_env.Client, nullptr, m_logger, Configuration::PageBlob::DefaultBufferSize, {0, 0}};
    std::vector<char> id(100, '\0');

    const auto length = file.GetUniqueId(id.data(), static_cast<int64_t>(id.size()));

    EXPECT_EQ(0, length);
}

TEST_F(WriteableFileTests, MoveConstructor_TransfersState_Correctly) {
    m_env.Blob->SetCapacity(Configuration::PageBlob::PageSize);
    const std::vector<char> testData(100, 'm');

    FakeWriteableFileImpl file1{"test.dat", m_env.Runtime, m_env.Client, nullptr, m_logger};
    file1.Append(testData);

    const FakeWriteableFileImpl file2{std::move(file1)};

    EXPECT_EQ(testData.size(), static_cast<size_t>(file2.GetFileSize()));
}

TEST_F(WriteableFileTests, Release_FreeListAllocationFailure_DoesNotTerminateAndKeepsTheUploadResult) {
    constexpr int64_t pageSize = Configuration::PageBlob::PageSize;
    m_env.Blob->SetCapacity(16 * pageSize);

    std::vector<char> expected;
    for (int64_t i = 0; i < 2 * pageSize + 100; ++i) {
        expected.push_back(static_cast<char>('a' + (i * 3) % 26));
    }

    auto file = MakeFile(3 * pageSize);
    file.Append(expected);
    m_env.Blob->DeferUploads = true;
    file.Flush();
    ASSERT_EQ(1U, m_env.Blob->PendingUploads());

    g_failFreeListGrowth = true;
    m_env.Blob->CompleteUpload(0);
    g_failFreeListGrowth = false;
    m_env.Blob->DeferUploads = false;
    file.Sync();

    EXPECT_EQ(expected, Prefix(m_env.Blob->Bytes(), expected.size()));
    EXPECT_EQ(static_cast<int64_t>(expected.size()), file.GetFileSize());
}

TEST_F(WriteableFileTests, MoveAssignment_IsDeleted) {
    static_assert(!std::is_move_assignable_v<FakeWriteableFileImpl>);
    static_assert(!std::is_copy_assignable_v<FakeWriteableFileImpl>);
}

// Invalidation runs from destructors and scope guards, so it must survive allocation failure without letting an
// older in-flight fetch repopulate the cache.
TEST(BlobMetadataCacheAllocationTests, InvalidationSurvivesAllocationFailureAndRejectsOlderFetches) {
    static_assert(
        noexcept(std::declval<const AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntimeBase&>().InvalidateBlobSize(
            std::string_view{})));
    const std::string longUrl(200, 'u');
    for (const std::string url : {std::string("a-new-url-tombstone-that-is-not-short-sso"), longUrl}) {
        AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntimeBase runtime;
        runtime.CacheBlobSize(url, 1, runtime.BlobSizeEpoch());
        const auto staleEpoch = runtime.BlobSizeEpoch();

        g_failAllAllocations = true;
        runtime.InvalidateBlobSize(url);
        g_failAllAllocations = false;

        EXPECT_FALSE(runtime.CachedBlobSize(url)) << url.size();
        runtime.CacheBlobSize(url, 2, staleEpoch);
        EXPECT_FALSE(runtime.CachedBlobSize(url)) << url.size();
        runtime.CacheBlobSize("other", 3, staleEpoch);
        EXPECT_FALSE(runtime.CachedBlobSize("other")) << "watermark fallback rejects older fetches";
        runtime.CacheBlobSize(url, 4, runtime.BlobSizeEpoch());
        EXPECT_EQ(4, runtime.CachedBlobSize(url)) << url.size();
    }
}

// Replaced so a test can inject an allocation failure; see g_failSmallAllocation.
void* operator new(const size_t size) {
    if (ShouldFailAllocation(size)) {
        throw std::bad_alloc();
    }
    if (void* p = std::malloc(size ? size : 1)) {
        TrackAllocation(p, size);
        return p;
    }
    throw std::bad_alloc();
}

void operator delete(void* p) noexcept {
    UntrackAllocation(p);
    std::free(p);
}

void operator delete(void* p, size_t) noexcept {
    UntrackAllocation(p);
    std::free(p);
}
