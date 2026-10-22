// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeBlobEnvironment.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::AbortAsyncReads;
using AVEVA::RocksDB::Plugin::Azure::Impl::AsyncReadTracker;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::PollAsyncReads;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobServer;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadableFile;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeReadWriteFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeSequentialFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeWriteableFileImpl;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;

// Defined next to the replaced operator new in WriteableFileTests.cpp.
extern thread_local size_t g_failLargeAllocationIn;
// Live and peak count of allocations of exactly g_trackedAllocationSize bytes; 0 disarms the tracking.
extern std::atomic<size_t> g_trackedAllocationSize;
extern std::atomic<size_t> g_liveTrackedAllocations;
extern std::atomic<size_t> g_peakTrackedAllocations;

namespace {
constexpr int64_t BlobSize = 4096;

struct CallbackRecord {
    std::atomic<int> Calls{0};
    std::thread::id Thread;
    rocksdb::IOStatus Status;
    std::string Data;
};

void RecordCallback(rocksdb::FSReadRequest& req, void* arg) {
    auto* record = static_cast<CallbackRecord*>(arg);
    record->Thread = std::this_thread::get_id();
    record->Status = req.status;
    record->Data.assign(req.result.data(), req.result.size());
    ++record->Calls;
}

struct IoHandle {
    void* Handle = nullptr;
    rocksdb::IOHandleDeleter Deleter;

    IoHandle() = default;
    IoHandle(const IoHandle&) = delete;
    IoHandle& operator=(const IoHandle&) = delete;
    ~IoHandle() { Reset(); }

    void Reset() {
        if (Handle != nullptr && Deleter) {
            Deleter(Handle);
        }
        Handle = nullptr;
    }
};

rocksdb::FSReadRequest MakeRequest(const uint64_t offset, std::vector<char>& scratch) {
    rocksdb::FSReadRequest req;
    req.offset = offset;
    req.len = scratch.size();
    req.scratch = scratch.data();
    return req;
}
} // namespace

class AsyncReadTests : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_deferredEnv;
    FakeBlobEnvironment m_inlineEnv;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger =
        std::make_shared<severity_logger_mt<severity_level>>();

    void SetUp() override {
        m_deferredEnv.Fill(BlobSize, 'R');
        m_inlineEnv.Fill(BlobSize, 'B');
        m_deferredEnv.Blob->ETag = "etag";
        m_inlineEnv.Blob->ETag = "etag";
        m_deferredEnv.Blob->DeferDownloads = true;
    }

    void TearDown() override {
        while (m_deferredEnv.Blob->PendingDownloads() > 0) {
            auto download = m_deferredEnv.Blob->TakeDownload();
            download.Fail(FakeBlobServer::Error(500, "test finished"));
        }
    }

    FakeReadableFile CreateFile(const FakeBlobEnvironment& env, std::shared_ptr<AsyncReadTracker> tracker = nullptr,
                                std::shared_ptr<std::atomic<int64_t>> budget = nullptr) {
        return FakeReadableFile{std::make_shared<FakeReadableFileImpl>(
            "test.sst", env.Runtime, env.Client, nullptr, m_logger, std::move(tracker), std::move(budget))};
    }

    static rocksdb::IOStatus Submit(FakeReadableFile& file, rocksdb::FSReadRequest& req, CallbackRecord& record,
                                    IoHandle& handle, const rocksdb::IOOptions& options = rocksdb::IOOptions{}) {
        return file.ReadAsync(req, options, RecordCallback, &record, &handle.Handle, &handle.Deleter, nullptr);
    }
};

TEST_F(AsyncReadTests, ReadAsync_DoesNotBlockAndPollDeliversOnCallerThread) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(128);
    auto req = MakeRequest(100, scratch);
    CallbackRecord record;
    IoHandle handle;

    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    ASSERT_NE(nullptr, handle.Handle);
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(0, record.Calls.load());

    auto download = m_deferredEnv.Blob->TakeDownload();
    EXPECT_EQ(100, download.Request.Offset);
    EXPECT_EQ(128, download.Request.Length);
    EXPECT_EQ("etag", download.Request.IfMatch);
    std::jthread network([download = std::move(download)]() mutable { download.Succeed(std::string(128, 'A')); });
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    network.join();

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_EQ(std::this_thread::get_id(), record.Thread);
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(128, 'A'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_CallbackThatSubmitsAnotherRead_DoesNotDeadlockAndBothComplete) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch1(64);
    std::vector<char> scratch2(64);
    auto req1 = MakeRequest(0, scratch1);
    auto req2 = MakeRequest(512, scratch2);
    CallbackRecord second;
    IoHandle handle1;
    IoHandle handle2;
    bool resubmitted = false;

    const auto reenter = [&](rocksdb::FSReadRequest&, void*) {
        resubmitted = Submit(file, req2, second, handle2).ok();
    };
    ASSERT_TRUE(
        file.ReadAsync(req1, rocksdb::IOOptions{}, reenter, nullptr, &handle1.Handle, &handle1.Deleter, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(64, 'A'));
    std::vector<void*> handles1{handle1.Handle};
    ASSERT_TRUE(PollAsyncReads(handles1).ok());
    ASSERT_TRUE(resubmitted);

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(64, 'B'));
    std::vector<void*> handles2{handle2.Handle};
    ASSERT_TRUE(PollAsyncReads(handles2).ok());
    EXPECT_EQ(1, second.Calls.load());
    EXPECT_EQ(std::string(64, 'B'), second.Data);
}

TEST_F(AsyncReadTests, ReadAsync_FileDestroyedWhileDownloadPending_StillDeliversTheResult) {
    std::vector<char> scratch(64);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    {
        auto file = CreateFile(m_deferredEnv);
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
        ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    }

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(64, 'Z'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_EQ(1, record.Calls.load());
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(64, 'Z'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_ConcurrentReadsAtCachedEnd_ShareOnePropertiesCall) {
    auto file = CreateFile(m_deferredEnv);
    const int before = m_deferredEnv.Blob->PropertiesRequests;
    m_deferredEnv.Blob->DeferProperties = true;

    std::vector<char> scratchA(64);
    std::vector<char> scratchB(64);
    auto reqA = MakeRequest(BlobSize, scratchA);
    auto reqB = MakeRequest(BlobSize, scratchB);
    CallbackRecord recordA;
    CallbackRecord recordB;
    IoHandle handleA;
    IoHandle handleB;
    ASSERT_TRUE(Submit(file, reqA, recordA, handleA).ok());
    ASSERT_TRUE(Submit(file, reqB, recordB, handleB).ok());
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingProperties());

    m_deferredEnv.Blob->CompleteProperties();
    std::vector<void*> handles{handleA.Handle, handleB.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(1, recordA.Calls.load());
    EXPECT_EQ(1, recordB.Calls.load());
    EXPECT_TRUE(recordA.Status.ok()) << recordA.Status.ToString();
    EXPECT_TRUE(recordB.Status.ok()) << recordB.Status.ToString();
    EXPECT_EQ(before + 1, m_deferredEnv.Blob->PropertiesRequests);
}

TEST_F(AsyncReadTests, ReadAsync_AtCachedEndWithMovedEtag_PublishesTheRefreshToTheRuntimeCache) {
    auto file = CreateFile(m_deferredEnv);
    m_deferredEnv.Blob->ETag = "etag2";

    std::vector<char> scratch(64);
    auto req = MakeRequest(BlobSize, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    const auto cached = m_deferredEnv.Runtime->CachedBlobMetadata(m_deferredEnv.Client->ResourceUrl());
    ASSERT_TRUE(cached.has_value());
    EXPECT_EQ("etag2", cached->ETag);
}

TEST_F(AsyncReadTests, ReadAsync_MetadataInitiationThrows_CompletesTheReadWithTheErrorAndLaterCallsStartFresh) {
    auto file = CreateFile(m_deferredEnv);
    m_deferredEnv.Http.FailNextInitiation(std::make_exception_ptr(std::runtime_error("cannot start")));
    std::vector<char> scratchA(64);
    auto reqA = MakeRequest(BlobSize, scratchA);
    CallbackRecord recordA;
    IoHandle handleA;

    ASSERT_TRUE(Submit(file, reqA, recordA, handleA).ok());
    std::vector<void*> handlesA{handleA.Handle};
    ASSERT_TRUE(PollAsyncReads(handlesA).ok());

    EXPECT_EQ(1, recordA.Calls.load());
    EXPECT_FALSE(recordA.Status.ok());

    m_deferredEnv.Blob->DeferProperties = true;
    std::vector<char> scratchB(64);
    auto reqB = MakeRequest(BlobSize, scratchB);
    CallbackRecord recordB;
    IoHandle handleB;
    ASSERT_TRUE(Submit(file, reqB, recordB, handleB).ok());
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingProperties());

    m_deferredEnv.Blob->CompleteProperties();
    std::vector<void*> handlesB{handleB.Handle};
    ASSERT_TRUE(PollAsyncReads(handlesB).ok());
    EXPECT_EQ(1, recordB.Calls.load());
    EXPECT_TRUE(recordB.Status.ok()) << recordB.Status.ToString();
}
TEST_F(AsyncReadTests, ReadAsync_RangeCoveredByPendingPrefetch_WaitsForItWithoutBlockingOrDownloadingTwice) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(1024, 2048, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());

    std::vector<char> scratch(64);
    auto req = MakeRequest(1100, scratch);
    CallbackRecord record;
    IoHandle handle;
    // Submit must return without waiting for the held download; a regression that blocks here is caught by the
    // CTest timeout rather than a fixed wall-clock budget.
    EXPECT_TRUE(Submit(file, req, record, handle).ok());
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(0, record.Calls.load());

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(2048, 'P'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(64, 'P'), record.Data);
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
}

TEST_F(AsyncReadTests, ReadAsync_ChainedOntoPrefetchThenHandleDeleted_NeverWritesScratch) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(1024, 2048, rocksdb::IOOptions{}, nullptr).ok());

    std::vector<char> scratch(64, 'x');
    auto req = MakeRequest(1100, scratch);
    CallbackRecord record;
    {
        IoHandle handle;
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(2048, 'P'));

    EXPECT_EQ(0, record.Calls.load());
    EXPECT_EQ(std::string(64, 'x'), std::string(scratch.begin(), scratch.end()));
}

TEST_F(AsyncReadTests, GetSize_RefreshThatFindsTheSameEtag_KeepsThePrefetchedBytes) {
    auto file = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 0, 512));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));
    m_deferredEnv.AdvanceMetadataClock(Configuration::MetadataCacheTtl);
    const auto propertiesBefore = m_deferredEnv.Blob->PropertiesRequests;

    EXPECT_EQ(BlobSize, file->GetSize());

    EXPECT_EQ(propertiesBefore + 1, m_deferredEnv.Blob->PropertiesRequests);
    std::vector<char> buffer(256);
    EXPECT_EQ(256U, file->TryReadFromCache(0, 256, buffer.data()).value_or(0));
    EXPECT_EQ(std::vector<char>(256, 'P'), buffer);
}

TEST_F(AsyncReadTests, GetSize_RefreshThatFindsANewEtag_DropsThePrefetchedBytes) {
    auto file = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 0, 512));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));
    m_deferredEnv.Blob->ETag = "etag2";
    m_deferredEnv.AdvanceMetadataClock(Configuration::MetadataCacheTtl);

    EXPECT_EQ(BlobSize, file->GetSize());

    std::vector<char> buffer(256);
    EXPECT_FALSE(file->TryReadFromCache(0, 256, buffer.data()).has_value());
}
TEST_F(AsyncReadTests, PrefetchCompletion_ThrowingWaiter_DoesNotEscapeOrSkipTheOtherWaiters) {
    auto file = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 1024, 2048));
    std::move_only_function<void()> throwing = [] { throw std::bad_alloc(); };
    bool secondRan = false;
    std::move_only_function<void()> second = [&secondRan] { secondRan = true; };
    ASSERT_TRUE(file->ChainOntoPendingPrefetch(1100, 64, throwing));
    ASSERT_TRUE(file->ChainOntoPendingPrefetch(1100, 64, second));

    EXPECT_NO_THROW({
        m_deferredEnv.Blob->TakeDownload().Succeed(std::string(2048, 'P'));
        EXPECT_TRUE(m_deferredEnv.Http.RunUntil([&] { return secondRan; }));
    });

    EXPECT_TRUE(secondRan);
}
TEST_F(AsyncReadTests, PrefetchCompletion_ManyInterleavedWaiters_EachRunsOnceAndOnlyForItsSlot) {
    auto file = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 0, 512));
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 1024, 512));
    constexpr int kWaitersPerSlot = 3000;
    std::vector<int> ranA(kWaitersPerSlot, 0);
    std::vector<int> ranB(kWaitersPerSlot, 0);
    for (int i = 0; i < kWaitersPerSlot; ++i) {
        std::move_only_function<void()> a = [&ranA, i] { ++ranA[i]; };
        std::move_only_function<void()> b = [&ranB, i] { ++ranB[i]; };
        ASSERT_TRUE(file->ChainOntoPendingPrefetch(100, 64, a));
        ASSERT_TRUE(file->ChainOntoPendingPrefetch(1100, 64, b));
    }

    auto first = m_deferredEnv.Blob->TakeDownload();
    ASSERT_EQ(0, first.Request.Offset);
    first.Succeed(std::string(512, 'A'));
    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return ranA.back() == 1; }));
    EXPECT_EQ(std::vector<int>(kWaitersPerSlot, 1), ranA);
    EXPECT_EQ(std::vector<int>(kWaitersPerSlot, 0), ranB);

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'B'));
    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return ranB.back() == 1; }));
    EXPECT_EQ(std::vector<int>(kWaitersPerSlot, 1), ranA);
    EXPECT_EQ(std::vector<int>(kWaitersPerSlot, 1), ranB);
}

TEST_F(AsyncReadTests, PrefetchCompletion_SlotInvalidatedDuringDownload_StillReleasesWaiters) {
    auto file = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 0, 512));
    int ran = 0;
    std::move_only_function<void()> waiter = [&ran] { ++ran; };
    ASSERT_TRUE(file->ChainOntoPendingPrefetch(100, 64, waiter));

    m_deferredEnv.Blob->ETag = "etag2";
    m_deferredEnv.AdvanceMetadataClock(Configuration::MetadataCacheTtl);
    EXPECT_EQ(BlobSize, file->GetSize());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));

    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return ran == 1; }));
    EXPECT_EQ(1, ran);
    std::vector<char> buffer(64);
    EXPECT_FALSE(file->TryReadFromCache(100, 64, buffer.data()).has_value());
}

TEST_F(AsyncReadTests, PrefetchChain_OntoSettledSlot_DeclinesAndKeepsTheCallback) {
    auto file = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 0, 512));
    int ran = 0;
    std::move_only_function<void()> waiter = [&ran] { ++ran; };
    ASSERT_TRUE(file->ChainOntoPendingPrefetch(100, 64, waiter));
    // A later chain onto a settled slot is declined and must not consume the callback.
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));
    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return ran == 1; }));
    std::move_only_function<void()> late = [&ran] { ++ran; };
    EXPECT_FALSE(file->ChainOntoPendingPrefetch(100, 64, late));
    EXPECT_TRUE(static_cast<bool>(late));
}

TEST_F(AsyncReadTests, ReadAsync_ChainedOntoFailedPrefetch_FallsBackToOwnDownload) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(1024, 2048, rocksdb::IOOptions{}, nullptr).ok());

    std::vector<char> scratch(64);
    auto req = MakeRequest(1100, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(500, "boom"));
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    auto download = m_deferredEnv.Blob->TakeDownload();
    EXPECT_EQ(1100, download.Request.Offset);
    download.Succeed(std::string(64, 'D'));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(64, 'D'), record.Data);
}

TEST_F(AsyncReadTests, Read_PartiallyOverlappingPrefetch_DownloadsOnlyTheRemainder) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));

    std::vector<char> scratch(512);
    rocksdb::Slice result;
    // The deferred environment holds every GET (a blocking read is indistinguishable from an async one at the HTTP
    // level), so the blocking Read runs on its own thread while this one serves the remainder.
    auto reading = std::async(std::launch::async, [&] {
        return file.Read(256, scratch.size(), rocksdb::IOOptions{}, &result, scratch.data(), nullptr);
    });
    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return m_deferredEnv.Blob->PendingDownloads() == 1; }));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(256, 'R'));
    const auto status = reading.get();

    EXPECT_TRUE(status.ok()) << status.ToString();
    ASSERT_GE(m_deferredEnv.Blob->Downloads.size(), 2U);
    EXPECT_EQ(512, m_deferredEnv.Blob->Downloads.back().Offset);
    EXPECT_EQ(256, m_deferredEnv.Blob->Downloads.back().Length);
    EXPECT_EQ(std::string(256, 'P') + std::string(256, 'R'), result.ToString());
}

TEST_F(AsyncReadTests, Read_BlobOverwrittenAfterPrefetch_NeverReturnsMixedVersions) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));

    std::vector<char> scratch(512);
    rocksdb::Slice result;
    auto reading = std::async(std::launch::async, [&] {
        return file.Read(256, scratch.size(), rocksdb::IOOptions{}, &result, scratch.data(), nullptr);
    });
    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return m_deferredEnv.Blob->PendingDownloads() == 1; }));
    // The blob is overwritten after the prefix was served from the prefetch; the suffix download then hits a 412.
    m_deferredEnv.Blob->ETag = "etag2";
    m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(412, "ConditionNotMet"));
    while (reading.wait_for(std::chrono::milliseconds{10}) != std::future_status::ready) {
        if (m_deferredEnv.Blob->PendingDownloads() > 0) {
            auto download = m_deferredEnv.Blob->TakeDownload();
            download.Succeed(std::string(static_cast<size_t>(download.Request.Length), 'N'));
        } else {
            (void)m_deferredEnv.Http.RunUntil([&] { return m_deferredEnv.Blob->PendingDownloads() > 0; },
                                              std::chrono::milliseconds{20});
        }
    }
    const auto status = reading.get();

    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(std::string(512, 'N'), result.ToString());
}

TEST_F(AsyncReadTests, Read_OverwrittenAfterPrefetchAndAgainDuringRecovery_SharesOneRefreshBudget) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));

    std::vector<char> scratch(512);
    rocksdb::Slice result;
    auto reading = std::async(std::launch::async, [&] {
        return file.Read(256, scratch.size(), rocksdb::IOOptions{}, &result, scratch.data(), nullptr);
    });

    // The suffix download is refused twice (the blob changed), then succeeds; the whole range is then re-read and
    // refused every time. Both phases must draw on the same stale-read budget.
    int downloads = 0;
    while (reading.wait_for(std::chrono::milliseconds{10}) != std::future_status::ready) {
        if (m_deferredEnv.Blob->PendingDownloads() > 0) {
            auto download = m_deferredEnv.Blob->TakeDownload();
            ++downloads;
            if (downloads == 1) {
                m_deferredEnv.Blob->ETag = "etag2";
            }
            if (downloads == 3) {
                download.Succeed(std::string(static_cast<size_t>(download.Request.Length), 'N'));
            } else {
                download.Fail(FakeBlobServer::Error(412, "ConditionNotMet"));
            }
        } else {
            (void)m_deferredEnv.Http.RunUntil([&] { return m_deferredEnv.Blob->PendingDownloads() > 0; },
                                              std::chrono::milliseconds{20});
        }
    }
    const auto status = reading.get();

    EXPECT_FALSE(status.ok());
    // At most kMaxStaleReadRetries refreshes across both phases: each refused download but the last spends one, and
    // the one successful suffix download is the only other request.
    EXPECT_LE(downloads, FakeReadableFileImpl::kMaxStaleReadRetries + 2);
}

TEST_F(AsyncReadTests, Read_PastTheEndOfPrefetchedRange_ReleasesItsBudget) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget)};
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));
    ASSERT_EQ(512, budget->load());

    std::vector<char> scratch(512);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(0, budget->load());
}

TEST_F(AsyncReadTests, Prefetch_ShortAtEof_KeepsChargeForTheWholeBackingBuffer) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget)};
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_EQ(512, budget->load());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(300, 'P'));

    EXPECT_EQ(512, budget->load());
}

TEST_F(AsyncReadTests, Prefetch_NonPowerOfTwoRange_ChargesTheSizeClassCapacity) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget, 1024)};

    ASSERT_TRUE(file.Prefetch(0, 600, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(1024, budget->load());
    // 600 logical bytes would leave room for another 424, but the real allocation fills the cap.
    EXPECT_TRUE(file.Prefetch(4096, 400, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(1024, budget->load());
}

TEST_F(AsyncReadTests, Prefetch_InvalidatedWhileDownloading_KeepsChargeUntilTheDownloadEnds) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    auto impl = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger, nullptr, budget, 1024);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(impl, 0, 1024));
    m_deferredEnv.Blob->ETag = "etag2";
    m_deferredEnv.AdvanceMetadataClock(Configuration::MetadataCacheTtl);
    EXPECT_EQ(BlobSize, impl->GetSize());
    ASSERT_EQ(1024, budget->load());

    // The paused download still owns its buffer, so a replacement prefetch must not fit yet.
    EXPECT_FALSE(FakeReadableFileImpl::Prefetch(impl, 0, 1024));
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(1024, 'P'));
    EXPECT_EQ(0, budget->load());
    EXPECT_TRUE(FakeReadableFileImpl::Prefetch(impl, 0, 1024));
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(1024, budget->load());
}

TEST_F(AsyncReadTests, Prefetch_FinishedRangeStillHeldElsewhere_IsNotCreditedWhenEvicted) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    auto impl = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger, nullptr, budget, 3072);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(impl, 0, 1024));
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(impl, 2048, 1024));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(1024, 'A'));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(1024, 'B'));

    // The slot budget forces the oldest finished range out, which frees its charge when nothing else holds it.
    EXPECT_TRUE(FakeReadableFileImpl::Prefetch(impl, 1024, 2048));
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(3072, budget->load());
}

TEST_F(AsyncReadTests, Read_ConcurrentReadersOfPrefetchedRange_AllGetDataAndBudgetIsReleased) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget)};
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));

    // Whichever reader reaches the end of the range drops the slot; the others must still have copied valid bytes.
    std::vector<std::future<std::string>> readers;
    for (int i = 0; i < 4; ++i) {
        readers.push_back(std::async(std::launch::async, [&file] {
            std::vector<char> scratch(512);
            rocksdb::Slice result;
            file.Read(0, scratch.size(), rocksdb::IOOptions{}, &result, scratch.data(), nullptr);
            return result.ToString();
        }));
    }
    for (auto& reader : readers) {
        const auto data = reader.wait_for(std::chrono::seconds(5)) == std::future_status::ready ? reader.get() : "";
        // A reader that lost the race falls back to a download, which the deferred environment holds.
        EXPECT_TRUE(data.empty() || data == std::string(512, 'P'));
    }
    EXPECT_EQ(0, budget->load());
    while (m_deferredEnv.Blob->PendingDownloads() > 0) {
        m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));
    }
}

TEST_F(AsyncReadTests, Prefetch_DeclinedForBudget_DoesNotEvictFinishedRange) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget)};
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_TRUE(file.Prefetch(1024, 512, rocksdb::IOOptions{}, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'A'));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'B'));

    budget->store(FakeReadableFileImpl::kDefaultPrefetchBudgetBytes);
    EXPECT_TRUE(file.Prefetch(2048, 1024, rocksdb::IOOptions{}, nullptr).ok());

    std::vector<char> scratch(256);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
}

TEST_F(AsyncReadTests, Prefetch_AllSlotsStillDownloading_DeclinedReturnsOk) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_TRUE(file.Prefetch(1024, 512, rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_TRUE(file.Prefetch(2048, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(2U, m_deferredEnv.Blob->PendingDownloads());
}

TEST_F(AsyncReadTests, Prefetch_OverSharedBudget_DeclinedReturnsOkAndFailedPrefetchReleasesBytes) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget)};

    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(512, budget->load());
    m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(500, "boom"));
    EXPECT_EQ(0, budget->load());

    budget->store(FakeReadableFileImpl::kDefaultPrefetchBudgetBytes);
    EXPECT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(FakeReadableFileImpl::kDefaultPrefetchBudgetBytes, budget->load());
}

TEST_F(AsyncReadTests, Prefetch_ConfiguredBudget_CapsPrefetchedBytes) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget, 1024)};

    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    // 512 + 1024 would exceed the 1024-byte budget, so no second download starts.
    EXPECT_TRUE(file.Prefetch(1024, 1024, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(512, budget->load());
}

TEST_F(AsyncReadTests, Prefetch_AllocationFailureWhileAdmitting_RestoresBudgetAndLeavesNoPendingSlot) {
    for (size_t failAt = 1; failAt <= 2; ++failAt) {
        auto budget = std::make_shared<std::atomic<int64_t>>(0);
        auto file = std::make_shared<FakeReadableFileImpl>("test-" + std::string(200, 'n'), m_deferredEnv.Runtime,
                                                           m_deferredEnv.Client, nullptr, m_logger, nullptr, budget);
        ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 0, 512));
        m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'A'));
        ASSERT_EQ(512, budget->load());

        g_failLargeAllocationIn = failAt;
        EXPECT_THROW((void)FakeReadableFileImpl::Prefetch(file, 1024, 512), std::bad_alloc) << "failAt=" << failAt;
        g_failLargeAllocationIn = 0;

        EXPECT_EQ(512, budget->load()) << "failAt=" << failAt;
        EXPECT_FALSE(file->HasPendingPrefetch(1024, 512)) << "failAt=" << failAt;
        EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads()) << "failAt=" << failAt;
        std::vector<char> buffer(256);
        EXPECT_EQ(256U, file->TryReadFromCache(0, 256, buffer.data()).value_or(0)) << "failAt=" << failAt;

        ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 1024, 512)) << "failAt=" << failAt;
        EXPECT_EQ(1U, m_deferredEnv.Blob->PendingDownloads()) << "failAt=" << failAt;
        m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'B'));
        EXPECT_EQ(256U, file->TryReadFromCache(1024, 256, buffer.data()).value_or(0)) << "failAt=" << failAt;
    }
}

TEST_F(AsyncReadTests, Prefetch_ZeroBudget_NeverStartsADownload) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>(
        "test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, nullptr, budget, 0)};

    EXPECT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(0, budget->load());
}

namespace {
void FillPattern(FakeBlobEnvironment& env, const int64_t size) {
    env.Fill(size);
    for (size_t i = 0; i < env.Blob->Data.size(); ++i) {
        env.Blob->Data[i] = static_cast<char>('a' + (i % 26));
    }
}

std::string Expected(const uint64_t offset, const size_t length) {
    std::string out(length, '\0');
    for (size_t i = 0; i < length; ++i) {
        out[i] = static_cast<char>('a' + ((offset + i) % 26));
    }
    return out;
}

void SetRequest(rocksdb::FSReadRequest& req, const uint64_t offset, std::vector<char>& scratch) {
    req.offset = offset;
    req.len = scratch.size();
    req.scratch = scratch.data();
}
} // namespace

TEST_F(AsyncReadTests, MultiRead_AdjacentAndNearbyRanges_AreFetchedWithOneDownload) {
    FillPattern(m_inlineEnv, 100000);
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> scratchA(256);
    std::vector<char> scratchB(256);
    std::vector<char> scratchC(100);
    std::vector<rocksdb::FSReadRequest> reqs(3);
    SetRequest(reqs[0], 256, scratchB);
    SetRequest(reqs[1], 0, scratchA);
    SetRequest(reqs[2], 600, scratchC);

    ASSERT_TRUE(file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr).ok());

    ASSERT_EQ(1U, m_inlineEnv.Blob->Downloads.size());
    EXPECT_EQ(0, m_inlineEnv.Blob->Downloads.front().Offset);
    EXPECT_EQ(700, m_inlineEnv.Blob->Downloads.front().Length);
    EXPECT_TRUE(reqs[0].status.ok());
    EXPECT_EQ(Expected(256, 256), reqs[0].result.ToString());
    EXPECT_EQ(Expected(0, 256), reqs[1].result.ToString());
    EXPECT_EQ(Expected(600, 100), reqs[2].result.ToString());
}

TEST_F(AsyncReadTests, MultiRead_OverlappingRanges_EachGetsItsOwnBytes) {
    FillPattern(m_inlineEnv, 100000);
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> scratchA(300);
    std::vector<char> scratchB(300);
    std::vector<rocksdb::FSReadRequest> reqs(2);
    SetRequest(reqs[0], 0, scratchA);
    SetRequest(reqs[1], 100, scratchB);

    ASSERT_TRUE(file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_EQ(1U, m_inlineEnv.Blob->Downloads.size());
    EXPECT_EQ(Expected(0, 300), reqs[0].result.ToString());
    EXPECT_EQ(Expected(100, 300), reqs[1].result.ToString());
}

TEST_F(AsyncReadTests, MultiRead_RangesFartherApartThanTheGap_AreFetchedSeparately) {
    FillPattern(m_inlineEnv, 100000);
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> scratchA(100);
    std::vector<char> scratchB(100);
    std::vector<rocksdb::FSReadRequest> reqs(2);
    SetRequest(reqs[0], 0, scratchA);
    SetRequest(reqs[1], 100 + 4097, scratchB);

    ASSERT_TRUE(file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_EQ(2U, m_inlineEnv.Blob->Downloads.size());
    EXPECT_EQ(Expected(0, 100), reqs[0].result.ToString());
    EXPECT_EQ(Expected(4197, 100), reqs[1].result.ToString());
}

TEST_F(AsyncReadTests, MultiRead_MergedGroupCrossingEof_GivesPartialAndEmptyResults) {
    FillPattern(m_inlineEnv, 1000);
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> scratchA(100);
    std::vector<char> scratchB(200);
    std::vector<char> scratchC(100);
    std::vector<rocksdb::FSReadRequest> reqs(3);
    SetRequest(reqs[0], 900, scratchA);
    SetRequest(reqs[1], 900 + 100 - 50, scratchB);
    SetRequest(reqs[2], 1100, scratchC);

    ASSERT_TRUE(file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_TRUE(reqs[0].status.ok()) << reqs[0].status.ToString();
    EXPECT_EQ(Expected(900, 100), reqs[0].result.ToString());
    EXPECT_TRUE(reqs[1].status.ok()) << reqs[1].status.ToString();
    EXPECT_EQ(Expected(950, 50), reqs[1].result.ToString());
    EXPECT_TRUE(reqs[2].status.ok()) << reqs[2].status.ToString();
    EXPECT_EQ(0U, reqs[2].result.size());
}

TEST_F(AsyncReadTests, MultiRead_MergedDownloadFails_EveryMemberReportsTheError) {
    FillPattern(m_inlineEnv, 100000);
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> scratchA(100);
    std::vector<char> scratchB(100);
    std::vector<rocksdb::FSReadRequest> reqs(2);
    SetRequest(reqs[0], 0, scratchA);
    SetRequest(reqs[1], 100, scratchB);
    m_inlineEnv.Blob->Fault = [](const auto operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
        if (operation == FakeBlobServer::Operation::Download) {
            return FakeBlobServer::Error(403, "denied");
        }
        return std::nullopt;
    };

    ASSERT_TRUE(file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_FALSE(reqs[0].status.ok());
    EXPECT_FALSE(reqs[1].status.ok());
    EXPECT_EQ(0U, reqs[0].result.size());
    EXPECT_EQ(0U, reqs[1].result.size());
}

TEST_F(AsyncReadTests, MultiRead_StartsTheNextDownloadAsSoonAsAnyFinishes) {
    FillPattern(m_deferredEnv, 1000000);
    auto file = CreateFile(m_deferredEnv);
    constexpr size_t count = 40;
    std::vector<std::vector<char>> scratch(count, std::vector<char>(16));
    std::vector<rocksdb::FSReadRequest> reqs(count);
    for (size_t i = 0; i < count; ++i) {
        SetRequest(reqs[i], i * 20000, scratch[i]);
    }

    rocksdb::IOStatus status;
    std::jthread reader([&] { status = file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr); });

    ASSERT_TRUE(m_deferredEnv.Blob->WaitForPendingDownloads(32));
    auto first = m_deferredEnv.Blob->TakeDownload();
    first.Succeed(Expected(first.Request.Offset, 16));
    // The window refills while the other 31 are still outstanding.
    ASSERT_TRUE(m_deferredEnv.Blob->WaitForPendingDownloads(32));

    size_t served = 1;
    while (served < count) {
        ASSERT_TRUE(m_deferredEnv.Blob->WaitForPendingDownloads(1));
        auto download = m_deferredEnv.Blob->TakeDownload();
        download.Succeed(Expected(download.Request.Offset, 16));
        ++served;
    }
    reader.join();

    EXPECT_TRUE(status.ok()) << status.ToString();
    for (size_t i = 0; i < count; ++i) {
        EXPECT_TRUE(reqs[i].status.ok()) << i;
        EXPECT_EQ(Expected(i * 20000, 16), reqs[i].result.ToString()) << i;
    }
}

TEST_F(AsyncReadTests, MultiRead_ManyMergedGroupsWithStalledOne_RetainsOnlyTheActiveWindowsBuffers) {
    FillPattern(m_deferredEnv, 3000000);
    auto file = CreateFile(m_deferredEnv);
    constexpr size_t groups = 80;
    // Each pair is 3000 bytes apart, so it merges into one 3016-byte download; pairs are farther apart than the gap.
    constexpr size_t mergedLength = 3016;
    std::vector<std::vector<char>> scratch(groups * 2, std::vector<char>(16));
    std::vector<rocksdb::FSReadRequest> reqs(groups * 2);
    for (size_t i = 0; i < groups; ++i) {
        SetRequest(reqs[2 * i], i * 30000, scratch[2 * i]);
        SetRequest(reqs[2 * i + 1], i * 30000 + 3000, scratch[2 * i + 1]);
    }

    g_liveTrackedAllocations = 0;
    g_peakTrackedAllocations = 0;
    g_trackedAllocationSize = mergedLength;
    rocksdb::IOStatus status;
    {
        std::jthread reader([&] { status = file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr); });

        // The first group stays stalled until every other one has been served, so finished buffers pile up if kept.
        ASSERT_TRUE(m_deferredEnv.Blob->WaitForPendingDownloads(1));
        auto stalled = m_deferredEnv.Blob->TakeDownload();
        ASSERT_EQ(0, stalled.Request.Offset);
        for (size_t served = 1; served < groups; ++served) {
            ASSERT_TRUE(m_deferredEnv.Blob->WaitForPendingDownloads(1));
            auto download = m_deferredEnv.Blob->TakeDownload();
            ASSERT_EQ(mergedLength, static_cast<size_t>(download.Request.Length));
            download.Succeed(Expected(download.Request.Offset, mergedLength));
        }
        stalled.Succeed(Expected(0, mergedLength));
    }
    g_trackedAllocationSize = 0;

    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_LE(g_peakTrackedAllocations.load(), 32U);
    EXPECT_EQ(0U, g_liveTrackedAllocations.load());
    for (size_t i = 0; i < groups; ++i) {
        EXPECT_EQ(Expected(i * 30000, 16), reqs[2 * i].result.ToString()) << i;
        EXPECT_EQ(Expected(i * 30000 + 3000, 16), reqs[2 * i + 1].result.ToString()) << i;
    }
}
TEST_F(AsyncReadTests, ReadAsync_PassesIoOptionsTimeoutToTheDownload) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(64);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    rocksdb::IOOptions options;
    options.timeout = std::chrono::microseconds(2500);

    ASSERT_TRUE(Submit(file, req, record, handle, options).ok());
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    auto download = m_deferredEnv.Blob->TakeDownload();
    EXPECT_EQ(std::chrono::milliseconds(3), download.Request.Timeout);
    download.Succeed(std::string(64, 'C'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
}

TEST_F(AsyncReadTests, ReadAsync_WithoutIoOptionsTimeoutRequestsNoDeadline) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(64);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;

    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    auto download = m_deferredEnv.Blob->TakeDownload();
    EXPECT_EQ(m_deferredEnv.Client->GetDefaultRequestOptions().GetTimeout(), download.Request.Timeout);
    download.Succeed(std::string(64, 'C'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
}

TEST_F(AsyncReadTests, ReadAsync_PausedPrefetchWithShortTimeout_TimesOutLeavesScratchAndKeepsThePrefetch) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(1024, 2048, rocksdb::IOOptions{}, nullptr).ok());
    std::vector<char> scratch(64, 'x');
    auto req = MakeRequest(1100, scratch);
    CallbackRecord record;
    IoHandle handle;
    rocksdb::IOOptions options;
    options.timeout = std::chrono::milliseconds(30);

    ASSERT_TRUE(Submit(file, req, record, handle, options).ok());
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_TRUE(record.Status.IsTimedOut()) << record.Status.ToString();
    // The shared prefetch is not cancelled for one waiter, and finishing it later must not revive the read.
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(2048, 'P'));
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(std::string(64, 'x'), std::string(scratch.begin(), scratch.end()));
}

TEST_F(AsyncReadTests, ReadAsync_DelayedMetadataWithShortTimeout_TimesOutWithoutCancellingTheRefresh) {
    auto file = CreateFile(m_deferredEnv);
    m_deferredEnv.Blob->DeferProperties = true;
    std::vector<char> scratch(64, 'x');
    auto req = MakeRequest(BlobSize, scratch);
    CallbackRecord record;
    IoHandle handle;
    rocksdb::IOOptions options;
    options.timeout = std::chrono::milliseconds(30);

    ASSERT_TRUE(Submit(file, req, record, handle, options).ok());
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingProperties());
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_TRUE(record.Status.IsTimedOut()) << record.Status.ToString();
    EXPECT_EQ(1U, m_deferredEnv.Blob->PendingProperties());
    m_deferredEnv.Blob->CompleteProperties();
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingProperties());
    EXPECT_EQ(std::string(64, 'x'), std::string(scratch.begin(), scratch.end()));
}

TEST_F(AsyncReadTests, ReadAsync_StaleReadAfterTheDeadline_TimesOutInsteadOfRefreshingAndRetrying) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    rocksdb::IOOptions options;
    options.timeout = std::chrono::milliseconds(150);

    ASSERT_TRUE(Submit(file, req, record, handle, options).ok());
    auto download = m_deferredEnv.Blob->TakeDownload();
    const int propertiesBefore = m_deferredEnv.Blob->PropertiesRequests;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    m_deferredEnv.Blob->ETag = "etag2";
    download.Fail(FakeBlobServer::Error(412, "ConditionNotMet"));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_TRUE(record.Status.IsTimedOut()) << record.Status.ToString();
    EXPECT_EQ(propertiesBefore, m_deferredEnv.Blob->PropertiesRequests);
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
    EXPECT_EQ(std::string(16, 'x'), std::string(scratch.begin(), scratch.end()));
}

TEST_F(AsyncReadTests, ReadAsync_InlineCompletion_PollDeliversData) {
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;

    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    EXPECT_EQ(0, record.Calls.load());
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_EQ(std::string(16, 'B'), record.Data);
}

TEST_F(AsyncReadTests, Poll_CalledRepeatedly_InvokesCallbackOnce) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(16, 'C'));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    ASSERT_TRUE(AbortAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
}

TEST_F(AsyncReadTests, ReadAsync_ReadPastEnd_TruncatesToBlobSize) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(64);
    auto req = MakeRequest(BlobSize - 10, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    auto download = m_deferredEnv.Blob->TakeDownload();
    EXPECT_EQ(10, download.Request.Length);
    download.Succeed(std::string(10, 'D'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(std::string(10, 'D'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_AtEndWithUnchangedEtag_ReturnsEmptyWithoutDownloading) {
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(BlobSize, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_TRUE(record.Status.ok());
    EXPECT_TRUE(record.Data.empty());
    EXPECT_TRUE(m_inlineEnv.Blob->Downloads.empty());
}

TEST_F(AsyncReadTests, ReadAsync_PreconditionFailed_RefreshesMetadataAndRetries) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    m_deferredEnv.Blob->ETag = "etag2";
    m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(412, "ConditionNotMet"));
    // The retry is issued from the failed request's completion, after the metadata refresh.
    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return m_deferredEnv.Blob->PendingDownloads() == 1; }));
    auto retry = m_deferredEnv.Blob->TakeDownload();
    EXPECT_EQ("etag2", retry.Request.IfMatch);
    retry.Succeed(std::string(16, 'E'));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(16, 'E'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_DownloadFails_PollReportsError) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(32);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(404, "BlobNotFound"));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_FALSE(record.Status.ok());
    EXPECT_TRUE(record.Data.empty());
}

TEST_F(AsyncReadTests, AbortIO_WhileInFlight_ReturnsImmediatelyAndLateDataIsDropped) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(AbortAsyncReads(handles).ok());
    EXPECT_EQ(1, record.Calls.load());
    EXPECT_TRUE(record.Status.IsAborted()) << record.Status.ToString();

    handle.Reset();
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(32, 'F'));
    EXPECT_EQ(std::string(32, 'x'), std::string(scratch.begin(), scratch.end()));
    EXPECT_EQ(1, record.Calls.load());
}

TEST_F(AsyncReadTests, AbortIO_AfterDownloadCompleted_ReportsTheRealStatusNotAborted) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(32, 'F'));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(AbortAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(32, 'F'), record.Data);
}

TEST_F(AsyncReadTests, AbortIO_AfterDownloadFailed_ReportsTheRealError) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(404, "BlobNotFound"));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(AbortAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_FALSE(record.Status.ok());
    EXPECT_FALSE(record.Status.IsAborted()) << record.Status.ToString();
}

TEST(AsyncReadRequestTests, Abort_NotifiesTheSharedSignalSoWaitersWakeUp) {
    auto signal = std::make_shared<AVEVA::RocksDB::Plugin::Azure::Impl::AsyncReadSignal>();
    std::vector<char> scratch(16);
    rocksdb::FSReadRequest fsRequest;
    fsRequest.offset = 0;
    fsRequest.len = scratch.size();
    fsRequest.scratch = scratch.data();
    AVEVA::RocksDB::Plugin::Azure::Impl::AsyncReadRequest request(fsRequest, RecordCallback, nullptr, signal);

    const auto seen = signal->Count();
    request.Abort();

    EXPECT_GT(signal->Count(), seen);
    signal->WaitPast(seen);
    EXPECT_TRUE(request.Finished());
}

TEST(AsyncReadRequestTests, DeliverCallback_NullCallback_DoesNotCrash) {
    std::vector<char> scratch(16);
    rocksdb::FSReadRequest fsRequest;
    fsRequest.offset = 0;
    fsRequest.len = scratch.size();
    fsRequest.scratch = scratch.data();
    AVEVA::RocksDB::Plugin::Azure::Impl::AsyncReadRequest request(fsRequest, nullptr, nullptr);
    request.Abort();

    EXPECT_NO_THROW(request.DeliverCallback());
}

TEST_F(AsyncReadTests, Read_RangeOverflowingInt64_ReturnsInvalidArgument) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16);
    rocksdb::Slice result;
    constexpr auto max = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());

    EXPECT_TRUE(file.Read(max, 1, rocksdb::IOOptions{}, &result, scratch.data(), nullptr).IsInvalidArgument());
    EXPECT_TRUE(file.Read(max - 4, 5, rocksdb::IOOptions{}, &result, scratch.data(), nullptr).IsInvalidArgument());
    EXPECT_TRUE(
        file.Read(std::numeric_limits<uint64_t>::max(), 1, rocksdb::IOOptions{}, &result, scratch.data(), nullptr)
            .IsInvalidArgument());
}

TEST_F(AsyncReadTests, ReadAsyncAndPrefetch_RangeOverflowingInt64_ReturnInvalidArgument) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - 4, scratch);
    CallbackRecord record;
    IoHandle handle;

    EXPECT_TRUE(Submit(file, req, record, handle).IsInvalidArgument());
    EXPECT_TRUE(
        file.Prefetch(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()), 1, rocksdb::IOOptions{}, nullptr)
            .IsInvalidArgument());
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
}

TEST_F(AsyncReadTests, MultiRead_OverflowingRequest_FailsOnlyThatRequest) {
    auto file = CreateFile(m_inlineEnv);
    std::vector<char> good(16);
    std::vector<char> bad(16);
    std::vector<rocksdb::FSReadRequest> reqs(2);
    reqs[0] = MakeRequest(0, good);
    reqs[1] = MakeRequest(UINT64_MAX - 3, bad);

    ASSERT_TRUE(file.MultiRead(reqs.data(), reqs.size(), rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_TRUE(reqs[0].status.ok()) << reqs[0].status.ToString();
    EXPECT_TRUE(reqs[1].status.IsInvalidArgument()) << reqs[1].status.ToString();
}

TEST_F(AsyncReadTests, MultiRead_AllocationFailurePartwayThroughSetup_EveryRequestGetsAFinalOutcome) {
    FillPattern(m_inlineEnv, 100000);
    {
        // Warms up the logger so its lazy first-use allocations are not what the armed allocation failure hits.
        auto warmFile = CreateFile(m_inlineEnv);
        std::vector<char> scratch(16);
        std::vector<rocksdb::FSReadRequest> warm(1);
        SetRequest(warm[0], 0, scratch);
        m_inlineEnv.Blob->Fault = [](const auto operation) -> std::optional<AVEVA::AzureClient::BlobStorageError> {
            if (operation == FakeBlobServer::Operation::Download) {
                return FakeBlobServer::Error(403, "denied");
            }
            return std::nullopt;
        };
        ASSERT_TRUE(warmFile.MultiRead(warm.data(), warm.size(), rocksdb::IOOptions{}, nullptr).ok());
        m_inlineEnv.Blob->Fault = nullptr;
    }
    bool sawFailure = false;
    for (size_t failAt = 1; failAt <= 6; ++failAt) {
        auto file = CreateFile(m_inlineEnv);
        std::vector<char> invalid(16);
        std::vector<char> mergedA(256);
        std::vector<char> mergedB(256);
        std::vector<char> separate(128);
        std::vector<rocksdb::FSReadRequest> reqs(4);
        reqs[0] = MakeRequest(UINT64_MAX - 3, invalid);
        SetRequest(reqs[1], 0, mergedA);
        SetRequest(reqs[2], 256, mergedB);
        SetRequest(reqs[3], 50000, separate);
        // Stale values a pending request must not keep.
        for (auto& req : reqs) {
            req.status = rocksdb::IOStatus::OK();
            req.result = rocksdb::Slice(req.scratch, 1);
        }

        const rocksdb::IOOptions options;
        g_failLargeAllocationIn = failAt;
        const auto status = file.MultiRead(reqs.data(), reqs.size(), options, nullptr);
        g_failLargeAllocationIn = 0;
        EXPECT_TRUE(reqs[0].status.IsInvalidArgument()) << "failAt=" << failAt << " " << reqs[0].status.ToString();
        const std::array<size_t, 4> lengths{0, 256, 256, 128};
        for (size_t i = 1; i < reqs.size(); ++i) {
            if (reqs[i].status.ok()) {
                EXPECT_EQ(lengths[i], reqs[i].result.size()) << "failAt=" << failAt << " request " << i;
            } else {
                sawFailure = true;
                EXPECT_EQ(0U, reqs[i].result.size()) << "failAt=" << failAt << " request " << i;
            }
        }
        if (status.ok() && reqs[1].status.ok() && reqs[3].status.ok()) {
            EXPECT_EQ(Expected(0, 256), reqs[1].result.ToString()) << "failAt=" << failAt;
            EXPECT_EQ(Expected(50000, 128), reqs[3].result.ToString()) << "failAt=" << failAt;
        }
    }
    EXPECT_TRUE(sawFailure);
}

TEST_F(AsyncReadTests, ReadAsync_NullCallback_ReturnsInvalidArgument) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    IoHandle handle;

    const auto status =
        file.ReadAsync(req, rocksdb::IOOptions{}, nullptr, nullptr, &handle.Handle, &handle.Deleter, nullptr);

    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
}

TEST_F(AsyncReadTests, ReadAsync_PreconditionFailedEveryTime_StopsAfterTheRetryLimitAndReportsAnError) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    for (int attempt = 0; attempt <= FakeReadableFileImpl::kMaxStaleReadRetries; ++attempt) {
        ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return m_deferredEnv.Blob->PendingDownloads() == 1; }))
            << "attempt " << attempt;
        m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(412, "ConditionNotMet"));
    }
    EXPECT_FALSE(m_deferredEnv.Http.RunUntil([&] { return m_deferredEnv.Blob->PendingDownloads() != 0; },
                                             std::chrono::milliseconds{200}));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_EQ(1, record.Calls.load());
    EXPECT_FALSE(record.Status.ok());
    EXPECT_TRUE(record.Data.empty());
}

TEST_F(AsyncReadTests, DeleteHandle_BeforeCompletion_NeverCallsBackOrWritesScratch) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    {
        IoHandle handle;
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(32, 'G'));

    EXPECT_EQ(0, record.Calls.load());
    EXPECT_EQ(std::string(32, 'x'), std::string(scratch.begin(), scratch.end()));
}

TEST_F(AsyncReadTests, ReadAsync_FileDestroyedBeforeCompletion_CompletionStillSafe) {
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    {
        auto file = CreateFile(m_deferredEnv);
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(16, 'H'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(std::string(16, 'H'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_ManyRequests_CompletedConcurrentlyFromOtherThreads) {
    auto file = CreateFile(m_deferredEnv);
    constexpr size_t Count = 16;
    std::vector<std::vector<char>> scratches(Count, std::vector<char>(64));
    std::vector<CallbackRecord> records(Count);
    std::vector<IoHandle> handles(Count);
    std::vector<void*> rawHandles;
    for (size_t i = 0; i < Count; ++i) {
        auto req = MakeRequest(i * 64, scratches[i]);
        ASSERT_TRUE(Submit(file, req, records[i], handles[i]).ok());
        rawHandles.push_back(handles[i].Handle);
    }

    std::vector<std::jthread> network;
    for (size_t i = 0; i < Count; ++i) {
        network.emplace_back([download = m_deferredEnv.Blob->TakeDownload()]() mutable {
            download.Succeed(std::string(64, static_cast<char>('a' + download.Request.Offset / 64)));
        });
    }
    ASSERT_TRUE(PollAsyncReads(rawHandles).ok());
    for (auto& thread : network) {
        thread.join();
    }

    for (size_t i = 0; i < Count; ++i) {
        EXPECT_EQ(1, records[i].Calls.load());
        EXPECT_EQ(std::string(64, static_cast<char>('a' + i)), records[i].Data);
    }
}

namespace {
class HostIoContext {
  public:
    HostIoContext() : m_work(boost::asio::make_work_guard(m_context)), m_thread([this] { m_context.run(); }) {}
    HostIoContext(const HostIoContext&) = delete;
    HostIoContext& operator=(const HostIoContext&) = delete;
    ~HostIoContext() {
        m_work.reset();
        m_thread.join();
    }

    boost::asio::io_context& Context() { return m_context; }

  private:
    boost::asio::io_context m_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_work;
    std::thread m_thread;
};
} // namespace

TEST_F(AsyncReadTests, MultiReadAndPoll_FromClientIoContextThread_FailInsteadOfHanging) {
    auto file = CreateFile(m_deferredEnv);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    std::vector<char> multiScratch(16);
    std::array<rocksdb::FSReadRequest, 1> multi{MakeRequest(64, multiScratch)};
    rocksdb::IOStatus multiStatus;
    rocksdb::IOStatus pollStatus;
    std::atomic<bool> done{false};
    boost::asio::post(m_deferredEnv.Http.get_executor(), [&] {
        multiStatus = file.MultiRead(multi.data(), multi.size(), rocksdb::IOOptions{}, nullptr);
        pollStatus = PollAsyncReads({handle.Handle});
        done = true;
    });
    ASSERT_TRUE(m_deferredEnv.Http.RunUntil([&] { return done.load(); }));

    EXPECT_TRUE(multiStatus.IsInvalidArgument()) << multiStatus.ToString();
    EXPECT_TRUE(pollStatus.IsInvalidArgument()) << pollStatus.ToString();
    EXPECT_EQ(0, record.Calls.load());
}

TEST_F(AsyncReadTests, Tracker_DrainWithNothingInFlightReturnsImmediately) {
    HostIoContext host;
    auto tracker = std::make_shared<AsyncReadTracker>(host.Context().get_executor());
    EXPECT_EQ(0U, tracker->InFlight());
    tracker->Drain();
}

TEST_F(AsyncReadTests, ReadAsync_HandleAndFilesystemDroppedMidRead_DrainWaitsUntilFileReleased) {
    HostIoContext host;
    auto tracker = std::make_shared<AsyncReadTracker>(host.Context().get_executor());
    FakeBlobEnvironment standalone;
    auto& server = *standalone.Blob;
    server.SetCapacity(BlobSize);
    server.Size = BlobSize;
    server.ETag = "etag";
    server.DeferDownloads = true;
    // A client owned only by this test (the environment's is also held by its server), so that expiry shows when the
    // in-flight read releases the file.
    auto client = std::make_shared<FakeBlobServer::BlobClientType>(
        standalone.Http, AVEVA::AzureClient::Tests::MakeBlobClientOptions("files", "test.sst"));
    std::weak_ptr<FakeBlobServer::BlobClientType> weakClient = client;

    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    {
        FakeReadableFile file{
            std::make_shared<FakeReadableFileImpl>("test.sst", standalone.Runtime, client, nullptr, m_logger, tracker)};
        IoHandle handle;
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }
    auto download = server.TakeDownload();
    client.reset();
    ASSERT_FALSE(weakClient.expired()) << "the in-flight read must keep the file alive";
    EXPECT_EQ(1U, tracker->InFlight());

    std::atomic<bool> fileReleasedWhenDrained{false};
    auto drained = std::async(std::launch::async, [&] {
        tracker->Drain();
        fileReleasedWhenDrained = weakClient.expired();
    });
    EXPECT_EQ(std::future_status::timeout, drained.wait_for(std::chrono::milliseconds(50)));

    boost::asio::post(host.Context(),
                      [download = std::move(download)]() mutable { download.Succeed(std::string(32, 'G')); });
    ASSERT_EQ(std::future_status::ready, drained.wait_for(std::chrono::seconds(10)));
    drained.get();

    EXPECT_TRUE(fileReleasedWhenDrained.load());
    EXPECT_EQ(0U, tracker->InFlight());
    EXPECT_EQ(0, record.Calls.load());
    EXPECT_EQ(std::string(32, 'x'), std::string(scratch.begin(), scratch.end()));
}

TEST_F(AsyncReadTests, ReadAsync_TrackedReadEndsAfterCompletionAndPoll) {
    HostIoContext host;
    auto tracker = std::make_shared<AsyncReadTracker>(host.Context().get_executor());
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime,
                                                                 m_deferredEnv.Client, nullptr, m_logger, tracker)};
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    EXPECT_EQ(1U, tracker->InFlight());

    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(16, 'T'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_EQ(std::string(16, 'T'), record.Data);

    tracker->Drain();
    EXPECT_EQ(0U, tracker->InFlight());
}

TEST_F(AsyncReadTests, ReadAsync_RestartAfter412ThatThrowsOnInitiation_FailsTheReadAndDrainsTheTracker) {
    HostIoContext host;
    auto tracker = std::make_shared<AsyncReadTracker>(host.Context().get_executor());
    FakeReadableFile file{std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime,
                                                                 m_deferredEnv.Client, nullptr, m_logger, tracker)};
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    m_deferredEnv.Blob->DeferProperties = true;
    auto download = m_deferredEnv.Blob->TakeDownload();

    EXPECT_NO_THROW({
        download.Fail(FakeBlobServer::Error(412, "ConditionNotMet"));
        ASSERT_EQ(1U, m_deferredEnv.Blob->PendingProperties());
        m_deferredEnv.Http.FailNextInitiation(std::make_exception_ptr(std::bad_alloc()));
        m_deferredEnv.Blob->CompleteProperties();
    });

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_EQ(1, record.Calls.load());
    EXPECT_FALSE(record.Status.ok());
    handle.Reset();
    EXPECT_EQ(0U, tracker->InFlight());
}

TEST_F(AsyncReadTests, ReadAsync_WaiterOnSharedMetadataRefreshThrows_TheOtherWaiterStillCompletes) {
    auto file = CreateFile(m_deferredEnv);
    m_deferredEnv.Blob->SetCapacity(BlobSize * 2);
    m_deferredEnv.Blob->Size = BlobSize * 2;
    m_deferredEnv.Blob->ETag = "etag2";
    m_deferredEnv.Blob->DeferProperties = true;

    std::vector<char> scratchA(16);
    std::vector<char> scratchB(16);
    auto reqA = MakeRequest(BlobSize, scratchA);
    auto reqB = MakeRequest(BlobSize, scratchB);
    CallbackRecord recordA;
    CallbackRecord recordB;
    IoHandle handleA;
    IoHandle handleB;
    ASSERT_TRUE(Submit(file, reqA, recordA, handleA).ok());
    ASSERT_TRUE(Submit(file, reqB, recordB, handleB).ok());
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingProperties());

    // The first waiter restarts its read and its download initiation throws; the second must not be skipped.
    EXPECT_NO_THROW({
        m_deferredEnv.Http.FailNextInitiation(std::make_exception_ptr(std::bad_alloc()));
        m_deferredEnv.Blob->CompleteProperties();
    });
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(16, 'W'));

    std::vector<void*> handles{handleA.Handle, handleB.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_EQ(1, recordA.Calls.load());
    EXPECT_FALSE(recordA.Status.ok());
    EXPECT_EQ(1, recordB.Calls.load());
    EXPECT_TRUE(recordB.Status.ok()) << recordB.Status.ToString();
    EXPECT_EQ(std::string(16, 'W'), recordB.Data);
}

// A responder records a held request before the transport registers it as pending. Counting the held requests in
// that window must not forget the request: doing so made the 32-wide MultiRead window tests wait for a download that
// the test had silently dropped.
TEST(HeldRequestsTests, CountingBeforeTheTransportRegistersTheRequestKeepsIt) {
    AVEVA::AzureClient::Tests::FakeHttpClient http;
    AVEVA::AzureClient::Tests::HeldRequests<> held;
    size_t countedWhileUnregistered = 0;
    http.SetResponder([&](const AVEVA::AzureClient::Tests::FakeHttpClient::RequestRecord& record) {
        AVEVA::AzureClient::Tests::FakeHttpClient::ScriptedResult result;
        result.Deferred = true;
        held.Add(record.Index);
        countedWhileUnregistered = held.Count(http);
        return result;
    });

    http.SendAsyncErased(
        AVEVA::HttpRequest{}, [](std::error_code, AVEVA::HttpResponse) {}, AVEVA::HttpRequestOptions{});

    EXPECT_EQ(1U, countedWhileUnregistered);
    EXPECT_EQ(1U, held.Count(http));
    EXPECT_TRUE(http.CompleteRequest(0));
    EXPECT_EQ(0U, held.Count(http));
}
