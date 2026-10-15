// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
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
        return FakeReadableFile{FakeReadableFileImpl{"test.sst", env.Runtime, env.Client, nullptr, m_logger,
                                                     std::move(tracker), std::move(budget)}};
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

TEST_F(AsyncReadTests, GetSize_RefreshThatFindsTheSameEtag_KeepsThePrefetchedBytes) {
    auto file = std::make_shared<FakeReadableFileImpl>("test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                                       m_logger);
    ASSERT_TRUE(FakeReadableFileImpl::Prefetch(file, 0, 512));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'P'));
    std::this_thread::sleep_for(Configuration::MetadataCacheTtl + std::chrono::milliseconds(50));
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
    std::this_thread::sleep_for(Configuration::MetadataCacheTtl + std::chrono::milliseconds(50));

    EXPECT_EQ(BlobSize, file->GetSize());

    std::vector<char> buffer(256);
    EXPECT_FALSE(file->TryReadFromCache(0, 256, buffer.data()).has_value());
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

TEST_F(AsyncReadTests, Read_PastTheEndOfPrefetchedRange_ReleasesItsBudget) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{FakeReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                               m_logger, nullptr, budget}};
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

TEST_F(AsyncReadTests, Prefetch_DeclinedForBudget_DoesNotEvictFinishedRange) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    FakeReadableFile file{FakeReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                               m_logger, nullptr, budget}};
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
    FakeReadableFile file{FakeReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr,
                                               m_logger, nullptr, budget}};

    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(512, budget->load());
    m_deferredEnv.Blob->TakeDownload().Fail(FakeBlobServer::Error(500, "boom"));
    EXPECT_EQ(0, budget->load());

    budget->store(FakeReadableFileImpl::kDefaultPrefetchBudgetBytes);
    EXPECT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(FakeReadableFileImpl::kDefaultPrefetchBudgetBytes, budget->load());
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
        FakeReadableFile file{FakeReadableFileImpl{"test.sst", standalone.Runtime, client, nullptr, m_logger, tracker}};
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
    FakeReadableFile file{
        FakeReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Client, nullptr, m_logger, tracker}};
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
