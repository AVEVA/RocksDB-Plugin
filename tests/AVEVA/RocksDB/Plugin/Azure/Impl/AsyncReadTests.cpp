// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include "FakeBlobEnvironment.hpp"
#include "FakeHttpClient.hpp"

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

using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::RocksDB::Plugin::Azure::ReadableFile;
using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::AbortAsyncReads;
using AVEVA::RocksDB::Plugin::Azure::Impl::AsyncReadTracker;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::PollAsyncReads;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakeBlobEnvironment;
using AVEVA::RocksDB::Plugin::Azure::Impl::Tests::FakePageBlobClient;
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
            download.Fail(FakePageBlobClient::Error(500, "test finished"));
        }
    }

    ReadableFile CreateFile(const FakeBlobEnvironment& env, std::shared_ptr<AsyncReadTracker> tracker = nullptr,
                            std::shared_ptr<std::atomic<int64_t>> budget = nullptr) {
        return ReadableFile{ReadableFileImpl{"test.sst", env.Runtime, env.Blob, nullptr, m_logger, std::move(tracker),
                                             std::move(budget)}};
    }

    static rocksdb::IOStatus Submit(ReadableFile& file, rocksdb::FSReadRequest& req, CallbackRecord& record,
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

TEST_F(AsyncReadTests, ReadAsync_RangeCoveredByPendingPrefetch_WaitsForItWithoutBlockingOrDownloadingTwice) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(1024, 2048, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads());

    std::vector<char> scratch(64);
    auto req = MakeRequest(1100, scratch);
    CallbackRecord record;
    IoHandle handle;
    auto done = std::async(std::launch::async, [&] { return Submit(file, req, record, handle); });

    ASSERT_EQ(std::future_status::ready, done.wait_for(std::chrono::seconds(5)));
    EXPECT_TRUE(done.get().ok());
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

TEST_F(AsyncReadTests, ReadAsync_ChainedOntoFailedPrefetch_FallsBackToOwnDownload) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(1024, 2048, rocksdb::IOOptions{}, nullptr).ok());

    std::vector<char> scratch(64);
    auto req = MakeRequest(1100, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    m_deferredEnv.Blob->TakeDownload().Fail(FakePageBlobClient::Error(500, "boom"));
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
    const auto status = file.Read(256, scratch.size(), rocksdb::IOOptions{}, &result, scratch.data(), nullptr);

    EXPECT_TRUE(status.ok()) << status.ToString();
    ASSERT_GE(m_deferredEnv.Blob->Downloads.size(), 2U);
    EXPECT_EQ(512, m_deferredEnv.Blob->Downloads.back().Offset);
    EXPECT_EQ(256, m_deferredEnv.Blob->Downloads.back().Length);
    EXPECT_EQ(std::string(256, 'P') + std::string(256, 'R'), result.ToString());
}

TEST_F(AsyncReadTests, Read_PastTheEndOfPrefetchedRange_ReleasesItsBudget) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    ReadableFile file{
        ReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Blob, nullptr, m_logger, nullptr, budget}};
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
    ReadableFile file{
        ReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Blob, nullptr, m_logger, nullptr, budget}};
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_TRUE(file.Prefetch(1024, 512, rocksdb::IOOptions{}, nullptr).ok());
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'A'));
    m_deferredEnv.Blob->TakeDownload().Succeed(std::string(512, 'B'));

    budget->store(ReadableFileImpl::kDefaultPrefetchBudgetBytes);
    EXPECT_TRUE(file.Prefetch(2048, 1024, rocksdb::IOOptions{}, nullptr).IsNotSupported());

    std::vector<char> scratch(256);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());
}

TEST_F(AsyncReadTests, Prefetch_AllSlotsStillDownloading_ReturnsNotSupported) {
    auto file = CreateFile(m_deferredEnv);
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_TRUE(file.Prefetch(1024, 512, rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_TRUE(file.Prefetch(2048, 512, rocksdb::IOOptions{}, nullptr).IsNotSupported());
    EXPECT_EQ(2U, m_deferredEnv.Blob->PendingDownloads());
}

TEST_F(AsyncReadTests, Prefetch_OverSharedBudget_ReturnsNotSupportedAndFailedPrefetchReleasesBytes) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    ReadableFile file{
        ReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Blob, nullptr, m_logger, nullptr, budget}};

    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(512, budget->load());
    m_deferredEnv.Blob->TakeDownload().Fail(FakePageBlobClient::Error(500, "boom"));
    EXPECT_EQ(0, budget->load());

    budget->store(ReadableFileImpl::kDefaultPrefetchBudgetBytes);
    EXPECT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).IsNotSupported());
    EXPECT_EQ(ReadableFileImpl::kDefaultPrefetchBudgetBytes, budget->load());
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
    EXPECT_EQ(m_deferredEnv.Blob->GetDefaultRequestOptions().GetTimeout(), download.Request.Timeout);
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
    m_deferredEnv.Blob->TakeDownload().Fail(FakePageBlobClient::Error(412, "ConditionNotMet"));
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

    m_deferredEnv.Blob->TakeDownload().Fail(FakePageBlobClient::Error(404, "BlobNotFound"));
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
    m_deferredEnv.Blob->TakeDownload().Fail(FakePageBlobClient::Error(404, "BlobNotFound"));

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

    for (int attempt = 0; attempt <= ReadableFileImpl::kMaxStaleReadRetries; ++attempt) {
        ASSERT_EQ(1U, m_deferredEnv.Blob->PendingDownloads()) << "attempt " << attempt;
        m_deferredEnv.Blob->TakeDownload().Fail(FakePageBlobClient::Error(412, "ConditionNotMet"));
    }
    EXPECT_EQ(0U, m_deferredEnv.Blob->PendingDownloads());

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
    boost::asio::io_context io;
    FakeHttpClient http;
    auto runtime = std::make_shared<ClientRuntime>(io);
    auto client = std::make_shared<FakePageBlobClient>(http);
    client->SetCapacity(BlobSize);
    client->Size = BlobSize;
    client->ETag = "etag";
    client->DeferDownloads = true;
    std::weak_ptr<FakePageBlobClient> weakClient = client;

    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    {
        ReadableFile file{ReadableFileImpl{"test.sst", runtime, client, nullptr, m_logger, tracker}};
        IoHandle handle;
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }
    auto download = client->TakeDownload();
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
    ReadableFile file{
        ReadableFileImpl{"test.sst", m_deferredEnv.Runtime, m_deferredEnv.Blob, nullptr, m_logger, tracker}};
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
