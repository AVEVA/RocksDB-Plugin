// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadRequest.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Mocks/BlobClientMock.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::HttpStatus;
using AVEVA::RocksDB::Plugin::Azure::ReadableFile;
using AVEVA::RocksDB::Plugin::Azure::RequestFailedException;
using AVEVA::RocksDB::Plugin::Azure::Impl::AbortAsyncReads;
using AVEVA::RocksDB::Plugin::Azure::Impl::AsyncReadTracker;
using AVEVA::RocksDB::Plugin::Azure::Impl::PollAsyncReads;
using AVEVA::RocksDB::Plugin::Azure::Impl::ReadableFileImpl;
using AVEVA::RocksDB::Plugin::Core::Mocks::BlobClientMock;
using boost::log::sources::severity_logger_mt;
using boost::log::trivial::severity_level;
using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {
constexpr int64_t BlobSize = 4096;

// Captures DownloadAsync completions so tests decide when (and on which thread) the "network" completes.
class DeferredBlobClient : public NiceMock<BlobClientMock> {
  public:
    struct PendingDownload {
        int64_t Offset;
        int64_t Length;
        std::string IfMatch;
        DownloadCallback Callback;
        std::chrono::milliseconds Timeout{};
    };

    void DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                       DownloadCallback callback) override {
        DownloadAsync(blobOffset, readLength, ifMatch, std::chrono::milliseconds::zero(), std::move(callback));
    }

    void DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                       std::chrono::milliseconds timeout, DownloadCallback callback) override {
        std::scoped_lock lock(m_mutex);
        m_pending.push_back({blobOffset, readLength, ifMatch, std::move(callback), timeout});
    }

    PendingDownload Take() {
        std::scoped_lock lock(m_mutex);
        if (m_pending.empty()) {
            throw std::logic_error("DeferredBlobClient::Take called with no pending download");
        }
        auto download = std::move(m_pending.front());
        m_pending.pop_front();
        return download;
    }

    size_t PendingCount() {
        std::scoped_lock lock(m_mutex);
        return m_pending.size();
    }

  private:
    std::mutex m_mutex;
    std::deque<PendingDownload> m_pending;
};

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

// Owns the io_handle the way RocksDB does: freed through the deleter supplied by the filesystem.
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

rocksdb::FSReadRequest MakeRequest(uint64_t offset, std::vector<char>& scratch) {
    rocksdb::FSReadRequest req;
    req.offset = offset;
    req.len = scratch.size();
    req.scratch = scratch.data();
    return req;
}
} // namespace

class AsyncReadTests : public ::testing::Test {
  protected:
    std::shared_ptr<DeferredBlobClient> m_deferred;
    std::shared_ptr<NiceMock<BlobClientMock>> m_inline;
    std::shared_ptr<severity_logger_mt<severity_level>> m_logger;

    void SetUp() override {
        m_logger = std::make_shared<severity_logger_mt<severity_level>>();
        m_deferred = std::make_shared<DeferredBlobClient>();
        m_inline = std::make_shared<NiceMock<BlobClientMock>>();
        for (BlobClientMock* client :
             {static_cast<BlobClientMock*>(m_deferred.get()), static_cast<BlobClientMock*>(m_inline.get())}) {
            ON_CALL(*client, GetSize()).WillByDefault(Return(BlobSize));
            ON_CALL(*client, GetEtag()).WillByDefault(Return(std::string{"etag"}));
        }
    }

    // Downloads a test leaves pending hold the file, which holds the client: a cycle that leaks the mock.
    void TearDown() override {
        while (m_deferred->PendingCount() > 0) {
            m_deferred->Take().Callback(std::make_exception_ptr(std::runtime_error("test finished")), {});
        }
    }

    ReadableFile CreateFile(std::shared_ptr<BlobClientMock> client) {
        return ReadableFile{ReadableFileImpl{"test.sst", std::move(client), nullptr, m_logger}};
    }

    static rocksdb::IOStatus Submit(ReadableFile& file, rocksdb::FSReadRequest& req, CallbackRecord& record,
                                    IoHandle& handle) {
        return file.ReadAsync(req, rocksdb::IOOptions{}, RecordCallback, &record, &handle.Handle, &handle.Deleter,
                              nullptr);
    }
};

TEST_F(AsyncReadTests, ReadAsync_DoesNotBlockAndPollDeliversOnCallerThread) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(128);
    auto req = MakeRequest(100, scratch);
    CallbackRecord record;
    IoHandle handle;

    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    ASSERT_NE(nullptr, handle.Handle);
    ASSERT_EQ(1U, m_deferred->PendingCount());
    EXPECT_EQ(0, record.Calls.load());

    auto download = m_deferred->Take();
    EXPECT_EQ(100, download.Offset);
    EXPECT_EQ(128, download.Length);
    EXPECT_EQ("etag", download.IfMatch);
    std::jthread network([&download] { download.Callback(nullptr, std::string(128, 'A')); });
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    network.join();

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_EQ(std::this_thread::get_id(), record.Thread);
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(128, 'A'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_RangeCoveredByPendingPrefetch_StartsOwnDownloadWithoutBlocking) {
    auto file = CreateFile(m_deferred);
    ASSERT_TRUE(file.Prefetch(1024, 2048, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_EQ(1U, m_deferred->PendingCount()); // The prefetch download never completes in this test.

    std::vector<char> scratch(64);
    auto req = MakeRequest(1100, scratch);
    CallbackRecord record;
    IoHandle handle;
    auto done = std::async(std::launch::async, [&] { return Submit(file, req, record, handle); });

    ASSERT_EQ(std::future_status::ready, done.wait_for(std::chrono::seconds(5)));
    EXPECT_TRUE(done.get().ok());
    EXPECT_EQ(2U, m_deferred->PendingCount());
}

TEST_F(AsyncReadTests, Prefetch_AllSlotsStillDownloading_ReturnsNotSupported) {
    auto file = CreateFile(m_deferred);
    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    ASSERT_TRUE(file.Prefetch(1024, 512, rocksdb::IOOptions{}, nullptr).ok());

    EXPECT_TRUE(file.Prefetch(2048, 512, rocksdb::IOOptions{}, nullptr).IsNotSupported());
    EXPECT_EQ(2U, m_deferred->PendingCount());
}

TEST_F(AsyncReadTests, Prefetch_OverSharedBudget_ReturnsNotSupportedAndFailedPrefetchReleasesBytes) {
    auto budget = std::make_shared<std::atomic<int64_t>>(0);
    ReadableFile file{ReadableFileImpl{"test.sst", m_deferred, nullptr, m_logger, nullptr, budget}};

    ASSERT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).ok());
    EXPECT_EQ(512, budget->load());
    m_deferred->Take().Callback(std::make_exception_ptr(std::runtime_error("boom")), {});
    EXPECT_EQ(0, budget->load());

    budget->store(ReadableFileImpl::kDefaultPrefetchBudgetBytes);
    EXPECT_TRUE(file.Prefetch(0, 512, rocksdb::IOOptions{}, nullptr).IsNotSupported());
    EXPECT_EQ(ReadableFileImpl::kDefaultPrefetchBudgetBytes, budget->load());
}

TEST_F(AsyncReadTests, ReadAsync_PassesIoOptionsTimeoutToTheDownload) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(64);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    rocksdb::IOOptions options;
    options.timeout = std::chrono::microseconds(2500);

    ASSERT_TRUE(file.ReadAsync(req, options, RecordCallback, &record, &handle.Handle, &handle.Deleter, nullptr).ok());
    ASSERT_EQ(1U, m_deferred->PendingCount());
    auto download = m_deferred->Take();
    EXPECT_EQ(std::chrono::milliseconds(3), download.Timeout);
    download.Callback(nullptr, std::string(64, 'C'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
}

TEST_F(AsyncReadTests, ReadAsync_WithoutIoOptionsTimeoutRequestsNoDeadline) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(64);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;

    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    auto download = m_deferred->Take();
    EXPECT_EQ(std::chrono::milliseconds::zero(), download.Timeout);
    download.Callback(nullptr, std::string(64, 'C'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
}

TEST_F(AsyncReadTests, ReadAsync_InlineCompletion_PollDeliversData) {
    EXPECT_CALL(*m_inline, Download(_, 0, 16, "etag"))
        .WillOnce([](std::span<char> buffer, int64_t, int64_t length, const std::string&) {
            std::fill_n(buffer.begin(), length, 'B');
            return length;
        });
    auto file = CreateFile(m_inline);
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
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    m_deferred->Take().Callback(nullptr, std::string(16, 'C'));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    ASSERT_TRUE(AbortAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
}

TEST_F(AsyncReadTests, ReadAsync_ReadPastEnd_TruncatesToBlobSize) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(64);
    auto req = MakeRequest(BlobSize - 10, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    auto download = m_deferred->Take();
    EXPECT_EQ(10, download.Length);
    download.Callback(nullptr, std::string(10, 'D'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(std::string(10, 'D'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_AtEndWithUnchangedEtag_ReturnsEmptyWithoutDownloading) {
    auto file = CreateFile(m_inline);
    EXPECT_CALL(*m_inline, Download(_, _, _, _)).Times(0);
    std::vector<char> scratch(16);
    auto req = MakeRequest(BlobSize, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_TRUE(record.Status.ok());
    EXPECT_TRUE(record.Data.empty());
}

TEST_F(AsyncReadTests, ReadAsync_PreconditionFailed_RefreshesMetadataAndRetries) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    EXPECT_CALL(*m_deferred, GetEtag()).WillRepeatedly(Return(std::string{"etag2"}));
    m_deferred->Take().Callback(
        std::make_exception_ptr(RequestFailedException(HttpStatus::PreconditionFailed, "ConditionNotMet", "", "", {})),
        {});
    auto retry = m_deferred->Take();
    EXPECT_EQ("etag2", retry.IfMatch);
    retry.Callback(nullptr, std::string(16, 'E'));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(16, 'E'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_DownloadFails_PollReportsError) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(32);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    m_deferred->Take().Callback(
        std::make_exception_ptr(RequestFailedException(HttpStatus::NotFound, "BlobNotFound", "missing", "id", {})), {});
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_FALSE(record.Status.ok());
    EXPECT_TRUE(record.Data.empty());
}

TEST_F(AsyncReadTests, AbortIO_WhileInFlight_ReturnsImmediatelyAndLateDataIsDropped) {
    auto file = CreateFile(m_deferred);
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
    m_deferred->Take().Callback(nullptr, std::string(32, 'F'));
    EXPECT_EQ(std::string(32, 'x'), std::string(scratch.begin(), scratch.end()));
    EXPECT_EQ(1, record.Calls.load());
}

TEST_F(AsyncReadTests, AbortIO_AfterDownloadCompleted_ReportsTheRealStatusNotAborted) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    m_deferred->Take().Callback(nullptr, std::string(32, 'F'));

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(AbortAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_TRUE(record.Status.ok()) << record.Status.ToString();
    EXPECT_EQ(std::string(32, 'F'), record.Data);
}

TEST_F(AsyncReadTests, AbortIO_AfterDownloadFailed_ReportsTheRealError) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    m_deferred->Take().Callback(
        std::make_exception_ptr(RequestFailedException(HttpStatus::NotFound, "BlobNotFound", "missing", "id", {})), {});

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(AbortAsyncReads(handles).ok());

    EXPECT_EQ(1, record.Calls.load());
    EXPECT_FALSE(record.Status.ok());
    EXPECT_FALSE(record.Status.IsAborted()) << record.Status.ToString();
}

TEST_F(AsyncReadTests, ReadAsync_PreconditionFailedEveryTime_StopsAfterTheRetryLimitAndReportsAnError) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());

    // The first download plus kMaxStaleReadRetries refreshed attempts, and no more.
    for (int attempt = 0; attempt <= ReadableFileImpl::kMaxStaleReadRetries; ++attempt) {
        ASSERT_EQ(1u, m_deferred->PendingCount()) << "attempt " << attempt;
        m_deferred->Take().Callback(std::make_exception_ptr(RequestFailedException(HttpStatus::PreconditionFailed,
                                                                                   "ConditionNotMet", "", "", {})),
                                    {});
    }
    EXPECT_EQ(0u, m_deferred->PendingCount());

    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_EQ(1, record.Calls.load());
    EXPECT_FALSE(record.Status.ok());
    EXPECT_TRUE(record.Data.empty());
}

TEST_F(AsyncReadTests, DeleteHandle_BeforeCompletion_NeverCallsBackOrWritesScratch) {
    auto file = CreateFile(m_deferred);
    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    {
        IoHandle handle;
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }

    m_deferred->Take().Callback(nullptr, std::string(32, 'G'));

    EXPECT_EQ(0, record.Calls.load());
    EXPECT_EQ(std::string(32, 'x'), std::string(scratch.begin(), scratch.end()));
}

TEST_F(AsyncReadTests, ReadAsync_FileDestroyedBeforeCompletion_CompletionStillSafe) {
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    {
        auto file = CreateFile(m_deferred);
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }

    m_deferred->Take().Callback(nullptr, std::string(16, 'H'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());

    EXPECT_EQ(std::string(16, 'H'), record.Data);
}

TEST_F(AsyncReadTests, ReadAsync_ManyRequests_CompletedConcurrentlyFromOtherThreads) {
    auto file = CreateFile(m_deferred);
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
        network.emplace_back([download = m_deferred->Take()]() mutable {
            download.Callback(nullptr, std::string(64, static_cast<char>('a' + download.Offset / 64)));
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
// Stands in for the host: runs the io_context that HTTP completions and the tracker's deferred End() run on.
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

// Mirrors the filesystem being torn down while RocksDB has abandoned a read: the handle and the file are gone, the
// download completes later on the io_context. The filesystem (Drain) must wait for it, and by the time it stops
// waiting the completion must already have released the file and everything the file owns.
TEST_F(AsyncReadTests, ReadAsync_HandleAndFilesystemDroppedMidRead_DrainWaitsUntilFileReleased) {
    HostIoContext host;
    auto tracker = std::make_shared<AsyncReadTracker>(host.Context().get_executor());
    auto client = std::make_shared<DeferredBlobClient>();
    ON_CALL(*client, GetSize()).WillByDefault(Return(BlobSize));
    ON_CALL(*client, GetEtag()).WillByDefault(Return(std::string{"etag"}));
    std::weak_ptr<DeferredBlobClient> weakClient = client;

    std::vector<char> scratch(32, 'x');
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    {
        ReadableFile file{ReadableFileImpl{"test.sst", client, nullptr, m_logger, tracker}};
        IoHandle handle;
        ASSERT_TRUE(Submit(file, req, record, handle).ok());
    }
    auto download = client->Take();
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
                      [callback = std::move(download.Callback)]() mutable { callback(nullptr, std::string(32, 'G')); });
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
    ReadableFile file{ReadableFileImpl{"test.sst", m_deferred, nullptr, m_logger, tracker}};
    std::vector<char> scratch(16);
    auto req = MakeRequest(0, scratch);
    CallbackRecord record;
    IoHandle handle;
    ASSERT_TRUE(Submit(file, req, record, handle).ok());
    EXPECT_EQ(1U, tracker->InFlight());

    m_deferred->Take().Callback(nullptr, std::string(16, 'T'));
    std::vector<void*> handles{handle.Handle};
    ASSERT_TRUE(PollAsyncReads(handles).ok());
    EXPECT_EQ(std::string(16, 'T'), record.Data);

    tracker->Drain();
    EXPECT_EQ(0U, tracker->InFlight());
}
