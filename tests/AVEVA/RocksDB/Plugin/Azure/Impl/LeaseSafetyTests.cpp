// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"

#include "FakeHttpClient.hpp"
#include "TestSupport.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/log/sources/severity_logger.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <thread>

using AVEVA::HttpResponse;
using AVEVA::AzureClient::Tests::FakeHttpClient;
using AVEVA::AzureClient::Tests::MakeBlobClientOptions;
using AVEVA::AzureClient::Tests::MakeCanonicalSuccessHeaders;
using AVEVA::RocksDB::Plugin::Azure::Impl::AsyncReadTracker;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntimeBase;
// The lock over the library's page blob client on the fake transport.
using LockFileImpl = AVEVA::RocksDB::Plugin::Azure::Impl::BasicLockFileImpl<AVEVA::AzureClient::Tests::PageBlobClient>;

using namespace std::chrono_literals;

TEST(WriteFenceTests, WritesAreAllowedUntilFenced) {
    boost::asio::io_context context;
    ClientRuntime runtime(context);

    EXPECT_FALSE(runtime.WritesFenced());
    EXPECT_NO_THROW(runtime.ThrowIfWritesFenced());

    runtime.FenceWrites();

    EXPECT_TRUE(runtime.WritesFenced());
    EXPECT_THROW(runtime.ThrowIfWritesFenced(), std::runtime_error);
}

TEST(AsyncReadTrackerDrainTests, DrainForTimesOutWhileAReadIsInFlight) {
    boost::asio::io_context context;
    auto tracker = std::make_shared<AsyncReadTracker>(context.get_executor());

    auto token = tracker->Begin();
    EXPECT_FALSE(tracker->DrainFor(20ms));
    EXPECT_EQ(tracker->InFlight(), 1U);

    // Released off the io_context threads, the registration ends the read directly.
    token.reset();
    EXPECT_EQ(tracker->InFlight(), 0U);
    EXPECT_TRUE(tracker->DrainFor(1s));
}

TEST(AsyncReadTrackerDrainTests, TokenReleasedOnTheIoThreadEndsFromAFreshTask) {
    boost::asio::io_context context;
    auto tracker = std::make_shared<AsyncReadTracker>(context.get_executor());
    auto token = tracker->Begin();

    // Inside a completion the end is deferred so the filesystem outlives the handler that is still running.
    std::size_t inFlightRightAfterRelease = 0;
    boost::asio::post(context, [&, token = std::move(token)]() mutable {
        token.reset();
        inFlightRightAfterRelease = tracker->InFlight();
    });
    context.run();

    EXPECT_EQ(inFlightRightAfterRelease, 1U);
    EXPECT_EQ(tracker->InFlight(), 0U);
}

TEST(AsyncReadTrackerDrainTests, DrainForReturnsImmediatelyWhenIdle) {
    boost::asio::io_context context;
    AsyncReadTracker tracker(context.get_executor());
    EXPECT_TRUE(tracker.DrainFor(0ms));
}

namespace {
class LeaseDeadlineTests : public ::testing::Test {
  protected:
    void SetUp() override {
        m_httpClient.CompleteInline() = true;
        m_httpClient.StartBackgroundPump();
        m_runtime = std::make_shared<ClientRuntimeBase>();
        m_logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    std::shared_ptr<ClientRuntimeBase> m_runtime;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
};
} // namespace

TEST_F(LeaseDeadlineTests, NotOverdueBeforeLockedOrRightAfterAcquiring) {
    auto client = std::make_unique<AVEVA::AzureClient::Tests::PageBlobClient>(m_httpClient,
                                                                              MakeBlobClientOptions("locks", "LOCK"));
    LockFileImpl lock(m_runtime, std::move(client), 20s, m_logger, "LOCK");
    EXPECT_FALSE(lock.IsRenewalOverdue());

    m_httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});
    ASSERT_TRUE(lock.Lock());

    EXPECT_FALSE(lock.IsRenewalOverdue());
    // The deadline leaves a safety margin before the end of the lease.
    EXPECT_LT(lock.RenewalDeadline(), std::chrono::steady_clock::now() + 20s);
    EXPECT_GT(lock.RenewalDeadline(), std::chrono::steady_clock::now() + 10s);
}

TEST(WriteDeadlineTests, WithoutASourceNothingIsBounded) {
    ClientRuntimeBase runtime;

    EXPECT_FALSE(runtime.WriteTimeRemaining().has_value());
    EXPECT_NO_THROW(runtime.ThrowIfWritesFenced());
    AVEVA::HttpRequestOptions options;
    options.SetTimeout(30s);
    EXPECT_EQ(30s, AVEVA::RocksDB::Plugin::Azure::Impl::BoundByWriteDeadline(runtime, options).GetTimeout());
}

TEST(WriteDeadlineTests, MutationsAreRejectedOncetheDeadlineHasPassed) {
    ClientRuntimeBase runtime;
    std::chrono::steady_clock::duration remaining = 5s;
    runtime.SetWriteDeadlineSource([&remaining] { return std::optional{remaining}; });

    EXPECT_NO_THROW(runtime.ThrowIfWritesFenced());
    remaining = 0s;
    EXPECT_THROW(runtime.ThrowIfWritesFenced(), std::runtime_error);

    runtime.SetWriteDeadlineSource({});
    EXPECT_NO_THROW(runtime.ThrowIfWritesFenced());
}

TEST(WriteDeadlineTests, RequestTimeoutIsCappedAtTheTimeLeft) {
    ClientRuntimeBase runtime;
    runtime.SetWriteDeadlineSource([] { return std::optional<std::chrono::steady_clock::duration>{2s}; });
    AVEVA::AzureClient::AzureRequestOptions options;
    options.SetTimeout(60s);
    options.SetTotalDeadline(120s);

    const auto bounded = AVEVA::RocksDB::Plugin::Azure::Impl::BoundByWriteDeadline(runtime, options);

    EXPECT_EQ(2s, bounded.GetTimeout());
    EXPECT_EQ(2s, bounded.GetTotalDeadline());
    // A transfer that is already shorter than the time left is untouched.
    options.SetTimeout(1s);
    EXPECT_EQ(1s, AVEVA::RocksDB::Plugin::Azure::Impl::BoundByWriteDeadline(runtime, options).GetTimeout());
}

TEST_F(LeaseDeadlineTests, DeleteIssuedAfterTheRenewalDeadlineIsRejectedWithoutSendingARequest) {
    AVEVA::AzureClient::Tests::PageBlobClient blob(m_httpClient, MakeBlobClientOptions("data", "000001.sst"));
    m_runtime->SetWriteDeadlineSource([] { return std::optional<std::chrono::steady_clock::duration>{0s}; });

    EXPECT_THROW(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Delete(*m_runtime, blob),
                 AVEVA::RocksDB::Plugin::Azure::WritesFencedException);
    EXPECT_EQ(0U, m_httpClient.RequestCount());

    m_runtime->FenceWrites();
    m_runtime->SetWriteDeadlineSource({});
    EXPECT_THROW(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Delete(*m_runtime, blob),
                 AVEVA::RocksDB::Plugin::Azure::WritesFencedException);
    EXPECT_EQ(0U, m_httpClient.RequestCount());
}

TEST_F(LeaseDeadlineTests, DeleteIsBoundedByTheTimeLeftOnTheWriteDeadline) {
    AVEVA::AzureClient::Tests::PageBlobClient blob(m_httpClient, MakeBlobClientOptions("data", "000001.sst"));
    m_runtime->SetWriteDeadlineSource([] { return std::optional<std::chrono::steady_clock::duration>{2s}; });
    m_httpClient.EnqueueResponse(HttpResponse{202, MakeCanonicalSuccessHeaders({}), ""});

    EXPECT_TRUE(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Delete(*m_runtime, blob));

    const auto requests = m_httpClient.Requests();
    ASSERT_EQ(1U, requests.size());
    EXPECT_LE(requests.front().Options.GetTimeout(), 2s);
}

TEST_F(LeaseDeadlineTests, AppendIssuedAfterTheRenewalDeadlineIsRejectedWithoutSendingARequest) {
    auto client = std::make_unique<AVEVA::AzureClient::Tests::PageBlobClient>(m_httpClient,
                                                                              MakeBlobClientOptions("locks", "LOCK"));
    LockFileImpl lock(m_runtime, std::move(client), 20s, m_logger, "LOCK");
    auto now = std::chrono::steady_clock::now();
    lock.SetClockForTesting([&now] { return now; });
    m_httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});
    ASSERT_TRUE(lock.Lock());
    m_runtime->SetWriteDeadlineSource([&lock] { return lock.RenewalTimeRemaining(); });

    AVEVA::AzureClient::Tests::PageBlobClient blob(m_httpClient, MakeBlobClientOptions("data", "000001.sst"));
    const std::vector<char> page(512, 'x');
    const auto requestsBefore = m_httpClient.RequestCount();

    // Renewal is stalled: the clock passes the deadline while the lease is still nominally held.
    now += 19s;
    EXPECT_THROW(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::UploadPages(*m_runtime, blob, page, 0),
                 std::runtime_error);
    EXPECT_EQ(requestsBefore, m_httpClient.RequestCount());
    m_runtime->SetWriteDeadlineSource({});
}
