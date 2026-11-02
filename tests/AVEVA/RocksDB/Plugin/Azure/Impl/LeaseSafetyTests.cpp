// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Support/TestLogger.hpp"

#include "FakeHttpClient.hpp"
#include "Fakes/BackgroundPump.hpp"
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
using AVEVA::RocksDB::Plugin::Azure::Impl::DrainResult;
using AVEVA::RocksDB::Plugin::Azure::Impl::LeaseOwnerId;
using AVEVA::RocksDB::Plugin::Azure::Impl::WriteFenceState;
// The lock over the library's page blob client on the fake transport.
using LockFileImpl = AVEVA::RocksDB::Plugin::Azure::Impl::BasicLockFileImpl<AVEVA::AzureClient::Tests::PageBlobClient>;
using LockResult = AVEVA::RocksDB::Plugin::Azure::LockFile::LockResult;

using namespace std::chrono_literals;

namespace {
// Publishes a deadline `remaining` from now, standing in for a held lease.
void SetDeadlineIn(ClientRuntimeBase& runtime, const std::chrono::steady_clock::duration remaining) {
    runtime.Fence().SetLeaseDeadline(LeaseOwnerId{&runtime}, std::chrono::steady_clock::now() + remaining);
}

void ClearDeadline(ClientRuntimeBase& runtime) {
    runtime.Fence().SetLeaseDeadline(LeaseOwnerId{&runtime}, std::nullopt);
}
} // namespace

TEST(WriteFenceTests, WritesAreAllowedUntilFenced) {
    boost::asio::io_context context;
    ClientRuntime runtime(context);

    EXPECT_EQ(runtime.Fence().State(), WriteFenceState::Open);
    EXPECT_NO_THROW(runtime.Fence().ThrowIfFenced());

    runtime.Fence().Fence();

    EXPECT_EQ(runtime.Fence().State(), WriteFenceState::Fenced);
    EXPECT_THROW(runtime.Fence().ThrowIfFenced(), std::runtime_error);
}

TEST(AsyncReadTrackerDrainTests, DrainForTimesOutWhileAReadIsInFlight) {
    boost::asio::io_context context;
    auto tracker = std::make_shared<AsyncReadTracker>(context.get_executor());

    auto token = tracker->Begin();
    EXPECT_EQ(tracker->DrainFor(20ms), DrainResult::TimedOut);
    EXPECT_EQ(tracker->InFlight(), 1U);

    // Released off the io_context threads, the registration ends the read directly.
    token.reset();
    EXPECT_EQ(tracker->InFlight(), 0U);
    EXPECT_EQ(tracker->DrainFor(1s), DrainResult::Drained);
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

// An owner that gives up waiting hands its state over; the last read to end, not the owner, releases it.
TEST(AsyncReadTrackerDrainTests, RetainedStateIsReleasedOnlyWhenTheLastReadEnds) {
    boost::asio::io_context context;
    auto tracker = std::make_shared<AsyncReadTracker>(context.get_executor());
    auto first = tracker->Begin();
    auto second = tracker->Begin();
    auto state = std::make_shared<int>(0);
    const std::weak_ptr<int> watch = state;

    tracker->RetainUntilIdle(std::move(state));
    first.reset();
    EXPECT_FALSE(watch.expired());

    second.reset();
    EXPECT_TRUE(watch.expired());
}

TEST(AsyncReadTrackerDrainTests, RetainedStateIsReleasedAtOnceWhenIdle) {
    boost::asio::io_context context;
    auto tracker = std::make_shared<AsyncReadTracker>(context.get_executor());
    auto state = std::make_shared<int>(0);
    const std::weak_ptr<int> watch = state;

    tracker->RetainUntilIdle(std::move(state));

    EXPECT_TRUE(watch.expired());
}

TEST(AsyncReadTrackerDrainTests, DrainForReturnsImmediatelyWhenIdle) {
    boost::asio::io_context context;
    AsyncReadTracker tracker(context.get_executor());
    EXPECT_EQ(tracker.DrainFor(0ms), DrainResult::Drained);
}

namespace {
class LeaseDeadlineTests : public ::testing::Test {
  protected:
    void SetUp() override {
        m_runtime = std::make_shared<ClientRuntimeBase>();
        m_logger = AVEVA::RocksDB::Plugin::Tests::MakeTestLogger();
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    AVEVA::RocksDB::Plugin::Azure::Impl::Tests::BackgroundPump m_pump{m_httpClient};
    std::shared_ptr<ClientRuntimeBase> m_runtime;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
};
} // namespace

TEST_F(LeaseDeadlineTests, NotOverdueBeforeLockedOrRightAfterAcquiring) {
    auto client = std::make_unique<AVEVA::AzureClient::Tests::PageBlobClient>(m_httpClient,
                                                                              MakeBlobClientOptions("locks", "LOCK"));
    LockFileImpl lock(m_runtime, std::move(client), 20s, m_logger, "LOCK");
    EXPECT_EQ(lock.RenewalDeadlineState(), AVEVA::AzureClient::DeadlineState::Open);

    m_httpClient.EnqueueResponse(
        HttpResponse{::AVEVA::HttpStatusCode{201}, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});
    ASSERT_EQ(lock.Lock(), LockResult::Acquired);

    EXPECT_EQ(lock.RenewalDeadlineState(), AVEVA::AzureClient::DeadlineState::Open);
    // The deadline leaves a safety margin before the end of the lease.
    EXPECT_LT(lock.RenewalDeadline(), std::chrono::steady_clock::now() + 20s);
    EXPECT_GT(lock.RenewalDeadline(), std::chrono::steady_clock::now() + 10s);
}

TEST(WriteDeadlineTests, WithoutASourceNothingIsBounded) {
    ClientRuntimeBase runtime;

    EXPECT_FALSE(runtime.Fence().TimeRemaining().has_value());
    EXPECT_NO_THROW(runtime.Fence().ThrowIfFenced());
    AVEVA::HttpRequestOptions options;
    options.SetTimeout(AVEVA::Timeout{30s});
    EXPECT_EQ(30s, AVEVA::RocksDB::Plugin::Azure::Impl::BoundByWriteDeadline(
                       runtime, AVEVA::AzureClient::AzureRequestOptions{options})
                       .GetTimeout()
                       .Value());
}

TEST(WriteDeadlineTests, MutationsAreRejectedOncetheDeadlineHasPassed) {
    ClientRuntimeBase runtime;
    SetDeadlineIn(runtime, 5s);

    EXPECT_NO_THROW(runtime.Fence().ThrowIfFenced());
    SetDeadlineIn(runtime, 0s);
    EXPECT_THROW(runtime.Fence().ThrowIfFenced(), std::runtime_error);

    ClearDeadline(runtime);
    EXPECT_NO_THROW(runtime.Fence().ThrowIfFenced());
}

TEST(WriteDeadlineTests, RequestTimeoutIsCappedAtTheTimeLeft) {
    ClientRuntimeBase runtime;
    SetDeadlineIn(runtime, 2s);
    AVEVA::AzureClient::AzureRequestOptions options;
    options.SetTimeout(AVEVA::Timeout{60s});
    options.SetTotalDeadline(120s);

    const auto bounded = AVEVA::RocksDB::Plugin::Azure::Impl::BoundByWriteDeadline(
        runtime, AVEVA::AzureClient::AzureRequestOptions{options});

    EXPECT_LE(bounded.GetTimeout().Value(), 2s);
    EXPECT_GT(bounded.GetTimeout().Value(), 1s);
    EXPECT_LE(*bounded.GetTotalDeadline(), 2s);
    EXPECT_GT(*bounded.GetTotalDeadline(), 1s);
    // A transfer that is already shorter than the time left is untouched.
    options.SetTimeout(AVEVA::Timeout{1s});
    EXPECT_EQ(1s, AVEVA::RocksDB::Plugin::Azure::Impl::BoundByWriteDeadline(
                      runtime, AVEVA::AzureClient::AzureRequestOptions{options})
                      .GetTimeout()
                      .Value());
}

TEST_F(LeaseDeadlineTests, DeleteIssuedAfterTheRenewalDeadlineIsRejectedWithoutSendingARequest) {
    AVEVA::AzureClient::Tests::PageBlobClient blob(m_httpClient, MakeBlobClientOptions("data", "000001.sst"));
    SetDeadlineIn(*m_runtime, 0s);

    EXPECT_THROW(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Delete(m_runtime, blob),
                 AVEVA::RocksDB::Plugin::Azure::WritesFencedException);
    EXPECT_EQ(0U, m_httpClient.RequestCount());

    m_runtime->Fence().Fence();
    ClearDeadline(*m_runtime);
    EXPECT_THROW(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Delete(m_runtime, blob),
                 AVEVA::RocksDB::Plugin::Azure::WritesFencedException);
    EXPECT_EQ(0U, m_httpClient.RequestCount());
}

TEST_F(LeaseDeadlineTests, DeleteIsBoundedByTheTimeLeftOnTheWriteDeadline) {
    AVEVA::AzureClient::Tests::PageBlobClient blob(m_httpClient, MakeBlobClientOptions("data", "000001.sst"));
    SetDeadlineIn(*m_runtime, 2s);
    m_httpClient.EnqueueResponse(HttpResponse{::AVEVA::HttpStatusCode{202}, MakeCanonicalSuccessHeaders({}), ""});

    EXPECT_EQ(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::Delete(m_runtime, blob),
              AVEVA::AzureClient::PriorState::Existed);

    const auto requests = m_httpClient.Requests();
    ASSERT_EQ(1U, requests.size());
    EXPECT_LE(requests.front().Options.GetTimeout().Value(), 2s);
}

TEST_F(LeaseDeadlineTests, HeldLeasePublishesItsDeadlineAndReleaseWithdrawsIt) {
    auto client = std::make_unique<AVEVA::AzureClient::Tests::PageBlobClient>(m_httpClient,
                                                                              MakeBlobClientOptions("locks", "LOCK"));
    LockFileImpl lock(m_runtime, std::move(client), 20s, m_logger, "LOCK");
    m_httpClient.EnqueueResponse(
        HttpResponse{::AVEVA::HttpStatusCode{201}, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});
    ASSERT_EQ(lock.Lock(), LockResult::Acquired);

    const auto remaining = m_runtime->Fence().ThrowIfFenced();
    ASSERT_TRUE(remaining.has_value());
    EXPECT_GT(*remaining, 10s);
    EXPECT_LT(*remaining, 20s);

    m_httpClient.EnqueueResponse(HttpResponse{::AVEVA::HttpStatus::Ok, MakeCanonicalSuccessHeaders({}), ""});
    lock.Unlock();
    EXPECT_FALSE(m_runtime->Fence().TimeRemaining().has_value());
}

TEST_F(LeaseDeadlineTests, AppendIssuedAfterTheRenewalDeadlineIsRejectedWithoutSendingARequest) {
    auto client = std::make_unique<AVEVA::AzureClient::Tests::PageBlobClient>(m_httpClient,
                                                                              MakeBlobClientOptions("locks", "LOCK"));
    LockFileImpl lock(m_runtime, std::move(client), 20s, m_logger, "LOCK");
    m_httpClient.EnqueueResponse(
        HttpResponse{::AVEVA::HttpStatusCode{201}, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});
    ASSERT_EQ(lock.Lock(), LockResult::Acquired);

    AVEVA::AzureClient::Tests::PageBlobClient blob(m_httpClient, MakeBlobClientOptions("data", "000001.sst"));
    const std::vector<char> page(512, 'x');
    const auto requestsBefore = m_httpClient.RequestCount();

    // Renewal is stalled: the lease's published deadline passes while the lease is still nominally held. The lock's
    // own clock cannot be moved after it publishes, so the deadline is published directly.
    m_runtime->Fence().SetLeaseDeadline(LeaseOwnerId{&lock}, std::chrono::steady_clock::now() - 1s);
    EXPECT_THROW(AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations::UploadPages(
                     m_runtime, blob, page, AVEVA::RocksDB::Plugin::Core::ByteOffset{}),
                 std::runtime_error);
    EXPECT_EQ(requestsBefore, m_httpClient.RequestCount());
}
