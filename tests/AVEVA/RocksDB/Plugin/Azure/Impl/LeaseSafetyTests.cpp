// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"

#include "FakeHttpClient.hpp"
#include "FakeHttpPump.hpp"
#include "TestFixtures.hpp"

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
using AVEVA::RocksDB::Plugin::Azure::Impl::BlockOnFor;
using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;
using AVEVA::RocksDB::Plugin::Azure::Impl::LockFileImpl;

using namespace std::chrono_literals;

TEST(BlockOnForTests, ReturnsValueWithoutCallingOnTimeout) {
    boost::asio::io_context context;
    std::promise<int> value;
    value.set_value(7);

    int timeouts = 0;
    EXPECT_EQ(BlockOnFor(context.get_executor(), value.get_future(), 5s, [&] { ++timeouts; }), 7);
    EXPECT_EQ(timeouts, 0);
}

TEST(BlockOnForTests, CallsOnTimeoutOnceThenStillWaitsForTheResult) {
    boost::asio::io_context context;
    std::promise<int> value;

    int timeouts = 0;
    std::jthread producer([&] {
        std::this_thread::sleep_for(300ms);
        value.set_value(11);
    });

    EXPECT_EQ(BlockOnFor(context.get_executor(), value.get_future(), 50ms, [&] { ++timeouts; }), 11);
    EXPECT_EQ(timeouts, 1);
}

TEST(BlockOnForTests, ThrowsWhenCalledFromContextThread) {
    boost::asio::io_context context;
    std::promise<bool> threw;
    boost::asio::post(context, [&] {
        std::promise<int> value;
        value.set_value(1);
        try {
            BlockOnFor(context.get_executor(), value.get_future(), 1s, [] {});
            threw.set_value(false);
        } catch (const std::logic_error&) {
            threw.set_value(true);
        }
    });
    context.run();
    EXPECT_TRUE(threw.get_future().get());
}

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
        m_pump = AVEVA::RocksDB::Plugin::Azure::Impl::Tests::StartFakeHttpPump(m_httpClient);
        m_runtime = std::make_shared<ClientRuntime>(m_context);
        m_logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    }

    boost::asio::io_context m_context;
    FakeHttpClient m_httpClient;
    std::shared_ptr<ClientRuntime> m_runtime;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    std::jthread m_pump;
};
} // namespace

TEST_F(LeaseDeadlineTests, NotOverdueBeforeLockedOrRightAfterAcquiring) {
    auto client =
        std::make_unique<AVEVA::AzureClient::PageBlobClient>(m_httpClient, MakeBlobClientOptions("locks", "LOCK"));
    LockFileImpl lock(m_runtime, std::move(client), 20s, m_logger, "LOCK");
    EXPECT_FALSE(lock.IsRenewalOverdue());

    m_httpClient.EnqueueResponse(HttpResponse{201, MakeCanonicalSuccessHeaders({{"x-ms-lease-id", "lease"}}), ""});
    ASSERT_TRUE(lock.Lock());

    EXPECT_FALSE(lock.IsRenewalOverdue());
    // The deadline leaves a safety margin before the end of the lease.
    EXPECT_LT(lock.RenewalDeadline(), std::chrono::steady_clock::now() + 20s);
    EXPECT_GT(lock.RenewalDeadline(), std::chrono::steady_clock::now() + 10s);
}
