// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/IdleSweeper.hpp"
#include "private/StreamTypes.hpp"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <thread>

namespace {
using namespace AVEVA::Private;
namespace asio = boost::asio;

struct SweeperFixture {
    SweeperFixture(asio::io_context& context, std::chrono::seconds interval)
        : sweeper(IdleSweeper::Create(4, interval)), plain(sweeper->PlainPool()), tls(sweeper->TlsPool()),
          context(context) {}

    void AddIdleConnection() {
        plain->Release({"127.0.0.1", "80"}, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});
    }

    std::shared_ptr<IdleSweeper> sweeper;
    std::shared_ptr<PlainConnectionPool> plain;
    std::shared_ptr<TlsConnectionPool> tls;
    asio::io_context& context;
};
} // namespace

// The scheduler thread is already sleeping towards a far-away deadline when a sweeper with a nearer one is
// scheduled; the nearer sweep must still run on time.
TEST(IdleSweeper, EarlierDeadlineWakesTheSchedulerThread) {
    asio::io_context context;
    SweeperFixture slow(context, std::chrono::hours(1));
    slow.AddIdleConnection();
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Let the thread start waiting on the slow deadline.

    SweeperFixture fast(context, std::chrono::seconds(1));
    fast.AddIdleConnection();

    const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (fast.plain->IdleCount() != 0 && std::chrono::steady_clock::now() < limit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(fast.plain->IdleCount(), 0U);
    EXPECT_EQ(slow.plain->IdleCount(), 1U);
}

// The shared scheduler thread belongs to the live sweepers: it is stopped and joined when the last one goes,
// and started afresh for the next.
TEST(IdleSweeper, SchedulerStopsWhenTheLastSweeperIsDestroyed) {
    asio::io_context context;
    EXPECT_FALSE(IdleSweeper::SchedulerIsAlive());
    {
        auto first = std::make_unique<SweeperFixture>(context, std::chrono::hours(1));
        first->AddIdleConnection();
        EXPECT_TRUE(IdleSweeper::SchedulerIsAlive());
        {
            SweeperFixture second(context, std::chrono::hours(1));
            second.AddIdleConnection();
        }
        EXPECT_TRUE(IdleSweeper::SchedulerIsAlive());
    }
    EXPECT_FALSE(IdleSweeper::SchedulerIsAlive());

    SweeperFixture again(context, std::chrono::seconds(1));
    again.AddIdleConnection();
    EXPECT_TRUE(IdleSweeper::SchedulerIsAlive());
    const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (again.plain->IdleCount() != 0 && std::chrono::steady_clock::now() < limit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(again.plain->IdleCount(), 0U);
}
