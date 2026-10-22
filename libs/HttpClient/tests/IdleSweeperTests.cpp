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

// The production floor is one second; tests sweep every few tens of milliseconds instead.
constexpr std::chrono::milliseconds MinInterval{5};
// Generous because a healthy run needs a fraction of it; it only bounds how long a failing run takes to report.
constexpr std::chrono::seconds Deadline{10};

template <typename Predicate> bool WaitUntil(Predicate done) {
    const auto limit = std::chrono::steady_clock::now() + Deadline;
    while (!done() && std::chrono::steady_clock::now() < limit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return done();
}

struct SweeperFixture {
    SweeperFixture(asio::io_context& context, std::chrono::milliseconds interval)
        : sweeper(IdleSweeper::Create(4, interval, PlainConnectionPool::DefaultProbeThreshold, MinInterval)),
          plain(sweeper->PlainPool()), tls(sweeper->TlsPool()), context(context) {}

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
    const auto waitsBefore = IdleSweeper::SchedulerWaitCount();
    slow.AddIdleConnection();
    // Let the thread start waiting on the slow deadline.
    ASSERT_TRUE(WaitUntil([&] { return IdleSweeper::SchedulerWaitCount() > waitsBefore; }));

    SweeperFixture fast(context, std::chrono::milliseconds(50));
    fast.AddIdleConnection();

    EXPECT_TRUE(WaitUntil([&] { return fast.plain->IdleCount() == 0; }));
    EXPECT_EQ(slow.plain->IdleCount(), 1U);
}

// A sweeper scheduled later with a nearer deadline runs first, and the earlier-scheduled one still gets its
// own sweep afterwards.
TEST(IdleSweeper, SweepersRunInDeadlineOrderNotSchedulingOrder) {
    asio::io_context context;
    // The 30x gap keeps later pending even if this thread is descheduled for a few hundred milliseconds.
    SweeperFixture later(context, std::chrono::milliseconds(1000));
    later.AddIdleConnection();
    SweeperFixture sooner(context, std::chrono::milliseconds(30));
    sooner.AddIdleConnection();

    ASSERT_TRUE(WaitUntil([&] { return sooner.plain->IdleCount() == 0; }));
    EXPECT_EQ(later.plain->IdleCount(), 1U);

    EXPECT_TRUE(WaitUntil([&] { return later.plain->IdleCount() == 0; }));
}

// A failed Schedule must not leave the pending flag set: a later release has to schedule a sweep again.
TEST(IdleSweeper, ReleaseSchedulesAgainAfterAFailedSchedule) {
    asio::io_context context;
    SweeperFixture fixture(context, std::chrono::milliseconds(20));

    IdleSweeper::FailNextSchedules(1);
    fixture.AddIdleConnection();
    EXPECT_EQ(fixture.plain->IdleCount(), 1U);

    // The pool is still non-empty, so this release must be the one that recovers the unscheduled sweep.
    fixture.plain->Release({"127.0.0.2", "80"},
                           PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});
    EXPECT_EQ(fixture.plain->IdleCount(), 2U);

    EXPECT_TRUE(WaitUntil([&] { return fixture.plain->IdleCount() == 0; }));
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

    SweeperFixture again(context, std::chrono::milliseconds(50));
    again.AddIdleConnection();
    EXPECT_TRUE(IdleSweeper::SchedulerIsAlive());
    EXPECT_TRUE(WaitUntil([&] { return again.plain->IdleCount() == 0; }));
}

// The scheduler thread may hold the last reference to the sweeper; Stop must have drained the pools on the caller's
// thread by then, so no disposal is posted to an io_context that has already been destroyed.
TEST(IdleSweeper, StopDrainsThePoolsBeforeTheIoContextGoesAway) {
    for (int i = 0; i < 50; ++i) {
        auto context = std::make_unique<asio::io_context>();
        auto fixture = std::make_unique<SweeperFixture>(*context, std::chrono::milliseconds(1));
        fixture->AddIdleConnection();
        auto sweeper = fixture->sweeper;

        sweeper->Stop();
        EXPECT_EQ(fixture->plain->IdleCount(), 0U);
        fixture.reset();
        context.reset();
        sweeper.reset();
    }
}
