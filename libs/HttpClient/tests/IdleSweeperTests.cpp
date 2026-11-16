// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/IdleSweeper.hpp"
#include "private/StreamTypes.hpp"

#include <gtest/gtest.h>

#include <boost/asio/executor_work_guard.hpp>
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

// Runs an io_context on its own thread the way a server would, until the runner is destroyed.
struct ContextRunner {
    ContextRunner() : guard(asio::make_work_guard(context)), thread([this] { context.run(); }) {}
    ~ContextRunner() {
        guard.reset();
        thread.join();
    }
    ContextRunner(const ContextRunner&) = delete;
    ContextRunner& operator=(const ContextRunner&) = delete;

    asio::io_context context;
    asio::executor_work_guard<asio::io_context::executor_type> guard;
    std::thread thread;
};

struct SweeperFixture {
    SweeperFixture(asio::io_context& ioContext, std::chrono::milliseconds interval)
        : sweeper(IdleSweeper::Create(ioContext, AVEVA::ConnectionCount{4}, AVEVA::Timeout{interval},
                                      PlainConnectionPool::DefaultProbeThreshold, nullptr, MinInterval)),
          plain(sweeper->PlainPool()), tls(sweeper->TlsPool()), context(ioContext) {}

    void AddIdleConnection() {
        plain->Release({"127.0.0.1", "80"}, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});
    }

    std::shared_ptr<IdleSweeper> sweeper;
    std::shared_ptr<PlainConnectionPool> plain;
    std::shared_ptr<TlsConnectionPool> tls;
    asio::io_context& context;
};
} // namespace

// The suite-wide environment disables sweeping; these tests are about sweeping.
class IdleSweeperSuite : public ::testing::Test {
  protected:
    void SetUp() override {
        AVEVA::Private::IdleSweeper::SetSweeping(AVEVA::Private::IdleSweeper::Sweeping::Enabled);
    }
    void TearDown() override {
        AVEVA::Private::IdleSweeper::SetSweeping(AVEVA::Private::IdleSweeper::Sweeping::Disabled);
    }
};

// Releasing a connection is enough to get it closed once it expires, with no request and no thread of the sweeper's own.
TEST_F(IdleSweeperSuite, ClosesAnExpiredIdleConnectionOnTheIoContext) {
    ContextRunner runner;
    SweeperFixture fixture(runner.context, std::chrono::milliseconds(20));
    fixture.AddIdleConnection();
    EXPECT_EQ(fixture.plain->IdleCount(), 1U);

    EXPECT_TRUE(WaitUntil([&] { return fixture.plain->IdleCount() == 0; }));
    fixture.sweeper->Stop();
}

// Once the pools have emptied, the sweeper goes quiet; the next release has to arm it again.
TEST_F(IdleSweeperSuite, SweepsAgainWhenALaterReleaseFollowsAnEmptyPool) {
    ContextRunner runner;
    SweeperFixture fixture(runner.context, std::chrono::milliseconds(20));
    fixture.AddIdleConnection();
    ASSERT_TRUE(WaitUntil([&] { return fixture.plain->IdleCount() == 0; }));

    fixture.AddIdleConnection();
    EXPECT_TRUE(WaitUntil([&] { return fixture.plain->IdleCount() == 0; }));
    fixture.sweeper->Stop();
}

// A sweeper with a far deadline must not delay one with a near deadline: each has its own timer.
TEST_F(IdleSweeperSuite, SweepersOnOneIoContextRunIndependently) {
    ContextRunner runner;
    SweeperFixture slow(runner.context, std::chrono::hours(1));
    slow.AddIdleConnection();
    SweeperFixture fast(runner.context, std::chrono::milliseconds(30));
    fast.AddIdleConnection();

    EXPECT_TRUE(WaitUntil([&] { return fast.plain->IdleCount() == 0; }));
    EXPECT_EQ(slow.plain->IdleCount(), 1U);
    slow.sweeper->Stop();
    fast.sweeper->Stop();
}

// Stop cancels the pending timer, so nothing of the sweeper is left on the io_context to wait for.
TEST_F(IdleSweeperSuite, StopCancelsThePendingSweep) {
    asio::io_context context;
    SweeperFixture fixture(context, std::chrono::hours(1));
    fixture.AddIdleConnection();

    fixture.sweeper->Stop();
    EXPECT_EQ(fixture.plain->IdleCount(), 0U);

    const auto start = std::chrono::steady_clock::now();
    context.run_for(Deadline);
    EXPECT_LT(std::chrono::steady_clock::now() - start, Deadline / 2);
}

// A sweeper dropped without Stop leaves a timer whose handler must find the sweeper gone and do nothing.
TEST_F(IdleSweeperSuite, ADestroyedSweeperIsNotSweptLater) {
    asio::io_context context;
    {
        SweeperFixture fixture(context, std::chrono::milliseconds(1));
        fixture.AddIdleConnection();
    }
    EXPECT_NO_THROW(context.run_for(std::chrono::milliseconds(50)));
}

// Stop must have drained the pools on the caller's thread, so no disposal is posted to an io_context that has
// already been destroyed.
TEST_F(IdleSweeperSuite, StopDrainsThePoolsBeforeTheIoContextGoesAway) {
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
