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
        : plain(std::make_shared<PlainConnectionPool>(4, interval)),
          tls(std::make_shared<TlsConnectionPool>(4, interval)),
          sweeper(std::make_shared<IdleSweeper>(plain, tls, interval)), context(context) {
        sweeper->BindSelf(sweeper);
        plain->SetNotifier(SweeperNotifier(sweeper));
    }

    void AddIdleConnection() {
        plain->Release({"127.0.0.1", "80"}, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});
    }

    std::shared_ptr<PlainConnectionPool> plain;
    std::shared_ptr<TlsConnectionPool> tls;
    std::shared_ptr<IdleSweeper> sweeper;
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
