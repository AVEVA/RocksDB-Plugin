// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <future>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::Impl::BlockOn;

TEST(BlockOnTests, ReturnsResultFromOutsideThread) {
    boost::asio::io_context context;
    auto guard = boost::asio::make_work_guard(context);
    std::thread worker([&] { context.run(); });

    auto future = boost::asio::post(context, boost::asio::use_future([] { return 42; }));
    EXPECT_EQ(BlockOn(context.get_executor(), std::move(future)), 42);

    guard.reset();
    worker.join();
}

TEST(BlockOnTests, ThrowsWhenCalledFromContextThread) {
    boost::asio::io_context context;
    std::promise<bool> threw;
    boost::asio::post(context, [&] {
        std::promise<int> value;
        value.set_value(1);
        try {
            BlockOn(context.get_executor(), value.get_future());
            threw.set_value(false);
        } catch (const std::logic_error&) {
            threw.set_value(true);
        }
    });
    context.run();
    EXPECT_TRUE(threw.get_future().get());
}

TEST(BlockOnTests, NeverThrowsSpuriouslyWhileOtherThreadsRunTheContext) {
    boost::asio::io_context context;
    auto guard = boost::asio::make_work_guard(context);
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([&] { context.run(); });
    }

    for (int i = 0; i < 2000; ++i) {
        std::promise<int> value;
        value.set_value(i);
        EXPECT_NO_THROW(EXPECT_EQ(BlockOn(context.get_executor(), value.get_future()), i));
    }

    guard.reset();
    for (auto& worker : workers) {
        worker.join();
    }
}

TEST(BlockOnTests, TypeErasedExecutor_DetectsContextThreadWithoutProbing) {
    boost::asio::io_context context;
    const boost::asio::any_io_executor erased = context.get_executor();
    std::promise<bool> threw;
    boost::asio::post(context, [&] {
        std::promise<int> value;
        value.set_value(1);
        try {
            BlockOn(erased, value.get_future());
            threw.set_value(false);
        } catch (const std::logic_error&) {
            threw.set_value(true);
        }
    });
    context.run();
    EXPECT_TRUE(threw.get_future().get());

    // From a thread that is not running the context, nothing may be posted to it: it is not being run at all here.
    std::promise<int> value;
    value.set_value(7);
    EXPECT_EQ(BlockOn(erased, value.get_future()), 7);
}
