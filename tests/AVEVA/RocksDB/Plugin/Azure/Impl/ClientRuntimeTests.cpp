// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <thread>

using AVEVA::RocksDB::Plugin::Azure::Impl::ClientRuntime;

TEST(ClientRuntimeTests, HttpClientUsesProvidedContext) {
    boost::asio::io_context context;
    ClientRuntime runtime(context);

    auto executor = runtime.HttpClient().get_executor();
    ASSERT_TRUE(executor == boost::asio::any_io_executor(context.get_executor()));
}

TEST(ClientRuntimeTests, DoesNotRunContext) {
    boost::asio::io_context context;
    ClientRuntime runtime(context);

    bool ran = false;
    boost::asio::post(runtime.HttpClient().get_executor(), [&ran]() { ran = true; });
    ASSERT_FALSE(ran);

    context.run();
    ASSERT_TRUE(ran);
}

TEST(ClientRuntimeTests, ContextKeepsRunningAfterRuntimeDestruction) {
    boost::asio::io_context context;
    auto workGuard = boost::asio::make_work_guard(context);
    // If an assertion fails, releasing the guard on stop lets the jthread join instead of hanging.
    std::jthread worker([&context, &workGuard](std::stop_token stop) {
        std::stop_callback release(stop, [&workGuard]() { workGuard.reset(); });
        context.run();
    });

    std::make_unique<ClientRuntime>(context).reset();

    ASSERT_FALSE(context.stopped());
    auto ran = boost::asio::post(context, boost::asio::use_future([]() { return 42; }));
    ASSERT_EQ(ran.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    ASSERT_EQ(ran.get(), 42);

    workGuard.reset();
    worker.join();
}
