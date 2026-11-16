// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/Detail/Mailbox.hpp"

#include <gtest/gtest.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <thread>
#include <variant>

namespace {
using AVEVA::Detail::Mailbox;
namespace asio = boost::asio;
constexpr auto Await = asio::as_tuple(asio::use_awaitable);

template <typename Body> void RunCoroutine(asio::io_context& context, Body body) {
    asio::co_spawn(context, std::move(body), asio::detached);
    context.run();
}

TEST(MailboxTests, ValueSentBeforeReceiveIsKept) {
    asio::io_context context;
    Mailbox<int> mailbox(context.get_executor());
    mailbox.Send(7);
    int received = 0;
    RunCoroutine(context, [&]() -> asio::awaitable<void> {
        const auto [error, value] = co_await mailbox.AsyncReceive(Await);
        EXPECT_FALSE(error);
        received = value;
    });
    EXPECT_EQ(received, 7);
}

TEST(MailboxTests, SendFromAnotherThreadWakesReceiver) {
    asio::io_context context;
    Mailbox<int> mailbox(context.get_executor());
    int received = 0;
    std::thread sender([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        mailbox.Send(42);
    });
    RunCoroutine(context, [&]() -> asio::awaitable<void> {
        const auto [error, value] = co_await mailbox.AsyncReceive(Await);
        EXPECT_FALSE(error);
        received = value;
    });
    sender.join();
    EXPECT_EQ(received, 42);
}

TEST(MailboxTests, LosingARaceCancelsTheWaitAndLeavesTheMailboxUsable) {
    asio::io_context context;
    Mailbox<int> mailbox(context.get_executor());
    size_t winner = 99;
    int received = 0;
    RunCoroutine(context, [&]() -> asio::awaitable<void> {
        using namespace asio::experimental::awaitable_operators;
        asio::steady_timer timer(co_await asio::this_coro::executor, std::chrono::milliseconds(10));
        const auto outcome = co_await (mailbox.AsyncReceive(asio::use_awaitable) || timer.async_wait(asio::use_awaitable));
        winner = outcome.index();
        mailbox.Send(5);
        const auto [error, value] = co_await mailbox.AsyncReceive(Await);
        EXPECT_FALSE(error);
        received = value;
    });
    EXPECT_EQ(winner, 1U);
    EXPECT_EQ(received, 5);
}

TEST(MailboxTests, CloseWakesReceiverWithShutDown) {
    asio::io_context context;
    Mailbox<int> mailbox(context.get_executor());
    boost::system::error_code result;
    RunCoroutine(context, [&]() -> asio::awaitable<void> {
        asio::steady_timer timer(co_await asio::this_coro::executor, std::chrono::milliseconds(10));
        timer.async_wait([&](auto) { mailbox.Close(); });
        const auto [error, value] = co_await mailbox.AsyncReceive(Await);
        result = error;
    });
    EXPECT_EQ(result, asio::error::shut_down);
}
} // namespace
