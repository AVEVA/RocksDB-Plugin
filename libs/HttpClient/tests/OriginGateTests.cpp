// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "HttpClientTestHelpers.hpp"
#include "private/OriginGate.hpp"

#include <gtest/gtest.h>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace {
namespace asio = boost::asio;
using AVEVA::Private::OriginGate;

// Waiting coroutines keep the io_context busy, so these tests poll the ready work instead of running to completion.
// Parameters (not captures) so the state lives in the coroutine frame.
asio::awaitable<void> AcquireAndHold(std::shared_ptr<OriginGate> gate, std::string origin,
                                     std::optional<OriginGate::Slot>& slot, std::error_code& error, int& finished,
                                     std::vector<int>* order = nullptr, int id = 0) {
    auto result = co_await gate->Acquire(std::move(origin));
    if (result) {
        slot.emplace(std::move(*result));
    } else {
        error = result.error();
    }
    if (order) {
        order->push_back(id);
    }
    ++finished;
}

constexpr auto Canceled = std::errc::operation_canceled;
} // namespace

TEST(OriginGate, EnforcesTheLimitPerOriginAndHandsSlotsOverInOrder) {
    asio::io_context context;
    const auto gate = std::make_shared<OriginGate>(context.get_executor(), 1);
    std::optional<OriginGate::Slot> slots[3];
    std::error_code errors[3];
    std::vector<int> order;
    int finished = 0;
    for (int i = 0; i < 3; ++i) {
        asio::co_spawn(context, AcquireAndHold(gate, "a", slots[i], errors[i], finished, &order, i), asio::detached);
    }
    std::optional<OriginGate::Slot> other;
    std::error_code otherError;
    asio::co_spawn(context, AcquireAndHold(gate, "b", other, otherError, finished), asio::detached);
    context.poll();

    EXPECT_EQ(finished, 2); // One slot on "a" and one on the independent origin "b".
    EXPECT_TRUE(slots[0]);
    EXPECT_TRUE(other);

    slots[0].reset();
    context.restart();
    context.poll();
    EXPECT_TRUE(slots[1]);
    EXPECT_FALSE(slots[2]);

    slots[1].reset();
    context.restart();
    context.poll();
    EXPECT_TRUE(slots[2]);
    EXPECT_EQ(order, (std::vector<int>{0, 1, 2}));
}

TEST(OriginGate, CancellingAWaiterFailsItWithoutLosingTheSlot) {
    asio::io_context context;
    const auto gate = std::make_shared<OriginGate>(context.get_executor(), 1);
    std::optional<OriginGate::Slot> first;
    std::optional<OriginGate::Slot> second;
    std::optional<OriginGate::Slot> third;
    std::error_code firstError;
    std::error_code secondError;
    std::error_code thirdError;
    int finished = 0;
    asio::cancellation_signal cancel;
    asio::co_spawn(context, AcquireAndHold(gate, "a", first, firstError, finished), asio::detached);
    asio::co_spawn(context, AcquireAndHold(gate, "a", second, secondError, finished),
                   asio::bind_cancellation_slot(cancel.slot(), asio::detached));
    asio::co_spawn(context, AcquireAndHold(gate, "a", third, thirdError, finished), asio::detached);
    context.poll();
    ASSERT_EQ(finished, 1);

    cancel.emit(asio::cancellation_type::terminal);
    context.restart();
    context.poll();
    EXPECT_EQ(secondError, std::make_error_code(Canceled));

    // The cancelled waiter no longer holds a place in the queue, so the next one gets the freed slot.
    first.reset();
    context.restart();
    context.poll();
    EXPECT_TRUE(third);
    EXPECT_FALSE(thirdError);
}

TEST(OriginGate, CloseFailsWaitersAndLaterAcquires) {
    asio::io_context context;
    const auto gate = std::make_shared<OriginGate>(context.get_executor(), 1);
    std::optional<OriginGate::Slot> held;
    std::optional<OriginGate::Slot> waiting;
    std::optional<OriginGate::Slot> late;
    std::error_code heldError;
    std::error_code waitingError;
    std::error_code lateError;
    int finished = 0;
    asio::co_spawn(context, AcquireAndHold(gate, "a", held, heldError, finished), asio::detached);
    asio::co_spawn(context, AcquireAndHold(gate, "a", waiting, waitingError, finished), asio::detached);
    context.poll();

    gate->Close();
    asio::co_spawn(context, AcquireAndHold(gate, "a", late, lateError, finished), asio::detached);
    context.restart();
    context.poll();

    EXPECT_EQ(finished, 3);
    EXPECT_TRUE(held); // A slot that was already granted stays valid.
    EXPECT_EQ(waitingError, std::make_error_code(Canceled));
    EXPECT_EQ(lateError, std::make_error_code(Canceled));
}
