// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/OriginLimiter.hpp"

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <latch>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace {
struct Recorder {
    std::vector<int> Started;
    std::vector<int> Aborted;
    std::vector<std::error_code> Errors;
};

class RecordingSink {
  public:
    explicit RecordingSink(std::shared_ptr<Recorder> recorder) : m_recorder(std::move(recorder)) {}

    void Start(int&& pending) const { m_recorder->Started.push_back(pending); }
    void Abort(int&& pending, std::error_code error) const {
        m_recorder->Aborted.push_back(pending);
        m_recorder->Errors.push_back(error);
    }

  private:
    std::shared_ptr<Recorder> m_recorder;
};

using Limiter = AVEVA::Private::OriginLimiter<int, RecordingSink>;
} // namespace

TEST(OriginLimiterTests, RunAfterCloseAbortsInsteadOfQueueingForever) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    limiter.Close();
    limiter.Run("origin", limiter.NextTicket(), 1);

    EXPECT_TRUE(recorder->Started.empty());
    ASSERT_EQ(recorder->Aborted.size(), 1U);
    EXPECT_EQ(recorder->Errors.front(), std::make_error_code(std::errc::operation_canceled));
}

TEST(OriginLimiterTests, CloseAbortsQueuedWaitersAndLeavesRunningOnesAlone) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    limiter.Run("origin", limiter.NextTicket(), 1);
    limiter.Run("origin", limiter.NextTicket(), 2);
    limiter.Close();
    limiter.Release("origin");

    EXPECT_EQ(recorder->Started, std::vector<int>{1});
    EXPECT_EQ(recorder->Aborted, std::vector<int>{2});
}

TEST(OriginLimiterTests, ReleaseHandsTheSlotToWaitersInFifoOrder) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    for (int id = 1; id <= 4; ++id) {
        limiter.Run("origin", limiter.NextTicket(), int{id});
    }
    EXPECT_EQ(recorder->Started, std::vector<int>{1});

    limiter.Release("origin");
    limiter.Release("origin");
    limiter.Release("origin");

    EXPECT_EQ(recorder->Started, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_TRUE(recorder->Aborted.empty());
}

TEST(OriginLimiterTests, OriginsAreCountedIndependently) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    limiter.Run("a", limiter.NextTicket(), 1);
    limiter.Run("b", limiter.NextTicket(), 2);
    limiter.Run("a", limiter.NextTicket(), 3);

    EXPECT_EQ(recorder->Started, (std::vector<int>{1, 2}));
}

TEST(OriginLimiterTests, CancelRemovesOnlyTheQueuedTicketAndTheRestKeepTheirOrder) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    const auto runningTicket = limiter.NextTicket();
    limiter.Run("origin", runningTicket, 1);
    std::vector<std::uint64_t> tickets;
    for (int id = 2; id <= 4; ++id) {
        tickets.push_back(limiter.NextTicket());
        limiter.Run("origin", tickets.back(), int{id});
    }

    limiter.Cancel("origin", tickets[1]);
    // A started request, an unknown ticket and an unknown origin are all left alone.
    limiter.Cancel("origin", runningTicket);
    limiter.Cancel("origin", 9999);
    limiter.Cancel("other", tickets[0]);

    ASSERT_EQ(recorder->Aborted, std::vector<int>{3});
    EXPECT_EQ(recorder->Errors.front(), std::make_error_code(std::errc::operation_canceled));

    limiter.Release("origin");
    limiter.Release("origin");
    EXPECT_EQ(recorder->Started, (std::vector<int>{1, 2, 4}));
    EXPECT_EQ(recorder->Aborted, std::vector<int>{3});
}

TEST(OriginLimiterTests, QueuedWaitersAreAbortedAtTheirDeadlineInDeadlineOrder) {
    using namespace std::chrono_literals;
    boost::asio::io_context context;
    auto recorder = std::make_shared<Recorder>();
    auto limiter = std::make_shared<Limiter>(1, RecordingSink{recorder});
    const auto expired = std::make_error_code(std::errc::timed_out);
    limiter->EnableDeadlines(context, expired);

    const auto now = Limiter::Clock::now();
    limiter->Run("origin", limiter->NextTicket(), 1, now + 1h);
    limiter->Run("origin", limiter->NextTicket(), 2, now + 120ms);
    limiter->Run("origin", limiter->NextTicket(), 3, now + 40ms);
    limiter->Run("origin", limiter->NextTicket(), 4, now + 1h);

    context.run_for(1s);

    EXPECT_EQ(recorder->Started, std::vector<int>{1});
    EXPECT_EQ(recorder->Aborted, (std::vector<int>{3, 2}));
    EXPECT_EQ(recorder->Errors.front(), expired);

    // The survivor is still queued and still gets the slot.
    limiter->Release("origin");
    EXPECT_EQ(recorder->Started, (std::vector<int>{1, 4}));
    EXPECT_EQ(recorder->Aborted, (std::vector<int>{3, 2}));
}

TEST(OriginLimiterTests, CancelFindsWaitersThatWereQueuedOutOfTicketOrder) {
    auto recorder = std::make_shared<Recorder>();
    Limiter limiter{1, RecordingSink{recorder}};

    limiter.Run("origin", limiter.NextTicket(), 1);
    const auto earlier = limiter.NextTicket();
    const auto later = limiter.NextTicket();
    // Racing submitters can reach the queue in the opposite order to their tickets.
    limiter.Run("origin", later, 3);
    limiter.Run("origin", earlier, 2);

    limiter.Cancel("origin", earlier);
    limiter.Cancel("origin", later);

    EXPECT_EQ(recorder->Aborted, (std::vector<int>{2, 3}));
}

namespace {
// Throws from Abort for chosen requests, after recording them, like a completion handler that throws.
class ThrowingSink {
  public:
    ThrowingSink(std::shared_ptr<Recorder> recorder, int throwOn)
        : m_recorder(std::move(recorder)), m_throwOn(throwOn) {}

    void Start(int&& pending) const { m_recorder->Started.push_back(pending); }
    void Abort(int&& pending, std::error_code error) const {
        m_recorder->Aborted.push_back(pending);
        m_recorder->Errors.push_back(error);
        if (pending == m_throwOn) {
            throw std::runtime_error("handler failed");
        }
    }

  private:
    std::shared_ptr<Recorder> m_recorder;
    int m_throwOn;
};
} // namespace

TEST(OriginLimiterTests, CloseAbortsEveryWaiterEvenWhenOneCompletionThrows) {
    auto recorder = std::make_shared<Recorder>();
    AVEVA::Private::OriginLimiter<int, ThrowingSink> limiter{1, ThrowingSink{recorder, 2}};
    limiter.Run("a", limiter.NextTicket(), 1);
    limiter.Run("a", limiter.NextTicket(), 2);
    limiter.Run("a", limiter.NextTicket(), 3);
    limiter.Run("b", limiter.NextTicket(), 4);
    limiter.Run("b", limiter.NextTicket(), 5);

    EXPECT_THROW(limiter.Close(), std::runtime_error);

    std::ranges::sort(recorder->Aborted);
    EXPECT_EQ(recorder->Aborted, (std::vector<int>{2, 3, 5}));
}

TEST(OriginLimiterTests, ExpiryThrowingCompletionDoesNotStrandLaterDeadlines) {
    using namespace std::chrono_literals;
    using ThrowingLimiter = AVEVA::Private::OriginLimiter<int, ThrowingSink>;
    boost::asio::io_context context;
    auto recorder = std::make_shared<Recorder>();
    auto limiter = std::make_shared<ThrowingLimiter>(1, ThrowingSink{recorder, 2});
    limiter->EnableDeadlines(context, std::make_error_code(std::errc::timed_out));

    const auto now = ThrowingLimiter::Clock::now();
    limiter->Run("origin", limiter->NextTicket(), 1, now + 1h);
    limiter->Run("origin", limiter->NextTicket(), 2, now + 20ms);
    limiter->Run("origin", limiter->NextTicket(), 3, now + 20ms);
    limiter->Run("origin", limiter->NextTicket(), 4, now + 150ms);

    EXPECT_THROW(context.run_for(1s), std::runtime_error);
    context.restart();
    context.run_for(1s);

    EXPECT_EQ(recorder->Started, std::vector<int>{1});
    EXPECT_EQ(recorder->Aborted, (std::vector<int>{2, 3, 4}));
}

namespace {
struct StressState {
    std::atomic<int> Active{0};
    std::atomic<int> MaxActive{0};
    std::atomic<int> Started{0};
    std::atomic<int> Aborted{0};
    std::atomic<int> Released{0};
    std::mutex Mutex;
    std::vector<int> ToRelease;
};

class StressSink {
  public:
    explicit StressSink(std::shared_ptr<StressState> state) : m_state(std::move(state)) {}

    void Start(int&& pending) const {
        const int active = ++m_state->Active;
        int seen = m_state->MaxActive.load();
        while (active > seen && !m_state->MaxActive.compare_exchange_weak(seen, active)) {
        }
        ++m_state->Started;
        const std::scoped_lock lock(m_state->Mutex);
        m_state->ToRelease.push_back(pending);
    }

    void Abort(int&&, std::error_code) const { ++m_state->Aborted; }

  private:
    std::shared_ptr<StressState> m_state;
};
} // namespace

// Every request ends in exactly one Start or Abort, and the per-origin cap is never exceeded, however Run,
// Release, Cancel and Close interleave.
TEST(OriginLimiterTests, ConcurrentRunReleaseCancelAndCloseStartOrAbortEveryRequestOnce) {
    constexpr std::size_t cap = 3;
    constexpr int producers = 4;
    constexpr int perProducer = 400;
    auto state = std::make_shared<StressState>();
    AVEVA::Private::OriginLimiter<int, StressSink> limiter{cap, StressSink{state}};

    std::atomic<bool> stopReleasing{false};
    const auto releaseOne = [&] {
        {
            const std::scoped_lock lock(state->Mutex);
            if (state->ToRelease.empty()) {
                return false;
            }
            state->ToRelease.pop_back();
        }
        --state->Active;
        limiter.Release("origin");
        ++state->Released;
        return true;
    };
    std::vector<std::jthread> releasers;
    for (int i = 0; i < 2; ++i) {
        releasers.emplace_back([&] {
            while (!stopReleasing) {
                if (!releaseOne()) {
                    std::this_thread::yield();
                }
            }
        });
    }

    // Close lands mid-run: after every producer has issued a share of its requests, before any has finished.
    std::latch producersUnderway{producers};
    {
        std::vector<std::jthread> workers;
        for (int p = 0; p < producers; ++p) {
            workers.emplace_back([&, p] {
                for (int i = 0; i < perProducer; ++i) {
                    const auto ticket = limiter.NextTicket();
                    limiter.Run("origin", ticket, p * perProducer + i);
                    if (i % 3 == 0) {
                        limiter.Cancel("origin", ticket);
                    }
                    if (i == perProducer / 4) {
                        producersUnderway.count_down();
                    }
                }
            });
        }
        workers.emplace_back([&] {
            producersUnderway.wait();
            limiter.Close();
        });
    }

    // Runs issued after Close are aborted on the spot; whatever started must be released by its owner.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((state->Released < state->Started || state->Started + state->Aborted < producers * perProducer) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    stopReleasing = true;
    releasers.clear();

    EXPECT_EQ(state->Started + state->Aborted, producers * perProducer);
    EXPECT_EQ(state->Released, state->Started);
    EXPECT_LE(state->MaxActive, static_cast<int>(cap));
}
