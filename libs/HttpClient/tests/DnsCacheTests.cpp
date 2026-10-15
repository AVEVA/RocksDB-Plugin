// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/DnsCache.hpp"

#include <boost/asio/io_context.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace {
using AVEVA::Private::BasicDnsCache;
using AVEVA::Private::Tcp;

// Records deliveries into caller-owned storage, so the tests need no type-erased callback.
struct RecordingWaiter {
    int* delivered = nullptr;
    boost::system::error_code* seenError = nullptr;

    void operator()(boost::system::error_code error, Tcp::resolver::results_type) const {
        if (delivered) {
            ++*delivered;
        }
        if (seenError) {
            *seenError = error;
        }
    }
};

// A manually advanced clock, so TTL expiry is tested without sleeping.
struct FakeClock {
    using rep = std::chrono::steady_clock::rep;
    using period = std::chrono::steady_clock::period;
    using duration = std::chrono::steady_clock::duration;
    using time_point = std::chrono::time_point<FakeClock>;
    static constexpr bool is_steady = true;

    static inline time_point Current{};
    static time_point now() { return Current; }
};

using TestDnsCache = BasicDnsCache<FakeClock, RecordingWaiter>;

Tcp::resolver::results_type Loopback(boost::asio::io_context& io) {
    Tcp::resolver resolver(io);
    return resolver.resolve("127.0.0.1", "80");
}
} // namespace

TEST(DnsCacheTests, ReturnsAStoredResultUntilTheTtlElapses) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache(std::chrono::seconds{1});
    FakeClock::Current = {};

    EXPECT_FALSE(cache.Find("host", "80").has_value());
    cache.Store("host", "80", results);
    const auto hit = cache.Find("host", "80");
    ASSERT_TRUE(hit.has_value());
    EXPECT_EQ(hit->begin()->endpoint(), results.begin()->endpoint());

    EXPECT_FALSE(cache.Find("host", "81").has_value());
    EXPECT_FALSE(cache.Find("other", "80").has_value());

    FakeClock::Current += std::chrono::milliseconds{999};
    EXPECT_TRUE(cache.Find("host", "80").has_value());
    FakeClock::Current += std::chrono::milliseconds{1};
    EXPECT_FALSE(cache.Find("host", "80").has_value());
}

TEST(DnsCacheTests, ConcurrentLookupsForOneOriginShareASingleLeader) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;

    RecordingWaiter leader;
    ASSERT_TRUE(cache.JoinOrLead("host", "80", leader));

    int delivered = 0;
    boost::system::error_code seenError = boost::asio::error::operation_aborted;
    for (int i = 0; i < 3; ++i) {
        RecordingWaiter waiter{&delivered, &seenError};
        EXPECT_FALSE(cache.JoinOrLead("host", "80", waiter));
    }
    RecordingWaiter other;
    EXPECT_TRUE(cache.JoinOrLead("host", "81", other));

    cache.Finish("host", "80", {}, results);
    EXPECT_EQ(delivered, 3);
    EXPECT_FALSE(seenError);
    EXPECT_TRUE(cache.Find("host", "80").has_value());

    // The key is free again once the leader has finished.
    RecordingWaiter next;
    EXPECT_TRUE(cache.JoinOrLead("host", "80", next));
    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
}

TEST(DnsCacheTests, AbortedLeaderDeliversTheErrorToFollowersCachesNothingAndFreesTheKey) {
    TestDnsCache cache;
    RecordingWaiter leader;
    ASSERT_TRUE(cache.JoinOrLead("host", "80", leader));

    int delivered = 0;
    boost::system::error_code seenError;
    RecordingWaiter follower{&delivered, &seenError};
    EXPECT_FALSE(cache.JoinOrLead("host", "80", follower));

    cache.Finish("host", "80", boost::asio::error::operation_aborted, {});

    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(seenError, boost::asio::error::operation_aborted);
    EXPECT_FALSE(cache.Find("host", "80").has_value());

    // A late follower becomes the new leader instead of inheriting the abort.
    RecordingWaiter next;
    EXPECT_TRUE(cache.JoinOrLead("host", "80", next));
    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
}

TEST(DnsCacheTests, InvalidateDropsOnlyTheNamedEntry) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    cache.Store("host", "80", results);
    cache.Store("host", "81", results);

    cache.Invalidate("host", "80");

    EXPECT_FALSE(cache.Find("host", "80").has_value());
    EXPECT_TRUE(cache.Find("host", "81").has_value());
    cache.Invalidate("missing", "80"); // Dropping an absent entry is a no-op.
}

TEST(DnsCacheTests, EmptyResultsAreNotCached) {
    TestDnsCache cache;
    cache.Store("host", "80", Tcp::resolver::results_type{});
    EXPECT_FALSE(cache.Find("host", "80").has_value());
}

// A leader whose io_context never runs again must not strand requests that run on another one.
TEST(DnsCacheTests, LookupsInDifferentScopesNeverJoinEachOther) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    int firstScope = 0;
    int secondScope = 0;

    RecordingWaiter stalledLeader;
    ASSERT_TRUE(cache.JoinOrLead("host", "80", stalledLeader, &firstScope));

    int delivered = 0;
    RecordingWaiter sameScope{&delivered, nullptr};
    EXPECT_FALSE(cache.JoinOrLead("host", "80", sameScope, &firstScope));

    RecordingWaiter otherLeader;
    EXPECT_TRUE(cache.JoinOrLead("host", "80", otherLeader, &secondScope));

    cache.Finish("host", "80", {}, results, &secondScope);
    EXPECT_EQ(delivered, 0);
    EXPECT_TRUE(cache.Find("host", "80").has_value());

    cache.Finish("host", "80", boost::asio::error::operation_aborted, {}, &firstScope);
    EXPECT_EQ(delivered, 1);
}
