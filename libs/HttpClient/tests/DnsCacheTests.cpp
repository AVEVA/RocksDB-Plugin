// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/DnsCache.hpp"
#include "private/RequestOperation.hpp"

#include <boost/asio/io_context.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <iostream>
#include <new>
#include <string>
#include <thread>

namespace AllocationFailure {
extern thread_local bool counting;
extern thread_local std::size_t allocationCount;
} // namespace AllocationFailure

namespace {
using AVEVA::Private::BasicDnsCache;
using AVEVA::Private::Tcp;

// Records deliveries into caller-owned storage, so the tests need no type-erased callback.
struct RecordingWaiter {
    int* delivered = nullptr;
    boost::system::error_code* seenError = nullptr;
    AVEVA::Private::ResolveResults* seenResults = nullptr;
    bool throwsAfterRecording = false;

    void operator()(boost::system::error_code error, AVEVA::Private::ResolveResults results) const {
        if (delivered) {
            ++*delivered;
        }
        if (seenError) {
            *seenError = error;
        }
        if (seenResults) {
            *seenResults = std::move(results);
        }
        if (throwsAfterRecording) {
            throw std::bad_alloc();
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

    EXPECT_FALSE(cache.Find("host", "80"));
    cache.Store("host", "80", results);
    const auto hit = cache.Find("host", "80");
    ASSERT_TRUE(hit);
    EXPECT_EQ(hit->begin()->endpoint(), results.begin()->endpoint());
    // Repeated hits return the same endpoints.
    ASSERT_TRUE(cache.Find("host", "80"));
    EXPECT_EQ(cache.Find("host", "80")->begin()->endpoint(), hit->begin()->endpoint());

    EXPECT_FALSE(cache.Find("host", "81"));
    EXPECT_FALSE(cache.Find("other", "80"));

    FakeClock::Current += std::chrono::milliseconds{999};
    EXPECT_TRUE(cache.Find("host", "80"));
    FakeClock::Current += std::chrono::milliseconds{1};
    EXPECT_FALSE(cache.Find("host", "80"));
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
    EXPECT_TRUE(cache.Find("host", "80"));

    // The key is free again once the leader has finished.
    RecordingWaiter next;
    EXPECT_TRUE(cache.JoinOrLead("host", "80", next));
    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
}

namespace {
// Stands in for an allocation failure while caching: Store reads the clock, so a throwing clock fails it.
struct ThrowingClock {
    using rep = std::chrono::steady_clock::rep;
    using period = std::chrono::steady_clock::period;
    using duration = std::chrono::steady_clock::duration;
    using time_point = std::chrono::time_point<ThrowingClock>;
    static constexpr bool is_steady = true;

    static inline bool fail = false;
    static time_point now() {
        if (fail) {
            throw std::bad_alloc();
        }
        return {};
    }
};
} // namespace

TEST(DnsCacheTests, FinishPublishesAndReleasesTheLeaderEvenWhenCachingFails) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    BasicDnsCache<ThrowingClock, RecordingWaiter> cache;

    RecordingWaiter leader;
    ASSERT_TRUE(cache.JoinOrLead("host", "80", leader));
    int delivered = 0;
    boost::system::error_code seenError = boost::asio::error::operation_aborted;
    AVEVA::Private::ResolveResults seenResults;
    for (int i = 0; i < 3; ++i) {
        RecordingWaiter waiter{&delivered, &seenError, &seenResults};
        EXPECT_FALSE(cache.JoinOrLead("host", "80", waiter));
    }

    ThrowingClock::fail = true;
    EXPECT_NO_THROW(cache.Finish("host", "80", {}, results));
    ThrowingClock::fail = false;

    EXPECT_EQ(delivered, 3);
    EXPECT_FALSE(seenError);
    EXPECT_EQ(seenResults.size(), results.size());
    EXPECT_FALSE(cache.Find("host", "80"));
    RecordingWaiter next;
    EXPECT_TRUE(cache.JoinOrLead("host", "80", next));
    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
}

TEST(DnsCacheTests, ThrowingWaiterDoesNotSkipLaterWaitersOrEscapeFinish) {
    TestDnsCache cache;
    RecordingWaiter leader;
    ASSERT_TRUE(cache.JoinOrLead("host", "80", leader));
    int delivered = 0;
    for (int i = 0; i < 3; ++i) {
        RecordingWaiter waiter{&delivered, nullptr, nullptr, i == 0};
        EXPECT_FALSE(cache.JoinOrLead("host", "80", waiter));
    }

    EXPECT_NO_THROW(cache.Finish("host", "80", boost::asio::error::host_not_found, {}));

    EXPECT_EQ(delivered, 3);
    RecordingWaiter next;
    EXPECT_TRUE(cache.JoinOrLead("host", "80", next));
    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
}
TEST(DnsCacheTests, WaiterFactoryRunsOnlyWhenJoining) {
    TestDnsCache cache;
    int built = 0;
    int delivered = 0;
    auto factory = [&] {
        ++built;
        return RecordingWaiter{&delivered, nullptr};
    };

    EXPECT_TRUE(cache.JoinOrLead("host", "80", factory));
    EXPECT_EQ(built, 0);
    EXPECT_FALSE(cache.JoinOrLead("host", "80", factory));
    EXPECT_EQ(built, 1);

    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
    EXPECT_EQ(delivered, 1);
}

TEST(DnsCacheTests, JoiningAPendingLookupDoesNotCopyTheKey) {
    TestDnsCache cache;
    const std::string host = "account-name.blob.core.windows.net";
    const std::string service = "a-service-name-beyond-sso";
    int delivered = 0;
    RecordingWaiter waiter{&delivered, nullptr};
    ASSERT_TRUE(cache.JoinOrLead(host, service, waiter));

    AllocationFailure::allocationCount = 0;
    AllocationFailure::counting = true;
    EXPECT_FALSE(cache.JoinOrLead(host, service, waiter));
    AllocationFailure::counting = false;
    // Only the waiter vector's first element may allocate; the two key strings must not.
    EXPECT_LE(AllocationFailure::allocationCount, 1U);
    std::cout << "join allocations: " << AllocationFailure::allocationCount << '\n';

    cache.Finish(host, service, boost::asio::error::host_not_found, {});
    EXPECT_EQ(delivered, 1);
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
    EXPECT_FALSE(cache.Find("host", "80"));

    // A late follower becomes the new leader instead of inheriting the abort.
    RecordingWaiter next;
    EXPECT_TRUE(cache.JoinOrLead("host", "80", next));
    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
}

TEST(DnsCacheTests, FinishPublishesTheResultToTheCacheAndEveryWaiter) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;

    RecordingWaiter leader;
    ASSERT_TRUE(cache.JoinOrLead("host", "80", leader));
    AVEVA::Private::ResolveResults first;
    AVEVA::Private::ResolveResults second;
    RecordingWaiter firstWaiter{nullptr, nullptr, &first};
    RecordingWaiter secondWaiter{nullptr, nullptr, &second};
    EXPECT_FALSE(cache.JoinOrLead("host", "80", firstWaiter));
    EXPECT_FALSE(cache.JoinOrLead("host", "80", secondWaiter));

    cache.Finish("host", "80", {}, results);

    ASSERT_FALSE(first.empty());
    EXPECT_EQ(first.begin()->endpoint(), results.begin()->endpoint());
    EXPECT_EQ(second.begin()->endpoint(), results.begin()->endpoint());
    ASSERT_TRUE(cache.Find("host", "80"));
    EXPECT_EQ(cache.Find("host", "80")->begin()->endpoint(), first.begin()->endpoint());
}

TEST(DnsCacheTests, ResolveWaiterDoesNotKeepAFinishedRequestAlive) {
    boost::asio::io_context io;
    auto operation = std::make_shared<AVEVA::Private::RequestOperation<AVEVA::Private::PlainStream>>(
        nullptr, std::make_unique<AVEVA::Private::PlainStream>(io), AVEVA::Detail::CompletionPointer{},
        AVEVA::HttpRequestOptions{}, nullptr, AVEVA::Private::BasicConnectionKey{"host", "80"},
        std::chrono::steady_clock::now() + std::chrono::seconds{30});
    std::weak_ptr<AVEVA::Private::RequestOperation<AVEVA::Private::PlainStream>> weak = operation;

    AVEVA::Private::ResolveWaiter waiter(operation);
    operation.reset();

    // A follower that timed out stays queued in the cache until the leader finishes; that must not pin it.
    EXPECT_TRUE(weak.expired());
    waiter(boost::asio::error::operation_aborted, Tcp::resolver::results_type{});
}

TEST(DnsCacheTests, InvalidateDropsOnlyTheNamedEntry) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    cache.Store("host", "80", results);
    cache.Store("host", "81", results);

    cache.Invalidate("host", "80");

    EXPECT_FALSE(cache.Find("host", "80"));
    EXPECT_TRUE(cache.Find("host", "81"));
    cache.Invalidate("missing", "80"); // Dropping an absent entry is a no-op.
}

TEST(DnsCacheTests, AcquireNeverLeadsAgainOnceTheLeaderHasPublished) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    const auto makeWaiter = [] { return RecordingWaiter{}; };
    using Admission = TestDnsCache::Admission;

    EXPECT_EQ(cache.Acquire("host", "80", makeWaiter).Kind, Admission::Leader);
    EXPECT_EQ(cache.Acquire("host", "80", makeWaiter).Kind, Admission::Joined);

    cache.Finish("host", "80", {}, results);
    // The publication a separate Find/JoinOrLead pair could miss: the entry is cached and the claim is free.
    const auto late = cache.Acquire("host", "80", makeWaiter);
    ASSERT_EQ(late.Kind, Admission::Cached);
    EXPECT_EQ(late.Results.begin()->endpoint(), results.begin()->endpoint());

    // An aborted leader caches nothing, so the next caller leads again.
    cache.Invalidate("host", "80");
    ASSERT_EQ(cache.Acquire("host", "80", makeWaiter).Kind, Admission::Leader);
    cache.Finish("host", "80", boost::asio::error::operation_aborted, {});
    EXPECT_EQ(cache.Acquire("host", "80", makeWaiter).Kind, Admission::Leader);
    cache.Finish("host", "80", boost::asio::error::operation_aborted, {});

    // Scopes still isolate in-flight lookups but share cached results.
    int firstScope = 0;
    int secondScope = 0;
    EXPECT_EQ(cache.Acquire("h2", "80", makeWaiter, &firstScope).Kind, Admission::Leader);
    EXPECT_EQ(cache.Acquire("h2", "80", makeWaiter, &secondScope).Kind, Admission::Leader);
    cache.Finish("h2", "80", {}, results, &secondScope);
    EXPECT_EQ(cache.Acquire("h2", "80", makeWaiter, &firstScope).Kind, Admission::Cached);
    cache.Finish("h2", "80", boost::asio::error::operation_aborted, {}, &firstScope);
}

TEST(DnsCacheTests, EmptyResultsAreNotCached) {
    TestDnsCache cache;
    cache.Store("host", "80", Tcp::resolver::results_type{});
    EXPECT_FALSE(cache.Find("host", "80"));
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
    EXPECT_TRUE(cache.Find("host", "80"));

    cache.Finish("host", "80", boost::asio::error::operation_aborted, {}, &firstScope);
    EXPECT_EQ(delivered, 1);
}

TEST(DnsCacheTests, RefreshingAKeyInAFullCacheKeepsEveryOtherEntry) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    FakeClock::Current = {};
    for (std::size_t i = 0; i < TestDnsCache::MaxEntries; ++i) {
        cache.Store("host" + std::to_string(i), "80", results);
    }

    cache.Store("host0", "80", results);

    for (std::size_t i = 0; i < TestDnsCache::MaxEntries; ++i) {
        EXPECT_TRUE(cache.Find("host" + std::to_string(i), "80")) << i;
    }
}

TEST(DnsCacheTests, ChurnBeyondTheCapEvictsOneEntryAtATimeAndStaysBounded) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    FakeClock::Current = {};
    constexpr std::size_t Origins = TestDnsCache::MaxEntries * 4;
    for (std::size_t i = 0; i < Origins; ++i) {
        cache.Store("host" + std::to_string(i), "80", results);
    }

    std::size_t live = 0;
    for (std::size_t i = 0; i < Origins; ++i) {
        live += cache.Find("host" + std::to_string(i), "80") ? 1 : 0;
    }
    // Never above the cap, and never emptied by hitting it.
    EXPECT_EQ(live, TestDnsCache::MaxEntries);
    EXPECT_TRUE(cache.Find("host" + std::to_string(Origins - 1), "80"));

    // Expired entries are swept first, so live ones survive a store at the cap.
    FakeClock::Current += std::chrono::seconds{6};
    cache.Store("fresh", "80", results);
    EXPECT_TRUE(cache.Find("fresh", "80"));
}
