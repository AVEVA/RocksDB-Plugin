// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/DnsCache.hpp"
#include "private/Exchange.hpp"

#include <boost/asio/io_context.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <iostream>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <variant>

namespace AllocationFailure {
extern thread_local bool counting;
extern thread_local std::size_t allocationCount;
} // namespace AllocationFailure

namespace {
using AVEVA::Private::BasicDnsCache;
using AVEVA::Private::ResolveResults;
using AVEVA::Private::Tcp;

// Records deliveries into caller-owned storage, so the tests need no type-erased callback.
struct RecordingWaiter {
    int* delivered = nullptr;
    boost::system::error_code* seenError = nullptr;
    ResolveResults* seenResults = nullptr;

    void operator()(boost::system::error_code error, const ResolveResults& results) const noexcept {
        if (delivered != nullptr) {
            ++*delivered;
        }
        if (seenError != nullptr) {
            *seenError = error;
        }
        if (seenResults != nullptr) {
            *seenResults = results;
        }
    }
};

struct ThrowingWaiter {
    void operator()(boost::system::error_code /*error*/, const ResolveResults& /*results*/) const {}
};
static_assert(!AVEVA::Private::DnsWaiter<ThrowingWaiter>, "A waiter that may throw is rejected at compile time");

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

template <typename Cache>
auto Acquire(Cache& cache, std::string_view host, std::string_view service, RecordingWaiter waiter = {},
             const void* scope = nullptr) {
    return cache.Acquire(host, service, [waiter] { return waiter; }, scope, nullptr);
}

template <typename Cache>
std::optional<typename Cache::LeaderToken> Lead(Cache& cache, std::string_view host, std::string_view service,
                                                const void* scope = nullptr) {
    auto admission = Acquire(cache, host, service, {}, scope);
    if (auto* const token = std::get_if<typename Cache::LeaderToken>(&admission)) {
        return std::move(*token);
    }
    return std::nullopt;
}

template <typename Cache>
bool Joins(Cache& cache, std::string_view host, std::string_view service, RecordingWaiter waiter,
           const void* scope = nullptr) {
    auto admission = Acquire(cache, host, service, waiter, scope);
    return std::holds_alternative<typename Cache::JoinedLookup>(admission);
}

// Probes the cache in a scope of its own, so an in-flight lookup is never joined; a miss leads a lookup that is
// abandoned at once, which publishes to nobody.
template <typename Cache>
std::optional<ResolveResults> Find(Cache& cache, std::string_view host, std::string_view service) {
    static const int probeScope = 0;
    auto admission = Acquire(cache, host, service, {}, &probeScope);
    if (auto* const cached = std::get_if<typename Cache::CachedAnswer>(&admission)) {
        return cached->results;
    }
    return std::nullopt;
}

template <typename Cache>
void Store(Cache& cache, std::string_view host, std::string_view service, const ResolveResults& results) {
    static const int storeScope = 0;
    auto token = Lead(cache, host, service, &storeScope);
    ASSERT_TRUE(token);
    std::move(*token).Publish({}, results);
}
} // namespace

TEST(DnsCacheTests, ReturnsAStoredResultUntilTheTtlElapses) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache(std::chrono::seconds{1});
    FakeClock::Current = {};

    EXPECT_FALSE(Find(cache, "host", "80"));
    Store(cache, "host", "80", results);
    const auto hit = Find(cache, "host", "80");
    ASSERT_TRUE(hit);
    EXPECT_EQ(hit->begin()->endpoint(), results.begin()->endpoint());

    EXPECT_FALSE(Find(cache, "host", "81"));
    EXPECT_FALSE(Find(cache, "other", "80"));

    FakeClock::Current += std::chrono::milliseconds{999};
    EXPECT_TRUE(Find(cache, "host", "80"));
    FakeClock::Current += std::chrono::milliseconds{1};
    EXPECT_FALSE(Find(cache, "host", "80"));
}

TEST(DnsCacheTests, ConcurrentLookupsForOneOriginShareASingleLeader) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;

    auto leader = Lead(cache, "host", "80");
    ASSERT_TRUE(leader);

    int delivered = 0;
    boost::system::error_code seenError = boost::asio::error::operation_aborted;
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(Joins(cache, "host", "80", RecordingWaiter{&delivered, &seenError}));
    }
    auto other = Lead(cache, "host", "81");
    EXPECT_TRUE(other);

    std::move(*leader).Publish({}, results);
    EXPECT_EQ(delivered, 3);
    EXPECT_FALSE(seenError);
    EXPECT_TRUE(Find(cache, "host", "80"));
    // A published token is spent: destroying it publishes nothing more.
    leader.reset();
    EXPECT_EQ(delivered, 3);
}

namespace {
// Stands in for an allocation failure while caching: storing reads the clock, so a throwing clock fails it.
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

TEST(DnsCacheTests, PublishReachesEveryWaiterAndReportsWhenCachingFails) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    BasicDnsCache<ThrowingClock, RecordingWaiter> cache;

    int reports = 0;
    auto sink = std::make_shared<const AVEVA::Private::DiagnosticsSink>(
        AVEVA::HttpDiagnosticsCallback{[&reports](const AVEVA::HttpDiagnostic& diagnostic) noexcept {
            reports += diagnostic.Kind == AVEVA::HttpDiagnosticKind::SwallowedException ? 1 : 0;
        }});
    auto admission = cache.Acquire("host", "80", [] { return RecordingWaiter{}; }, nullptr, sink);
    auto* const leader = std::get_if<decltype(cache)::LeaderToken>(&admission);
    ASSERT_NE(leader, nullptr);
    int delivered = 0;
    boost::system::error_code seenError = boost::asio::error::operation_aborted;
    ResolveResults seenResults;
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(Joins(cache, "host", "80", RecordingWaiter{&delivered, &seenError, &seenResults}));
    }

    ThrowingClock::fail = true;
    std::move(*leader).Publish({}, results);
    ThrowingClock::fail = false;

    EXPECT_EQ(reports, 1);
    EXPECT_EQ(delivered, 3);
    EXPECT_FALSE(seenError);
    EXPECT_EQ(seenResults.size(), results.size());
    // Nothing was cached, and the claim is free again.
    EXPECT_TRUE(Lead(cache, "host", "80"));
}

TEST(DnsCacheTests, WaiterFactoryRunsOnlyWhenJoining) {
    TestDnsCache cache;
    int built = 0;
    int delivered = 0;
    auto factory = [&] {
        ++built;
        return RecordingWaiter{&delivered, nullptr};
    };

    auto leader = cache.Acquire("host", "80", factory, nullptr, nullptr);
    EXPECT_TRUE(std::holds_alternative<TestDnsCache::LeaderToken>(leader));
    EXPECT_EQ(built, 0);
    EXPECT_TRUE(
        std::holds_alternative<TestDnsCache::JoinedLookup>(cache.Acquire("host", "80", factory, nullptr, nullptr)));
    EXPECT_EQ(built, 1);

    std::get<TestDnsCache::LeaderToken>(std::move(leader)).Publish(boost::asio::error::host_not_found, {});
    EXPECT_EQ(delivered, 1);
}

TEST(DnsCacheTests, JoiningAPendingLookupDoesNotCopyTheKey) {
    TestDnsCache cache;
    const std::string host = "account-name.blob.core.windows.net";
    const std::string service = "a-service-name-beyond-sso";
    int delivered = 0;
    auto leader = Lead(cache, host, service);
    ASSERT_TRUE(leader);

    AllocationFailure::allocationCount = 0;
    AllocationFailure::counting = true;
    const auto joined = Joins(cache, host, service, RecordingWaiter{&delivered, nullptr});
    AllocationFailure::counting = false;
    EXPECT_TRUE(joined);
    // Only the waiter vector's first element may allocate; the two key strings must not.
    EXPECT_LE(AllocationFailure::allocationCount, 1U);
    std::cout << "join allocations: " << AllocationFailure::allocationCount << '\n';

    std::move(*leader).Publish(boost::asio::error::host_not_found, {});
    EXPECT_EQ(delivered, 1);
}

TEST(DnsCacheTests, AbandonedLeaderDeliversAnAbortToFollowersCachesNothingAndFreesTheKey) {
    TestDnsCache cache;
    auto leader = Lead(cache, "host", "80");
    ASSERT_TRUE(leader);

    int delivered = 0;
    boost::system::error_code seenError;
    EXPECT_TRUE(Joins(cache, "host", "80", RecordingWaiter{&delivered, &seenError}));

    leader.reset(); // Destroyed without publishing, as when the io_context running the lookup is torn down.

    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(seenError, boost::asio::error::operation_aborted);
    EXPECT_FALSE(Find(cache, "host", "80"));

    // A late follower becomes the new leader instead of inheriting the abort.
    EXPECT_TRUE(Lead(cache, "host", "80"));
}

TEST(DnsCacheTests, MovedFromTokenPublishesNothing) {
    TestDnsCache cache;
    auto leader = Lead(cache, "host", "80");
    ASSERT_TRUE(leader);
    int delivered = 0;
    EXPECT_TRUE(Joins(cache, "host", "80", RecordingWaiter{&delivered, nullptr}));

    {
        auto moved = std::move(*leader);
        leader.reset();
        EXPECT_EQ(delivered, 0);
    }
    EXPECT_EQ(delivered, 1);
}

TEST(DnsCacheTests, PublishSendsTheResultToTheCacheAndEveryWaiter) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;

    auto leader = Lead(cache, "host", "80");
    ASSERT_TRUE(leader);
    ResolveResults first;
    ResolveResults second;
    EXPECT_TRUE(Joins(cache, "host", "80", RecordingWaiter{nullptr, nullptr, &first}));
    EXPECT_TRUE(Joins(cache, "host", "80", RecordingWaiter{nullptr, nullptr, &second}));

    std::move(*leader).Publish({}, results);

    ASSERT_FALSE(first.empty());
    EXPECT_EQ(first.begin()->endpoint(), results.begin()->endpoint());
    EXPECT_EQ(second.begin()->endpoint(), results.begin()->endpoint());
    const auto cached = Find(cache, "host", "80");
    ASSERT_TRUE(cached);
    EXPECT_EQ(cached->begin()->endpoint(), first.begin()->endpoint());
}

TEST(DnsCacheTests, ResolveWaiterDeliversTheLeadersOutcomeThroughItsChannel) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    auto channel = std::make_shared<AVEVA::Private::ResolveChannel>(io.get_executor(), 1);

    const AVEVA::Private::ResolveWaiter waiter(channel, nullptr);
    waiter({}, results);

    // The value is buffered, so the joiner sees it even though it only starts listening afterwards.
    int received = 0;
    EXPECT_TRUE(channel->try_receive([&](boost::system::error_code error, Tcp::resolver::results_type delivered) {
        ++received;
        EXPECT_FALSE(error);
        EXPECT_EQ(delivered.begin()->endpoint(), results.begin()->endpoint());
    }));
    EXPECT_EQ(received, 1);
}

TEST(DnsCacheTests, InvalidateDropsOnlyTheNamedEntry) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    Store(cache, "host", "80", results);
    Store(cache, "host", "81", results);

    cache.Invalidate("host", "80");

    EXPECT_FALSE(Find(cache, "host", "80"));
    EXPECT_TRUE(Find(cache, "host", "81"));
    cache.Invalidate("missing", "80"); // Dropping an absent entry is a no-op.
}

TEST(DnsCacheTests, AcquireNeverLeadsAgainOnceTheLeaderHasPublished) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;

    auto leader = Lead(cache, "host", "80");
    ASSERT_TRUE(leader);
    EXPECT_TRUE(Joins(cache, "host", "80", {}));

    std::move(*leader).Publish({}, results);
    // The publication a separate find-then-join pair could miss: the entry is cached and the claim is free.
    auto late = Acquire(cache, "host", "80");
    ASSERT_TRUE(std::holds_alternative<TestDnsCache::CachedAnswer>(late));
    EXPECT_EQ(std::get<TestDnsCache::CachedAnswer>(late).results.begin()->endpoint(), results.begin()->endpoint());

    // An aborted leader caches nothing, so the next caller leads again.
    cache.Invalidate("host", "80");
    auto aborted = Lead(cache, "host", "80");
    ASSERT_TRUE(aborted);
    std::move(*aborted).Publish(boost::asio::error::operation_aborted, {});
    EXPECT_TRUE(Lead(cache, "host", "80"));

    // Scopes still isolate in-flight lookups but share cached results.
    const int firstScope = 0;
    const int secondScope = 0;
    auto first = Lead(cache, "h2", "80", &firstScope);
    auto second = Lead(cache, "h2", "80", &secondScope);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    std::move(*second).Publish({}, results);
    EXPECT_TRUE(std::holds_alternative<TestDnsCache::CachedAnswer>(Acquire(cache, "h2", "80", {}, &firstScope)));
}

TEST(DnsCacheTests, EmptyResultsAreNotCached) {
    TestDnsCache cache;
    Store(cache, "host", "80", Tcp::resolver::results_type{});
    EXPECT_FALSE(Find(cache, "host", "80"));
}

// A leader whose io_context never runs again must not strand requests that run on another one.
TEST(DnsCacheTests, LookupsInDifferentScopesNeverJoinEachOther) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    const int firstScope = 0;
    const int secondScope = 0;

    auto stalledLeader = Lead(cache, "host", "80", &firstScope);
    ASSERT_TRUE(stalledLeader);

    int delivered = 0;
    EXPECT_TRUE(Joins(cache, "host", "80", RecordingWaiter{&delivered, nullptr}, &firstScope));

    auto otherLeader = Lead(cache, "host", "80", &secondScope);
    ASSERT_TRUE(otherLeader);

    std::move(*otherLeader).Publish({}, results);
    EXPECT_EQ(delivered, 0);
    EXPECT_TRUE(Find(cache, "host", "80"));

    stalledLeader.reset();
    EXPECT_EQ(delivered, 1);
}

TEST(DnsCacheTests, RefreshingAKeyInAFullCacheKeepsEveryOtherEntry) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    FakeClock::Current = {};
    // A lookup in another scope that publishes once host0 is already cached refreshes the entry.
    const int otherScope = 0;
    auto refresh = Lead(cache, "host0", "80", &otherScope);
    ASSERT_TRUE(refresh);
    for (std::size_t i = 0; i < TestDnsCache::MaxEntries; ++i) {
        Store(cache, "host" + std::to_string(i), "80", results);
    }

    std::move(*refresh).Publish({}, results);

    for (std::size_t i = 0; i < TestDnsCache::MaxEntries; ++i) {
        EXPECT_TRUE(Find(cache, "host" + std::to_string(i), "80")) << i;
    }
}

TEST(DnsCacheTests, ChurnBeyondTheCapEvictsOneEntryAtATimeAndStaysBounded) {
    boost::asio::io_context io;
    const auto results = Loopback(io);
    TestDnsCache cache;
    FakeClock::Current = {};
    constexpr std::size_t Origins = TestDnsCache::MaxEntries * 4;
    for (std::size_t i = 0; i < Origins; ++i) {
        Store(cache, "host" + std::to_string(i), "80", results);
    }

    std::size_t live = 0;
    for (std::size_t i = 0; i < Origins; ++i) {
        live += Find(cache, "host" + std::to_string(i), "80") ? 1 : 0;
    }
    // Never above the cap, and never emptied by hitting it.
    EXPECT_EQ(live, TestDnsCache::MaxEntries);
    EXPECT_TRUE(Find(cache, "host" + std::to_string(Origins - 1), "80"));

    // Expired entries are swept first, so live ones survive a store at the cap.
    FakeClock::Current += std::chrono::seconds{6};
    Store(cache, "fresh", "80", results);
    EXPECT_TRUE(Find(cache, "fresh", "80"));
}
