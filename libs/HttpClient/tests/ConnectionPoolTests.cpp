// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/ConnectionPool.hpp"
#include "private/StreamTypes.hpp"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// Lets a test make the Nth upcoming allocation on this thread throw; -1 means disarmed. A global replacement is the
// only way to reach the pool's allocation failures without changing the pool itself.
namespace AllocationFailure {
thread_local int allocationsUntilFailure = -1;
// Allocation counting for HttpClientAllocationTests.cpp, which cannot replace operator new a second time.
thread_local bool counting = false;
thread_local std::size_t allocationCount = 0;
} // namespace AllocationFailure

void* operator new(std::size_t size) {
    if (AllocationFailure::counting) {
        ++AllocationFailure::allocationCount;
    }
    if (AllocationFailure::allocationsUntilFailure >= 0 && AllocationFailure::allocationsUntilFailure-- == 0) {
        throw std::bad_alloc();
    }
    if (void* memory = std::malloc(size != 0 ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc();
}

void operator delete(void* memory) noexcept { std::free(memory); }

void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

namespace {
using AVEVA::Private::BasicConnectionKey;
using AVEVA::Private::ConnectionKey;
using AVEVA::Private::ConnectionPool;
using AVEVA::Private::PlainStream;
using AVEVA::Private::PooledConnection;
using AVEVA::Private::TlsConnectionKey;
namespace asio = boost::asio;
using Tcp = asio::ip::tcp;

// BasicConnectionKey and TlsConnectionKey must remain distinct, non-interchangeable types:
// a key for one connection pool must never compile against the other pool's Acquire/Release.
static_assert(!std::is_same_v<BasicConnectionKey, TlsConnectionKey>,
              "Basic and TLS connection keys must be distinct types");
static_assert(!std::is_constructible_v<TlsConnectionKey, BasicConnectionKey>,
              "A basic connection key must not be usable as a TLS connection key");

// The pool never touches the stream itself, so a tracked stand-in is enough to observe both
// identity (which connection came back) and lifetime (when a connection was closed).
struct TrackedStream {
    TrackedStream(int streamIdentity, int& streamLiveCount) : identity(streamIdentity), liveCount(&streamLiveCount) {
        ++*this->liveCount;
    }

    TrackedStream(const TrackedStream&) = delete;
    TrackedStream& operator=(const TrackedStream&) = delete;

    ~TrackedStream() { --*liveCount; }

    int identity;
    int* liveCount;
};

// A manually advanced clock so idle-timeout behaviour does not depend on real time.
struct FakeClock {
    using rep = std::chrono::steady_clock::rep;
    using period = std::chrono::steady_clock::period;
    using duration = std::chrono::steady_clock::duration;
    using time_point = std::chrono::time_point<FakeClock>;
    static constexpr bool is_steady = true;

    static time_point now() noexcept { return time_point{Offset}; }

    static void Advance(duration amount) noexcept { Offset += amount; }

    static inline duration Offset{};
};

using Pool = ConnectionPool<TrackedStream>;
using FakePool = ConnectionPool<TrackedStream, FakeClock>;
using Key = ConnectionKey<TrackedStream>;

PooledConnection<TrackedStream> MakeConnection(int identity, int& liveCount) {
    return PooledConnection<TrackedStream>{std::make_unique<TrackedStream>(identity, liveCount), {}};
}

Key MakeKey(std::string host) { return Key{std::move(host), "80"}; }

constexpr std::chrono::seconds LongTimeout{300};
constexpr std::chrono::seconds ShortTimeout{30};

TEST(ConnectionPool, AcquireOnEmptyPoolReturnsNothing) {
    Pool pool(4, LongTimeout);
    EXPECT_FALSE(pool.Acquire(MakeKey("example.com")).has_value());
}

TEST(ConnectionPool, ReleasedConnectionIsReusedForTheSameOrigin) {
    int live = 0;
    Pool pool(4, LongTimeout);
    pool.Release(MakeKey("example.com"), MakeConnection(1, live));

    auto acquired = pool.Acquire(MakeKey("example.com"));
    ASSERT_TRUE(acquired.has_value());
    EXPECT_EQ(acquired->stream->identity, 1);
    EXPECT_EQ(live, 1);

    EXPECT_FALSE(pool.Acquire(MakeKey("example.com")).has_value());
}

TEST(ConnectionPool, ConnectionsAreNotSharedAcrossOrigins) {
    int live = 0;
    Pool pool(4, LongTimeout);
    pool.Release(MakeKey("example.com"), MakeConnection(1, live));

    EXPECT_FALSE(pool.Acquire(MakeKey("other.com")).has_value());
    EXPECT_FALSE(pool.Acquire(Key{"example.com", "8080"}).has_value());
    EXPECT_TRUE(pool.Acquire(MakeKey("example.com")).has_value());
}

TEST(ConnectionPool, AcquireReturnsTheMostRecentlyReleasedConnection) {
    int live = 0;
    Pool pool(4, LongTimeout);
    const auto key = MakeKey("example.com");
    pool.Release(key, MakeConnection(1, live));
    pool.Release(key, MakeConnection(2, live));
    pool.Release(key, MakeConnection(3, live));

    auto first = pool.Acquire(key);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->stream->identity, 3);
    auto second = pool.Acquire(key);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->stream->identity, 2);
    auto third = pool.Acquire(key);
    ASSERT_TRUE(third.has_value());
    EXPECT_EQ(third->stream->identity, 1);
    EXPECT_FALSE(pool.Acquire(key).has_value());
}

TEST(ConnectionPool, ExceedingTheIdleLimitEvictsTheOldestConnection) {
    int live = 0;
    Pool pool(2, LongTimeout);
    const auto key = MakeKey("example.com");
    pool.Release(key, MakeConnection(1, live));
    pool.Release(key, MakeConnection(2, live));
    pool.Release(key, MakeConnection(3, live));

    EXPECT_EQ(live, 2);
    auto first = pool.Acquire(key);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->stream->identity, 3);
    auto second = pool.Acquire(key);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->stream->identity, 2);
    EXPECT_FALSE(pool.Acquire(key).has_value());
}

TEST(ConnectionPool, ZeroIdleLimitDisablesPooling) {
    int live = 0;
    Pool pool(0, LongTimeout);
    const auto key = MakeKey("example.com");
    pool.Release(key, MakeConnection(1, live));

    EXPECT_EQ(live, 0);
    EXPECT_FALSE(pool.Acquire(key).has_value());
}

TEST(ConnectionPool, ExpiredConnectionsAreClosedInsteadOfReused) {
    int live = 0;
    FakePool pool(4, ShortTimeout);
    const auto key = MakeKey("example.com");
    pool.Release(key, MakeConnection(1, live));
    ASSERT_EQ(live, 1);

    FakeClock::Advance(ShortTimeout * 2);

    EXPECT_FALSE(pool.Acquire(key).has_value());
    EXPECT_EQ(live, 0);
}

TEST(ConnectionPool, ExpiredConnectionsOfOtherOriginsAreReclaimed) {
    int live = 0;
    FakePool pool(4, ShortTimeout);
    pool.Release(MakeKey("stale.com"), MakeConnection(1, live));
    ASSERT_EQ(live, 1);

    FakeClock::Advance(ShortTimeout * 2);

    // Touching an unrelated origin still sweeps the pool-wide LRU list.
    pool.Release(MakeKey("fresh.com"), MakeConnection(2, live));
    EXPECT_EQ(live, 1);

    EXPECT_FALSE(pool.Acquire(MakeKey("stale.com")).has_value());
}

TEST(ConnectionPool, SweepClosesExpiredConnectionsWithoutAnyRequest) {
    int live = 0;
    FakePool pool(4, ShortTimeout);
    pool.Release(MakeKey("a.com"), MakeConnection(1, live));
    FakeClock::Advance(ShortTimeout / 2);
    pool.Release(MakeKey("b.com"), MakeConnection(2, live));
    FakeClock::Advance(ShortTimeout * 3 / 4);

    EXPECT_EQ(pool.Sweep(), 1U);
    EXPECT_EQ(live, 1);

    FakeClock::Advance(ShortTimeout);
    EXPECT_EQ(pool.Sweep(), 0U);
    EXPECT_EQ(live, 0);
}

TEST(ConnectionPool, TotalIdleCapEvictsTheOldestAcrossOrigins) {
    int live = 0;
    Pool pool(4, LongTimeout, 2);
    pool.Release(MakeKey("a.com"), MakeConnection(1, live));
    pool.Release(MakeKey("b.com"), MakeConnection(2, live));
    pool.Release(MakeKey("c.com"), MakeConnection(3, live));

    EXPECT_EQ(live, 2);
    EXPECT_EQ(pool.IdleCount(), 2U);
    EXPECT_FALSE(pool.Acquire(MakeKey("a.com")).has_value());
    EXPECT_TRUE(pool.Acquire(MakeKey("b.com")).has_value());
    EXPECT_TRUE(pool.Acquire(MakeKey("c.com")).has_value());
}

// Enough distinct origins to force the origin map to rehash several times, which invalidates iterators.
constexpr int ManyOrigins = 512;

TEST(ConnectionPool, SweepAfterOriginMapRehashClosesEveryExpiredConnection) {
    int live = 0;
    FakePool pool(4, ShortTimeout, ManyOrigins * 2);
    for (int i = 0; i < ManyOrigins; ++i) {
        pool.Release(MakeKey("host" + std::to_string(i)), MakeConnection(i, live));
    }
    ASSERT_EQ(live, ManyOrigins);

    FakeClock::Advance(ShortTimeout * 2);

    EXPECT_EQ(pool.Sweep(), 0U);
    EXPECT_EQ(live, 0);
    EXPECT_EQ(pool.IdleCount(), 0U);

    pool.Release(MakeKey("host0"), MakeConnection(1, live));
    auto reused = pool.Acquire(MakeKey("host0"));
    ASSERT_TRUE(reused.has_value());
    EXPECT_EQ(reused->stream->identity, 1);
}

TEST(ConnectionPool, GlobalCapEvictionAfterOriginMapRehashKeepsAccountingExact) {
    int live = 0;
    constexpr std::size_t cap = 4;
    Pool pool(4, LongTimeout, cap);
    for (int i = 0; i < ManyOrigins; ++i) {
        pool.Release(MakeKey("host" + std::to_string(i)), MakeConnection(i, live));
        ASSERT_LE(live, static_cast<int>(cap));
    }

    EXPECT_EQ(live, static_cast<int>(cap));
    EXPECT_EQ(pool.IdleCount(), cap);
    EXPECT_FALSE(pool.Acquire(MakeKey("host0")).has_value());
    for (int i = ManyOrigins - static_cast<int>(cap); i < ManyOrigins; ++i) {
        auto connection = pool.Acquire(MakeKey("host" + std::to_string(i)));
        ASSERT_TRUE(connection.has_value());
        EXPECT_EQ(connection->stream->identity, i);
    }
    EXPECT_EQ(pool.IdleCount(), 0U);
}

TEST(ConnectionPool, ZeroTotalIdleLimitDisablesPooling) {
    int live = 0;
    Pool pool(4, LongTimeout, 0);
    const auto key = MakeKey("example.com");
    pool.Release(key, MakeConnection(1, live));

    EXPECT_EQ(live, 0);
    EXPECT_EQ(pool.IdleCount(), 0U);
    EXPECT_FALSE(pool.Acquire(key).has_value());
}

struct CountingNotifier {
    int* notifications = nullptr;

    void operator()() const noexcept { ++*notifications; }
};

TEST(ConnectionPool, NotifiesOnEverySuccessfulRelease) {
    int live = 0;
    int notifications = 0;
    ConnectionPool<TrackedStream, std::chrono::steady_clock, CountingNotifier> pool(4, LongTimeout, 256,
                                                                                    CountingNotifier{&notifications});

    pool.Release(MakeKey("a.com"), MakeConnection(1, live));
    pool.Release(MakeKey("b.com"), MakeConnection(2, live));
    EXPECT_EQ(notifications, 2);

    EXPECT_TRUE(pool.Acquire(MakeKey("a.com")).has_value());
    EXPECT_TRUE(pool.Acquire(MakeKey("b.com")).has_value());
    pool.Release(MakeKey("a.com"), MakeConnection(3, live));
    EXPECT_EQ(notifications, 3);
}

TEST(ConnectionPool, DestroyingThePoolClosesIdleConnections) {
    int live = 0;
    {
        Pool pool(4, LongTimeout);
        pool.Release(MakeKey("example.com"), MakeConnection(1, live));
        pool.Release(MakeKey("other.com"), MakeConnection(2, live));
        ASSERT_EQ(live, 2);
    }
    EXPECT_EQ(live, 0);
}

TEST(ConnectionPool, RepeatedAcquireReleaseCyclesStayConsistent) {
    int live = 0;
    Pool pool(2, LongTimeout);
    const auto first = MakeKey("first.com");
    const auto second = MakeKey("second.com");

    for (int cycle = 0; cycle < 200; ++cycle) {
        pool.Release(first, MakeConnection(cycle, live));
        pool.Release(second, MakeConnection(cycle, live));
        auto reused = pool.Acquire(first);
        ASSERT_TRUE(reused.has_value());
        EXPECT_EQ(reused->stream->identity, cycle);
        pool.Release(first, std::move(*reused));
    }

    EXPECT_LE(live, 4);
    EXPECT_TRUE(pool.Acquire(first).has_value());
    EXPECT_TRUE(pool.Acquire(second).has_value());
}

TEST(ConnectionPool, ReleasingAnEmptyConnectionIsIgnored) {
    Pool pool(4, LongTimeout);
    const auto key = MakeKey("example.com");
    pool.Release(key, PooledConnection<TrackedStream>{});
    EXPECT_FALSE(pool.Acquire(key).has_value());
}

// Fails the Nth allocation inside Release: 0 is the node, 1 is the new origin's map entry.
class ConnectionPoolAllocationFailure : public ::testing::TestWithParam<int> {};

TEST_P(ConnectionPoolAllocationFailure, ReleaseThatRunsOutOfMemoryLeavesThePoolConsistent) {
    int live = 0;
    Pool pool(4, LongTimeout);
    const auto kept = MakeKey("kept.com");
    const auto failing = MakeKey("failing.com");
    pool.Release(kept, MakeConnection(1, live));

    auto doomed = MakeConnection(2, live);
    AllocationFailure::allocationsUntilFailure = GetParam();
    bool threw = false;
    try {
        pool.Release(failing, std::move(doomed));
    } catch (const std::bad_alloc&) {
        threw = true;
    }
    AllocationFailure::allocationsUntilFailure = -1;
    doomed = PooledConnection<TrackedStream>{};

    ASSERT_TRUE(threw);
    EXPECT_EQ(live, 1);
    EXPECT_FALSE(pool.Acquire(failing).has_value());

    pool.Release(failing, MakeConnection(3, live));
    auto reused = pool.Acquire(failing);
    ASSERT_TRUE(reused.has_value());
    EXPECT_EQ(reused->stream->identity, 3);
    auto original = pool.Acquire(kept);
    ASSERT_TRUE(original.has_value());
    EXPECT_EQ(original->stream->identity, 1);
}

INSTANTIATE_TEST_SUITE_P(Allocations, ConnectionPoolAllocationFailure, ::testing::Values(0, 1));

TEST(ConnectionPool, RealSocketLivenessCheckDiscardsPeerClosedConnection) {
    asio::io_context context;
    // A raw synchronous accept is used on purpose: the test needs the peer socket itself, to close it and make the
    // pooled client socket observe a FIN, which ScriptedServer does not expose.
    Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
    Tcp::socket server(context);
    PlainStream stream(context);
    AVEVA::Private::beast::get_lowest_layer(stream).socket().connect(
        {asio::ip::make_address("127.0.0.1"), acceptor.local_endpoint().port()});
    acceptor.accept(server);

    // Wait until the peer's FIN is visible on the client socket instead of sleeping for an arbitrary time.
    boost::system::error_code ignored;
    server.shutdown(Tcp::socket::shutdown_both, ignored);
    server.close(ignored);
    AVEVA::Private::beast::get_lowest_layer(stream).socket().wait(Tcp::socket::wait_read, ignored);

    ConnectionPool<PlainStream, FakeClock> pool(4, LongTimeout);
    const ConnectionKey<PlainStream> key{"127.0.0.1", "80"};
    pool.Release(key, PooledConnection<PlainStream>{std::make_unique<PlainStream>(std::move(stream)), {}});
    // Connections idle for less than the probe threshold are handed out unchecked, so age it past that.
    FakeClock::Advance(std::chrono::seconds(1));

    EXPECT_FALSE(pool.Acquire(key).has_value());
}

TEST(ConnectionPool, SweepHandsExpiredSocketsToTheirExecutorInsteadOfClosingThemInline) {
    asio::io_context context;
    ConnectionPool<PlainStream, FakeClock> pool(4, ShortTimeout);
    pool.Release({"127.0.0.1", "80"}, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});
    FakeClock::Advance(ShortTimeout * 2);

    EXPECT_EQ(pool.Sweep(), 0U);
    // The disposal is queued on the io_context rather than performed on the sweeping thread.
    EXPECT_EQ(context.poll(), 1U);
}

TEST(ConnectionPool, AcquireDropsExpiredSocketsThroughTheirExecutorToo) {
    asio::io_context context;
    ConnectionPool<PlainStream, FakeClock> pool(4, ShortTimeout);
    const ConnectionKey<PlainStream> key{"127.0.0.1", "80"};
    pool.Release(key, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});
    FakeClock::Advance(ShortTimeout * 2);

    EXPECT_FALSE(pool.Acquire(key).has_value());
    EXPECT_EQ(context.poll(), 1U);
}

TEST(ConnectionPool, EvictionOnReleaseHandsTheEvictedSocketToItsExecutor) {
    asio::io_context context;
    ConnectionPool<PlainStream, FakeClock> pool(1, LongTimeout);
    const ConnectionKey<PlainStream> key{"127.0.0.1", "80"};
    EXPECT_TRUE(pool.Release(key, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}}));
    EXPECT_EQ(context.poll(), 0U);
    context.restart(); // poll() with no work leaves the context stopped.

    // The per-origin cap of one evicts the first connection.
    EXPECT_TRUE(pool.Release(key, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}}));
    EXPECT_EQ(context.poll(), 1U);
}

TEST(ConnectionPool, ReleaseLeavesAConnectionItDidNotTakeWithTheCaller) {
    asio::io_context context;
    ConnectionPool<PlainStream, FakeClock> disabled(0, LongTimeout);
    PooledConnection<PlainStream> connection{std::make_unique<PlainStream>(context), {}};

    EXPECT_FALSE(disabled.Release({"127.0.0.1", "80"}, std::move(connection)));
    EXPECT_NE(connection.stream, nullptr);
}

TEST(ConnectionPool, DestroyingThePoolFromAnotherThreadHandsIdleSocketsToTheirExecutor) {
    asio::io_context context;
    auto pool = std::make_unique<ConnectionPool<PlainStream, FakeClock>>(4, LongTimeout);
    pool->Release({"127.0.0.1", "80"}, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});
    pool->Release({"127.0.0.2", "80"}, PooledConnection<PlainStream>{std::make_unique<PlainStream>(context), {}});

    std::thread destroyer([&pool] { pool.reset(); });
    destroyer.join();

    // Teardown is queued on the io_context, one handler per idle socket, not performed on the destroying thread.
    EXPECT_EQ(context.poll(), 2U);
}

TEST(ConnectionPool, IdleBuffersAboveTheRetentionCapAreReleasedAndSmallOnesKept) {
    namespace beast = boost::beast;
    beast::flat_buffer small;
    small.prepare(AVEVA::Private::MaxRetainedBufferCapacity);
    AVEVA::Private::TrimIdleBuffer(small);
    EXPECT_GE(small.capacity(), AVEVA::Private::MaxRetainedBufferCapacity);

    beast::flat_buffer large;
    large.prepare(AVEVA::Private::MaxRetainedBufferCapacity * 4);
    AVEVA::Private::TrimIdleBuffer(large);
    EXPECT_LE(large.capacity(), AVEVA::Private::MaxRetainedBufferCapacity);
}

TEST(ConnectionPool, ConcurrentAcquireReleaseAcrossThreadsStaysConsistent) {
    struct ConcurrentTrackedStream {
        explicit ConcurrentTrackedStream(int streamIdentity, std::atomic<int>& streamLiveCount)
            : identity(streamIdentity), liveCount(&streamLiveCount) {
            ++*liveCount;
        }

        ConcurrentTrackedStream(const ConcurrentTrackedStream&) = delete;
        ConcurrentTrackedStream& operator=(const ConcurrentTrackedStream&) = delete;

        ~ConcurrentTrackedStream() { --*liveCount; }

        int identity;
        std::atomic<int>* liveCount;
    };

    using ConcurrentPool = ConnectionPool<ConcurrentTrackedStream>;
    using ConcurrentKey = ConnectionKey<ConcurrentTrackedStream>;

    auto makeConnection = [](int identity, std::atomic<int>& liveCount) {
        return PooledConnection<ConcurrentTrackedStream>{std::make_unique<ConcurrentTrackedStream>(identity, liveCount),
                                                         {}};
    };

    std::atomic<int> live{0};
    std::atomic<int> nextIdentity{1};
    ConcurrentPool pool(8, LongTimeout, 16);
    const std::array<ConcurrentKey, 2> keys{ConcurrentKey{"a.example", "80"}, ConcurrentKey{"b.example", "80"}};

    constexpr int ThreadCount = 8;
    constexpr int Iterations = 400;
    std::barrier start(ThreadCount);
    std::vector<std::thread> workers;
    workers.reserve(ThreadCount);
    for (int threadIndex = 0; threadIndex < ThreadCount; ++threadIndex) {
        workers.emplace_back([&, threadIndex] {
            start.arrive_and_wait();
            for (int iteration = 0; iteration < Iterations; ++iteration) {
                const ConcurrentKey& key = keys.at(static_cast<std::size_t>(threadIndex + iteration) % keys.size());
                auto acquired = pool.Acquire(key);
                if (!acquired.has_value()) {
                    pool.Release(key, makeConnection(nextIdentity.fetch_add(1), live));
                    continue;
                }
                pool.Release(key, std::move(*acquired));
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    int drained = 0;
    for (const ConcurrentKey& key : keys) {
        while (pool.Acquire(key).has_value()) {
            ++drained;
        }
    }

    EXPECT_LE(drained, 16);
    EXPECT_EQ(live.load(), 0);
}
TEST(ConnectionPoolBuffers, SmallIdleBufferKeepsItsCapacityAndLargeOneIsReleased) {
    boost::beast::flat_buffer small;
    small.prepare(4096);
    const auto capacity = small.capacity();
    AVEVA::Private::TrimIdleBuffer(small);
    EXPECT_EQ(small.capacity(), capacity);

    boost::beast::flat_buffer large;
    large.prepare(AVEVA::Private::MaxRetainedBufferCapacity * 4);
    AVEVA::Private::TrimIdleBuffer(large);
    EXPECT_LT(large.capacity(), AVEVA::Private::MaxRetainedBufferCapacity);
}
} // namespace
