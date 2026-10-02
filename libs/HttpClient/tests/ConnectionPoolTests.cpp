#include "private/ConnectionPool.hpp"
#include "private/StreamTypes.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

namespace
{
    using AVEVA::Private::BasicConnectionKey;
    using AVEVA::Private::ConnectionKey;
    using AVEVA::Private::ConnectionPool;
    using AVEVA::Private::PooledConnection;
    using AVEVA::Private::TlsConnectionKey;

    // BasicConnectionKey and TlsConnectionKey must remain distinct, non-interchangeable types:
    // a key for one connection pool must never compile against the other pool's Acquire/Release.
    static_assert(!std::is_same_v<BasicConnectionKey, TlsConnectionKey>,
        "Basic and TLS connection keys must be distinct types");
    static_assert(!std::is_constructible_v<TlsConnectionKey, BasicConnectionKey>,
        "A basic connection key must not be usable as a TLS connection key");

    // The pool never touches the stream itself, so a tracked stand-in is enough to observe both
    // identity (which connection came back) and lifetime (when a connection was closed).
    struct TrackedStream
    {
        TrackedStream(int identity, int& liveCount) : identity(identity), liveCount(&liveCount)
        {
            ++*this->liveCount;
        }

        TrackedStream(const TrackedStream&) = delete;
        TrackedStream& operator=(const TrackedStream&) = delete;

        ~TrackedStream()
        {
            --*liveCount;
        }

        int identity;
        int* liveCount;
    };

    using Pool = ConnectionPool<TrackedStream>;
    using Key = ConnectionKey<TrackedStream>;

    PooledConnection<TrackedStream> MakeConnection(int identity, int& liveCount)
    {
        return PooledConnection<TrackedStream>{std::make_unique<TrackedStream>(identity, liveCount), {}};
    }

    Key MakeKey(std::string host)
    {
        return Key{std::move(host), "80"};
    }

    constexpr std::chrono::seconds LongTimeout{300};
    constexpr std::chrono::seconds ImmediateTimeout{0};

    TEST(ConnectionPool, AcquireOnEmptyPoolReturnsNothing)
    {
        Pool pool(4, LongTimeout);
        EXPECT_FALSE(pool.Acquire(MakeKey("example.com")).has_value());
    }

    TEST(ConnectionPool, ReleasedConnectionIsReusedForTheSameOrigin)
    {
        int live = 0;
        Pool pool(4, LongTimeout);
        pool.Release(MakeKey("example.com"), MakeConnection(1, live));

        auto acquired = pool.Acquire(MakeKey("example.com"));
        ASSERT_TRUE(acquired.has_value());
        EXPECT_EQ(acquired->stream->identity, 1);
        EXPECT_EQ(live, 1);

        EXPECT_FALSE(pool.Acquire(MakeKey("example.com")).has_value());
    }

    TEST(ConnectionPool, ConnectionsAreNotSharedAcrossOrigins)
    {
        int live = 0;
        Pool pool(4, LongTimeout);
        pool.Release(MakeKey("example.com"), MakeConnection(1, live));

        EXPECT_FALSE(pool.Acquire(MakeKey("other.com")).has_value());
        EXPECT_FALSE(pool.Acquire(Key{"example.com", "8080"}).has_value());
        EXPECT_TRUE(pool.Acquire(MakeKey("example.com")).has_value());
    }

    TEST(ConnectionPool, AcquireReturnsTheMostRecentlyReleasedConnection)
    {
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

    TEST(ConnectionPool, ExceedingTheIdleLimitEvictsTheOldestConnection)
    {
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

    TEST(ConnectionPool, ZeroIdleLimitDisablesPooling)
    {
        int live = 0;
        Pool pool(0, LongTimeout);
        const auto key = MakeKey("example.com");
        pool.Release(key, MakeConnection(1, live));

        EXPECT_EQ(live, 0);
        EXPECT_FALSE(pool.Acquire(key).has_value());
    }

    TEST(ConnectionPool, ExpiredConnectionsAreClosedInsteadOfReused)
    {
        int live = 0;
        Pool pool(4, ImmediateTimeout);
        const auto key = MakeKey("example.com");
        pool.Release(key, MakeConnection(1, live));
        ASSERT_EQ(live, 1);

        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        EXPECT_FALSE(pool.Acquire(key).has_value());
        EXPECT_EQ(live, 0);
    }

    TEST(ConnectionPool, ExpiredConnectionsOfOtherOriginsAreReclaimed)
    {
        int live = 0;
        Pool pool(4, ImmediateTimeout);
        pool.Release(MakeKey("stale.com"), MakeConnection(1, live));
        ASSERT_EQ(live, 1);

        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        // Touching an unrelated origin still sweeps the pool-wide LRU list.
        pool.Release(MakeKey("fresh.com"), MakeConnection(2, live));
        EXPECT_EQ(live, 1);

        EXPECT_FALSE(pool.Acquire(MakeKey("stale.com")).has_value());
    }

    TEST(ConnectionPool, DestroyingThePoolClosesIdleConnections)
    {
        int live = 0;
        {
            Pool pool(4, LongTimeout);
            pool.Release(MakeKey("example.com"), MakeConnection(1, live));
            pool.Release(MakeKey("other.com"), MakeConnection(2, live));
            ASSERT_EQ(live, 2);
        }
        EXPECT_EQ(live, 0);
    }

    TEST(ConnectionPool, RepeatedAcquireReleaseCyclesStayConsistent)
    {
        int live = 0;
        Pool pool(2, LongTimeout);
        const auto first = MakeKey("first.com");
        const auto second = MakeKey("second.com");

        for (int cycle = 0; cycle < 200; ++cycle)
        {
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

    TEST(ConnectionPool, ReleasingAnEmptyConnectionIsIgnored)
    {
        Pool pool(4, LongTimeout);
        const auto key = MakeKey("example.com");
        pool.Release(key, PooledConnection<TrackedStream>{});
        EXPECT_FALSE(pool.Acquire(key).has_value());
    }
} // namespace
