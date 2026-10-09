// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "private/DnsCache.hpp"

#include <boost/asio/io_context.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace
{
    using AVEVA::Private::BasicDnsCache;
    using AVEVA::Private::Tcp;

    Tcp::resolver::results_type Loopback(boost::asio::io_context& io)
    {
        Tcp::resolver resolver(io);
        return resolver.resolve("127.0.0.1", "80");
    }
} // namespace

TEST(DnsCacheTests, ReturnsAStoredResultUntilTheTtlElapses)
{
    boost::asio::io_context io;
    const auto results = Loopback(io);
    BasicDnsCache<> cache(std::chrono::seconds{1});

    EXPECT_FALSE(cache.Find("host", "80").has_value());
    cache.Store("host", "80", results);
    const auto hit = cache.Find("host", "80");
    ASSERT_TRUE(hit.has_value());
    EXPECT_EQ(hit->begin()->endpoint(), results.begin()->endpoint());

    EXPECT_FALSE(cache.Find("host", "81").has_value());
    EXPECT_FALSE(cache.Find("other", "80").has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds{1100});
    EXPECT_FALSE(cache.Find("host", "80").has_value());
}

TEST(DnsCacheTests, ConcurrentLookupsForOneOriginShareASingleLeader)
{
    boost::asio::io_context io;
    const auto results = Loopback(io);
    BasicDnsCache<> cache;

    BasicDnsCache<>::Waiter leader = [](boost::system::error_code, Tcp::resolver::results_type) {};
    ASSERT_TRUE(cache.JoinOrLead("host", "80", leader));

    int delivered = 0;
    boost::system::error_code seenError = boost::asio::error::operation_aborted;
    for (int i = 0; i < 3; ++i)
    {
        BasicDnsCache<>::Waiter waiter = [&](boost::system::error_code error, Tcp::resolver::results_type)
        {
            ++delivered;
            seenError = error;
        };
        EXPECT_FALSE(cache.JoinOrLead("host", "80", waiter));
    }
    BasicDnsCache<>::Waiter other = [](boost::system::error_code, Tcp::resolver::results_type) {};
    EXPECT_TRUE(cache.JoinOrLead("host", "81", other));

    cache.Finish("host", "80", {}, results);
    EXPECT_EQ(delivered, 3);
    EXPECT_FALSE(seenError);
    EXPECT_TRUE(cache.Find("host", "80").has_value());

    // The key is free again once the leader has finished.
    BasicDnsCache<>::Waiter next = [](boost::system::error_code, Tcp::resolver::results_type) {};
    EXPECT_TRUE(cache.JoinOrLead("host", "80", next));
    cache.Finish("host", "80", boost::asio::error::host_not_found, {});
}

TEST(DnsCacheTests, EmptyResultsAreNotCached)
{
    BasicDnsCache<> cache;
    cache.Store("host", "80", Tcp::resolver::results_type{});
    EXPECT_FALSE(cache.Find("host", "80").has_value());
}
