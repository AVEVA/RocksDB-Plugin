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

TEST(DnsCacheTests, EmptyResultsAreNotCached)
{
    BasicDnsCache<> cache;
    cache.Store("host", "80", Tcp::resolver::results_type{});
    EXPECT_FALSE(cache.Find("host", "80").has_value());
}
