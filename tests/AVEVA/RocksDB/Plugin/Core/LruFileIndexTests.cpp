// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "LruFileIndex.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <new>
#include <string>
#include <vector>

using AVEVA::RocksDB::Plugin::Core::LruFileIndex;

// A pin taken on an entry that is later replaced must not leave the replacement pinned (or underflow its count).
TEST(LruFileIndexTests, PinOnAReplacedEntryDoesNotPinItsSuccessor) {
    LruFileIndex index("cache", 100);
    (void)index.RegisterEntry("k", 40);

    {
        auto pin = index.TryPin("k");
        ASSERT_TRUE(pin.has_value());
        (void)index.RegisterEntry("k", 40);
    }

    const auto evicted = index.SetCapacity(0);
    EXPECT_EQ(1U, evicted.size());
    EXPECT_EQ(0U, index.GetUsage());
}

TEST(LruFileIndexTests, PinnedEntryIsNotEvictedUntilThePinIsReleased) {
    LruFileIndex index("cache", 100);
    (void)index.RegisterEntry("k", 40);

    auto pin = index.TryPin("k");
    ASSERT_TRUE(pin.has_value());
    EXPECT_TRUE(index.SetCapacity(0).empty());
    EXPECT_EQ(40U, index.GetUsage());

    pin.reset();
    EXPECT_EQ(1U, index.SetCapacity(0).size());
    EXPECT_EQ(0U, index.GetUsage());
}

// Reserving room for a replacement evicts the other entries and never the key being replaced, and leaves that key
// evictable afterwards (the reservation's pin is released).
TEST(LruFileIndexTests, ReserveCapacity_EvictsOthersNotTheReservedKeyAndReleasesItsPin) {
    LruFileIndex index("cache", 100);
    (void)index.RegisterEntry("a", 60);
    (void)index.RegisterEntry("b", 30);

    const auto evicted = index.ReserveCapacity("a", 95, true);

    ASSERT_TRUE(evicted.has_value());
    ASSERT_EQ(1U, evicted->size());
    EXPECT_NE(std::string::npos, evicted->front().first.find('b'));
    EXPECT_EQ(60U, index.GetUsage());
    EXPECT_EQ(1U, index.SetCapacity(0).size());
}

namespace {
// Fails the Nth allocation of at least kMinFailedAllocation bytes after arming (1-based); 0 means disarmed. Small
// allocations (e.g. debug-STL iterator proxies) are skipped because they occur in noexcept contexts.
constexpr size_t kMinFailedAllocation = 64;
} // namespace
// Shared with the other tests of this executable, which see the same replaced operator new.
thread_local size_t g_failAllocationIn = 0;

void* operator new(const size_t size) {
    if (size >= kMinFailedAllocation && g_failAllocationIn != 0 && --g_failAllocationIn == 0)
        throw std::bad_alloc();
    if (void* p = std::malloc(size ? size : 1))
        return p;
    throw std::bad_alloc();
}

void operator delete(void* p) noexcept { std::free(p); }

void operator delete(void* p, size_t) noexcept { std::free(p); }

// A reservation that throws (while reserving the eviction list or building eviction paths) must release its
// temporary pin and size discount, so the replaced key stays accounted for and evictable.
TEST(LruFileIndexTests, ReserveCapacity_ThrowingEvictionReleasesPinAndSizeDiscount) {
    const std::string longDir(64, 'd');
    for (size_t failAt = 1; failAt <= 3; ++failAt) {
        LruFileIndex index(longDir, 100);
        (void)index.RegisterEntry("a", 60);
        (void)index.RegisterEntry("b", 30);

        g_failAllocationIn = failAt;
        EXPECT_THROW((void)index.ReserveCapacity("a", 95, true), std::bad_alloc) << "failAt=" << failAt;
        g_failAllocationIn = 0;

        EXPECT_EQ(90U, index.GetUsage()) << "failAt=" << failAt;
        EXPECT_EQ(2U, index.SetCapacity(0).size()) << "failAt=" << failAt;
        EXPECT_EQ(0U, index.GetUsage()) << "failAt=" << failAt;
    }
}

// A throw while registering (list node or index growth) must leave usage, lookup, touch, remove and shrink consistent.
TEST(LruFileIndexTests, RegisterEntry_ThrowLeavesIndexConsistent) {
    for (size_t existing = 0; existing <= 40; ++existing) {
        for (size_t failAt = 1; failAt <= 4; ++failAt) {
            LruFileIndex index("cache", 1'000'000);
            for (size_t i = 0; i < existing; ++i)
                (void)index.RegisterEntry("e" + std::to_string(i), 10);

            g_failAllocationIn = failAt;
            bool threw = false;
            try {
                (void)index.RegisterEntry("new", 7);
            } catch (const std::bad_alloc&) {
                threw = true;
            }
            g_failAllocationIn = 0;

            const size_t expected = existing * 10 + (threw ? 0 : 7);
            ASSERT_EQ(expected, index.GetUsage()) << existing << "/" << failAt;
            EXPECT_EQ(!threw, index.Touch("new"));
            EXPECT_EQ(!threw, index.TryPin("new").has_value());
            EXPECT_EQ(threw, index.Remove("new").first.empty());
            ASSERT_EQ(existing * 10, index.GetUsage());
            EXPECT_EQ(existing, index.SetCapacity(0).size());
            EXPECT_EQ(0U, index.GetUsage());
        }
    }
}

// If building the path of a later victim throws, no entry may be half-removed: earlier victims stay tracked so a
// retry still returns every file to retire.
TEST(LruFileIndexTests, SetCapacity_ThrowWhileBuildingLaterVictimKeepsAllEntriesTracked) {
    const std::string longDir(64, 'd');
    for (size_t failAt = 1; failAt <= 8; ++failAt) {
        LruFileIndex index(longDir, 1000);
        (void)index.RegisterEntry("a", 10);
        (void)index.RegisterEntry("b", 10);
        (void)index.RegisterEntry("c", 10);

        g_failAllocationIn = failAt;
        bool threw = false;
        LruFileIndex::EvictList first;
        try {
            first = index.SetCapacity(0);
        } catch (const std::bad_alloc&) {
            threw = true;
        }
        g_failAllocationIn = 0;

        if (threw) {
            ASSERT_EQ(30U, index.GetUsage()) << "failAt=" << failAt;
            first = index.SetCapacity(0);
        }
        EXPECT_EQ(3U, first.size()) << "failAt=" << failAt;
        EXPECT_EQ(0U, index.GetUsage()) << "failAt=" << failAt;
    }
}
// Pinned entries at the LRU tail must be skipped without disturbing eviction order of the unpinned ones.
TEST(LruFileIndexTests, EvictionSkipsPinnedTailAndKeepsLruOrder) {
    constexpr size_t kPinned = 3000;
    constexpr size_t kFree = 3000;
    LruFileIndex index("cache", kPinned + kFree);
    std::vector<LruFileIndex::ScopedPin> pins;
    for (size_t i = 0; i < kPinned; ++i) {
        const auto name = "p" + std::to_string(i);
        (void)index.RegisterEntry(name, 1);
        pins.push_back(*index.TryPin(name));
    }
    for (size_t i = 0; i < kFree; ++i)
        (void)index.RegisterEntry("f" + std::to_string(i), 1);

    const auto evicted = index.SetCapacity(kPinned);

    ASSERT_EQ(kFree, evicted.size());
    EXPECT_NE(std::string::npos, evicted.front().first.find("f0"));
    EXPECT_EQ(kPinned, index.GetUsage());
}
