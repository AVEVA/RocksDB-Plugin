// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "LruFileIndex.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <new>
#include <string>
#include <vector>

using AVEVA::RocksDB::Plugin::Core::ByteCount;
using AVEVA::RocksDB::Plugin::Core::LruFileIndex;

// A pin taken on an entry that is later replaced must not leave the replacement pinned (or underflow its count).
TEST(LruFileIndexTests, PinOnAReplacedEntryDoesNotPinItsSuccessor) {
    LruFileIndex index("cache", ByteCount{100});
    (void)index.RegisterEntry("k", ByteCount{40});

    {
        auto pin = index.TryPin("k");
        ASSERT_TRUE(pin.has_value());
        (void)index.RegisterEntry("k", ByteCount{40});
    }

    const auto evicted = index.SetCapacity(ByteCount{});
    EXPECT_EQ(1U, evicted.size());
    EXPECT_EQ(0U, index.GetUsage().Value());
}

TEST(LruFileIndexTests, PinnedEntryIsNotEvictedUntilThePinIsReleased) {
    LruFileIndex index("cache", ByteCount{100});
    (void)index.RegisterEntry("k", ByteCount{40});

    auto pin = index.TryPin("k");
    ASSERT_TRUE(pin.has_value());
    EXPECT_TRUE(index.SetCapacity(ByteCount{}).empty());
    EXPECT_EQ(40U, index.GetUsage().Value());

    pin.reset();
    EXPECT_EQ(1U, index.SetCapacity(ByteCount{}).size());
    EXPECT_EQ(0U, index.GetUsage().Value());
}

// Reserving room for a replacement evicts the other entries and never the key being replaced, and leaves that key
// evictable afterwards (the reservation's pin is released).
TEST(LruFileIndexTests, ReserveCapacity_EvictsOthersNotTheReservedKeyAndReleasesItsPin) {
    LruFileIndex index("cache", ByteCount{100});
    (void)index.RegisterEntry("a", ByteCount{60});
    (void)index.RegisterEntry("b", ByteCount{30});

    const auto evicted = index.ReserveCapacity("a", ByteCount{95}, LruFileIndex::InsertMode::Force);

    ASSERT_TRUE(evicted.has_value());
    ASSERT_EQ(1U, evicted->size());
    EXPECT_NE(std::string::npos, evicted->front().OriginalPath.Value().find('b'));
    EXPECT_EQ(60U, index.GetUsage().Value());
    EXPECT_EQ(1U, index.SetCapacity(ByteCount{}).size());
}

namespace {
// Fails the Nth allocation of at least kMinFailedAllocation bytes after arming (1-based); 0 means disarmed. Small
// allocations (e.g. debug-STL iterator proxies) are skipped because they occur in noexcept contexts.
constexpr size_t kMinFailedAllocation = 64;
} // namespace
// Shared with the other tests of this executable, which see the same replaced operator new.
thread_local size_t g_failAllocationIn = 0;
// When non-zero, every allocation of at least this many bytes throws.
thread_local size_t g_failAllocationsAtLeast = 0;

void* operator new(const size_t size) {
    if (g_failAllocationsAtLeast != 0 && size >= g_failAllocationsAtLeast)
        throw std::bad_alloc();
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
        LruFileIndex index(longDir, ByteCount{100});
        (void)index.RegisterEntry("a", ByteCount{60});
        (void)index.RegisterEntry("b", ByteCount{30});

        g_failAllocationIn = failAt;
        EXPECT_THROW((void)index.ReserveCapacity("a", ByteCount{95}, LruFileIndex::InsertMode::Force), std::bad_alloc)
            << "failAt=" << failAt;
        g_failAllocationIn = 0;

        EXPECT_EQ(90U, index.GetUsage().Value()) << "failAt=" << failAt;
        EXPECT_EQ(2U, index.SetCapacity(ByteCount{}).size()) << "failAt=" << failAt;
        EXPECT_EQ(0U, index.GetUsage().Value()) << "failAt=" << failAt;
    }
}

// A throw while registering (list node or index growth) must leave usage, lookup, touch, remove and shrink consistent.
TEST(LruFileIndexTests, RegisterEntry_ThrowLeavesIndexConsistent) {
    for (size_t existing = 0; existing <= 40; ++existing) {
        for (size_t failAt = 1; failAt <= 4; ++failAt) {
            LruFileIndex index("cache", ByteCount{1'000'000});
            for (size_t i = 0; i < existing; ++i)
                (void)index.RegisterEntry("e" + std::to_string(i), ByteCount{10});

            g_failAllocationIn = failAt;
            bool threw = false;
            try {
                (void)index.RegisterEntry("new", ByteCount{7});
            } catch (const std::bad_alloc&) {
                threw = true;
            }
            g_failAllocationIn = 0;

            const size_t expected = existing * 10 + (threw ? 0 : 7);
            ASSERT_EQ(expected, index.GetUsage().Value()) << existing << "/" << failAt;
            EXPECT_EQ(threw ? LruFileIndex::TouchResult::NotFound : LruFileIndex::TouchResult::Touched,
                      index.Touch("new"));
            EXPECT_EQ(!threw, index.TryPin("new").has_value());
            EXPECT_EQ(threw, !index.Remove("new").has_value());
            ASSERT_EQ(existing * 10, index.GetUsage().Value());
            EXPECT_EQ(existing, index.SetCapacity(ByteCount{}).size());
            EXPECT_EQ(0U, index.GetUsage().Value());
        }
    }
}

// If building the path of a later victim throws, no entry may be half-removed: earlier victims stay tracked so a
// retry still returns every file to retire.
TEST(LruFileIndexTests, SetCapacity_ThrowWhileBuildingLaterVictimKeepsAllEntriesTracked) {
    const std::string longDir(64, 'd');
    for (size_t failAt = 1; failAt <= 8; ++failAt) {
        LruFileIndex index(longDir, ByteCount{1000});
        (void)index.RegisterEntry("a", ByteCount{10});
        (void)index.RegisterEntry("b", ByteCount{10});
        (void)index.RegisterEntry("c", ByteCount{10});

        g_failAllocationIn = failAt;
        bool threw = false;
        LruFileIndex::EvictList first;
        try {
            first = index.SetCapacity(ByteCount{});
        } catch (const std::bad_alloc&) {
            threw = true;
        }
        g_failAllocationIn = 0;

        if (threw) {
            ASSERT_EQ(30U, index.GetUsage().Value()) << "failAt=" << failAt;
            first = index.SetCapacity(ByteCount{});
        }
        EXPECT_EQ(3U, first.size()) << "failAt=" << failAt;
        EXPECT_EQ(0U, index.GetUsage().Value()) << "failAt=" << failAt;
    }
}
// Pinned entries at the LRU tail must be skipped without disturbing eviction order of the unpinned ones.
TEST(LruFileIndexTests, EvictionSkipsPinnedTailAndKeepsLruOrder) {
    constexpr size_t kPinned = 3000;
    constexpr size_t kFree = 3000;
    LruFileIndex index("cache", ByteCount{kPinned + kFree});
    std::vector<LruFileIndex::ScopedPin> pins;
    for (size_t i = 0; i < kPinned; ++i) {
        const auto name = "p" + std::to_string(i);
        (void)index.RegisterEntry(name, ByteCount{1});
        pins.push_back(*index.TryPin(name));
    }
    for (size_t i = 0; i < kFree; ++i)
        (void)index.RegisterEntry("f" + std::to_string(i), ByteCount{1});

    const auto evicted = index.SetCapacity(ByteCount{kPinned});

    ASSERT_EQ(kFree, evicted.size());
    EXPECT_NE(std::string::npos, evicted.front().OriginalPath.Value().find("f0"));
    EXPECT_EQ(kPinned, index.GetUsage().Value());
}

// Replacing one victim must not allocate storage proportional to the index size.
TEST(LruFileIndexTests, SingleVictimEvictionDoesNotAllocateInProportionToIndexSize) {
    constexpr size_t kEntries = 20000;
    LruFileIndex index("cache", ByteCount{kEntries});
    for (size_t i = 0; i < kEntries; ++i)
        (void)index.RegisterEntry("e" + std::to_string(i), ByteCount{1});

    g_failAllocationsAtLeast = 4096;
    LruFileIndex::EvictList evicted;
    try {
        evicted = index.RegisterEntry("new", ByteCount{1});
    } catch (const std::bad_alloc&) {
        g_failAllocationsAtLeast = 0;
        FAIL() << "a single-victim eviction allocated a large block";
    }
    g_failAllocationsAtLeast = 0;

    ASSERT_EQ(1U, evicted.size());
    EXPECT_NE(std::string::npos, evicted.front().OriginalPath.Value().find("e0"));
    EXPECT_EQ(kEntries, index.GetUsage().Value());
}