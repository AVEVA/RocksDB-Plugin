// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "SecondaryCache/ShardedIndex.hpp"

#include <gtest/gtest.h>

namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;

TEST(SecondaryCacheShardedIndexTests, UpsertFindTouchAndRemoveWork) {
    Secondary::ShardedIndex index(8);
    Secondary::Location location{1, 2, 3, 4, 0, 0};
    EXPECT_FALSE(index.Upsert(11, location).has_value());

    Secondary::Location found{};
    ASSERT_TRUE(index.FindAndTouch(11, found));
    EXPECT_EQ(found.regionId, 1u);
    EXPECT_EQ(index.LiveEntries(), 1u);

    const auto removed = index.Remove(11);
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed->generation, 4u);
    EXPECT_EQ(index.LiveEntries(), 0u);
}

TEST(SecondaryCacheShardedIndexTests, RemoveIfInRegionHonorsGeneration) {
    Secondary::ShardedIndex index(4);
    ASSERT_FALSE(index.Upsert(7, Secondary::Location{2, 3, 4, 5, 0, 0}).has_value());

    EXPECT_FALSE(index.RemoveIfInRegion(7, 2, 4).has_value());
    EXPECT_TRUE(index.RemoveIfInRegion(7, 2, 5).has_value());
    EXPECT_EQ(index.LiveEntries(), 0u);
}

TEST(SecondaryCacheShardedIndexTests, UpsertReturnsReplacedLocation) {
    Secondary::ShardedIndex index(4);
    ASSERT_FALSE(index.Upsert(9, Secondary::Location{1, 1, 1, 1, 0, 0}).has_value());
    const auto replaced = index.Upsert(9, Secondary::Location{2, 2, 2, 2, 0, 0});
    ASSERT_TRUE(replaced.has_value());
    EXPECT_EQ(replaced->regionId, 1u);
    EXPECT_EQ(index.LiveEntries(), 1u);
}

TEST(SecondaryCacheShardedIndexTests, RemoveIfMatchesOnlyRemovesIdenticalLocation) {
    Secondary::ShardedIndex index(4);
    const Secondary::Location oldLoc{1, 1, 1, 1, 0, 0};
    const Secondary::Location newLoc{2, 2, 2, 2, 0, 0};
    (void)index.Upsert(5, oldLoc);
    (void)index.Upsert(5, newLoc);

    EXPECT_FALSE(index.RemoveIfMatches(5, oldLoc).has_value());
    EXPECT_EQ(index.LiveEntries(), 1u);
    EXPECT_TRUE(index.RemoveIfMatches(5, newLoc).has_value());
    EXPECT_EQ(index.LiveEntries(), 0u);
}

TEST(SecondaryCacheShardedIndexTests, ClearRemovesEverything) {
    Secondary::ShardedIndex index(4);
    for (uint64_t i = 0; i < 20; ++i) {
        (void)index.Upsert(i, Secondary::Location{1, 1, 1, 1, 0, 0});
    }
    index.Clear();
    EXPECT_EQ(index.LiveEntries(), 0u);
}
