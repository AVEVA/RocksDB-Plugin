// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "SecondaryCache/Index.hpp"
#include <gtest/gtest.h>

namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;

TEST(SecondaryCacheIndexTests, ReplacementReturnsOldLocationAndClearReleasesAllocation) {
    Secondary::MemoryBudget budget{1024 * 1024};
    Secondary::Index index(budget);
    const Secondary::Location first{1, 2, 3, 4};
    const Secondary::Location second{2, 3, 4, 5};
    EXPECT_FALSE(index.Upsert(11, first));
    ASSERT_EQ(index.Upsert(11, second), first);
    EXPECT_EQ(index.Find(11), second);
    EXPECT_EQ(index.Size(), 1u);
    EXPECT_GT(budget.used.load(), 0u);
    EXPECT_EQ(index.Remove(11), second);
    index.Clear();
    EXPECT_EQ(budget.used.load(), 0u);
}

TEST(SecondaryCacheIndexTests, AllocationFailureThrowsRatherThanTerminates) {
    Secondary::MemoryBudget budget{0};
    Secondary::Index index(budget);
    EXPECT_THROW(index.Upsert(1, {}), Secondary::BudgetExceeded);
    EXPECT_EQ(index.Size(), 0u);
    EXPECT_EQ(budget.used.load(), 0u);
}
