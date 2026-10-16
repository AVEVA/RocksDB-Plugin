// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

using namespace AVEVA::RocksDB::Plugin::Azure::Impl;

namespace {
constexpr int64_t Max = std::numeric_limits<int64_t>::max();
constexpr uint64_t UMax = std::numeric_limits<uint64_t>::max();
} // namespace

TEST(RangeCheckTests, Unsigned_RangeEndingExactlyAtInt64Max_IsValid) {
    EXPECT_TRUE(RangeCheck::IsValid(uint64_t{0}, static_cast<uint64_t>(Max)));
    EXPECT_TRUE(RangeCheck::IsValid(static_cast<uint64_t>(Max), uint64_t{0}));
    EXPECT_TRUE(RangeCheck::IsValid(static_cast<uint64_t>(Max) - 10, uint64_t{10}));
}

TEST(RangeCheckTests, Unsigned_OffsetPlusLengthOverflowing_IsInvalid) {
    EXPECT_FALSE(RangeCheck::IsValid(static_cast<uint64_t>(Max) - 10, uint64_t{11}));
    EXPECT_FALSE(RangeCheck::IsValid(static_cast<uint64_t>(Max), uint64_t{1}));
    EXPECT_FALSE(RangeCheck::IsValid(uint64_t{1}, static_cast<uint64_t>(Max)));
    EXPECT_FALSE(RangeCheck::IsValid(UMax, uint64_t{0}));
    EXPECT_FALSE(RangeCheck::IsValid(uint64_t{0}, UMax));
}

TEST(RangeCheckTests, Signed_NegativeOrOverflowingRange_IsInvalid) {
    EXPECT_TRUE(RangeCheck::IsValid(int64_t{0}, Max));
    EXPECT_TRUE(RangeCheck::IsValid(Max - 5, int64_t{5}));
    EXPECT_FALSE(RangeCheck::IsValid(Max - 5, int64_t{6}));
    EXPECT_FALSE(RangeCheck::IsValid(int64_t{-1}, int64_t{1}));
    EXPECT_FALSE(RangeCheck::IsValid(int64_t{1}, int64_t{-1}));
}

TEST(RangeCheckTests, ThrowIfInvalid_InvalidRange_ThrowsInvalidArgument) {
    EXPECT_NO_THROW(RangeCheck::ThrowIfInvalid(0, 10, "Test"));
    EXPECT_THROW(RangeCheck::ThrowIfInvalid(Max, 1, "Test"), std::invalid_argument);
}
