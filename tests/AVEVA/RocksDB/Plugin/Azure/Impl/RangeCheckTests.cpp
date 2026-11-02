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

using RangeCheck::Bounds;

TEST(RangeCheckTests, Unsigned_RangeEndingExactlyAtInt64Max_IsValid) {
    EXPECT_EQ(RangeCheck::Check(uint64_t{0}, static_cast<uint64_t>(Max)), Bounds::InRange);
    EXPECT_EQ(RangeCheck::Check(static_cast<uint64_t>(Max), uint64_t{0}), Bounds::InRange);
    EXPECT_EQ(RangeCheck::Check(static_cast<uint64_t>(Max) - 10, uint64_t{10}), Bounds::InRange);
}

TEST(RangeCheckTests, Unsigned_OffsetPlusLengthOverflowing_IsInvalid) {
    EXPECT_EQ(RangeCheck::Check(static_cast<uint64_t>(Max) - 10, uint64_t{11}), Bounds::OutOfRange);
    EXPECT_EQ(RangeCheck::Check(static_cast<uint64_t>(Max), uint64_t{1}), Bounds::OutOfRange);
    EXPECT_EQ(RangeCheck::Check(uint64_t{1}, static_cast<uint64_t>(Max)), Bounds::OutOfRange);
    EXPECT_EQ(RangeCheck::Check(UMax, uint64_t{0}), Bounds::OutOfRange);
    EXPECT_EQ(RangeCheck::Check(uint64_t{0}, UMax), Bounds::OutOfRange);
}

TEST(RangeCheckTests, Signed_NegativeOrOverflowingRange_IsInvalid) {
    EXPECT_EQ(RangeCheck::Check(int64_t{0}, Max), Bounds::InRange);
    EXPECT_EQ(RangeCheck::Check(Max - 5, int64_t{5}), Bounds::InRange);
    EXPECT_EQ(RangeCheck::Check(Max - 5, int64_t{6}), Bounds::OutOfRange);
    EXPECT_EQ(RangeCheck::Check(int64_t{-1}, int64_t{1}), Bounds::OutOfRange);
    EXPECT_EQ(RangeCheck::Check(int64_t{1}, int64_t{-1}), Bounds::OutOfRange);
}

TEST(RangeCheckTests, ThrowIfInvalid_InvalidRange_ThrowsInvalidArgument) {
    EXPECT_NO_THROW(RangeCheck::ThrowIfInvalid(0, 10, "Test"));
    EXPECT_THROW(RangeCheck::ThrowIfInvalid(Max, 1, "Test"), std::invalid_argument);
}
