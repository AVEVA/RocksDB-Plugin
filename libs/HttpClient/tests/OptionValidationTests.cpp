// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <stdexcept>

namespace {
using namespace AVEVA;

TEST(HttpOptionValidation, NonPositiveTimeoutsAreRejectedWhenBuilt) {
    EXPECT_THROW(Timeout{std::chrono::seconds{-1}}, std::invalid_argument);
    EXPECT_THROW(Timeout{std::chrono::milliseconds{0}}, std::invalid_argument);
}

TEST(HttpOptionValidation, HugeTimeoutsAreClampedInsteadOfOverflowing) {
    EXPECT_EQ(Timeout{std::chrono::milliseconds::max()}, Timeout{Timeout::Max});
    EXPECT_EQ(Timeout{std::chrono::microseconds{1}}.Value(), std::chrono::milliseconds{1});
}

TEST(HttpOptionValidation, IdleTimeoutIsKept) {
    HttpClientOptions options;
    EXPECT_EQ(options.GetIdleConnectionTimeout(), Timeout{std::chrono::seconds{30}});
    options.SetIdleConnectionTimeout(Timeout{std::chrono::seconds{5}});
    EXPECT_EQ(options.GetIdleConnectionTimeout(), Timeout{std::chrono::seconds{5}});
}

TEST(HttpOptionValidation, IdleProbeThresholdDefaultsToFiftyMillisecondsAndRejectsNegatives) {
    HttpClientOptions options;
    EXPECT_EQ(options.GetIdleProbeThreshold(), std::chrono::milliseconds{50});
    options.SetIdleProbeThreshold(std::chrono::milliseconds{0});
    EXPECT_EQ(options.GetIdleProbeThreshold(), std::chrono::milliseconds{0});
    EXPECT_THROW(options.SetIdleProbeThreshold(std::chrono::milliseconds{-1}), std::invalid_argument);
    EXPECT_EQ(options.GetIdleProbeThreshold(), std::chrono::milliseconds{0});
}
} // namespace
