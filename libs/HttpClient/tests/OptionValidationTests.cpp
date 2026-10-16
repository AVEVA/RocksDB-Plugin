// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClientOptions.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <stdexcept>

namespace {
using namespace AVEVA;

TEST(HttpOptionValidation, NegativeIdleTimeoutIsRejected) {
    HttpClientOptions options;
    const auto before = options.GetIdleConnectionTimeout();
    EXPECT_THROW(options.SetIdleConnectionTimeout(std::chrono::seconds{-1}), std::invalid_argument);
    EXPECT_EQ(options.GetIdleConnectionTimeout(), before);
}

TEST(HttpOptionValidation, ZeroIdleTimeoutIsAccepted) {
    HttpClientOptions options;
    options.SetIdleConnectionTimeout(std::chrono::seconds{0});
    EXPECT_EQ(options.GetIdleConnectionTimeout(), std::chrono::seconds{0});
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
