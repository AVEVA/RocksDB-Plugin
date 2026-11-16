// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClientOptions.hpp"
#include "AVEVA/HttpClient/HttpRequestOptions.hpp"
#include "AVEVA/HttpClient/RetryOptions.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>

namespace {
using namespace AVEVA;

TEST(HttpRequestRetryOptions, AreEmptyByDefaultAndRoundTrip) {
    HttpRequestOptions options;
    EXPECT_FALSE(options.GetRetryOptions().has_value());

    RetryOptions retry;
    retry.MaxRetries = RetryCount{5};
    retry.InitialDelay = std::chrono::milliseconds{10};
    retry.MaxDelay = std::chrono::milliseconds{20};
    options.SetRetryOptions(retry);

    const HttpRequestOptions copy = options;
    ASSERT_TRUE(copy.GetRetryOptions().has_value());
    EXPECT_EQ(copy.GetRetryOptions()->MaxRetries, RetryCount{5});
    EXPECT_EQ(copy.GetRetryOptions()->InitialDelay, std::chrono::milliseconds{10});
    EXPECT_EQ(copy.GetRetryOptions()->MaxDelay, std::chrono::milliseconds{20});

    options.ClearRetryOptions();
    EXPECT_FALSE(options.GetRetryOptions().has_value());
    EXPECT_TRUE(copy.GetRetryOptions().has_value());
}

struct ZeroGenerator {
    using result_type = std::uint64_t;
    static constexpr result_type min() { return 0; }
    static constexpr result_type max() { return std::numeric_limits<std::uint64_t>::max(); }
    result_type operator()() const { return 0; }
};

TEST(HttpRequestRetryOptions, BackoffDoublesWithJitterAndIsCapped) {
    RetryOptions retry;
    retry.InitialDelay = std::chrono::milliseconds{100};
    retry.MaxDelay = std::chrono::milliseconds{250};
    ZeroGenerator generator;

    EXPECT_EQ(ComputeBackoff(retry, 1, generator), std::chrono::milliseconds{80});
    EXPECT_EQ(ComputeBackoff(retry, 2, generator), std::chrono::milliseconds{160});
    EXPECT_EQ(ComputeBackoff(retry, 3, generator), std::chrono::milliseconds{250});
}

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
