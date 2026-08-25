// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

using AVEVA::RocksDB::Plugin::Azure::Impl::LogRateLimiter;
using AVEVA::RocksDB::Plugin::Azure::Impl::RateCheckResult;
using AVEVA::RocksDB::Plugin::Azure::Impl::RateDecision;

// All tests use a 1-second cooldown so they can exercise window expiry without
// requiring real clock manipulation (steady_clock is not mockable here, so
// expiry tests sleep for just over 1 s via the helper below).
namespace {
constexpr auto kCooldown = std::chrono::seconds{1};
constexpr const char* kStallingWritesFormat = "Stalling writes because we have 5 level-0 files";
constexpr const char* kStoppingWritesFormat = "Stopping writes because we have 20 level-0 files";
constexpr const char* kBlobNotFoundErrorCode = "BlobNotFound";
constexpr const char* kOtherFormat = "Compaction started for column family [default]";
} // namespace

// ─── Non-matching patterns ────────────────────────────────────────────────────

TEST(LogRateLimiterTests, NonMatchingFormat_AlwaysAllowed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};

    // Act / Assert — non-matching messages are always Allow, regardless of how
    // many times they are called.
    for (int i = 0; i < 10; ++i)
    {
        const auto result = limiter.CheckAndRecord(kOtherFormat);
        EXPECT_EQ(RateDecision::Allow, result.decision);
        EXPECT_EQ(0u, result.suppressedCount);
    }
}

TEST(LogRateLimiterTests, NullFormat_AlwaysAllowed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};

    // Act
    const auto result = limiter.CheckAndRecord(nullptr);

    // Assert
    EXPECT_EQ(RateDecision::Allow, result.decision);
    EXPECT_EQ(0u, result.suppressedCount);
}

// ─── First occurrence ─────────────────────────────────────────────────────────

TEST(LogRateLimiterTests, MatchingFormat_FirstCall_Allowed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};

    // Act
    const auto result = limiter.CheckAndRecord(kStallingWritesFormat);

    // Assert
    EXPECT_EQ(RateDecision::Allow, result.decision);
    EXPECT_EQ(0u, result.suppressedCount);
}

// ─── Within-window suppression ───────────────────────────────────────────────

TEST(LogRateLimiterTests, MatchingFormat_SecondCallWithinWindow_Suppressed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};
    limiter.CheckAndRecord(kStallingWritesFormat); // first — opens the window

    // Act
    const auto result = limiter.CheckAndRecord(kStallingWritesFormat);

    // Assert
    EXPECT_EQ(RateDecision::Suppress, result.decision);
    EXPECT_EQ(0u, result.suppressedCount);
}

TEST(LogRateLimiterTests, MatchingFormat_MultipleCallsWithinWindow_AllSuppressed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};
    limiter.CheckAndRecord(kStallingWritesFormat); // first — opens the window

    // Act / Assert
    for (int i = 0; i < 5; ++i)
    {
        const auto result = limiter.CheckAndRecord(kStallingWritesFormat);
        EXPECT_EQ(RateDecision::Suppress, result.decision);
    }
}

// ─── Window expiry ───────────────────────────────────────────────────────────

TEST(LogRateLimiterTests, MatchingFormat_AfterCooldown_WithSuppressed_AllowWithSummary)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};
    limiter.CheckAndRecord(kStallingWritesFormat);        // Allow — opens window
    limiter.CheckAndRecord(kStallingWritesFormat);        // Suppress (count = 1)
    limiter.CheckAndRecord(kStallingWritesFormat);        // Suppress (count = 2)

    std::this_thread::sleep_for(kCooldown + std::chrono::milliseconds{50});

    // Act
    const auto result = limiter.CheckAndRecord(kStallingWritesFormat);

    // Assert
    EXPECT_EQ(RateDecision::AllowWithSummary, result.decision);
    EXPECT_EQ(2u, result.suppressedCount);
}

TEST(LogRateLimiterTests, MatchingFormat_AfterCooldown_NoSuppressed_Allow)
{
    // Arrange — open window then let it expire with no intervening suppressions
    LogRateLimiter limiter{kCooldown};
    limiter.CheckAndRecord(kStallingWritesFormat);

    std::this_thread::sleep_for(kCooldown + std::chrono::milliseconds{50});

    // Act
    const auto result = limiter.CheckAndRecord(kStallingWritesFormat);

    // Assert — no suppressions occurred, so no summary is needed
    EXPECT_EQ(RateDecision::Allow, result.decision);
    EXPECT_EQ(0u, result.suppressedCount);
}

// ─── Cooldown accessor ───────────────────────────────────────────────────────

TEST(LogRateLimiterTests, Cooldown_ReturnsConfiguredValue)
{
    // Arrange
    const auto expected = std::chrono::seconds{42};
    LogRateLimiter limiter{expected};

    // Act / Assert
    EXPECT_EQ(expected, limiter.Cooldown());
}

// ─── Stopping writes pattern ─────────────────────────────────────────────────

TEST(LogRateLimiterTests, StoppingWritesFormat_FirstCall_Allowed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};

    // Act
    const auto result = limiter.CheckAndRecord(kStoppingWritesFormat);

    // Assert
    EXPECT_EQ(RateDecision::Allow, result.decision);
    EXPECT_EQ(0u, result.suppressedCount);
}

TEST(LogRateLimiterTests, StoppingWritesFormat_SecondCallWithinWindow_Suppressed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};
    limiter.CheckAndRecord(kStoppingWritesFormat); // first — opens the window

    // Act
    const auto result = limiter.CheckAndRecord(kStoppingWritesFormat);

    // Assert
    EXPECT_EQ(RateDecision::Suppress, result.decision);
}

// ─── Independent patterns ────────────────────────────────────────────────────

TEST(LogRateLimiterTests, DifferentPatterns_TrackedIndependently)
{
    // Arrange — stalling and stopping writes are separate patterns and must each
    // have their own independent cooldown bucket.
    LogRateLimiter limiter{kCooldown};

    // Open the stalling-writes window.
    const auto firstStalling = limiter.CheckAndRecord(kStallingWritesFormat);
    EXPECT_EQ(RateDecision::Allow, firstStalling.decision);

    // Stopping-writes window is independent — first call must still be allowed.
    const auto firstStopping = limiter.CheckAndRecord(kStoppingWritesFormat);
    EXPECT_EQ(RateDecision::Allow, firstStopping.decision);

    // Both are now within their own windows and should suppress.
    const auto secondStalling = limiter.CheckAndRecord(kStallingWritesFormat);
    EXPECT_EQ(RateDecision::Suppress, secondStalling.decision);

    const auto secondStopping = limiter.CheckAndRecord(kStoppingWritesFormat);
    EXPECT_EQ(RateDecision::Suppress, secondStopping.decision);
}

// ─── BlobNotFound pattern ─────────────────────────────────────────────────────

TEST(LogRateLimiterTests, BlobNotFound_FirstCall_Allowed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};

    // Act
    const auto result = limiter.CheckAndRecord(kBlobNotFoundErrorCode);

    // Assert — first occurrence must always be emitted
    EXPECT_EQ(RateDecision::Allow, result.decision);
    EXPECT_EQ(0u, result.suppressedCount);
}

TEST(LogRateLimiterTests, BlobNotFound_SecondCallWithinWindow_Suppressed)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};
    limiter.CheckAndRecord(kBlobNotFoundErrorCode); // first — opens the window

    // Act
    const auto result = limiter.CheckAndRecord(kBlobNotFoundErrorCode);

    // Assert — repeated 404s within the cooldown window must be dropped
    EXPECT_EQ(RateDecision::Suppress, result.decision);
}

TEST(LogRateLimiterTests, BlobNotFound_AfterCooldown_WithSuppressed_AllowWithSummary)
{
    // Arrange
    LogRateLimiter limiter{kCooldown};
    limiter.CheckAndRecord(kBlobNotFoundErrorCode); // Allow — opens window
    limiter.CheckAndRecord(kBlobNotFoundErrorCode); // Suppress (count = 1)
    limiter.CheckAndRecord(kBlobNotFoundErrorCode); // Suppress (count = 2)

    std::this_thread::sleep_for(kCooldown + std::chrono::milliseconds{50});

    // Act
    const auto result = limiter.CheckAndRecord(kBlobNotFoundErrorCode);

    // Assert — first call after the window expires must report suppressed count
    EXPECT_EQ(RateDecision::AllowWithSummary, result.decision);
    EXPECT_EQ(2u, result.suppressedCount);
}
