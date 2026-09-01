// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    // Outcome of CheckAndRecord() — tells the caller how to handle the message.
    enum class RateDecision
    {
        Allow,           // emit the message as normal
        Suppress,        // drop the message entirely
        AllowWithSummary // cooldown just expired: emit a suppression summary, then the message
    };

    // Result type returned by CheckAndRecord().
    struct RateCheckResult
    {
        RateDecision decision;
        uint32_t suppressedCount; // valid only when decision == AllowWithSummary; 0 otherwise
    };

    // Thread-safe rate limiter for log messages that match a set of known noisy prefixes.
    //
    // The first matching message in a cooldown window is allowed through.  All subsequent
    // matching messages within the same window are suppressed and counted.  The first
    // message after the window expires carries the suppression count so the caller can
    // prepend a summary line.
    //
    // Non-matching messages are never rate-limited.
    class LogRateLimiter
    {
    public:
        // Constructs a limiter with the given patterns and cooldown window.
        // <param name="patterns">  Substrings to search for in the log format string.</param>
        // <param name="cooldown">  Minimum interval between two allowed emissions of a matched pattern. Defaults to 30 seconds. </param>
        explicit LogRateLimiter(std::vector<std::string> patterns, std::chrono::seconds cooldown = std::chrono::seconds{30});

        // Returns the configured cooldown window.
        std::chrono::seconds Cooldown() const { return m_cooldown; }

        // Inspects the format string and decides whether to allow or suppress the message.
        // Safe to call from multiple threads concurrently.
        // <param name="format">  The printf-style format string passed to Logv (may be null). </param>
        RateCheckResult CheckAndRecord(const char* format);

    private:
        struct PatternState
        {
            std::optional<std::chrono::steady_clock::time_point> lastEmitted;
            uint32_t suppressedCount{0};
        };

        const std::chrono::seconds m_cooldown;
        std::vector<std::string> m_patterns;
        std::mutex m_mutex;
        // Key is a string_view into m_patterns (stable: vector is never modified after construction).
        std::unordered_map<std::string_view, PatternState> m_state;
    };
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
