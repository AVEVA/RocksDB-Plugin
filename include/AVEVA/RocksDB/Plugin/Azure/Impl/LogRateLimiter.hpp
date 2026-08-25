// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string_view>
#include <unordered_map>

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
        // Constructs a limiter with the given cooldown window.
        // @param cooldown  Minimum interval between two allowed emissions of the same
        //                  matched prefix.  Defaults to 30 seconds.
        explicit LogRateLimiter(std::chrono::seconds cooldown = std::chrono::seconds{30});

        // Returns the configured cooldown window.
        std::chrono::seconds Cooldown() const { return m_cooldown; }

        // Inspects the format string and decides whether to allow or suppress the message.
        // Safe to call from multiple threads concurrently.
        // @param format  The printf-style format string passed to Logv (never null).
        RateCheckResult CheckAndRecord(const char* format);

    private:
        // Patterns matched via std::string_view::starts_with against the format string.
        // Extended by adding entries to the initialiser list in the .cpp file.
        static const std::string_view k_patterns[];
        static const std::size_t k_patternCount;

        struct PatternState
        {
            std::chrono::steady_clock::time_point lastEmitted{};
            uint32_t suppressedCount{0};
        };

        const std::chrono::seconds m_cooldown;
        std::mutex m_mutex;
        // Key is the matched pattern string_view's data() pointer — cheap identity comparison
        // since all pattern pointers come from the static k_patterns array.
        std::unordered_map<const char*, PatternState> m_state;
    };
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
