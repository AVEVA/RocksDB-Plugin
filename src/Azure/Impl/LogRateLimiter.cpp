// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"

#include <optional>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {

LogRateLimiter::LogRateLimiter(std::vector<std::string> patterns, std::chrono::seconds cooldown)
    : m_cooldown(cooldown), m_patterns(std::move(patterns)) {}

RateCheckResult LogRateLimiter::CheckAndRecord(const char* format)
{
    if (format == nullptr)
    {
        return {RateDecision::Allow, 0};
    }

    const std::string_view sv{format};

    // Find the first matching pattern (if any).
    std::optional<std::string_view> matched;
    for (const auto& p : m_patterns)
    {
        if (sv.find(p) != std::string_view::npos)
        {
            matched = std::string_view{p};
            break;
        }
    }

    if (!matched)
    {
        // Not a noisy pattern — pass straight through, no state update.
        return {RateDecision::Allow, 0};
    }

    const auto now = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(m_mutex);
    auto& state = m_state[*matched]; // default-inserts PatternState{} on first access

    // nullopt means this pattern has never been emitted — always allow on first occurrence.
    const bool cooldownExpired = !state.lastEmitted ||
                                 (now - *state.lastEmitted) >= m_cooldown;

    if (cooldownExpired)
    {
        // Cooldown has expired (or this is the very first occurrence).
        const uint32_t suppressed = state.suppressedCount;
        state.lastEmitted = now;
        state.suppressedCount = 0;

        if (suppressed > 0)
        {
            return {RateDecision::AllowWithSummary, suppressed};
        }
        return {RateDecision::Allow, 0};
    }

    // Still within the cooldown window — suppress and count.
    ++state.suppressedCount;
    return {RateDecision::Suppress, 0};
}

} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
