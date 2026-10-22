// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"

#include <algorithm>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {

LogRateLimiter::LogRateLimiter(std::vector<std::string> patterns, std::chrono::seconds cooldown)
    : m_cooldown(cooldown), m_patterns(std::move(patterns)) {}

RateCheckResult LogRateLimiter::CheckAndRecord(const char* format) {
    return CheckAndRecord(format, std::chrono::steady_clock::now());
}

RateCheckResult LogRateLimiter::CheckAndRecord(const char* format, std::chrono::steady_clock::time_point now) {
    if (format == nullptr) {
        return {RateDecision::Allow, 0};
    }

    const std::string_view sv{format};

    // The first matching pattern wins.
    const auto matched =
        std::ranges::find_if(m_patterns, [&](const std::string& p) { return sv.find(p) != std::string_view::npos; });

    if (matched == m_patterns.end()) {
        // Not a noisy pattern — pass straight through, no state update.
        return {RateDecision::Allow, 0};
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    auto& state = m_state[std::string_view{*matched}]; // default-inserts PatternState{} on first access

    // nullopt means this pattern has never been emitted — always allow on first occurrence.
    const bool cooldownExpired = !state.lastEmitted || (now - *state.lastEmitted) >= m_cooldown;

    if (cooldownExpired) {
        // Cooldown has expired (or this is the very first occurrence).
        const uint32_t suppressed = state.suppressedCount;
        state.lastEmitted = now;
        state.suppressedCount = 0;

        if (suppressed > 0) {
            return {RateDecision::AllowWithSummary, suppressed};
        }
        return {RateDecision::Allow, 0};
    }

    // Still within the cooldown window — suppress and count.
    ++state.suppressedCount;
    return {RateDecision::Suppress, 0};
}

} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
