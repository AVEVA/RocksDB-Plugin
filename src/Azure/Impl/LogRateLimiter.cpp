// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"

#include <string_view>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {

// Noisy RocksDB log prefixes that are subject to rate limiting.
// Add new entries here as additional patterns are identified.
const std::string_view LogRateLimiter::k_patterns[] = {
    "Stalling writes because we have",
    "Stopping writes because we have",
    "BlobNotFound",
};
const std::size_t LogRateLimiter::k_patternCount = std::size(LogRateLimiter::k_patterns);

LogRateLimiter::LogRateLimiter(std::chrono::seconds cooldown) : m_cooldown(cooldown) {}

RateCheckResult LogRateLimiter::CheckAndRecord(const char* format)
{
    if (format == nullptr)
    {
        return {RateDecision::Allow, 0};
    }

    const std::string_view sv{format};

    // Find the first matching pattern (if any).
    const std::string_view* matched = nullptr;
    for (std::size_t i = 0; i < k_patternCount; ++i)
    {
        if (sv.starts_with(k_patterns[i]))
        {
            matched = &k_patterns[i];
            break;
        }
    }

    if (matched == nullptr)
    {
        // Not a noisy pattern — pass straight through, no state update.
        return {RateDecision::Allow, 0};
    }

    const auto now = std::chrono::steady_clock::now();
    const char* key = matched->data(); // stable pointer into static storage

    std::lock_guard<std::mutex> lock(m_mutex);
    auto& state = m_state[key]; // default-inserts PatternState{} on first access

    const auto elapsed = now - state.lastEmitted;

    if (elapsed >= m_cooldown)
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
