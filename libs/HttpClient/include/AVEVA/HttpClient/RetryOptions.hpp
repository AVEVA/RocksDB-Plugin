// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Units.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>

namespace AVEVA {
/// <summary>
/// The default delay before the first retry; later retries back off exponentially from it. See
/// <see cref="RetryOptions"/>.
/// </summary>
inline constexpr std::chrono::milliseconds DefaultRetryInitialDelay{800};

/// <summary>
/// The default cap on the exponential backoff delay. See <see cref="RetryOptions"/>.
/// </summary>
inline constexpr std::chrono::milliseconds DefaultRetryMaxDelay{std::chrono::minutes{1}};

/// <summary>
/// Automatic retry of transient failures (HTTP 408/429/500/502/503/504 and transport errors such as
/// resolve/connect/read/write failures and timeouts), as honoured by retrying layers such as the Azure clients. Each
/// retry waits an exponential backoff with jitter (<c>InitialDelay * 2^n</c>, capped at <c>MaxDelay</c>); a server
/// Retry-After hint takes precedence and is honoured up to the larger of <c>MaxDelay</c> and one minute (a bogus hint
/// cannot stall a request for hours). Cancellation stops retrying immediately.
/// </summary>
struct RetryOptions {
    /// <summary>
    /// Number of retry attempts after the initial attempt (0 disables retries).
    /// </summary>
    RetryCount MaxRetries{3};

    /// <summary>
    /// The backoff before the first retry, doubled for each further one.
    /// </summary>
    std::chrono::milliseconds InitialDelay = DefaultRetryInitialDelay;

    /// <summary>
    /// The upper bound of the exponential backoff delay.
    /// </summary>
    std::chrono::milliseconds MaxDelay = DefaultRetryMaxDelay;
};

/// <summary>
/// The multiplicative jitter range applied to each backoff delay, to de-synchronize retries, and the largest exponent
/// applied, which keeps <c>InitialDelay * 2^n</c> representable even for absurd retry counts.
/// </summary>
inline constexpr double MinBackoffJitter = 0.8;
inline constexpr double MaxBackoffJitter = 1.3;
inline constexpr int MaxBackoffExponent = 62;
/// <summary>The longest delay a backoff can be configured to, so that adding it to a clock reading cannot
/// overflow.</summary>
inline constexpr std::chrono::milliseconds MaxBackoffBound{std::chrono::hours{24 * 365 * 10}};

/// <summary>
/// Exponential backoff (InitialDelay * 2^(retry-1)) with jitter in [0.8, 1.3), capped at MaxDelay. The generator is a
/// parameter so tests can make the jitter deterministic.
/// </summary>
/// <typeparam name="Generator">A random number generator type.</typeparam>
/// <param name="retry">The retry policy that supplies the initial and maximum delay.</param>
/// <param name="retryNumber">The number of the retry that is about to wait, starting at 1.</param>
/// <param name="generator">The source of the jitter.</param>
/// <returns>The delay to wait before the retry.</returns>
template <typename Generator>
[[nodiscard]] std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber,
                                                       Generator& generator) {
    std::uniform_real_distribution<double> jitter(MinBackoffJitter, MaxBackoffJitter);

    // Bound the delays in integers first: int64 max rounds up to 2^63 as a double, which llround cannot convert.
    constexpr auto maxBound = MaxBackoffBound;
    const double maxDelay =
        static_cast<double>(std::clamp(retry.MaxDelay, std::chrono::milliseconds{0}, maxBound).count());
    const double initial =
        static_cast<double>(std::clamp(retry.InitialDelay, std::chrono::milliseconds{0}, maxBound).count());
    const double exponential = initial * std::ldexp(1.0, std::clamp(retryNumber - 1, 0, MaxBackoffExponent));
    const double delay = std::min(exponential * jitter(generator), maxDelay);
    return std::chrono::milliseconds{static_cast<std::int64_t>(std::llround(delay))};
}

/// <summary>
/// As the generator overload, drawing the jitter from a per-thread random generator.
/// </summary>
/// <param name="retry">The retry policy that supplies the initial and maximum delay.</param>
/// <param name="retryNumber">The number of the retry that is about to wait, starting at 1.</param>
/// <returns>The delay to wait before the retry.</returns>
[[nodiscard]] std::chrono::milliseconds ComputeBackoff(const RetryOptions& retry, int retryNumber);
} // namespace AVEVA
