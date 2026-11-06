// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Units.hpp>

#include <chrono>

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
} // namespace AVEVA
