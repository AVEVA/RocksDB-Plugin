// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClientError.hpp"
#include "AVEVA/HttpClient/HttpClientOptions.hpp"
#include "AVEVA/HttpClient/HttpMethod.hpp"
#include "AVEVA/HttpClient/HttpResponse.hpp"
#include "AVEVA/HttpClient/HttpStatusCode.hpp"
#include "AVEVA/HttpClient/RetryOptions.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

namespace AVEVA::Private {
/// <summary>
/// Whether sending the request again can have no effect beyond the first send. Only the side-effect-free methods
/// qualify: a PUT or DELETE is idempotent in HTTP, but whether the caller's operation is must be the caller's call.
/// </summary>
[[nodiscard]] constexpr bool IsReplaySafe(HttpMethod method) noexcept {
    return method == HttpMethod::Get || method == HttpMethod::Head || method == HttpMethod::Options ||
           method == HttpMethod::Trace;
}

/// <summary>
/// Whether a failure with no response is worth another attempt. A failure before the request was written cannot
/// have had any effect on the server; after that, only a replay-safe method may be sent again. The request's own
/// deadline (TimedOut) is never retried.
/// </summary>
[[nodiscard]] inline bool ShouldRetryFailure(std::error_code error, HttpRequestPhase phase,
                                             HttpMethod method) noexcept {
    const bool transient = error == HttpClientError::ResolveFailed || error == HttpClientError::ConnectFailed ||
                           error == HttpClientError::WriteFailed || error == HttpClientError::ReadFailed;
    if (!transient) {
        return false;
    }
    const bool notSent = phase == HttpRequestPhase::Queued || phase == HttpRequestPhase::Resolve ||
                         phase == HttpRequestPhase::Connect || phase == HttpRequestPhase::Handshake;
    return notSent || IsReplaySafe(method);
}

/// <summary>
/// Whether a response status is a transient server-side condition worth another attempt of a replay-safe request.
/// </summary>
[[nodiscard]] inline bool ShouldRetryStatus(HttpStatusCode status, HttpMethod method) noexcept {
    const auto value = status.Value();
    const bool transient =
        value == 408U || value == 429U || value == 500U || value == 502U || value == 503U || value == 504U;
    return transient && IsReplaySafe(method);
}

/// <summary>
/// The server's Retry-After hint, in its delay-seconds form. The HTTP-date form is ignored.
/// </summary>
[[nodiscard]] inline std::optional<std::chrono::milliseconds> RetryAfterHint(const HttpResponse& response) noexcept {
    constexpr std::uint64_t MillisecondsPerSecond = 1000U;
    constexpr std::uint64_t MaxSeconds = 24U * 60U * 60U;
    for (const auto& header : response.GetHeaders()) {
        const auto& name = header.GetName();
        constexpr std::string_view wanted = "retry-after";
        if (name.size() != wanted.size() ||
            !std::equal(name.begin(), name.end(), wanted.begin(),
                        [](char lhs, char rhs) { return std::tolower(static_cast<unsigned char>(lhs)) == rhs; })) {
            continue;
        }
        const auto& value = header.GetValue();
        std::uint64_t seconds = 0;
        const auto* const end = value.data() + value.size();
        const auto parsed = std::from_chars(value.data(), end, seconds);
        if (parsed.ec != std::errc{} || parsed.ptr != end) {
            return std::nullopt;
        }
        return std::chrono::milliseconds{
            static_cast<std::chrono::milliseconds::rep>(std::min(seconds, MaxSeconds) * MillisecondsPerSecond)};
    }
    return std::nullopt;
}

/// <summary>
/// How long to wait before retry number <paramref name="retryNumber"/>: the server's hint when it gave one (capped
/// so a hostile server cannot park the caller for long), else the policy's exponential backoff.
/// </summary>
[[nodiscard]] inline std::chrono::milliseconds RetryDelay(const RetryOptions& retry, int retryNumber,
                                                          const HttpResponse* response) {
    if (response != nullptr) {
        if (const auto hint = RetryAfterHint(*response); hint.has_value()) {
            return std::min(*hint, retry.MaxDelay);
        }
    }
    return ComputeBackoff(retry, retryNumber);
}
} // namespace AVEVA::Private
