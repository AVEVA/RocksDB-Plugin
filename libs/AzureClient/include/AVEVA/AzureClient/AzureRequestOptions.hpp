// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <chrono>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient {
/// <summary>
/// Per-request options for the Azure clients: everything <see cref="HttpRequestOptions"/> offers, plus the
/// retry budget and retry count that only the retrying request pipeline honours. An HttpRequestOptions
/// converts implicitly, so callers that do not need the extras can keep passing one.
/// </summary>
class AzureRequestOptions : public HttpRequestOptions {
  public:
    AzureRequestOptions() = default;

    /// <summary>
    /// Adopts the plain HTTP options, leaving the retry budget and count unset.
    /// </summary>
    /// <param name="options">The HTTP-level options.</param>
    AzureRequestOptions(HttpRequestOptions options) noexcept(std::is_nothrow_move_constructible_v<HttpRequestOptions>)
        : HttpRequestOptions(std::move(options)) {}

    /// <summary>
    /// The retry budget for this request, retries and backoff included.
    /// </summary>
    /// <returns>The deadline measured from the start of the operation; empty when none was set.</returns>
    std::optional<std::chrono::milliseconds> GetTotalDeadline() const noexcept { return m_totalDeadline; }

    /// <summary>
    /// The number of retries after the first attempt that replaces the client's retry policy for this request.
    /// </summary>
    /// <returns>The override; empty when the client's policy applies.</returns>
    std::optional<int> GetMaxRetries() const noexcept { return m_maxRetries; }

    /// <summary>
    /// Bounds the whole operation, retries and backoff included. When it elapses the in-flight attempt is
    /// cancelled and the operation completes with <c>std::errc::timed_out</c>. Each attempt's own timeout is
    /// additionally clamped to the time that remains.
    /// </summary>
    /// <param name="totalDeadline">The deadline; a non-positive value expires immediately.</param>
    void SetTotalDeadline(std::chrono::milliseconds totalDeadline) noexcept { m_totalDeadline = totalDeadline; }

    /// <summary>
    /// Replaces the client's retry count for this request (0 disables retries).
    /// </summary>
    /// <param name="maxRetries">Retries after the first attempt.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="maxRetries"/> is negative.</exception>
    void SetMaxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw std::invalid_argument("The retry count must not be negative.");
        }
        m_maxRetries = maxRetries;
    }

  private:
    std::optional<std::chrono::milliseconds> m_totalDeadline;
    std::optional<int> m_maxRetries;
};
} // namespace AVEVA::AzureClient
