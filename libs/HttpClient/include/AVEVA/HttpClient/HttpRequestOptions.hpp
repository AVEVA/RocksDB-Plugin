// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/RetryOptions.hpp>
#include <AVEVA/HttpClient/Units.hpp>

#include <chrono>
#include <cstdint>
#include <optional>

namespace AVEVA {
/// <summary>
/// Whether a TLS connection closed without close_notify may end an EOF-delimited response body. Rejecting is the
/// default because an attacker can truncate such a body undetected.
/// </summary>
enum class TruncatedTlsBodyPolicy : std::uint8_t { Reject, Accept };

/// <summary>
/// Per-request knobs: the overall deadline, the limits that bound how much of a response is buffered. Defaults are a 30
/// second timeout, an 8 MiB body limit, and a 64 KiB header limit.
/// </summary>
class HttpRequestOptions {
  public:
    /// <summary>
    /// The deadline covering the whole request.
    /// </summary>
    /// <returns>The timeout; 30 seconds unless set otherwise.</returns>
    [[nodiscard]] Timeout GetTimeout() const noexcept;

    /// <summary>
    /// The maximum response body size in bytes.
    /// </summary>
    /// <returns>The body limit; 8 MiB unless set otherwise.</returns>
    [[nodiscard]] ByteCount GetResponseBodyLimit() const noexcept;

    /// <summary>
    /// The maximum combined size in bytes of the response status line and headers.
    /// </summary>
    /// <returns>The header limit; 64 KiB unless set otherwise.</returns>
    [[nodiscard]] ByteCount GetResponseHeaderLimit() const noexcept;

    /// <summary>
    /// Sets the deadline covering the whole request.
    /// </summary>
    /// <param name="timeout">The deadline.</param>
    void SetTimeout(Timeout timeout) noexcept;

    /// <summary>
    /// Sets the maximum response body size in bytes; larger responses fail with
    /// <c>HttpClientError::ResponseTooLarge</c>.
    /// </summary>
    /// <param name="responseBodyLimit">The body limit in bytes.</param>
    void SetResponseBodyLimit(ByteCount responseBodyLimit) noexcept;

    /// <summary>
    /// Sets the maximum size in bytes of the response status line and headers; larger responses fail with
    /// <c>HttpClientError::ResponseTooLarge</c>.
    /// </summary>
    /// <param name="responseHeaderLimit">The header limit in bytes.</param>
    void SetResponseHeaderLimit(ByteCount responseHeaderLimit) noexcept;

    /// <summary>
    /// Whether a TLS connection closed without close_notify may end an EOF-delimited response body.
    /// </summary>
    /// <returns>The policy; <c>Reject</c> unless set otherwise.</returns>
    [[nodiscard]] TruncatedTlsBodyPolicy GetTruncatedTlsBodyPolicy() const noexcept;

    /// <summary>
    /// Opts in to accepting EOF-delimited bodies whose TLS stream ended without close_notify.
    /// </summary>
    /// <param name="policy">Whether to accept such bodies.</param>
    void SetTruncatedTlsBodyPolicy(TruncatedTlsBodyPolicy policy) noexcept;

    /// <summary>
    /// The retry policy for this request. When set, <c>HttpClient</c> sends the request again after a transient
    /// failure (a connection that could not be made, or a dropped one, or a 408/429/500/502/503/504 response), up to
    /// <c>MaxRetries</c> times with exponential backoff or the server's Retry-After. Because only the caller knows
    /// whether a resend is safe, a request that may already have reached the server is retried only when its method
    /// is GET, HEAD, OPTIONS or TRACE. The request's timeout covers every attempt and wait. Layers above the client
    /// that do their own retrying (such as the Azure clients) honour this policy themselves.
    /// </summary>
    /// <returns>The policy; empty (no retries by <c>HttpClient</c>) unless set.</returns>
    [[nodiscard]] const std::optional<RetryOptions>& GetRetryOptions() const noexcept;

    /// <summary>
    /// Opts in to retrying this request with the given policy.
    /// </summary>
    /// <param name="retry">The policy for this request.</param>
    void SetRetryOptions(RetryOptions retry) noexcept;

    /// <summary>
    /// Removes the retry policy, so the request is sent once. A layer that retries by itself uses this on the options
    /// it passes down, so the attempts are not multiplied.
    /// </summary>
    void ClearRetryOptions() noexcept;

  private:
    std::optional<RetryOptions> m_retry;
    TruncatedTlsBodyPolicy m_truncatedTlsBodyPolicy = TruncatedTlsBodyPolicy::Reject;
    Timeout m_timeout{std::chrono::seconds{30}};
    ByteCount m_responseBodyLimit{8 * 1024 * 1024};
    ByteCount m_responseHeaderLimit{64 * 1024};
};
} // namespace AVEVA
