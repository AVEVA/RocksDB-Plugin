// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Units.hpp>

#include <chrono>
#include <cstdint>

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
    [[nodiscard]] std::chrono::milliseconds GetTimeout() const noexcept;

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
    /// <param name="timeout">The deadline. A non-positive value makes the request invalid.</param>
    void SetTimeout(std::chrono::milliseconds timeout) noexcept;

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

  private:
    TruncatedTlsBodyPolicy m_truncatedTlsBodyPolicy = TruncatedTlsBodyPolicy::Reject;
    std::chrono::milliseconds m_timeout{30000};
    ByteCount m_responseBodyLimit{8 * 1024 * 1024};
    ByteCount m_responseHeaderLimit{64 * 1024};
};
} // namespace AVEVA
