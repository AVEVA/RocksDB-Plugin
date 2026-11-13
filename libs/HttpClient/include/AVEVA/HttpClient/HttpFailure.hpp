// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <expected>
#include <system_error>

namespace AVEVA {
/// <summary>
/// The low-level error a transport step (connect, TLS handshake, write or read) failed with, such as
/// <c>connection_refused</c> or a certificate-verification failure. It is kept apart from the request's own error,
/// which is coarser, so a caller can tell why without the two being confused.
/// </summary>
class TransportCause {
  public:
    /// <summary>
    /// No transport cause: the request failed for a reason of its own (a timeout, a rejected request, ...).
    /// </summary>
    TransportCause() noexcept = default;

    /// <summary>
    /// Wraps the error a transport step failed with.
    /// </summary>
    /// <param name="error">The low-level error.</param>
    explicit TransportCause(std::error_code error) noexcept : m_error(error) {}

    /// <summary>
    /// The low-level error.
    /// </summary>
    /// <returns>The error; a default (success) code when there is no transport cause.</returns>
    [[nodiscard]] std::error_code Error() const noexcept { return m_error; }

  private:
    std::error_code m_error;
};

/// <summary>
/// Why a request produced no response: the request-level error and, when a transport step failed, the low-level
/// cause behind it.
/// </summary>
class HttpFailure {
  public:
    /// <summary>
    /// Constructs a failure.
    /// </summary>
    /// <param name="error">The request-level error.</param>
    /// <param name="cause">The low-level transport error behind it, if any.</param>
    explicit HttpFailure(std::error_code error, TransportCause cause = {}) noexcept : m_error(error), m_cause(cause) {}

    /// <summary>
    /// The request-level error.
    /// </summary>
    /// <returns>The error.</returns>
    [[nodiscard]] std::error_code Error() const noexcept { return m_error; }

    /// <summary>
    /// The low-level transport error behind <see cref="Error"/>.
    /// </summary>
    /// <returns>The cause; empty when no transport step failed.</returns>
    [[nodiscard]] TransportCause Cause() const noexcept { return m_cause; }

  private:
    std::error_code m_error;
    TransportCause m_cause;
};

/// <summary>
/// The outcome of a request: the response, or why there is none. An HTTP error status is a response, not a failure.
/// </summary>
using HttpResult = std::expected<HttpResponse, HttpFailure>;
} // namespace AVEVA
