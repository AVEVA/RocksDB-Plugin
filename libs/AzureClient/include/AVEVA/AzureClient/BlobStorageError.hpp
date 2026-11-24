// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/Outcomes.hpp>
#include <AVEVA/HttpClient/HttpStatusCode.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <system_error>

namespace AVEVA::AzureClient {
/// <summary>
/// Describes a failed Blob Storage operation. Invariants:
///  - Code is always populated for a failure delivered through std::unexpected; its category() is the
///    BlobStorageErrorCategory for service-reported failures, or the system/generic category for
///    transport and client-side validation errors.
///  - StatusCode is the HTTP status for service-reported failures and 0 for transport and client-side
///    errors (no response was received or none was sent).
///  - ErrorCode (the x-ms-error-code header, falling back to the XML body's Code) is empty when the
///    service sent none and for transport/client-side errors. An unrecognized service code still sets
///    ErrorCode verbatim, while Code maps to BlobStorageErrorCode::ServiceError.
///  - RequestId (x-ms-request-id) is empty when no response was received or the header was absent.
///  - Message is the service's message, or Code.message() for client-side errors.
/// </summary>
struct BlobStorageError {
    /// <summary>
    /// The HTTP status of the failed response; 0 when no response was received or none was sent.
    /// </summary>
    HttpStatusCode StatusCode;

    /// <summary>
    /// The service's error code string, verbatim; empty when the service sent none.
    /// </summary>
    std::string ErrorCode;

    /// <summary>
    /// The service's message, or <c>Code.message()</c> for client-side errors.
    /// </summary>
    std::string Message;

    /// <summary>
    /// The x-ms-request-id of the failed response; empty when no response was received or the header was absent.
    /// </summary>
    std::string RequestId;

    /// <summary>
    /// The std::error_code classifying this failure. For server-reported failures this is the mapped
    /// <see cref="BlobStorageErrorCode"/>; otherwise it is the transport error (e.g. a dropped connection or
    /// timeout) or a client-side validation error such as <c>std::errc::invalid_argument</c>. Formerly
    /// named TransportError.
    /// </summary>
    std::error_code Code;

    /// <summary>
    /// The service's AuthenticationErrorDetail, which may echo the string-to-sign. It is kept out of Message so
    /// it does not end up in logs and exception text; read it only when diagnosing a signature mismatch.
    /// </summary>
    std::string AuthenticationDetail;

    /// <summary>
    /// The service's retry hint (x-ms-retry-after-ms or Retry-After), if the failed response carried one.
    /// </summary>
    std::optional<std::chrono::milliseconds> RetryAfter;
};

/// <summary>
/// Transient when retrying the same request may succeed: transport failures such as timeouts and
/// dropped connections, and 408/429/500/502/503/504 responses (e.g. ServerBusy,
/// OperationTimedOut, InternalError). This is the classification the built-in retry policy uses.
/// </summary>
/// <param name="error">The failure to classify.</param>
/// <returns><c>Transience::Transient</c> when a retry may succeed; otherwise <c>Transience::Permanent</c>.</returns>
[[nodiscard]] Transience TransienceOf(const BlobStorageError& error) noexcept;
} // namespace AVEVA::AzureClient
