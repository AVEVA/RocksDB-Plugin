// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <system_error>
#include <type_traits>

namespace AVEVA {
/// <summary>
/// Failure reasons reported by the client through <c>std::error_code</c>. Values are specialized as an
/// error-code enum, so an error can be compared directly (for example
/// <c>error == HttpClientError::TimedOut</c>).
/// </summary>
enum class HttpClientError {
    /// <summary>The URL could not be parsed, or is not an http(s) URL that can be sent to.</summary>
    InvalidUrl = 1,
    /// <summary>The method, a header, or an option value is not something that can be sent.</summary>
    InvalidRequest,
    /// <summary>DNS resolution of the host failed.</summary>
    ResolveFailed,
    /// <summary>The TCP connection could not be established.</summary>
    ConnectFailed,
    /// <summary>TLS setup, the handshake, or certificate verification failed.</summary>
    TlsFailed,
    /// <summary>The request could not be written to the connection.</summary>
    WriteFailed,
    /// <summary>The response could not be read, or the connection ended mid-message.</summary>
    ReadFailed,
    /// <summary>The request's timeout elapsed before the response was complete.</summary>
    TimedOut,
    /// <summary>The response headers or body exceeded the configured limit.</summary>
    ResponseTooLarge,
    /// <summary>The peer sent something that is not valid or supported HTTP.</summary>
    ProtocolError
};

/// <summary>
/// Found via ADL by <c>std::error_code</c>'s constructor; do not call directly, compare
/// <c>std::error_code</c> values against <see cref="HttpClientError"/> instead
/// (for example <c>error == HttpClientError::TimedOut</c>).
/// </summary>
/// <param name="error">The error to wrap.</param>
/// <returns>An error code carrying <paramref name="error"/> in the client's category.</returns>
std::error_code make_error_code(HttpClientError error) noexcept;
} // namespace AVEVA

namespace std {
template <> struct is_error_code_enum<AVEVA::HttpClientError> : true_type {};
} // namespace std
