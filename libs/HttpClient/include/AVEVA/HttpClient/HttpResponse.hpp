// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>

#include <string>
#include <system_error>
#include <vector>

namespace AVEVA {
/// <summary>
/// A complete HTTP response: the status code, every header field in the order received, and the fully
/// buffered body.
/// </summary>
class HttpResponse {
  public:
    /// <summary>
    /// Constructs an empty response with status 0, no headers, and no body.
    /// </summary>
    HttpResponse();

    /// <summary>
    /// Constructs a response from a status, headers, and a body.
    /// </summary>
    /// <param name="status">The HTTP status code.</param>
    /// <param name="headers">The response headers, in the order received.</param>
    /// <param name="body">The response body.</param>
    HttpResponse(unsigned int status, std::vector<HttpHeader> headers, std::string body);

    /// <summary>
    /// The HTTP status code.
    /// </summary>
    /// <returns>The status code; 0 on a default-constructed response.</returns>
    [[nodiscard]] unsigned int GetStatus() const noexcept;

    /// <summary>
    /// The response headers.
    /// </summary>
    /// <returns>The headers, in the order the server sent them.</returns>
    [[nodiscard]] const std::vector<HttpHeader>& GetHeaders() const& noexcept AVEVA_LIFETIMEBOUND;

    /// <summary>
    /// Moves the headers out of an rvalue response, so a caller that owns the response can take them
    /// without copying.
    /// </summary>
    /// <returns>The headers; the response is left with none.</returns>
    [[nodiscard]] std::vector<HttpHeader> GetHeaders() && noexcept;

    /// <summary>
    /// The response body.
    /// </summary>
    /// <returns>The fully buffered body; empty for a bodiless response.</returns>
    [[nodiscard]] const std::string& GetBody() const& noexcept AVEVA_LIFETIMEBOUND;

    /// <summary>
    /// Moves the body out of an rvalue response, so a caller that owns the response can take it
    /// without copying.
    /// </summary>
    /// <returns>The body; the response is left with none.</returns>
    [[nodiscard]] std::string GetBody() && noexcept;

    /// <summary>
    /// Appends a header to the response.
    /// </summary>
    /// <param name="header">The header to append.</param>
    void AddHeader(HttpHeader header);

    /// <summary>
    /// The underlying transport error (an asio, TLS or socket code) behind a failed request, which the
    /// <c>HttpClientError</c> reported alongside the response collapses to a coarse category. Useful for
    /// telling, for example, a certificate verification failure from a refused connection.
    /// </summary>
    /// <returns>The cause; empty on success or when the failure had no lower-level code.</returns>
    [[nodiscard]] std::error_code GetCause() const noexcept;

    /// <summary>
    /// Records the underlying transport error of a failed request.
    /// </summary>
    /// <param name="cause">The lower-level error code.</param>
    void SetCause(std::error_code cause) noexcept;

  private:
    unsigned int m_status = 0;
    std::vector<HttpHeader> m_headers;
    std::string m_body;
    std::error_code m_cause;
};
} // namespace AVEVA
