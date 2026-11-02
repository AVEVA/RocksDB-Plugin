// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpStatusCode.hpp>

#include <string>
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
    HttpResponse(HttpStatusCode status, std::vector<HttpHeader> headers, std::string body);

    /// <summary>
    /// The HTTP status code.
    /// </summary>
    /// <returns>The status code; <c>HttpStatus::NoResponse</c> on a default-constructed response.</returns>
    [[nodiscard]] HttpStatusCode GetStatus() const noexcept;

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

  private:
    HttpStatusCode m_status;
    std::vector<HttpHeader> m_headers;
    std::string m_body;
};
} // namespace AVEVA
