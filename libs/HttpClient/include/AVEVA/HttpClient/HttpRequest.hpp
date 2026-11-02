// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <AVEVA/HttpClient/HttpBody.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/Units.hpp>

#include <string>
#include <vector>

namespace AVEVA {
/// <summary>
/// An outbound HTTP request: method, absolute http(s) URL, headers, and an optional body. Copying a request shares
/// its body (see <see cref="HttpBody"/>) rather than copying the bytes.
/// </summary>
class HttpRequest {
  public:
    /// <summary>
    /// Constructs an empty GET request with no URL, headers, or body.
    /// </summary>
    HttpRequest();

    /// <summary>
    /// The request method.
    /// </summary>
    /// <returns>The method; <c>HttpMethod::Get</c> unless set otherwise.</returns>
    [[nodiscard]] HttpMethod GetMethod() const noexcept;

    /// <summary>
    /// The request URL, exactly as supplied; it is not parsed or validated here.
    /// </summary>
    /// <returns>The URL, empty unless set; valid for as long as the request is.</returns>
    [[nodiscard]] const std::string& GetUrl() const& noexcept AVEVA_LIFETIMEBOUND;
    [[nodiscard]] std::string GetUrl() && noexcept;

    /// <summary>
    /// The request headers.
    /// </summary>
    /// <returns>The headers, in the order they were added; valid for as long as the request is.</returns>
    [[nodiscard]] const std::vector<HttpHeader>& GetHeaders() const& noexcept AVEVA_LIFETIMEBOUND;
    [[nodiscard]] std::vector<HttpHeader> GetHeaders() && noexcept;

    /// <summary>
    /// The request body.
    /// </summary>
    /// <returns>A copy of the body, sharing its bytes; empty unless set.</returns>
    [[nodiscard]] HttpBody GetBody() const noexcept;

    /// <summary>
    /// Sets the request method.
    /// </summary>
    /// <param name="method">The method to send.</param>
    void SetMethod(HttpMethod method) noexcept;

    /// <summary>
    /// Sets the absolute request URL.
    /// </summary>
    /// <param name="url">The absolute http(s) URL to request.</param>
    void SetUrl(HttpUrl url) noexcept;

    /// <summary>
    /// Appends a header. Duplicate names are kept as separate entries rather than merged. Headers the client
    /// manages itself (Connection, Transfer-Encoding, Host and Content-Length) are silently dropped when sent.
    /// </summary>
    /// <param name="header">The header to append.</param>
    void AddHeader(HttpHeader header);

    /// <summary>
    /// Removes every header with a name, compared case-insensitively as HTTP field names are. Replacing a header is
    /// removing it and adding the new one.
    /// </summary>
    /// <param name="name">The field name to remove.</param>
    void RemoveHeaders(const HeaderName& name) noexcept;

    /// <summary>
    /// Sets the request body, replacing any previous one.
    /// </summary>
    /// <param name="body">The body to send.</param>
    void SetBody(HttpBody body) noexcept;

  private:
    HttpMethod m_method = HttpMethod::Get;
    std::string m_url;
    std::vector<HttpHeader> m_headers;
    HttpBody m_body;
};
} // namespace AVEVA
