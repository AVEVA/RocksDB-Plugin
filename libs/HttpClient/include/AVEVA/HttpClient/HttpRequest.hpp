// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <AVEVA/HttpClient/HttpBody.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/Units.hpp>

#include <boost/unordered/unordered_flat_set.hpp>

#include <cstddef>
#include <string>

namespace AVEVA {
/// <summary>
/// Hashes and compares headers by field name only, case-insensitively as HTTP field names are, so a set of headers
/// holds at most one header per name.
/// </summary>
struct HttpHeaderNameHash {
    [[nodiscard]] std::size_t operator()(const HttpHeader& header) const noexcept;
};

/// <summary>
/// Equality by field name only, case-insensitively; see <see cref="HttpHeaderNameHash"/>.
/// </summary>
struct HttpHeaderNameEqual {
    [[nodiscard]] bool operator()(const HttpHeader& left, const HttpHeader& right) const noexcept;
};

/// <summary>
/// An open-addressing set of headers, unique by name. Iteration order is unspecified.
/// </summary>
using HttpHeaderSet = boost::unordered_flat_set<HttpHeader, HttpHeaderNameHash, HttpHeaderNameEqual>;
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
    /// <returns>The headers, unique by name, in unspecified order; valid for as long as the request is.</returns>
    [[nodiscard]] const HttpHeaderSet& GetHeaders() const& noexcept AVEVA_LIFETIMEBOUND;
    [[nodiscard]] HttpHeaderSet GetHeaders() && noexcept;

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
    /// Adds a header. Names are unique, compared case-insensitively: adding a name that is already present replaces
    /// the earlier header (last wins). Headers the client manages itself (Connection, Transfer-Encoding,
    /// Host and Content-Length) are silently dropped when sent.
    /// </summary>
    /// <param name="header">The header to add.</param>
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
    HttpHeaderSet m_headers;
    HttpBody m_body;
};
} // namespace AVEVA
