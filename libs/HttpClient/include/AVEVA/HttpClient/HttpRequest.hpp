// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace AVEVA {
/// <summary>
/// An outbound HTTP request: method, absolute http(s) URL, headers, and an optional body.
///
/// The body has two mutually exclusive representations. <see cref="SetBody"/> stores an owned copy;
/// <see cref="SetBodyView"/> stores a non-owning view over caller memory and avoids the copy at the cost of
/// a lifetime contract. Setting one clears the other, and <see cref="GetBodySize"/> reports the active one.
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
    HttpMethod GetMethod() const noexcept;

    /// <summary>
    /// The request URL, exactly as supplied; it is not parsed or validated here.
    /// </summary>
    /// <returns>The URL, empty unless set.</returns>
    const std::string& GetUrl() const noexcept;

    /// <summary>
    /// The request headers.
    /// </summary>
    /// <returns>The headers, in the order they were added.</returns>
    const std::vector<HttpHeader>& GetHeaders() const noexcept;

    /// <summary>
    /// The request headers, for in-place modification before the request is sent.
    /// </summary>
    /// <returns>A mutable reference to the headers.</returns>
    std::vector<HttpHeader>& GetHeaders() noexcept;

    /// <summary>
    /// Returns the owned body set via <see cref="SetBody"/>.
    /// </summary>
    /// <returns>The owned body. Empty if a non-owning body was set via <see cref="SetBodyView"/> -- use
    /// <see cref="GetBodyView"/> (and <see cref="HasBodyView"/>) in that case.</returns>
    const std::string& GetBody() const noexcept;

    /// <summary>
    /// Sets the request method.
    /// </summary>
    /// <param name="method">The method to send.</param>
    void SetMethod(HttpMethod method) noexcept;

    /// <summary>
    /// Sets the absolute request URL.
    /// </summary>
    /// <param name="url">The absolute http(s) URL to request.</param>
    void SetUrl(std::string url);

    /// <summary>
    /// Replaces every header.
    /// </summary>
    /// <param name="headers">The headers to send.</param>
    void SetHeaders(std::vector<HttpHeader> headers);

    /// <summary>
    /// Appends a header. Duplicate names are kept as separate entries rather than merged.
    /// </summary>
    /// <param name="header">The header to append.</param>
    void AddHeader(HttpHeader header);

    /// <summary>
    /// Reserves room for headers, to avoid reallocating while they are added.
    /// </summary>
    /// <param name="count">The number of headers to make room for.</param>
    void ReserveHeaders(std::size_t count);

    /// <summary>
    /// Sets an owned request body. The bytes are copied synchronously; the caller's buffer
    /// need not outlive this call. Clears any body view previously set via <see cref="SetBodyView"/>.
    /// </summary>
    /// <param name="body">The body to copy.</param>
    void SetBody(std::string body);

    /// <summary>
    /// Sets a non-owning request body view over caller-owned memory.
    ///
    /// LIFETIME CONTRACT: unlike <see cref="SetBody"/>, this does not copy the content. The memory it
    /// points to MUST remain valid and unmodified from this call until the completion handler for
    /// the <c>HttpClient::SendAsync()</c> (or equivalent) operation that consumes this request has run --
    /// NOT merely until the initiating call returns. Destroying or mutating the referenced buffer before
    /// completion is undefined behavior. Prefer <see cref="SetBody"/> unless you can guarantee this extended
    /// lifetime (for example the buffer is owned by a <c>std::shared_ptr</c> kept alive by the caller for the
    /// duration of the operation), or use the overload that takes a keep-alive.
    /// </summary>
    /// <param name="content">The caller-owned bytes to send, subject to the lifetime contract above.</param>
    void SetBodyView(std::span<const std::byte> content) noexcept;

    /// <summary>
    /// Like <see cref="SetBodyView"/>, but the request (and every copy of it, and the HTTP operation that
    /// sends it) also holds the keep-alive, so the viewed memory stays valid for as long as anything can
    /// still read it. This is the safe way to satisfy the body-view lifetime contract.
    /// </summary>
    /// <param name="content">The bytes to send, owned by <paramref name="keepAlive"/>.</param>
    /// <param name="keepAlive">An owner of <paramref name="content"/>, held for the whole operation.</param>
    void SetBodyView(std::span<const std::byte> content, std::shared_ptr<const void> keepAlive) noexcept;

    /// <summary>
    /// The owner passed to <c>SetBodyView(content, keepAlive)</c>.
    /// </summary>
    /// <returns>The keep-alive, or null if none was given.</returns>
    [[nodiscard]] const std::shared_ptr<const void>& GetBodyKeepAlive() const noexcept;

    /// <summary>
    /// Moves the owned body out of the request, leaving it empty. Has no effect on a body view.
    /// </summary>
    /// <returns>The previously owned body.</returns>
    [[nodiscard]] std::string ReleaseBody() noexcept;

    /// <summary>
    /// Whether the active body is a view rather than an owned copy.
    /// </summary>
    /// <returns>True if the body was set via <see cref="SetBodyView"/> rather than
    /// <see cref="SetBody"/>.</returns>
    [[nodiscard]] bool HasBodyView() const noexcept;

    /// <summary>
    /// Returns the non-owning body view set via <see cref="SetBodyView"/>.
    /// </summary>
    /// <returns>The view; empty (and meaningless) if <see cref="HasBodyView"/> is false.</returns>
    [[nodiscard]] std::span<const std::byte> GetBodyView() const noexcept;

    /// <summary>
    /// The body length regardless of which representation (<see cref="SetBody"/> or
    /// <see cref="SetBodyView"/>) is active.
    /// </summary>
    /// <returns>The body length in bytes.</returns>
    [[nodiscard]] std::size_t GetBodySize() const noexcept;

  private:
    HttpMethod m_method = HttpMethod::Get;
    std::string m_url;
    std::vector<HttpHeader> m_headers;
    std::string m_body;
    std::span<const std::byte> m_bodyView;
    std::shared_ptr<const void> m_bodyKeepAlive;
    bool m_hasBodyView = false;
};
} // namespace AVEVA
