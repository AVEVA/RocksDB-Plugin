// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <optional>
#include <stdexcept>
#include <utility>

namespace AVEVA::AzureClient {
/// <summary>
/// Pairs a parsed, strongly typed result model with the raw HTTP response it was derived
/// from, mirroring the Response&lt;T&gt; pattern used by the Azure SDK for C++.
/// </summary>
/// <typeparam name="T">The parsed result model.</typeparam>
template <class T> class Response {
  public:
    /// <summary>
    /// Creates a response with a default-constructed value, an empty raw response and no error.
    /// </summary>
    Response() = default;

    /// <summary>
    /// Creates a response from a parsed value and the raw response it came from.
    /// </summary>
    /// <param name="value">The parsed result model.</param>
    /// <param name="rawResponse">The HTTP response the value was derived from.</param>
    /// <param name="error">The suppressed service error, when the operation treated one as success.</param>
    Response(T value, HttpResponse rawResponse, std::optional<BlobStorageError> error = std::nullopt)
        : m_value(std::move(value)), m_rawResponse(std::move(rawResponse)), m_error(std::move(error)) {}

    /// <summary>
    /// The parsed result model.
    /// </summary>
    /// <returns>The value.</returns>
    [[nodiscard]] const T& Value() const& noexcept { return m_value; }

    /// <summary>
    /// Mutable access to the parsed result model.
    /// </summary>
    /// <returns>A reference to the value.</returns>
    [[nodiscard]] T& Value() & noexcept { return m_value; }

    /// <summary>
    /// Moves the parsed result model out of a response that is no longer needed.
    /// </summary>
    /// <returns>The value as an rvalue.</returns>
    [[nodiscard]] T&& Value() && noexcept { return std::move(m_value); }

    /// <summary>
    /// The HTTP response the value was derived from.
    /// </summary>
    /// <returns>The raw response.</returns>
    [[nodiscard]] const HttpResponse& RawResponse() const& noexcept { return m_rawResponse; }

    /// <summary>
    /// Mutable access to the HTTP response the value was derived from.
    /// </summary>
    /// <returns>A reference to the raw response.</returns>
    [[nodiscard]] HttpResponse& RawResponse() & noexcept { return m_rawResponse; }

    /// <summary>
    /// Moves the raw HTTP response out of a response that is no longer needed.
    /// </summary>
    /// <returns>The raw response as an rvalue.</returns>
    [[nodiscard]] HttpResponse&& RawResponse() && noexcept { return std::move(m_rawResponse); }

    /// <summary>
    /// Empty for ordinary successes. Engaged only when the operation deliberately treated a service
    /// error as success (e.g. DeleteIfExists on a missing blob, CreateIfNotExists on an existing one):
    /// it then describes the suppressed error, <c>RawResponse()</c> is the real error response (404/409/...)
    /// and <c>Value()</c> is default-constructed. Genuine failures never reach a Response; they are
    /// reported through the operation's error channel instead.
    /// </summary>
    /// <returns>The suppressed error, if any.</returns>
    [[nodiscard]] const std::optional<BlobStorageError>& Error() const& noexcept { return m_error; }

    /// <summary>
    /// Mutable access to the suppressed error.
    /// </summary>
    /// <returns>A reference to the optional error.</returns>
    [[nodiscard]] std::optional<BlobStorageError>& Error() & noexcept { return m_error; }

    /// <summary>
    /// Moves the suppressed error out of a response that is no longer needed.
    /// </summary>
    /// <returns>The optional error as an rvalue.</returns>
    [[nodiscard]] std::optional<BlobStorageError>&& Error() && noexcept { return std::move(m_error); }

  private:
    T m_value{};
    HttpResponse m_rawResponse;
    std::optional<BlobStorageError> m_error;
};

} // namespace AVEVA::AzureClient
