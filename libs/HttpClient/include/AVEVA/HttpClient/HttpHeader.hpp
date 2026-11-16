// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <AVEVA/HttpClient/Units.hpp>

#include <string>

namespace AVEVA {
/// <summary>
/// A single HTTP header field: a name and its value. Neither is validated here; a request carrying an invalid one
/// fails with <c>HttpClientError::InvalidRequest</c> when it is sent.
/// </summary>
class HttpHeader {
  public:
    /// <summary>
    /// Constructs a header from a name and a value.
    /// </summary>
    /// <param name="name">The field name.</param>
    /// <param name="value">The field value.</param>
    HttpHeader(HeaderName name, HeaderValue value) noexcept;

    /// <summary>
    /// The field name.
    /// </summary>
    /// <returns>The name, valid for as long as the header is.</returns>
    [[nodiscard]] const std::string& GetName() const& noexcept AVEVA_LIFETIMEBOUND;
    [[nodiscard]] std::string GetName() && noexcept;

    /// <summary>
    /// The field value.
    /// </summary>
    /// <returns>The value, valid for as long as the header is.</returns>
    [[nodiscard]] const std::string& GetValue() const& noexcept AVEVA_LIFETIMEBOUND;
    [[nodiscard]] std::string GetValue() && noexcept;

  private:
    std::string m_name;
    std::string m_value;
};
} // namespace AVEVA
