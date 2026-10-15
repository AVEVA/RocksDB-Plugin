// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <string>

namespace AVEVA {
/// <summary>
/// A single HTTP header field: a name and its value. Neither is validated here.
/// </summary>
class HttpHeader {
  public:
    /// <summary>
    /// Constructs a header with an empty name and value.
    /// </summary>
    HttpHeader();

    /// <summary>
    /// Constructs a header from a name and a value.
    /// </summary>
    /// <param name="name">The field name.</param>
    /// <param name="value">The field value.</param>
    HttpHeader(std::string name, std::string value);

    /// <summary>
    /// The field name.
    /// </summary>
    /// <returns>The name, empty on a default-constructed header.</returns>
    const std::string& GetName() const noexcept;

    /// <summary>
    /// The field value.
    /// </summary>
    /// <returns>The value, empty on a default-constructed header.</returns>
    const std::string& GetValue() const noexcept;

    /// <summary>
    /// Replaces the field name.
    /// </summary>
    /// <param name="name">The new field name.</param>
    void SetName(std::string name);

    /// <summary>
    /// Replaces the field value.
    /// </summary>
    /// <param name="value">The new field value.</param>
    void SetValue(std::string value);

  private:
    std::string m_name;
    std::string m_value;
};
} // namespace AVEVA
