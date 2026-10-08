// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpHeader.hpp"

#include <utility>

namespace AVEVA
{
    HttpHeader::HttpHeader() = default;

    HttpHeader::HttpHeader(std::string name, std::string value)
        : m_name(std::move(name)), m_value(std::move(value))
    {
    }

    const std::string& HttpHeader::GetName() const noexcept
    {
        return m_name;
    }

    const std::string& HttpHeader::GetValue() const noexcept
    {
        return m_value;
    }

    void HttpHeader::SetName(std::string name)
    {
        m_name = std::move(name);
    }

    void HttpHeader::SetValue(std::string value)
    {
        m_value = std::move(value);
    }
} // namespace AVEVA
