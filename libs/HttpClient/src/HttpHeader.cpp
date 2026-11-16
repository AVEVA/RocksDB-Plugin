// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpHeader.hpp"

#include <utility>

namespace AVEVA {
HttpHeader::HttpHeader(HeaderName name, HeaderValue value) noexcept
    : m_name(std::move(name).Value()), m_value(std::move(value).Value()) {}

const std::string& HttpHeader::GetName() const& noexcept { return m_name; }

std::string HttpHeader::GetName() && noexcept { return std::move(m_name); }

const std::string& HttpHeader::GetValue() const& noexcept { return m_value; }

std::string HttpHeader::GetValue() && noexcept { return std::move(m_value); }
} // namespace AVEVA
