// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpRequest.hpp"

#include <algorithm>
#include <string_view>
#include <utility>

namespace AVEVA {
namespace {
// ASCII-only fold: std::tolower follows the process locale, and field names are ASCII tokens.
char FoldCase(char character) noexcept {
    return character >= 'A' && character <= 'Z' ? static_cast<char>(character - 'A' + 'a') : character;
}

bool SameFieldName(std::string_view left, std::string_view right) noexcept {
    return std::ranges::equal(left, right, {}, FoldCase, FoldCase);
}
} // namespace

HttpRequest::HttpRequest() = default;

HttpMethod HttpRequest::GetMethod() const noexcept { return m_method; }

const std::string& HttpRequest::GetUrl() const& noexcept { return m_url; }

std::string HttpRequest::GetUrl() && noexcept { return std::move(m_url); }

const std::vector<HttpHeader>& HttpRequest::GetHeaders() const& noexcept { return m_headers; }

std::vector<HttpHeader> HttpRequest::GetHeaders() && noexcept { return std::move(m_headers); }

HttpBody HttpRequest::GetBody() const noexcept { return m_body; }

void HttpRequest::SetMethod(HttpMethod method) noexcept { m_method = method; }

void HttpRequest::SetUrl(HttpUrl url) noexcept { m_url = std::move(url).Value(); }

void HttpRequest::AddHeader(HttpHeader header) { m_headers.push_back(std::move(header)); }

void HttpRequest::RemoveHeaders(const HeaderName& name) noexcept {
    std::erase_if(m_headers,
                  [&name](const HttpHeader& header) { return SameFieldName(header.GetName(), name.Value()); });
}

void HttpRequest::SetBody(HttpBody body) noexcept { m_body = std::move(body); }
} // namespace AVEVA
