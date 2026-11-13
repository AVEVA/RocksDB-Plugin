// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpRequest.hpp"

#include <boost/container_hash/hash.hpp>

#include <algorithm>
#include <cstddef>
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

std::size_t HttpHeaderNameHash::operator()(const HttpHeader& header) const noexcept {
    // Hash the case-folded name so names differing only in case hash alike.
    std::size_t seed = 0;
    for (const char character : header.GetName()) {
        boost::hash_combine(seed, FoldCase(character));
    }
    return seed;
}

bool HttpHeaderNameEqual::operator()(const HttpHeader& left, const HttpHeader& right) const noexcept {
    return SameFieldName(left.GetName(), right.GetName());
}

HttpRequest::HttpRequest() = default;

HttpMethod HttpRequest::GetMethod() const noexcept { return m_method; }

const std::string& HttpRequest::GetUrl() const& noexcept { return m_url; }

std::string HttpRequest::GetUrl() && noexcept { return std::move(m_url); }

const HttpHeaderSet& HttpRequest::GetHeaders() const& noexcept { return m_headers; }

HttpHeaderSet HttpRequest::GetHeaders() && noexcept { return std::move(m_headers); }

HttpBody HttpRequest::GetBody() const noexcept { return m_body; }

void HttpRequest::SetMethod(HttpMethod method) noexcept { m_method = method; }

void HttpRequest::SetUrl(HttpUrl url) noexcept { m_url = std::move(url).Value(); }

void HttpRequest::AddHeader(HttpHeader header) {
    // A set insert keeps the existing element on a name clash, so erase first to make the last header win.
    m_headers.erase(header);
    m_headers.insert(std::move(header));
}

void HttpRequest::RemoveHeaders(const HeaderName& name) noexcept {
    boost::unordered::erase_if(
        m_headers, [&name](const HttpHeader& header) { return SameFieldName(header.GetName(), name.Value()); });
}

void HttpRequest::SetBody(HttpBody body) noexcept { m_body = std::move(body); }
} // namespace AVEVA
