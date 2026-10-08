// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpResponse.hpp"

#include <utility>

namespace AVEVA
{
    HttpResponse::HttpResponse() = default;

    HttpResponse::HttpResponse(unsigned int status, std::vector<HttpHeader> headers, std::string body)
        : m_status(status), m_headers(std::move(headers)), m_body(std::move(body))
    {
    }

    unsigned int HttpResponse::GetStatus() const noexcept
    {
        return m_status;
    }

    const std::vector<HttpHeader>& HttpResponse::GetHeaders() const& noexcept
    {
        return m_headers;
    }

    std::vector<HttpHeader> HttpResponse::GetHeaders() && noexcept
    {
        return std::move(m_headers);
    }

    const std::string& HttpResponse::GetBody() const& noexcept
    {
        return m_body;
    }

    std::string HttpResponse::GetBody() && noexcept
    {
        return std::move(m_body);
    }

    void HttpResponse::AddHeader(HttpHeader header)
    {
        m_headers.push_back(std::move(header));
    }
} // namespace AVEVA
