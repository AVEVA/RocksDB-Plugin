#include "AVEVA/HttpClient/HttpRequest.hpp"

#include <utility>

namespace AVEVA
{
    HttpRequest::HttpRequest() = default;

    HttpMethod HttpRequest::GetMethod() const noexcept
    {
        return m_method;
    }

    const std::string& HttpRequest::GetUrl() const noexcept
    {
        return m_url;
    }

    const std::vector<HttpHeader>& HttpRequest::GetHeaders() const noexcept
    {
        return m_headers;
    }

    std::vector<HttpHeader>& HttpRequest::GetHeaders() noexcept
    {
        return m_headers;
    }

    const std::string& HttpRequest::GetBody() const noexcept
    {
        return m_body;
    }

    void HttpRequest::SetMethod(HttpMethod method) noexcept
    {
        m_method = method;
    }

    void HttpRequest::SetUrl(std::string url)
    {
        m_url = std::move(url);
    }

    void HttpRequest::SetHeaders(std::vector<HttpHeader> headers)
    {
        m_headers = std::move(headers);
    }

    void HttpRequest::AddHeader(HttpHeader header)
    {
        m_headers.push_back(std::move(header));
    }

    void HttpRequest::ReserveHeaders(std::size_t count)
    {
        m_headers.reserve(count);
    }

    void HttpRequest::SetBody(std::string body)
    {
        m_body = std::move(body);
        m_bodyView = {};
        m_hasBodyView = false;
    }

    void HttpRequest::SetBodyView(std::span<const std::byte> content) noexcept
    {
        m_bodyView = content;
        m_hasBodyView = true;
        m_body.clear();
    }

    bool HttpRequest::HasBodyView() const noexcept
    {
        return m_hasBodyView;
    }

    std::span<const std::byte> HttpRequest::GetBodyView() const noexcept
    {
        return m_bodyView;
    }

    std::size_t HttpRequest::GetBodySize() const noexcept
    {
        return m_hasBodyView ? m_bodyView.size() : m_body.size();
    }
} // namespace AVEVA
