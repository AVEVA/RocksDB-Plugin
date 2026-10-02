#include "HttpErrorCategory.hpp"

#include "AVEVA/HttpClient/HttpClientError.hpp"

namespace AVEVA::Private
{
    const char* HttpErrorCategory::name() const noexcept
    {
        return "aveva.http-client";
    }

    std::string HttpErrorCategory::message(int value) const
    {
        switch (static_cast<HttpClientError>(value))
        {
        case HttpClientError::InvalidUrl:
            return "Invalid or unsupported HTTP URL";
        case HttpClientError::InvalidRequest:
            return "Invalid HTTP request or options";
        case HttpClientError::ResolveFailed:
            return "Host resolution failed";
        case HttpClientError::ConnectFailed:
            return "Connection failed";
        case HttpClientError::TlsFailed:
            return "TLS setup or handshake failed";
        case HttpClientError::WriteFailed:
            return "Request write failed";
        case HttpClientError::ReadFailed:
            return "Response read failed";
        case HttpClientError::TimedOut:
            return "Request timed out";
        case HttpClientError::ResponseTooLarge:
            return "Response body limit exceeded";
        case HttpClientError::ProtocolError:
            return "Invalid or unsupported HTTP response";
        default:
            return "Unknown HTTP client error";
        }
    }
} // namespace AVEVA::Private

namespace AVEVA
{
    std::error_code make_error_code(HttpClientError error) noexcept
    {
        static Private::HttpErrorCategory category;
        return {static_cast<int>(error), category};
    }
} // namespace AVEVA
