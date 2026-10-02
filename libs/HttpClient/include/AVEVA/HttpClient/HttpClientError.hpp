#pragma once

#include <system_error>
#include <type_traits>

namespace AVEVA
{
    enum class HttpClientError
    {
        InvalidUrl = 1,
        InvalidRequest,
        ResolveFailed,
        ConnectFailed,
        TlsFailed,
        WriteFailed,
        ReadFailed,
        TimedOut,
        ResponseTooLarge,
        ProtocolError
    };

    // Found via ADL by std::error_code's constructor; do not call directly, compare
    // std::error_code values against HttpClientError instead (e.g. error == HttpClientError::TimedOut).
    std::error_code make_error_code(HttpClientError error) noexcept;
} // namespace AVEVA

namespace std
{
    template <> struct is_error_code_enum<AVEVA::HttpClientError> : true_type
    {
    };
} // namespace std
