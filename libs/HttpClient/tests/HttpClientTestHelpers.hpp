#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include <boost/beast/http.hpp>

#include <string>

namespace HttpClientTests
{
    namespace http = boost::beast::http;

    struct ExchangeResult
    {
        std::error_code error;
        AVEVA::HttpResponse response;
        http::request<http::string_body> received;
        std::string authority;
    };

    ExchangeResult Exchange(AVEVA::HttpRequest request,
        std::string wireResponse,
        AVEVA::HttpRequestOptions options = {},
        bool holdResponse = false);
} // namespace HttpClientTests