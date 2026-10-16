// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "RequestValidation.hpp"
#include "HttpVerb.hpp"

#include "AVEVA/HttpClient/HttpClientError.hpp"

#include <algorithm>
#include <boost/beast/http/fields.hpp>
#include <boost/system/system_error.hpp>
#include <boost/url/scheme.hpp>
#include <new>
#include <stdexcept>
#include <string_view>

namespace AVEVA::Private {
bool IsToken(const std::string& value) {
    return !value.empty() && std::all_of(value.begin(), value.end(), [](unsigned char character) {
        return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
               (character >= '0' && character <= '9') ||
               std::string_view("!#$%&'*+-.^_`|~").find(static_cast<char>(character)) != std::string_view::npos;
    });
}

bool IsValidHeader(const std::string& name, const std::string& value) {
    return IsToken(name) && name.size() <= boost::beast::http::fields::max_name_size &&
           value.size() <= boost::beast::http::fields::max_value_size &&
           std::none_of(value.begin(), value.end(), [](unsigned char character) {
               return (character < 32 && character != '\t') || character == 127;
           });
}

bool IsValidRequestUrl(const boost::urls::url_view& url) {
    if (!url.has_authority() || url.host().empty() || url.has_userinfo() ||
        (url.scheme_id() != boost::urls::scheme::http && url.scheme_id() != boost::urls::scheme::https) ||
        (url.has_port() && url.port_number() == 0)) {
        return false;
    }
    const std::string host = url.host_address();
    return std::none_of(host.begin(), host.end(),
                        [](unsigned char character) { return character <= 32 || character >= 127; });
}

std::error_code ValidateRequest(const HttpRequest& request, const HttpRequestOptions& options,
                                const boost::urls::url_view& url) {
    if (!IsValidRequestUrl(url)) {
        return HttpClientError::InvalidUrl;
    }
    const HttpMethod method = request.GetMethod();
    if (ToBeastVerb(method) == boost::beast::http::verb::unknown || method == HttpMethod::Connect ||
        (method == HttpMethod::Trace && request.GetBodySize() != 0) || options.GetTimeout().count() <= 0) {
        return HttpClientError::InvalidRequest;
    }
    const auto& headers = request.GetHeaders();
    if (!std::all_of(headers.begin(), headers.end(),
                     [](const HttpHeader& header) { return IsValidHeader(header.GetName(), header.GetValue()); })) {
        return HttpClientError::InvalidRequest;
    }
    return {};
}

std::error_code ClassifyBuildFailure(std::exception_ptr failure) noexcept {
    try {
        std::rethrow_exception(std::move(failure));
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    } catch (const std::length_error&) {
        return HttpClientError::InvalidRequest;
    } catch (const std::invalid_argument&) {
        return HttpClientError::InvalidRequest;
    } catch (const boost::system::system_error&) {
        return HttpClientError::InvalidRequest;
    } catch (...) {
        return std::make_error_code(std::errc::state_not_recoverable);
    }
}
} // namespace AVEVA::Private
