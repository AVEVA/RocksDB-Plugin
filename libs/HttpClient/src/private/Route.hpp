// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "RequestValidation.hpp"
#include "StreamTypes.hpp"

#include <boost/url.hpp>

#include <string>
#include <system_error>

namespace AVEVA::Private {
/// <summary>
/// Where a request is going and how to address it, from the one URL parse each request gets. The limiter
/// key, the pool key, and the request line and Host header are all derived from it.
/// </summary>
struct Route {
    bool Parsed = false;    // The URL parsed at all.
    std::error_code Error;  // Set when the parsed request failed validation; otherwise empty.
    bool Tls = false;
    bool HostIsName = false;
    std::string Scheme;
    std::string Host;
    std::string Port;
    std::string Target;     // Request target, always starting with '/'.
    std::string Authority;  // The Host header value.

    /// <summary>
    /// Whether a pooled connection may be used: the request passes the same validation the operation applies,
    /// so failing an invalid one cannot cost a healthy connection.
    /// </summary>
    bool Sendable() const noexcept { return Parsed && !Error; }

    /// <summary>
    /// Parses and validates the request's URL.
    /// </summary>
    /// <param name="request">The request whose URL to parse.</param>
    /// <param name="options">The options the request is validated against.</param>
    /// <returns>The route; <c>Parsed</c> is false when the URL is not valid.</returns>
    static Route Of(const HttpRequest& request, const HttpRequestOptions& options) {
        Route route;
        const auto parsed = urls::parse_uri(request.GetUrl());
        if (!parsed) {
            return route;
        }
        route.Parsed = true;
        route.Error = ValidateRequest(request, options, *parsed);
        route.Tls = parsed->scheme_id() == urls::scheme::https;
        route.Scheme = std::string(parsed->scheme());
        route.Host = parsed->host_address();
        route.Port = parsed->has_port() ? std::string(parsed->port()) : (route.Tls ? "443" : "80");
        route.HostIsName = parsed->host_type() == urls::host_type::name;
        route.Target = std::string(parsed->encoded_target());
        // With an authority the path is empty or absolute, so an empty path only needs the root slash.
        if (route.Target.empty() || route.Target.front() != '/') {
            route.Target.insert(route.Target.begin(), '/');
        }
        route.Authority = std::string(parsed->encoded_host());
        if (parsed->has_port()) {
            route.Authority += ':';
            route.Authority += parsed->port();
        }
        return route;
    }

    /// <summary>
    /// The "scheme://host:port" the limiter counts against.
    /// </summary>
    std::string Origin() const { return Scheme + "://" + Host + ':' + Port; }
};
} // namespace AVEVA::Private
