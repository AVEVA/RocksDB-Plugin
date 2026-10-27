// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "RequestValidation.hpp"
#include "StreamTypes.hpp"

#include <boost/url/parse.hpp>
#include <boost/url/scheme.hpp>
#include <boost/url/url_view.hpp>
#include <gsl/util>

#include <algorithm>
#include <string>
#include <string_view>
#include <system_error>

namespace AVEVA::Private {
/// <summary>
/// Where a request is going and how to address it, from the one URL parse each request gets. The limiter
/// key, the pool key, and the request line and Host header are all derived from it.
/// </summary>
struct Route {
    bool Parsed = false;   // The URL parsed at all.
    std::error_code Error; // Set when the parsed request failed validation; otherwise empty.
    bool Tls = false;
    bool HostIsName = false;
    std::string Host;
    std::string Port;
    std::string Target;    // Request target, always starting with '/'.
    std::string Authority; // The Host header value.

    /// <summary>
    /// Whether the request can be sent at all: the URL parsed and the request passed validation. The client
    /// fails every other route at submission, so nothing past that point ever sees one.
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
        // Scheme and DNS host names are case-insensitive, so they are folded here: the pool, limiter, DNS cache and TLS
        // session keys are all derived from them and must not split one origin into several.
        route.Host = parsed->host_address();
        if (parsed->host_type() == urls::host_type::ipv6) {
            const bool bracketed = route.Host.starts_with('[');
            route.Host = parsed->host_ipv6_address().to_string();
            if (bracketed) {
                route.Host = '[' + route.Host + ']';
            }
        } else if (parsed->host_type() == urls::host_type::ipv4) {
            route.Host = parsed->host_ipv4_address().to_string();
        } else if (parsed->host_type() == urls::host_type::name) {
            // ASCII-only fold: std::tolower follows the process locale.
            std::ranges::transform(route.Host, route.Host.begin(), [](const char c) {
                return c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
            });
        }
        // Keys use the numeric port and the canonical IP text so ":80" and ":080", or two spellings of one IPv6
        // address, share pool and limiter state. The Host header (Authority) keeps the caller's spelling.
        route.Port = parsed->has_port() && !parsed->port().empty() ? std::to_string(parsed->port_number())
                                                                   : (route.Tls ? "443" : "80");
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
    /// The URL scheme this route uses; only http and https routes are ever sendable.
    /// </summary>
    std::string_view SchemeName() const noexcept { return Tls ? "https" : "http"; }

    /// <summary>
    /// The "scheme://host:port" the limiter counts against.
    /// </summary>
    std::string Origin() const {
        const std::string_view scheme = SchemeName();
        std::string origin;
        origin.reserve(scheme.size() + 3 + Host.size() + 1 + Port.size());
        origin.append(scheme).append("://").append(Host).append(1, ':').append(Port);
        return origin;
    }
};
} // namespace AVEVA::Private
