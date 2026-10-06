#pragma once

#include <boost/url/url_view.hpp>

#include <string>

namespace AVEVA::Private
{
    // Checks whether value is a valid HTTP token (RFC 7230 section 3.2.6), as required for header names.
    bool IsToken(const std::string& value);

    // Checks that a parsed URL is one the client may send a request to: http(s), a non-empty host without
    // control, space or non-ASCII characters, no userinfo and no port 0. Shared by the pooled-connection fast
    // path and BuildRequest so invalid URLs are rejected before a pooled connection is acquired.
    bool IsValidRequestUrl(const boost::urls::url_view& url);
} // namespace AVEVA::Private
