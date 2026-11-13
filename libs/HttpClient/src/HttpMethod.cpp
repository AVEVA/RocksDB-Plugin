// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpMethod.hpp"

#include "private/HttpVerb.hpp"

#include <boost/beast/http/verb.hpp>

#include <ostream> // Needed by Boost's string_view stream operator pulled in via Beast.
namespace AVEVA {
std::string_view ToString(HttpMethod method) {
    const auto verb = Private::ToBeastVerb(method);
    if (verb == boost::beast::http::verb::unknown) {
        return {};
    }
    return boost::beast::http::to_string(verb); // Beast returns views of static literals.
}
} // namespace AVEVA
