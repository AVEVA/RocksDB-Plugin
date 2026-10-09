// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpMethod.hpp"

#include "private/HttpVerb.hpp"

#include <boost/beast/http/verb.hpp>
#include <ostream>
namespace AVEVA
{
    std::string ToString(HttpMethod method)
    {
        const auto verb = Private::ToBeastVerb(method);
        if (verb == boost::beast::http::verb::unknown)
        {
            return {};
        }
        return std::string(boost::beast::http::to_string(verb));
    }
} // namespace AVEVA
