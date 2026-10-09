// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpMethod.hpp"

#include <boost/beast/http/verb.hpp>

namespace AVEVA::Private
{
    // The public enum stays Beast-free; this is the only place that knows how it maps onto Beast's verbs.
    // Returns verb::unknown for a value outside the enumeration.
    inline boost::beast::http::verb ToBeastVerb(HttpMethod method) noexcept
    {
        using boost::beast::http::verb;
        switch (method)
        {
        case HttpMethod::Get:
            return verb::get;
        case HttpMethod::Post:
            return verb::post;
        case HttpMethod::Put:
            return verb::put;
        case HttpMethod::Patch:
            return verb::patch;
        case HttpMethod::Delete:
            return verb::delete_;
        case HttpMethod::Head:
            return verb::head;
        case HttpMethod::Options:
            return verb::options;
        case HttpMethod::Trace:
            return verb::trace;
        case HttpMethod::Connect:
            return verb::connect;
        default:
            return verb::unknown;
        }
    }
} // namespace AVEVA::Private
