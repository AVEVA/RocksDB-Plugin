// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/HttpClient/HttpRequest.hpp"
#include "AVEVA/HttpClient/HttpRequestOptions.hpp"

#include <boost/url/url_view.hpp>

#include <exception>
#include <string>
#include <system_error>

namespace AVEVA::Private {
/// <summary>
/// Checks whether a value is a valid HTTP token (RFC 7230 section 3.2.6), as required for header names.
/// </summary>
/// <param name="value">The candidate token.</param>
/// <returns>True if it is a non-empty token.</returns>
bool IsToken(const std::string& value);

/// <summary>
/// Checks that a header can be sent: a token name, a value without control characters, and sizes within
/// what Beast accepts (longer fields make fields::insert throw).
/// </summary>
/// <param name="name">The field name.</param>
/// <param name="value">The field value.</param>
/// <returns>True if the header can be sent.</returns>
bool IsValidHeader(const std::string& name, const std::string& value);

/// <summary>
/// Checks that a parsed URL is one a request may be sent to: http(s), a non-empty host without control,
/// space or non-ASCII characters, no userinfo and no port 0.
/// </summary>
/// <param name="url">The parsed URL.</param>
/// <returns>True if a request may be sent to it.</returns>
bool IsValidRequestUrl(const boost::urls::url_view& url);

/// <summary>
/// The single definition of what may be sent: the URL, method, per-request timeout and every header. Used both to
/// decide pool eligibility and to build the wire request, so the two can never disagree. Called from Route::Of, which
/// is where validation actually happens.
/// </summary>
/// <param name="request">The request to check.</param>
/// <param name="options">The options it would be sent with.</param>
/// <param name="url">The request's URL, already parsed by the caller (which needs it afterwards).</param>
/// <returns>An empty error code if the request may be sent; otherwise InvalidUrl or InvalidRequest.</returns>
std::error_code ValidateRequest(const HttpRequest& request, const HttpRequestOptions& options,
                                const boost::urls::url_view& url);

/// <summary>
/// Maps an exception thrown while building the wire request to the error reported to the caller. Only the
/// errors Beast and the validation layer raise for input that cannot be represented are the request's fault;
/// running out of memory or any other failure is not, and must not be reported as an invalid request.
/// </summary>
/// <param name="failure">The exception that was caught.</param>
/// <returns>InvalidRequest for rejected input, not_enough_memory for bad_alloc, otherwise
/// state_not_recoverable.</returns>
std::error_code ClassifyBuildFailure(std::exception_ptr failure) noexcept;
} // namespace AVEVA::Private
