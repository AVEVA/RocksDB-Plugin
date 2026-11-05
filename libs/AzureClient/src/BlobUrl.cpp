// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>

#include <boost/url/encode.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <boost/url/grammar/string_token.hpp>
#include <boost/url/rfc/unreserved_chars.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Percent-encoding and query-string helpers shared by every request builder.
namespace AVEVA::AzureClient::Private {

std::string_view TrimTrailingSlashes(std::string_view value) noexcept {
    // npos + 1 wraps to 0, which yields the empty view for an all-slash input.
    return value.substr(0U, value.find_last_not_of('/') + 1U);
}

std::string_view TrimLeadingQuestionMark(std::string_view value) noexcept {
    if (!value.empty() && value.front() == '?') {
        value.remove_prefix(1U);
    }
    return value;
}

void UrlEncodeAppend(std::string& out, std::string_view value, std::string_view extraSafeChars) {
    // Boost's CharSet concept requires a predicate, so this stays local rather than becoming an interface.
    const auto isSafe = [extraSafeChars](char character) noexcept {
        return boost::urls::unreserved_chars(character) || extraSafeChars.contains(character);
    };
    boost::urls::encode(value, isSafe, {}, boost::urls::string_token::append_to(out));
}

std::string UrlEncode(std::string_view value, std::string_view extraSafeChars) {
    std::string encoded;
    encoded.reserve(value.size());
    UrlEncodeAppend(encoded, value, extraSafeChars);
    return encoded;
}

std::string BuildQueryString(const std::vector<std::pair<std::string, std::string>>& parameters) {
    std::string query;
    for (const auto& [name, value] : parameters) {
        if (!query.empty()) {
            query.push_back('&');
        }
        UrlEncodeAppend(query, name, {});
        query.push_back('=');
        UrlEncodeAppend(query, value, {});
    }
    return query;
}
} // namespace AVEVA::AzureClient::Private
