// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "BlobRequestHelpers.hpp"

#include <boost/url/encode.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <boost/url/rfc/unreserved_chars.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Percent-encoding and query-string helpers shared by every request builder.
namespace AVEVA::AzureClient::Private
{
    namespace
    {
        struct ExtraSafeCharacters
        {
            std::string_view Characters;

            [[nodiscard]] bool operator()(char value) const noexcept
            {
                return boost::urls::unreserved_chars(value) || Characters.contains(value);
            }
        };
    } // namespace

    std::string_view TrimTrailingSlashes(std::string_view value) noexcept
    {
        while (!value.empty() && value.back() == '/')
        {
            value.remove_suffix(1U);
        }
        return value;
    }

    std::string_view TrimLeadingQuestionMark(std::string_view value) noexcept
    {
        if (!value.empty() && value.front() == '?')
        {
            value.remove_prefix(1U);
        }
        return value;
    }

    std::string UrlEncode(std::string_view value, std::string_view extraSafeChars)
    {
        return boost::urls::encode(value, ExtraSafeCharacters{extraSafeChars});
    }

    std::string BuildQueryString(const std::vector<std::pair<std::string, std::string>>& parameters)
    {
        std::string query;
        for (const auto& [name, value] : parameters)
        {
            if (!query.empty())
            {
                query.push_back('&');
            }
            query.append(UrlEncode(name, {}));
            query.push_back('=');
            query.append(UrlEncode(value, {}));
        }
        return query;
    }
} // namespace AVEVA::AzureClient::Private
