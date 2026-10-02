#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <string>
#include <string_view>

// Any future logging of requests, URLs or form bodies MUST pass the text through these helpers first
// (see CONTRIBUTING.md). Nothing in the library logs today; this keeps secrets out when that changes.
namespace AVEVA::AzureClient::Private
{
    inline constexpr std::string_view RedactedValue = "[REDACTED]";

    namespace RedactionDetail
    {
        [[nodiscard]] inline bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) noexcept
        {
            return std::ranges::equal(lhs,
                rhs,
                [](char a, char b)
            {
                return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
            });
        }

        template <std::size_t N>
        [[nodiscard]] bool IsOneOf(std::string_view name, const std::array<std::string_view, N>& names) noexcept
        {
            return std::ranges::any_of(names,
                [name](std::string_view candidate)
            {
                return EqualsIgnoreCase(name, candidate);
            });
        }

        // Replaces the value of every `name=value` pair (separated by '&') whose name is in `names`.
        template <std::size_t N>
        [[nodiscard]] std::string RedactPairs(std::string_view text, const std::array<std::string_view, N>& names)
        {
            std::string result;
            result.reserve(text.size());
            while (!text.empty())
            {
                std::size_t end = std::min(text.find('&'), text.size());
                const std::string_view pair = text.substr(0, end);
                const std::size_t equals = pair.find('=');
                if (equals != std::string_view::npos && IsOneOf(pair.substr(0, equals), names))
                {
                    result.append(pair.substr(0, equals + 1)).append(RedactedValue);
                }
                else
                {
                    result.append(pair);
                }
                if (end < text.size())
                {
                    result.push_back('&');
                    ++end;
                }
                text.remove_prefix(end);
            }
            return result;
        }
    } // namespace RedactionDetail

    // Redacts the SAS signature (and the signed-identifier-bearing `sig`) in a URL's query string.
    [[nodiscard]] inline std::string RedactUrlForDiagnostics(std::string_view url)
    {
        constexpr std::array<std::string_view, 1> SecretParameters{"sig"};
        const std::size_t question = url.find('?');
        if (question == std::string_view::npos)
        {
            return std::string{url};
        }
        const std::size_t fragment = url.find('#', question);
        const std::string_view query = url.substr(question + 1,
            fragment == std::string_view::npos ? std::string_view::npos : fragment - question - 1);
        std::string result{url.substr(0, question + 1)};
        result += RedactionDetail::RedactPairs(query, SecretParameters);
        if (fragment != std::string_view::npos)
        {
            result.append(url.substr(fragment));
        }
        return result;
    }

    // Header values that carry credentials are replaced. `x-ms-copy-source` and any value that is itself an
    // http(s) URL (which usually carries a SAS `sig`) have their query secrets redacted; every other header is
    // returned unchanged. Percent-encoded parameter names such as `%73ig` are not decoded (out of scope).
    [[nodiscard]] inline std::string RedactHeaderForDiagnostics(std::string_view name, std::string_view value)
    {
        constexpr std::array<std::string_view, 5> SecretHeaders{"authorization",
            "proxy-authorization",
            "x-ms-copy-source-authorization",
            "cookie",
            "set-cookie"};
        if (RedactionDetail::IsOneOf(name, SecretHeaders))
        {
            return std::string{RedactedValue};
        }
        const auto startsWithIgnoreCase = [value](std::string_view prefix)
        {
            return value.size() >= prefix.size() &&
                   RedactionDetail::EqualsIgnoreCase(value.substr(0, prefix.size()), prefix);
        };
        if (RedactionDetail::EqualsIgnoreCase(name, "x-ms-copy-source") || startsWithIgnoreCase("http://") ||
            startsWithIgnoreCase("https://"))
        {
            return RedactUrlForDiagnostics(value);
        }
        return std::string{value};
    }

    // Redacts secret fields of an application/x-www-form-urlencoded body (token endpoint requests).
    [[nodiscard]] inline std::string RedactFormBodyForDiagnostics(std::string_view body)
    {
        constexpr std::array<std::string_view, 7> SecretFields{"client_secret",
            "client_assertion",
            "assertion",
            "refresh_token",
            "access_token",
            "password",
            "code"};
        return RedactionDetail::RedactPairs(body, SecretFields);
    }
} // namespace AVEVA::AzureClient::Private
