#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/pct_string_view.hpp>
#include <boost/url/url_view.hpp>
#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/params.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <format>
#include <iterator>
#include <limits>
#include <locale>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Shared Key authorization: canonicalization of the request, the HMAC-SHA256 signer and the
// Authorization header value. Kept separate so the security-sensitive code can be reviewed in isolation.
namespace AVEVA::AzureClient::Private
{
    namespace
    {
        constexpr std::size_t MaxHmacDigestBytes = EVP_MAX_MD_SIZE;

        struct ParsedUrl
        {
            std::string Path;
            std::vector<std::pair<std::string, std::string>> Query;
        };

        // The storage service sorts canonicalized x-ms-* headers with .NET en-US culture-aware ordering, which
        // is not byte order ('-' is ignored at the primary level, '_' sorts before digits). These sort-key
        // tables and the comparator are ported from the Azure SDK for C++ (MIT, Microsoft Corporation),
        // sdk/storage/azure-storage-common/src/shared_key_policy.cpp.
        constexpr std::array<int, 128> CultureTableLevel0{
            0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,
            0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,
            0x0,   0x0,   0x0,   0x0,   0x0,   0x71c, 0x0,   0x71f, 0x721, 0x723, 0x725, 0x0,   0x0,   0x0,
            0x72d, 0x803, 0x0,   0x0,   0x733, 0x0,   0xd03, 0xd1a, 0xd1c, 0xd1e, 0xd20, 0xd22, 0xd24, 0xd26,
            0xd28, 0xd2a, 0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0x0,   0xe02, 0xe09, 0xe0a, 0xe1a, 0xe21,
            0xe23, 0xe25, 0xe2c, 0xe32, 0xe35, 0xe36, 0xe48, 0xe51, 0xe70, 0xe7c, 0xe7e, 0xe89, 0xe8a, 0xe91,
            0xe99, 0xe9f, 0xea2, 0xea4, 0xea6, 0xea7, 0xea9, 0x0,   0x0,   0x0,   0x743, 0x744, 0x748, 0xe02,
            0xe09, 0xe0a, 0xe1a, 0xe21, 0xe23, 0xe25, 0xe2c, 0xe32, 0xe35, 0xe36, 0xe48, 0xe51, 0xe70, 0xe7c,
            0xe7e, 0xe89, 0xe8a, 0xe91, 0xe99, 0xe9f, 0xea2, 0xea4, 0xea6, 0xea7, 0xea9, 0x0,   0x74c, 0x0,
            0x750, 0x0};

        [[nodiscard]] constexpr std::array<int, 128> MakeCultureTableLevel2()
        {
            std::array<int, 128> table{};
            for (std::size_t index = 0x41; index <= 0x5a; ++index)
            {
                table.at(index) = 0x12;
            }
            return table;
        }

        [[nodiscard]] constexpr std::array<int, 128> MakeCultureTableLevel4()
        {
            std::array<int, 128> table{};
            table.at(0x27) = 0x8012;
            table.at(0x2d) = 0x8212;
            return table;
        }

        constexpr std::array<int, 128> CultureTableLevel2 = MakeCultureTableLevel2();
        constexpr std::array<int, 128> CultureTableLevel4 = MakeCultureTableLevel4();

        [[nodiscard]] bool CultureAwareLess(std::string_view lhs, std::string_view rhs)
        {
            const std::array<const std::array<int, 128>*, 3> tables{&CultureTableLevel0,
                &CultureTableLevel2,
                &CultureTableLevel4};
            const auto weightAt = [](const std::array<int, 128>& table, std::string_view text, std::size_t position)
            {
                if (position >= text.size())
                {
                    return 0x1;
                }
                const auto byte = static_cast<unsigned char>(text[position]);
                return byte < table.size() ? table.at(byte) : 0;
            };

            std::size_t level = 0;
            std::size_t i = 0;
            std::size_t j = 0;
            while (level < tables.size())
            {
                if (level == tables.size() - 1 && i != j)
                {
                    return i > j;
                }
                const int weight1 = weightAt(*tables.at(level), lhs, i);
                const int weight2 = weightAt(*tables.at(level), rhs, j);
                if (weight1 == 0x1 && weight2 == 0x1)
                {
                    i = 0;
                    j = 0;
                    ++level;
                }
                else if (weight1 == weight2)
                {
                    ++i;
                    ++j;
                }
                else if (weight1 == 0)
                {
                    ++i;
                }
                else if (weight2 == 0)
                {
                    ++j;
                }
                else
                {
                    return weight1 < weight2;
                }
            }
            return false;
        }

        [[nodiscard]] ParsedUrl ParseUrl(std::string_view url)
        {
            ParsedUrl parsed;
            const auto parseResult = boost::urls::parse_uri(url);
            if (!parseResult.has_value())
            {
                parsed.Path = "/";
                return parsed;
            }

            const boost::urls::url_view& parsedUrl = parseResult.value();
            parsed.Path = parsedUrl.encoded_path().empty() ? "/" : std::string{parsedUrl.encoded_path()};
            for (const auto& parameter : parsedUrl.encoded_params())
            {
                parsed.Query.emplace_back(ToLowerAscii(parameter.key.decode({true})),
                    parameter.has_value ? parameter.value.decode({true}) : std::string{});
            }
            return parsed;
        }

        [[nodiscard]] std::string_view GetRequestHeaderValue(const HttpRequest& request,
            std::string_view headerName) noexcept
        {
            for (const auto& header : request.GetHeaders())
            {
                if (IEquals(header.GetName(), headerName))
                {
                    return header.GetValue();
                }
            }
            return {};
        }

        [[nodiscard]] std::string_view MethodToString(HttpMethod method)
        {
            switch (method)
            {
            case HttpMethod::Get:
                return "GET";
            case HttpMethod::Put:
                return "PUT";
            case HttpMethod::Head:
                return "HEAD";
            case HttpMethod::Delete:
                return "DELETE";
            case HttpMethod::Post:
                return "POST";
            case HttpMethod::Patch:
                return "PATCH";
            case HttpMethod::Options:
                return "OPTIONS";
            case HttpMethod::Trace:
                return "TRACE";
            case HttpMethod::Connect:
                return "CONNECT";
            }

            throw std::invalid_argument("Unsupported HTTP method.");
        }

        [[nodiscard]] std::vector<unsigned char> Base64Decode(std::string_view value)
        {
            if (value.empty())
            {
                return {};
            }
            if (value.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
            {
                throw std::invalid_argument("SharedKey.AccountKey must be a valid base64 string.");
            }

            const std::size_t decodedLength = ValidateBase64AndGetDecodedLength(value, true);
            std::vector<unsigned char> decoded(((value.size() + 3U) / 4U) * 3U);
            const int written = EVP_DecodeBlock(decoded.data(),
                reinterpret_cast<const unsigned char*>(value.data()),
                static_cast<int>(value.size()));
            if (written < 0)
            {
                throw std::invalid_argument("SharedKey.AccountKey must be a valid base64 string.");
            }

            decoded.resize(decodedLength);
            return decoded;
        }

    } // namespace

    // Reservation hints only; an under- or over-estimate costs at most one reallocation.
    constexpr std::size_t EstimatedCanonicalizedHeaderLength = 32U;
    constexpr std::size_t EstimatedCanonicalizedQueryLength = 24U;

    namespace
    {
        // The service canonicalizes a header value by trimming it and replacing each run of linear whitespace with
        // one space.
        [[nodiscard]] std::string CollapseWhitespace(std::string_view value)
        {
            value = TrimWhitespace(value);
            std::string result;
            result.reserve(value.size());
            bool inRun = false;
            for (const char character : value)
            {
                const bool space = character == ' ' || character == '\t';
                if (space && inRun)
                {
                    continue;
                }
                inRun = space;
                result.push_back(space ? ' ' : character);
            }
            return result;
        }
    } // namespace

    std::string BuildSharedKeyStringToSign(std::string_view accountName, const HttpRequest& request)
    {
        // `parsedUrl` is a fresh local value (not shared with the caller), so its Query vector can be
        // sorted in place instead of being copied into a second vector purely to sort it.
        ParsedUrl parsedUrl = ParseUrl(request.GetUrl());

        const std::vector<HttpHeader>& headers = request.GetHeaders();
        std::vector<std::size_t> canonicalizedHeaderIndices;
        canonicalizedHeaderIndices.reserve(headers.size());
        for (std::size_t index = 0; index < headers.size(); ++index)
        {
            if (IStartsWith(headers.at(index).GetName(), "x-ms-"))
            {
                canonicalizedHeaderIndices.push_back(index);
            }
        }
        // Names are lowercased before comparing because the culture-aware tables only rank lowercase letters.
        std::vector<std::string> lowercaseNames(headers.size());
        for (const std::size_t index : canonicalizedHeaderIndices)
        {
            lowercaseNames.at(index) = ToLowerAscii(headers.at(index).GetName());
        }
        std::ranges::stable_sort(canonicalizedHeaderIndices,
            [&lowercaseNames](std::size_t lhs, std::size_t rhs)
        {
            return CultureAwareLess(lowercaseNames.at(lhs), lowercaseNames.at(rhs));
        });

        std::string canonicalizedHeadersText;
        canonicalizedHeadersText.reserve(canonicalizedHeaderIndices.size() * EstimatedCanonicalizedHeaderLength);
        // Repeated header names are combined into one `name:v1,v2` line, in their original order.
        for (std::size_t position = 0; position < canonicalizedHeaderIndices.size(); ++position)
        {
            const HttpHeader& header = headers.at(canonicalizedHeaderIndices.at(position));
            if (position == 0 ||
                !IEquals(headers.at(canonicalizedHeaderIndices.at(position - 1)).GetName(), header.GetName()))
            {
                if (position != 0)
                {
                    canonicalizedHeadersText.push_back('\n');
                }
                std::format_to(std::back_inserter(canonicalizedHeadersText),
                    "{}:{}",
                    lowercaseNames.at(canonicalizedHeaderIndices.at(position)),
                    CollapseWhitespace(header.GetValue()));
            }
            else
            {
                std::format_to(std::back_inserter(canonicalizedHeadersText),
                    ",{}",
                    CollapseWhitespace(header.GetValue()));
            }
        }
        if (!canonicalizedHeadersText.empty())
        {
            canonicalizedHeadersText.push_back('\n');
        }

        std::ranges::sort(parsedUrl.Query);

        std::string canonicalizedResource;
        canonicalizedResource.reserve(accountName.size() + parsedUrl.Path.size() +
                                      (parsedUrl.Query.size() * EstimatedCanonicalizedQueryLength) + 1U);
        std::format_to(std::back_inserter(canonicalizedResource), "/{}{}", accountName, parsedUrl.Path);
        // Repeated query parameters are combined into one `name:v1,v2` line with sorted values.
        for (std::size_t position = 0; position < parsedUrl.Query.size(); ++position)
        {
            const auto& [name, value] = parsedUrl.Query.at(position);
            if (position != 0 && parsedUrl.Query.at(position - 1).first == name)
            {
                std::format_to(std::back_inserter(canonicalizedResource), ",{}", value);
            }
            else
            {
                std::format_to(std::back_inserter(canonicalizedResource), "\n{}:{}", name, value);
            }
        }

        const std::size_t bodySize = request.GetBodySize();
        const std::string_view method = MethodToString(request.GetMethod());
        const std::array<std::string_view, 11> fields{GetRequestHeaderValue(request, ContentEncodingHeaderName),
            GetRequestHeaderValue(request, ContentLanguageHeaderName),
            std::string_view{},
            GetRequestHeaderValue(request, ContentMd5HeaderName),
            GetRequestHeaderValue(request, ContentTypeHeaderName),
            std::string_view{}, // Date slot: always empty because x-ms-date is used instead
            GetRequestHeaderValue(request, IfModifiedSinceHeaderName),
            GetRequestHeaderValue(request, IfMatchHeaderName),
            GetRequestHeaderValue(request, IfNoneMatchHeaderName),
            GetRequestHeaderValue(request, IfUnmodifiedSinceHeaderName),
            GetRequestHeaderValue(request, RangeHeaderName)};
        constexpr std::size_t ContentLengthField = 2;

        // The string-to-sign is assembled in one reserved buffer rather than through intermediate strings.
        std::string stringToSign;
        std::size_t reservedSize = method.size() + 1U + canonicalizedHeadersText.size() + canonicalizedResource.size() +
                                   20U /* decimal Content-Length digits */ + fields.size() + 1U;
        for (const std::string_view field : fields)
        {
            reservedSize += field.size();
        }
        stringToSign.reserve(reservedSize);
        stringToSign.append(method).push_back('\n');
        for (std::size_t index = 0; index < fields.size(); ++index)
        {
            if (index == ContentLengthField)
            {
                if (bodySize != 0)
                {
                    std::format_to(std::back_inserter(stringToSign), "{}", bodySize);
                }
            }
            else
            {
                stringToSign.append(fields[index]);
            }
            stringToSign.push_back('\n');
        }
        stringToSign.append(canonicalizedHeadersText).append(canonicalizedResource);
        return stringToSign;
    }

    struct SharedKeySigner::Impl
    {
        std::unique_ptr<EVP_MAC_CTX, decltype(&EVP_MAC_CTX_free)> KeyedContext{nullptr, &EVP_MAC_CTX_free};
    };

    SharedKeySigner::SharedKeySigner(std::string accountName, std::string_view accountKeyBase64)
        : m_accountName(std::move(accountName)), m_impl(std::make_unique<Impl>())
    {
        std::vector<unsigned char> key = Base64Decode(accountKeyBase64);

        std::unique_ptr<EVP_MAC, decltype(&EVP_MAC_free)> const mac(EVP_MAC_fetch(nullptr, "HMAC", nullptr),
            &EVP_MAC_free);
        if (!mac)
        {
            throw std::runtime_error("Failed to initialize Shared Key authorization signature algorithm.");
        }
        m_impl->KeyedContext.reset(EVP_MAC_CTX_new(mac.get()));
        if (!m_impl->KeyedContext)
        {
            throw std::runtime_error("Failed to allocate Shared Key authorization signature context.");
        }

        std::array digestName{'S', 'H', 'A', '2', '5', '6', '\0'};
        std::array<OSSL_PARAM, 2> parameters{
            OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, digestName.data(), 0),
            OSSL_PARAM_construct_end()};
        const int initialized = EVP_MAC_init(m_impl->KeyedContext.get(), key.data(), key.size(), parameters.data());
        OPENSSL_cleanse(key.data(), key.size());
        if (initialized != 1)
        {
            throw std::runtime_error("Failed to initialize Shared Key authorization signature context.");
        }
    }

    SharedKeySigner::~SharedKeySigner() = default;

    std::string SharedKeySigner::Sign(std::string_view stringToSign) const
    {
        // The keyed template context is never updated, so concurrent EVP_MAC_CTX_dup calls are safe.
        std::unique_ptr<EVP_MAC_CTX, decltype(&EVP_MAC_CTX_free)> const context(
            EVP_MAC_CTX_dup(m_impl->KeyedContext.get()),
            &EVP_MAC_CTX_free);
        if (!context || EVP_MAC_update(context.get(),
                            reinterpret_cast<const unsigned char*>(stringToSign.data()),
                            stringToSign.size()) != 1)
        {
            throw std::runtime_error("Failed to compute Shared Key authorization signature.");
        }

        std::array<unsigned char, MaxHmacDigestBytes> digest{};
        std::size_t digestLength = 0;
        if (EVP_MAC_final(context.get(), digest.data(), &digestLength, digest.size()) != 1)
        {
            throw std::runtime_error("Failed to compute Shared Key authorization signature.");
        }
        return Base64Encode(std::as_bytes(std::span{digest.data(), digestLength}));
    }

    std::string SharedKeySigner::Authorize(const HttpRequest& request) const
    {
        return "SharedKey " + m_accountName + ':' + Sign(BuildSharedKeyStringToSign(m_accountName, request));
    }
} // namespace AVEVA::AzureClient::Private
