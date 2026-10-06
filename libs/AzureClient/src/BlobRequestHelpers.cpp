#include "BlobRequestHelpers.hpp"

#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobServiceClient.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/ITokenCredential.hpp"
#include "AVEVA/AzureClient/Models/BlobContainerModels.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "BlobStorageErrorCategory.hpp"
#include "BlobXmlParser.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/url/encode.hpp> // IWYU pragma: keep (symbol is defined in a Boost impl/ header)
#include <boost/url/parse.hpp>
#include <boost/url/pct_string_view.hpp>
#include <boost/url/rfc/unreserved_chars.hpp>
#include <boost/url/url.hpp>
#include <boost/url/url_view.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iterator>

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <format>
#include <limits>
#include <locale>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Private
{
    namespace
    {
        constexpr std::size_t MinContainerNameLength = 3U;
        constexpr std::size_t MaxContainerNameLength = 63U;
        constexpr std::size_t MaxBlockIdDecodedBytes = 64U;

        [[nodiscard]] constexpr bool IsAsciiWhitespace(char value) noexcept
        {
            switch (value)
            {
            case ' ':
            case '\t':
            case '\n':
            case '\v':
            case '\f':
            case '\r':
                return true;
            default:
                return false;
            }
        }

        [[nodiscard]] constexpr bool IsAsciiAlphaNumeric(char value) noexcept
        {
            return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9');
        }

        [[nodiscard]] boost::urls::url ParseAbsoluteUrl(std::string_view url)
        {
            const auto parseResult = boost::urls::parse_uri(url);
            if (!parseResult.has_value())
            {
                throw std::invalid_argument("ServiceEndpoint must be a valid absolute URL.");
            }

            return {parseResult.value()};
        }

    } // end anonymous namespace

    std::string ToLowerAscii(std::string_view value)
    {
        return boost::algorithm::to_lower_copy(std::string{value}, std::locale::classic());
    }

    [[nodiscard]] std::string_view TrimWhitespace(std::string_view value) noexcept
    {
        while (!value.empty() && IsAsciiWhitespace(value.front()))
        {
            value.remove_prefix(1U);
        }
        while (!value.empty() && IsAsciiWhitespace(value.back()))
        {
            value.remove_suffix(1U);
        }
        return value;
    }

    bool HasHeaderControlCharacters(std::string_view value) noexcept
    {
        return value.find_first_of(std::string_view{"\r\n\0", 3U}) != std::string_view::npos;
    }

    [[nodiscard]] constexpr bool IsBase64Character(char value) noexcept
    {
        return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') ||
               value == '+' || value == '/';
    }

    [[nodiscard]] std::size_t ValidateBase64AndGetDecodedLength(std::string_view value, bool allowEmpty)
    {
        if (value.empty())
        {
            if (allowEmpty)
            {
                return 0U;
            }

            throw std::invalid_argument("Block IDs must be non-empty base64 strings.");
        }
        if ((value.size() % 4U) != 0U)
        {
            throw std::invalid_argument("Value must be a valid base64 string.");
        }

        std::size_t paddingCount = 0U;
        bool seenPadding = false;
        for (const char ch : value)
        {
            if (ch == '=')
            {
                seenPadding = true;
                ++paddingCount;
                if (paddingCount > 2U)
                {
                    throw std::invalid_argument("Value must be a valid base64 string.");
                }
                continue;
            }

            if (seenPadding || !IsBase64Character(ch))
            {
                throw std::invalid_argument("Value must be a valid base64 string.");
            }
        }

        if (paddingCount == 1U && value.back() != '=')
        {
            throw std::invalid_argument("Value must be a valid base64 string.");
        }
        if (paddingCount == 2U && value.substr(value.size() - 2U) != "==")
        {
            throw std::invalid_argument("Value must be a valid base64 string.");
        }

        return ((value.size() / 4U) * 3U) - paddingCount;
    }

    // Reservation hint only; an under- or over-estimate costs at most one reallocation.
    constexpr std::size_t TypicalRequestHeaderCount = 16U;

    namespace
    {
        struct ParsedErrorBody
        {
            std::string Code;
            std::string Message;
            std::string AuthenticationDetail;
        };

        [[nodiscard]] ParsedErrorBody ParseErrorBody(std::string_view xml)
        {
            const auto tree = TryReadXml(xml);
            const XmlNode* root = tree ? tree->Root() : nullptr;
            if (root == nullptr)
            {
                return {};
            }
            return ParsedErrorBody{.Code = GetChildTextOrEmpty(*root, "Code"),
                .Message = GetChildTextOrEmpty(*root, "Message"),
                .AuthenticationDetail = GetChildTextOrEmpty(*root, "AuthenticationErrorDetail")};
        }

        void NormalizeTokenFields(std::shared_ptr<ITokenCredential>& tokenCredential,
            std::vector<std::string>& scopes,
            std::string& bearerToken)
        {
            if (scopes.empty())
            {
                scopes.emplace_back("https://storage.azure.com/.default");
            }
            if (!tokenCredential && !bearerToken.empty())
            {
                tokenCredential =
                    CachingTokenCredential::Create(std::make_shared<StaticTokenCredential>(bearerToken));
                bearerToken.clear();
            }
        }

        // Splits a validated endpoint into "scheme://host[:port]" and its encoded path (no trailing
        // '/', empty for the root) once, so request URLs are built by concatenation.
        struct SplitEndpointParts
        {
            std::string BasePrefix;
            std::string BasePath;
        };

        [[nodiscard]] SplitEndpointParts SplitEndpoint(std::string_view endpoint)
        {
            const boost::urls::url parsed = ParseAbsoluteUrl(endpoint);
            SplitEndpointParts parts;
            parts.BasePrefix = std::string(parsed.scheme()) + "://" + std::string(parsed.encoded_host());
            if (parsed.has_port())
            {
                parts.BasePrefix += ':';
                parts.BasePrefix += std::string(parsed.port());
            }
            parts.BasePath.assign(parsed.encoded_path().begin(), parsed.encoded_path().end());
            while (!parts.BasePath.empty() && parts.BasePath.back() == '/')
            {
                parts.BasePath.pop_back();
            }
            return parts;
        }

        void ValidateEndpoint(std::string_view endpoint)
        {
            if (endpoint.empty())
            {
                throw std::invalid_argument("ServiceEndpoint must not be empty.");
            }

            const std::size_t schemePos = endpoint.find("://");
            if (schemePos == std::string_view::npos || schemePos == 0U)
            {
                throw std::invalid_argument("ServiceEndpoint must be an absolute http or https URL.");
            }

            const std::string scheme = ToLowerAscii(endpoint.substr(0, schemePos));
            if (scheme != "http" && scheme != "https")
            {
                throw std::invalid_argument("ServiceEndpoint must use http or https.");
            }

            // Allow an optional path component (for Azurite or proxy endpoints), but reject query/fragment.
            const std::string_view remainder = endpoint.substr(schemePos + 3U);
            const std::string_view authority = remainder.substr(0, remainder.find('/'));
            if (authority.empty() || authority.front() == ':' || authority.front() == '@')
            {
                throw std::invalid_argument("ServiceEndpoint must contain a host.");
            }
            if (remainder.empty() || remainder.contains('?') || remainder.contains('#'))
            {
                throw std::invalid_argument("ServiceEndpoint must contain at least a host and may include a path but "
                                            "must not contain query or fragment.");
            }
        }

        void ValidateContainerName(std::string_view name)
        {
            // The service's special containers; their names would otherwise fail the length and character rules.
            if (name == "$root" || name == "$logs" || name == "$web")
            {
                return;
            }
            if (name.size() < MinContainerNameLength || name.size() > MaxContainerNameLength)
            {
                throw std::invalid_argument("ContainerName must be between 3 and 63 characters.");
            }
            if (!IsAsciiAlphaNumeric(name.front()) || !IsAsciiAlphaNumeric(name.back()))
            {
                throw std::invalid_argument("ContainerName must start and end with a letter or number.");
            }

            bool previousDash = false;
            for (const char value : name)
            {
                const bool isLowerAlpha = value >= 'a' && value <= 'z';
                const bool isDigit = value >= '0' && value <= '9';
                const bool isDash = value == '-';
                if (!isLowerAlpha && !isDigit && !isDash)
                {
                    throw std::invalid_argument(
                        "ContainerName may contain only lowercase letters, numbers, and dashes.");
                }
                if (previousDash && isDash)
                {
                    throw std::invalid_argument("ContainerName may not contain consecutive dashes.");
                }
                previousDash = isDash;
            }
        }

        void ValidateSharedKey(const SharedKeyCredentialOptions& sharedKey)
        {
            if (sharedKey.AccountName.empty() != sharedKey.AccountKey.empty())
            {
                throw std::invalid_argument(
                    "SharedKey.AccountName and SharedKey.AccountKey must either both be set or both be empty.");
            }
            // The key itself is decoded (and rejected if malformed) once, by the SharedKeySigner.
        }

        // Host of an already-validated endpoint, without userinfo, port or brackets-stripped IPv6 form kept as-is.
        [[nodiscard]] std::string EndpointHost(std::string_view endpoint)
        {
            std::string_view authority = endpoint.substr(endpoint.find("://") + 3U);
            authority = authority.substr(0, authority.find('/'));
            if (const std::size_t at = authority.rfind('@'); at != std::string_view::npos)
            {
                authority.remove_prefix(at + 1U);
            }
            if (authority.starts_with('['))
            {
                return ToLowerAscii(authority.substr(0, authority.find(']') + 1U));
            }
            return ToLowerAscii(authority.substr(0, authority.find(':')));
        }

        [[nodiscard]] bool IsLoopbackEndpoint(std::string_view endpoint)
        {
            const std::string host = EndpointHost(endpoint);
            return host == "localhost" || host == "127.0.0.1" || host == "[::1]";
        }

        // Credentials (bearer tokens, SAS and SharedKey signatures) must not travel in clear text, except to a loopback emulator/proxy.
        void ValidateTokenTransport(std::string_view endpoint)
        {
            const std::string scheme = ToLowerAscii(endpoint.substr(0, endpoint.find("://")));
            if (scheme == "http" && !IsLoopbackEndpoint(endpoint))
            {
                throw std::invalid_argument("Credentials require an https ServiceEndpoint (http is only allowed for loopback).");
            }
        }

        template <class TOptions> void ValidateConnectionOptions(const TOptions& options)
        {
            ValidateEndpoint(options.ServiceEndpoint);
            ValidateSharedKey(options.SharedKey);

            std::size_t credentialCount = 0U;
            credentialCount += options.SasToken.empty() ? 0U : 1U;
            credentialCount += options.SharedKey.AccountName.empty() ? 0U : 1U;
            credentialCount += options.TokenCredential ? 1U : 0U;
            credentialCount += options.BearerToken.empty() ? 0U : 1U;
            if (credentialCount > 1U)
            {
                throw std::invalid_argument(
                    "Specify at most one credential type: SasToken, SharedKey, TokenCredential, or BearerToken.");
            }
            if (credentialCount > 0U)
            {
                ValidateTokenTransport(options.ServiceEndpoint);
            }
        }

        template <class TOptions>
        [[nodiscard]] std::shared_ptr<const ConnectionState> BuildConnection(TOptions& options)
        {
            options.ServiceEndpoint = std::string{TrimTrailingSlashes(options.ServiceEndpoint)};
            options.SasToken = std::string{TrimLeadingQuestionMark(options.SasToken)};
            ValidateConnectionOptions(options);

            auto state = std::make_shared<ConnectionState>();
            state->ServiceEndpoint = std::move(options.ServiceEndpoint);
            state->SasToken = std::move(options.SasToken);
            state->ApiVersion = std::move(options.ApiVersion);
            NormalizeTokenFields(options.TokenCredential, options.TokenScopes, options.BearerToken);
            state->TokenCredential = std::move(options.TokenCredential);
            state->TokenScopes = std::move(options.TokenScopes);
            if (!options.SharedKey.AccountName.empty())
            {
                state->Signer = std::make_shared<const SharedKeySigner>(std::move(options.SharedKey.AccountName),
                    options.SharedKey.AccountKey);
            }
            SplitEndpointParts endpointParts = SplitEndpoint(state->ServiceEndpoint);
            state->BasePrefix = std::move(endpointParts.BasePrefix);
            state->BasePath = std::move(endpointParts.BasePath);
            state->Retry = options.Retry;
            state->DefaultRequestOptions = std::move(options.DefaultRequestOptions);
            return state;
        }

        template <class TOptions>
        void AppendQueryParts(std::string& query, const TOptions& options, std::string_view queryString)
        {
            const auto append = [&query](std::string_view part)
            {
                if (part.empty())
                {
                    return;
                }
                if (!query.empty())
                {
                    query += '&';
                }
                query += part;
            };
            append(TrimLeadingQuestionMark(queryString));
            append(options.SasToken);
        }

        template <class TOptions>
        [[nodiscard]] HttpRequest BuildRequest(const TOptions& options, HttpMethod method, std::string url)
        {
            HttpRequest request;
            request.SetMethod(method);
            request.SetUrl(std::move(url));
            request.GetHeaders().reserve(TypicalRequestHeaderCount);
            AddHeader(request, XMsVersionHeaderName, options.ApiVersion);
            AddHeader(request, XMsDateHeaderName, BuildDateHeaderValue(std::chrono::system_clock::now()));
            AddHeader(request, XMsClientRequestIdHeaderName, CreateClientRequestId());
            return request;
        }

        [[nodiscard]] std::size_t GetDecodedBlockIdLength(std::string_view blockId)
        {
            try
            {
                return ValidateBase64AndGetDecodedLength(blockId, false);
            }
            catch (const std::invalid_argument&)
            {
                throw std::invalid_argument(blockId.empty() ? "Block IDs must be non-empty base64 strings."
                                                            : "Block IDs must be valid base64 strings.");
            }
        }
    } // namespace

    bool IEquals(std::string_view lhs, std::string_view rhs) noexcept
    {
        return boost::algorithm::iequals(lhs, rhs, std::locale::classic());
    }

    bool IStartsWith(std::string_view value, std::string_view prefix) noexcept
    {
        return boost::algorithm::istarts_with(value, prefix, std::locale::classic());
    }

    std::string BuildDateHeaderValue(std::chrono::system_clock::time_point now)
    {
        const auto seconds = std::chrono::floor<std::chrono::seconds>(now);
        const auto days = std::chrono::floor<std::chrono::days>(seconds);
        const std::chrono::year_month_day calendarDate{days};
        const std::chrono::weekday weekday{days};
        const std::chrono::hh_mm_ss timeOfDay{seconds - days};

        return std::format("{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
            ShortWeekdayNames.at(static_cast<std::size_t>(weekday.c_encoding())),
            static_cast<unsigned>(calendarDate.day()),
            MonthNames.at(static_cast<std::size_t>(static_cast<unsigned>(calendarDate.month()) - 1U)),
            static_cast<int>(calendarDate.year()),
            timeOfDay.hours().count(),
            timeOfDay.minutes().count(),
            timeOfDay.seconds().count());
    }

    std::string CreateClientRequestId()
    {
        thread_local boost::uuids::random_generator generator;
        return boost::uuids::to_string(generator());
    }

    BlobStorageError MakeFailureFromCurrentException()
    {
        try
        {
            throw;
        }
        catch (const std::bad_alloc&)
        {
            return MakeClientError(std::make_error_code(std::errc::not_enough_memory), "Out of memory.");
        }
        catch (const std::invalid_argument& e)
        {
            return MakeClientError(std::make_error_code(std::errc::invalid_argument), e.what());
        }
        catch (const std::out_of_range& e)
        {
            return MakeClientError(std::make_error_code(std::errc::invalid_argument), e.what());
        }
        catch (const std::system_error& e)
        {
            return MakeClientError(e.code(), e.what());
        }
        catch (const std::exception& e)
        {
            return MakeClientError(std::make_error_code(std::errc::state_not_recoverable), e.what());
        }
        catch (...)
        {
            return MakeClientError(std::make_error_code(std::errc::state_not_recoverable), "Unknown error.");
        }
    }

    std::span<char> AsChars(std::span<std::byte> bytes) noexcept
    {
        return {reinterpret_cast<char*>(bytes.data()), bytes.size()};
    }

    std::string_view AsChars(std::span<const std::byte> bytes) noexcept
    {
        return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    }

    std::string BytesToString(std::span<const std::byte> bytes)
    {
        return std::string{AsChars(bytes)};
    }

    std::string Base64Encode(std::span<const std::byte> bytes)
    {
        if (bytes.empty())
        {
            return {};
        }

        const auto* data = reinterpret_cast<const unsigned char*>(bytes.data());
        std::string encoded;
        encoded.resize(((bytes.size() + 2U) / 3U) * 4U);
        const int written =
            EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()), data, static_cast<int>(bytes.size()));
        encoded.resize(written > 0 ? static_cast<std::size_t>(written) : 0U);
        return encoded;
    }

    std::string BuildRangeHeaderValue(const Models::BlobByteRange& range)
    {
        if (range.Length.has_value())
        {
            if (*range.Length == 0U)
            {
                throw std::invalid_argument("Range.Length must be greater than zero when specified.");
            }
            if ((*range.Length - 1U) > (std::numeric_limits<std::uint64_t>::max() - range.Offset))
            {
                throw std::invalid_argument("Range exceeds the maximum representable blob offset.");
            }
            return "bytes=" + std::to_string(range.Offset) + '-' + std::to_string(range.Offset + *range.Length - 1U);
        }

        return "bytes=" + std::to_string(range.Offset) + '-';
    }

    RequestFailure DetermineBlobStorageFailure(std::error_code transportError, const HttpResponse& response)
    {
        if (transportError)
        {
            return {.Error = transportError, .Details = std::nullopt};
        }

        if (response.GetStatus() < HttpStatusMultipleChoices)
        {
            return {};
        }

        const std::string headerCode = std::string{FindHeaderValue(response, XMsErrorCodeHeaderName)};
        const ParsedErrorBody errorBody = ParseErrorBody(response.GetBody());
        const std::string errorCode = !headerCode.empty() ? headerCode : errorBody.Code;
        BlobStorageError details;
        details.StatusCode = response.GetStatus();
        details.ErrorCode = errorCode;
        details.Message = errorBody.Message;
        details.AuthenticationDetail = errorBody.AuthenticationDetail;
        details.RequestId = std::string{FindHeaderValue(response, XMsRequestIdHeaderName)};
        BlobStorageErrorCode code =
            errorCode.empty() ? BlobStorageErrorCode::ServiceError : ParseBlobStorageErrorCode(errorCode);
        if (errorCode.empty() && response.GetStatus() == HttpStatusNotModified)
        {
            // A conditional GET/HEAD whose If-None-Match / If-Modified-Since condition failed.
            code = BlobStorageErrorCode::ConditionNotMet;
        }
        return {.Error = make_error_code(code), .Details = std::move(details)};
    }

    RequestFailure MakeInvalidResponseFailure(const HttpResponse& response, std::string message)
    {
        BlobStorageError details;
        details.StatusCode = response.GetStatus();
        details.Message = std::move(message);
        details.RequestId = std::string{FindHeaderValue(response, XMsRequestIdHeaderName)};
        return {.Error = make_error_code(BlobStorageErrorCode::InvalidResponse), .Details = std::move(details)};
    }

    void ApplyTransactionalHashes(HttpRequest& request, std::string_view md5, StringLabel<TransactionalCrc64Tag> crc64)
    {
        AddHeaderIfNotEmpty(request, ContentMd5HeaderName, md5);
        AddHeaderIfNotEmpty(request, XMsContentCrc64HeaderName, crc64.Value);
    }

    std::shared_ptr<const ConnectionState> MakeConnectionState(BlobServiceClientOptions options)
    {
        return BuildConnection(options);
    }

    std::shared_ptr<const ConnectionState> MakeConnectionState(BlobContainerClientOptions options)
    {
        return BuildConnection(options);
    }

    std::shared_ptr<const ConnectionState> MakeConnectionState(BlobClientOptions options)
    {
        return BuildConnection(options);
    }

    ContainerTarget MakeContainerTarget(std::shared_ptr<const ConnectionState> connection, std::string containerName)
    {
        ValidateContainerName(containerName);
        return ContainerTarget{.Connection = std::move(connection), .ContainerName = std::move(containerName)};
    }

    BlobTarget MakeBlobTarget(std::shared_ptr<const ConnectionState> connection,
        std::string containerName,
        std::string blobName)
    {
        ValidateContainerName(containerName);
        if (blobName.empty())
        {
            throw std::invalid_argument("BlobName must not be empty.");
        }
        return BlobTarget{.Connection = std::move(connection),
            .ContainerName = std::move(containerName),
            .BlobName = std::move(blobName)};
    }

    ContainerTarget MakeContainerTarget(const BlobContainerClientOptions& options)
    {
        return MakeContainerTarget(MakeConnectionState(options), options.ContainerName);
    }

    BlobTarget MakeBlobTarget(const BlobClientOptions& options)
    {
        return MakeBlobTarget(MakeConnectionState(options),
            options.ContainerName,
            options.BlobName);
    }

    RequestAuth MakeRequestAuth(const ConnectionState& connection)
    {
        RequestAuth auth;
        if (!connection.SasToken.empty())
        {
            return auth;
        }
        if (connection.Signer)
        {
            auth.Type = RequestAuth::Kind::SharedKey;
            auth.Signer = connection.Signer;
            return auth;
        }
        if (connection.TokenCredential)
        {
            auth.Type = RequestAuth::Kind::Token;
            auth.TokenCredential = connection.TokenCredential;
            auth.TokenScopes = connection.TokenScopes;
        }
        return auth;
    }

    void SendAuthorizedRequestAsync(IHttpClient& httpClient,
        const ConnectionState& connection,
        HttpRequest request,
        IHttpClient::CompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        // Checked here rather than per operation so every request carrying metadata, including the
        // internal commits issued by multi-part uploads, is rejected before reaching the service.
        for (const HttpHeader& header : request.GetHeaders())
        {
            const std::string_view name = header.GetName();
            if (HasHeaderControlCharacters(name) || HasHeaderControlCharacters(header.GetValue()))
            {
                PostCompletion(httpClient,
                    std::move(completion),
                    std::make_error_code(std::errc::invalid_argument),
                    HttpResponse{});
                return;
            }
            if (IStartsWith(name, XMsMetaHeaderPrefix) && !IsValidMetadataName(name.substr(XMsMetaHeaderPrefix.size())))
            {
                PostCompletion(httpClient,
                    std::move(completion),
                    std::error_code{BlobStorageErrorCode::InvalidMetadata},
                    HttpResponse{});
                return;
            }
        }
        SendWithRetryAsync(httpClient,
            MakeRequestAuth(connection),
            connection.Retry,
            std::move(request),
            std::move(completion),
            requestOptions);
    }

    namespace
    {
        // "<prefix><basePath>/<seg1>[/<seg2>][?query]" with pre-encoded segments.
        [[nodiscard]] std::string ComposeUrl(const ConnectionState& connection,
            std::initializer_list<std::string_view> segments,
            std::string_view query)
        {
            std::size_t size = connection.BasePrefix.size() + connection.BasePath.size() + query.size() + 2U;
            for (const std::string_view segment : segments)
            {
                size += segment.size() + 1U;
            }
            std::string url;
            url.reserve(size);
            url += connection.BasePrefix;
            url += connection.BasePath;
            for (const std::string_view segment : segments)
            {
                url += '/';
                url += segment;
            }
            if (!query.empty())
            {
                url += '?';
                url += query;
            }
            return url;
        }
    } // namespace

    std::string BuildBlobUrl(const BlobTarget& target, std::string_view queryString)
    {
        std::string query;
        AppendQueryParts(query, *target.Connection, queryString);
        return ComposeUrl(*target.Connection,
            {UrlEncode(target.ContainerName, {}), UrlEncode(target.BlobName, "/")},
            query);
    }

    std::string BuildContainerUrl(const ContainerTarget& target, std::string_view queryString)
    {
        std::string query = "restype=container";
        AppendQueryParts(query, *target.Connection, queryString);
        return ComposeUrl(*target.Connection, {UrlEncode(target.ContainerName, {})}, query);
    }

    std::string BuildServiceUrl(const ConnectionState& connection, std::string_view queryString)
    {
        std::string query;
        AppendQueryParts(query, connection, queryString);
        // The service root has no path ("https://host?comp=list"); a path-style endpoint keeps its path.
        return ComposeUrl(connection, {}, query);
    }

    HttpRequest BuildBlobRequest(const BlobTarget& target, HttpMethod method, std::string_view queryString)
    {
        return BuildRequest(*target.Connection, method, BuildBlobUrl(target, queryString));
    }

    HttpRequest BuildContainerRequest(const ContainerTarget& target, HttpMethod method, std::string_view queryString)
    {
        return BuildRequest(*target.Connection, method, BuildContainerUrl(target, queryString));
    }

    std::string BuildBlobUrl(const BlobClientOptions& options, std::string_view queryString)
    {
        return BuildBlobUrl(MakeBlobTarget(options), queryString);
    }

    HttpRequest BuildBlobRequest(const BlobClientOptions& options, HttpMethod method, std::string_view queryString)
    {
        return BuildBlobRequest(MakeBlobTarget(options), method, queryString);
    }

    std::string BuildContainerUrl(const BlobContainerClientOptions& options, std::string_view queryString)
    {
        return BuildContainerUrl(MakeContainerTarget(options), queryString);
    }

    HttpRequest BuildContainerRequest(const BlobContainerClientOptions& options,
        HttpMethod method,
        std::string_view queryString)
    {
        return BuildContainerRequest(MakeContainerTarget(options), method, queryString);
    }

    std::string BuildServiceUrl(const BlobServiceClientOptions& options, std::string_view queryString)
    {
        return BuildServiceUrl(*MakeConnectionState(options), queryString);
    }

    void AddHeader(HttpRequest& request, std::string_view name, std::string_view value)
    {
        request.AddHeader(HttpHeader{std::string{name}, std::string{value}});
    }

    void AddHeaderIfNotEmpty(HttpRequest& request, std::string_view name, std::string_view value)
    {
        if (!value.empty())
        {
            AddHeader(request, name, value);
        }
    }

    void ApplyBlobRequestConditions(HttpRequest& request, const Models::BlobRequestConditions& conditions)
    {
        AddHeaderIfNotEmpty(request, XMsLeaseIdHeaderName, conditions.LeaseId);
        AddHeaderIfNotEmpty(request, IfMatchHeaderName, conditions.IfMatch);
        AddHeaderIfNotEmpty(request, IfNoneMatchHeaderName, conditions.IfNoneMatch);
        if (conditions.IfModifiedSince.has_value())
        {
            AddHeaderIfNotEmpty(request, IfModifiedSinceHeaderName, BuildDateHeaderValue(*conditions.IfModifiedSince));
        }
        if (conditions.IfUnmodifiedSince.has_value())
        {
            AddHeaderIfNotEmpty(request,
                IfUnmodifiedSinceHeaderName,
                BuildDateHeaderValue(*conditions.IfUnmodifiedSince));
        }
    }

    void ApplyBlobHttpHeadersForUpload(HttpRequest& request, const Models::BlobHttpHeaders& headers)
    {
        AddHeaderIfNotEmpty(request, XMsBlobContentTypeHeaderName, headers.ContentType);
        AddHeaderIfNotEmpty(request, XMsBlobContentMd5HeaderName, headers.ContentMd5);
        AddHeaderIfNotEmpty(request, XMsBlobCacheControlHeaderName, headers.CacheControl);
        AddHeaderIfNotEmpty(request, XMsBlobContentEncodingHeaderName, headers.ContentEncoding);
        AddHeaderIfNotEmpty(request, XMsBlobContentLanguageHeaderName, headers.ContentLanguage);
        AddHeaderIfNotEmpty(request, XMsBlobContentDispositionHeaderName, headers.ContentDisposition);
    }

    bool IsValidMetadataName(std::string_view name) noexcept
    {
        const auto isAsciiLetter = [](char c) noexcept
        {
            return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
        };
        if (name.empty() || (!isAsciiLetter(name.front()) && name.front() != '_'))
        {
            return false;
        }
        return std::ranges::all_of(name,
            [&](char c)
        {
            return isAsciiLetter(c) || (c >= '0' && c <= '9') || c == '_';
        });
    }

    void ApplyMetadata(HttpRequest& request, const Models::MetadataMap& metadata)
    {
        for (const auto& [key, value] : metadata)
        {
            AddHeader(request, std::string{XMsMetaHeaderPrefix} + key, value);
        }
    }

    void AuthorizeRequest(const SharedKeyCredentialOptions& options, HttpRequest& request)
    {
        AuthorizeRequest(SharedKeySigner{options.AccountName, options.AccountKey}, request);
    }

    void AuthorizeRequest(const SharedKeySigner& signer, HttpRequest& request)
    {
        AddHeader(request, AuthorizationHeaderName, signer.Authorize(request));
    }

    void ValidateBlockId(std::string_view blockId)
    {
        const std::size_t decodedLength = GetDecodedBlockIdLength(blockId);
        if (decodedLength > MaxBlockIdDecodedBytes)
        {
            throw std::invalid_argument("Block IDs must decode to at most 64 bytes.");
        }
    }
} // namespace AVEVA::AzureClient::Private
