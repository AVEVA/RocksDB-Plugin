// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>

#include "AVEVA/AzureClient/ITokenCredential.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/post.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/scheme.hpp>
#include <boost/url/url_view.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <fstream>
#include <ios>
#include <iterator>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient
{
    namespace
    {
        using Clock = std::chrono::system_clock;
        using GetTokenCompletionHandler = ITokenCredential::GetTokenCompletionHandler;

        // Accepted token lifetime range; values outside it are treated as an invalid response so a
        // hostile value cannot overflow the time point or force a tight refresh loop.
        constexpr std::int64_t MinTokenLifetimeSeconds = 1;
        constexpr std::int64_t MaxTokenLifetimeSeconds = 24 * 60 * 60;

        [[nodiscard]] std::string GetEnvironmentValue(const char* name)
        {
#ifdef _WIN32
            char* buffer = nullptr;
            std::size_t size = 0;
            if (_dupenv_s(&buffer, &size, name) != 0 || buffer == nullptr)
            {
                return {};
            }
            const std::unique_ptr<char, decltype(&std::free)> owner{buffer, &std::free};
            return std::string{buffer};
#else
            const char* value = std::getenv(name);
            return value == nullptr ? std::string{} : std::string{value};
#endif
        }

        [[nodiscard]] std::optional<std::int64_t> ParseInteger(std::string_view text) noexcept
        {
            std::int64_t value = 0;
            const auto* const end = std::to_address(text.end());
            // Bounded by `end`, so null termination is not required.
            const auto [ptr, ec] = std::from_chars(std::to_address(text.begin()), end, value);
            if (ec != std::errc{} || ptr != end)
            {
                return std::nullopt;
            }
            return value;
        }

        // Managed identity requests carry a secret header, so the endpoint must be https or a local/link-local
        // address (App Service uses loopback, IMDS uses 169.254.169.254).
        [[nodiscard]] bool IsAcceptableIdentityEndpoint(std::string_view endpoint) noexcept
        {
            // Parse rather than prefix-match: prefixes accept hosts such as "127.evil.com" and userinfo tricks
            // such as "127.0.0.1@evil.example".
            const auto parsed = boost::urls::parse_uri(endpoint);
            if (!parsed)
            {
                return false;
            }
            const boost::urls::url_view url = *parsed;
            if (url.has_userinfo())
            {
                return false;
            }
            if (url.scheme_id() == boost::urls::scheme::https)
            {
                return true;
            }
            if (url.scheme_id() != boost::urls::scheme::http)
            {
                return false;
            }
            switch (url.host_type())
            {
            case boost::urls::host_type::ipv4: {
                const auto bytes = url.host_ipv4_address().to_bytes();
                return bytes[0] == 127 || (bytes[0] == 169 && bytes[1] == 254);
            }
            case boost::urls::host_type::ipv6:
                return url.host_ipv6_address().is_loopback();
            case boost::urls::host_type::name:
                return url.encoded_host_name() == "localhost";
            default:
                return false;
            }
        }

        // Returns a scalar member as text (numbers are serialized) or an empty string.
        [[nodiscard]] std::string JsonText(const boost::json::object& object, std::string_view key)
        {
            const auto it = object.find(key);
            if (it == object.end())
            {
                return {};
            }
            if (it->value().is_string())
            {
                return std::string{it->value().as_string()};
            }
            if (it->value().is_number())
            {
                return boost::json::serialize(it->value());
            }
            return {};
        }

        // Entra ID returns {"access_token", "expires_in": seconds}; managed identity endpoints return
        // "expires_on" (Unix seconds) and possibly a string "expires_in".
        [[nodiscard]] std::expected<AccessToken, std::error_code> ParseTokenResponse(std::error_code error,
            const HttpResponse& response)
        {
            if (error)
            {
                return std::unexpected(error);
            }
            constexpr unsigned int MinSuccess = 200;
            constexpr unsigned int MaxSuccess = 299;
            if (response.GetStatus() < MinSuccess || response.GetStatus() > MaxSuccess)
            {
                return std::unexpected(make_error_code(BlobStorageErrorCode::AuthenticationFailed));
            }
            const auto invalid = std::unexpected(make_error_code(BlobStorageErrorCode::InvalidResponse));
            boost::system::error_code parseError;
            const boost::json::value root = boost::json::parse(std::string_view{response.GetBody()}, parseError);
            if (parseError || !root.is_object())
            {
                return invalid;
            }
            const boost::json::object& tree = root.as_object();
            AccessToken token;
            token.Token = JsonText(tree, "access_token");
            if (token.Token.empty())
            {
                return invalid;
            }
            if (const auto expiresIn = ParseInteger(JsonText(tree, "expires_in"));
                expiresIn.has_value() && *expiresIn >= MinTokenLifetimeSeconds)
            {
                // Tokens living longer than the cap are accepted but refreshed at the cap.
                const auto lifetime = std::min(*expiresIn, MaxTokenLifetimeSeconds);
                token.ExpiresOn =
                    std::chrono::time_point_cast<Clock::duration>(Clock::now() + std::chrono::seconds{lifetime});
            }
            else if (const auto expiresOn = ParseInteger(JsonText(tree, "expires_on"));
                expiresOn.has_value() && *expiresOn >= 0)
            {
                const auto now = Clock::now();
                const std::int64_t nowSeconds =
                    std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
                const std::int64_t lifetime = *expiresOn - nowSeconds;
                if (lifetime < MinTokenLifetimeSeconds)
                {
                    return invalid;
                }
                token.ExpiresOn = std::chrono::time_point_cast<Clock::duration>(
                    now + std::chrono::seconds{std::min(lifetime, MaxTokenLifetimeSeconds)});
            }
            else
            {
                return invalid;
            }
            return token;
        }

        void Fail(IHttpClient& httpClient, GetTokenCompletionHandler completion, std::error_code error)
        {
            boost::asio::post(httpClient.get_executor(),
                [completion = std::move(completion), error]() mutable
            {
                completion(error, AccessToken{});
            });
        }

        void SendTokenRequest(IHttpClient& httpClient,
            HttpRequest request,
            HttpRequestOptions requestOptions,
            RetryOptions retry,
            bool retryNotFoundAndGone,
            GetTokenCompletionHandler completion)
        {
            for (const HttpHeader& header : request.GetHeaders())
            {
                if (Private::HasHeaderControlCharacters(header.GetName()) ||
                    Private::HasHeaderControlCharacters(header.GetValue()))
                {
                    Fail(httpClient, std::move(completion), std::make_error_code(std::errc::invalid_argument));
                    return;
                }
            }
            Private::SendWithRetryAsync(httpClient,
                Private::RequestAuth{},
                retry,
                std::move(request),
                [completion = std::move(completion)](std::error_code error, const HttpResponse& response) mutable
            {
                auto token = ParseTokenResponse(error, response);
                if (token.has_value())
                {
                    completion(std::error_code{}, std::move(*token));
                }
                else
                {
                    completion(token.error(), AccessToken{});
                }
            },
                requestOptions,
                retryNotFoundAndGone);
        }

        [[nodiscard]] std::string FormEncode(std::string_view value)
        {
            return Private::UrlEncode(value, {});
        }

        [[nodiscard]] std::string JoinScopes(const std::vector<std::string>& scopes)
        {
            std::string joined;
            for (const std::string& scope : scopes)
            {
                joined += joined.empty() ? "" : " ";
                joined += scope;
            }
            return joined;
        }

        // Tenant ids are GUIDs or domain names; anything else could alter the token endpoint path.
        [[nodiscard]] bool IsValidTenantId(std::string_view tenantId) noexcept
        {
            return !tenantId.empty() && std::ranges::all_of(tenantId,
                                            [](char c)
            {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' ||
                       c == '.';
            });
        }

        // Rejects authority hosts that would send credentials to an unintended endpoint and returns
        // the host without a trailing slash.
        [[nodiscard]] std::string NormalizeAuthorityHost(std::string_view authorityHost)
        {
            std::string_view host = authorityHost;
            while (host.ends_with('/'))
            {
                host.remove_suffix(1);
            }
            const auto parsed = boost::urls::parse_uri(host);
            if (!parsed || parsed->scheme_id() != boost::urls::scheme::https || !parsed->has_authority() ||
                parsed->host().empty())
            {
                throw std::invalid_argument("AuthorityHost must be an https URL with a non-empty host.");
            }
            if (parsed->has_userinfo() || !parsed->encoded_path().empty() || parsed->has_query() ||
                parsed->has_fragment() || host.find('\\') != std::string_view::npos)
            {
                throw std::invalid_argument("AuthorityHost must not contain userinfo, a path, query or fragment.");
            }
            if (parsed->has_port() && parsed->port_number() == 0)
            {
                throw std::invalid_argument("AuthorityHost has an invalid port.");
            }
            return std::string{host};
        }

        // Tags the tenant id with its own type so it's never adjacent-and-same-type with the
        // authority host parameter.
        struct TenantIdTag
        {
        };

        using TenantIdLabel = Private::StringLabel<TenantIdTag>;

        [[nodiscard]] HttpRequest BuildEntraTokenRequest(std::string_view authorityHost,
            TenantIdLabel tenantId,
            std::string body)
        {
            std::string url = NormalizeAuthorityHost(authorityHost);
            url += '/';
            url += tenantId.Value;
            url += "/oauth2/v2.0/token";

            HttpRequest request;
            request.SetMethod(HttpMethod::Post);
            request.SetUrl(std::move(url));
            request.AddHeader(HttpHeader{"Content-Type", "application/x-www-form-urlencoded"});
            request.SetBody(std::move(body));
            return request;
        }

        [[nodiscard]] std::optional<std::string> ReadTokenFile(const std::string& path)
        {
            std::ifstream file{path, std::ios::binary};
            if (!file)
            {
                return std::nullopt;
            }
            std::string content{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
            constexpr std::string_view Whitespace = " \t\r\n";
            const std::size_t first = content.find_first_not_of(Whitespace);
            if (first == std::string::npos)
            {
                return std::nullopt;
            }
            const std::size_t last = content.find_last_not_of(Whitespace);
            content = content.substr(first, last - first + 1);
            if (content.find_first_of(Whitespace) != std::string::npos)
            {
                return std::nullopt;
            }
            return content;
        }
    } // namespace

    ClientSecretCredential::ClientSecretCredential(IHttpClient& httpClient, ClientSecretCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options))
    {
        static_cast<void>(NormalizeAuthorityHost(m_options.AuthorityHost));
    }

    void ClientSecretCredential::GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion)
    {
        if (!IsValidTenantId(m_options.TenantId) || m_options.ClientId.empty() || m_options.ClientSecret.empty() ||
            scopes.empty())
        {
            Fail(*m_httpClient, std::move(completion), std::make_error_code(std::errc::invalid_argument));
            return;
        }
        std::string body = "grant_type=client_credentials&client_id=" + FormEncode(m_options.ClientId) +
                           "&client_secret=" + FormEncode(m_options.ClientSecret) +
                           "&scope=" + FormEncode(JoinScopes(scopes));
        SendTokenRequest(*m_httpClient,
            BuildEntraTokenRequest(m_options.AuthorityHost, m_options.TenantId, std::move(body)),
            m_options.RequestOptions,
            m_options.Retry,
            false,
            std::move(completion));
    }

    WorkloadIdentityCredentialOptions WorkloadIdentityCredentialOptions::FromEnvironment()
    {
        WorkloadIdentityCredentialOptions options;
        options.TenantId = GetEnvironmentValue("AZURE_TENANT_ID");
        options.ClientId = GetEnvironmentValue("AZURE_CLIENT_ID");
        options.TokenFilePath = GetEnvironmentValue("AZURE_FEDERATED_TOKEN_FILE");
        if (std::string authorityHost = GetEnvironmentValue("AZURE_AUTHORITY_HOST"); !authorityHost.empty())
        {
            options.AuthorityHost = std::move(authorityHost);
        }
        return options;
    }

    WorkloadIdentityCredential::WorkloadIdentityCredential(IHttpClient& httpClient,
        WorkloadIdentityCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options))
    {
        static_cast<void>(NormalizeAuthorityHost(m_options.AuthorityHost));
    }

    void WorkloadIdentityCredential::GetTokenAsync(std::vector<std::string> scopes,
        GetTokenCompletionHandler completion)
    {
        if (!IsValidTenantId(m_options.TenantId) || m_options.ClientId.empty() || m_options.TokenFilePath.empty() ||
            scopes.empty())
        {
            Fail(*m_httpClient, std::move(completion), std::make_error_code(std::errc::invalid_argument));
            return;
        }
        const std::optional<std::string> assertion = ReadTokenFile(m_options.TokenFilePath);
        if (!assertion.has_value())
        {
            Fail(*m_httpClient, std::move(completion), std::make_error_code(std::errc::no_such_file_or_directory));
            return;
        }
        std::string body =
            "grant_type=client_credentials&client_id=" + FormEncode(m_options.ClientId) +
            "&client_assertion_type=" + FormEncode("urn:ietf:params:oauth:client-assertion-type:jwt-bearer") +
            "&client_assertion=" + FormEncode(*assertion) + "&scope=" + FormEncode(JoinScopes(scopes));
        SendTokenRequest(*m_httpClient,
            BuildEntraTokenRequest(m_options.AuthorityHost, m_options.TenantId, std::move(body)),
            m_options.RequestOptions,
            m_options.Retry,
            false,
            std::move(completion));
    }

    ManagedIdentityCredentialOptions ManagedIdentityCredentialOptions::FromEnvironment()
    {
        ManagedIdentityCredentialOptions options;
        options.IdentityEndpoint = GetEnvironmentValue("IDENTITY_ENDPOINT");
        options.IdentityHeader = GetEnvironmentValue("IDENTITY_HEADER");
        return options;
    }

    ManagedIdentityCredential::ManagedIdentityCredential(IHttpClient& httpClient,
        ManagedIdentityCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options))
    {
    }

    void ManagedIdentityCredential::GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion)
    {
        const bool appService = !m_options.IdentityEndpoint.empty();
        if (scopes.size() != 1U || scopes.front().empty() ||
            (!m_options.ClientId.empty() && !m_options.ResourceId.empty()) ||
            (appService && m_options.IdentityHeader.empty()) || (!appService && m_options.ImdsEndpoint.empty()) ||
            !IsAcceptableIdentityEndpoint(appService ? m_options.IdentityEndpoint : m_options.ImdsEndpoint))
        {
            Fail(*m_httpClient, std::move(completion), std::make_error_code(std::errc::invalid_argument));
            return;
        }
        std::string_view resource = scopes.front();
        if (constexpr std::string_view DefaultSuffix = "/.default"; resource.ends_with(DefaultSuffix))
        {
            resource.remove_suffix(DefaultSuffix.size());
        }

        std::vector<std::pair<std::string, std::string>> query{
            {"api-version", appService ? "2019-08-01" : "2018-02-01"},
            {"resource", std::string{resource}}};
        if (!m_options.ClientId.empty())
        {
            query.emplace_back("client_id", m_options.ClientId);
        }
        if (!m_options.ResourceId.empty())
        {
            query.emplace_back(appService ? "mi_res_id" : "msi_res_id", m_options.ResourceId);
        }
        const std::string& endpoint = appService ? m_options.IdentityEndpoint : m_options.ImdsEndpoint;

        HttpRequest request;
        request.SetMethod(HttpMethod::Get);
        request.SetUrl(endpoint + (!endpoint.contains('?') ? "?" : "&") + Private::BuildQueryString(query));
        if (appService)
        {
            request.AddHeader(HttpHeader{"X-IDENTITY-HEADER", m_options.IdentityHeader});
        }
        else
        {
            request.AddHeader(HttpHeader{"Metadata", "true"});
        }
        SendTokenRequest(*m_httpClient,
            std::move(request),
            m_options.RequestOptions,
            m_options.Retry,
            true,
            std::move(completion));
    }
} // namespace AVEVA::AzureClient
