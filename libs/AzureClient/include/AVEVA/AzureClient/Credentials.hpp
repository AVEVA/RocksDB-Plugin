// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/AzureClient/TokenEndpointError.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/awaitable.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <functional>
#include <gsl/pointers>
#include <gsl/zstring>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

// Microsoft Entra ID token credentials that request tokens through an HTTP client (any HttpClientLike).
//
// Lifetime: each credential keeps a non-owning reference to the HTTP client, which must outlive the credential and
// every in-flight GetTokenAsync coroutine. The credential itself must also outlive every in-flight GetTokenAsync:
// the coroutines use `this`, so destroying a credential while a call is pending is undefined behaviour. Each
// GetTokenAsync is a coroutine that runs on the executor of its awaiting coroutine; cancelling it cancels the
// token request.
//
// Every GetTokenAsync sends a token request; wrap the credential in CachingTokenCredential to reuse tokens until
// they approach expiry.
//
// Errors: missing or invalid configuration -> std::errc::invalid_argument (no request is sent); an unreadable
// federated token file -> std::errc::no_such_file_or_directory; a non-2xx token endpoint response ->
// a TokenEndpointErrorCategory code carrying the HTTP status and OAuth `error` (TokenEndpointError.hpp); a 2xx
// response without a usable access_token/expiry ->
// BlobStorageErrorCode::InvalidResponse; transport errors are passed through.
namespace AVEVA::AzureClient {
inline constexpr std::string_view DefaultAuthorityHost = "https://login.microsoftonline.com/";
inline constexpr std::string_view DefaultImdsEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token";

struct ClientSecretCredentialOptions {
    std::string TenantId;
    std::string ClientId;
    std::string ClientSecret;
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    AzureRequestOptions RequestOptions;
    // Retries of transient token endpoint failures (408/429/5xx and transport errors).
    RetryOptions Retry;

    // Reads AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and (when set) AZURE_AUTHORITY_HOST. Unset variables
    // leave the matching field empty, which the credential rejects when asked for a token.
    [[nodiscard]] static ClientSecretCredentialOptions FromEnvironment();
};

struct WorkloadIdentityCredentialOptions {
    std::string TenantId;
    std::string ClientId;
    // File holding the federated (e.g. Kubernetes service account) token; re-read on every request.
    std::string TokenFilePath;
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    AzureRequestOptions RequestOptions;
    // Retries of transient token endpoint failures (408/429/5xx and transport errors).
    RetryOptions Retry;

    // Reads AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE and (when set) AZURE_AUTHORITY_HOST, as
    // injected by Azure Workload Identity.
    [[nodiscard]] static WorkloadIdentityCredentialOptions FromEnvironment();
};

// The signed JWT to present as the client assertion, or the reason it could not be obtained.
using AssertionResult = std::expected<std::string, std::error_code>;

struct ClientAssertionCredentialOptions {
    std::string TenantId;
    std::string ClientId;
    // Supplies a fresh client assertion for every token request. An error from it is passed through as the token
    // error and no token request is sent. It runs in the coroutine that requests the token.
    std::function<boost::asio::awaitable<AssertionResult>()> AssertionProvider;
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    AzureRequestOptions RequestOptions;
    // Retries of transient token endpoint failures (408/429/5xx and transport errors).
    RetryOptions Retry;
};

struct ManagedIdentityCredentialOptions { // A user-assigned identity, by client id or by ARM resource id (at most one);
                                          // both empty selects the
    // system-assigned identity.
    std::string ClientId;
    std::string ResourceId;
    // App Service / Functions / Container Apps identity endpoint and secret header. When IdentityEndpoint is
    // empty the Azure Instance Metadata Service (ImdsEndpoint) is used.
    std::string IdentityEndpoint;
    std::string IdentityHeader;
    std::string ImdsEndpoint = std::string{DefaultImdsEndpoint};
    AzureRequestOptions RequestOptions;
    // Retries of transient token endpoint failures; HTTP 404 and 410 are also retried (IMDS answers these while the
    // identity is still being provisioned).
    RetryOptions Retry;

    // Reads IDENTITY_ENDPOINT and IDENTITY_HEADER (set by App Service, Functions and Container Apps).
    [[nodiscard]] static ManagedIdentityCredentialOptions FromEnvironment();
};

namespace Private {
using Clock = std::chrono::system_clock;

// Accepted token lifetime range; values outside it are treated as an invalid response so a
// hostile value cannot overflow the time point or force a tight refresh loop.
constexpr std::int64_t MinTokenLifetimeSeconds = 1;
constexpr std::int64_t MaxTokenLifetimeSeconds = 24 * 60 * 60;

[[nodiscard]] std::string GetEnvironmentValue(gsl::not_null<gsl::czstring> name);

[[nodiscard]] std::optional<std::int64_t> ParseInteger(std::string_view text) noexcept;

// Managed identity requests carry a secret header, so the endpoint must be https or a local/link-local
// address (App Service uses loopback, IMDS uses 169.254.169.254).
[[nodiscard]] bool IsAcceptableIdentityEndpoint(std::string_view endpoint) noexcept;

// Returns a scalar member as text (numbers are serialized) or an empty string.

// Entra ID returns {"access_token", "expires_in": seconds}; managed identity endpoints return
// "expires_on" (Unix seconds) and possibly a string "expires_in".
[[nodiscard]] std::expected<AccessToken, std::error_code> ParseTokenResponse(std::error_code error,
                                                                             const HttpResponse& response);

[[nodiscard]] inline std::unexpected<std::error_code> InvalidArgument() {
    return std::unexpected(std::make_error_code(std::errc::invalid_argument));
}

// Sends a token endpoint request (with retries) and parses the response into a token.
template <HttpClientLike THttp>
boost::asio::awaitable<TokenResult> SendTokenRequest(THttp& httpClient, HttpRequest request,
                                                     AzureRequestOptions requestOptions, RetryOptions retry,
                                                     bool retryNotFoundAndGone) {
    for (const HttpHeader& header : request.GetHeaders()) {
        if (Private::HasHeaderControlCharacters(header.GetName()) ||
            Private::HasHeaderControlCharacters(header.GetValue())) {
            co_return InvalidArgument();
        }
    }
    HttpResult result =
        co_await Private::SendWithRetry(httpClient, Private::RequestAuth{}, std::shared_ptr<NoTokenCredential>{}, retry,
                                        std::move(request), std::move(requestOptions), retryNotFoundAndGone);
    if (!result.has_value()) {
        co_return std::unexpected(result.error());
    }
    co_return ParseTokenResponse(std::error_code{}, *result);
}

[[nodiscard]] inline std::string FormEncode(std::string_view value) { return Private::UrlEncode(value, {}); }

[[nodiscard]] inline std::string JoinScopes(const std::vector<std::string>& scopes) {
    return scopes | std::views::join_with(' ') | std::ranges::to<std::string>();
}

[[nodiscard]] inline std::string BuildClientAssertionBody(std::string_view clientId, std::string_view assertion,
                                                          const std::vector<std::string>& scopes) {
    return "grant_type=client_credentials&client_id=" + FormEncode(clientId) +
           "&client_assertion_type=" + FormEncode("urn:ietf:params:oauth:client-assertion-type:jwt-bearer") +
           "&client_assertion=" + FormEncode(assertion) + "&scope=" + FormEncode(JoinScopes(scopes));
}

// Tenant ids are GUIDs or domain names; anything else could alter the token endpoint path.
[[nodiscard]] bool IsValidTenantId(std::string_view tenantId) noexcept;

// Rejects authority hosts that would send credentials to an unintended endpoint and returns
// the host without a trailing slash.
[[nodiscard]] std::string NormalizeAuthorityHost(std::string_view authorityHost);

// Tags the tenant id with its own type so it's never adjacent-and-same-type with the
// authority host parameter.
struct TenantIdTag {};

using TenantIdLabel = Private::StringLabel<TenantIdTag>;

[[nodiscard]] HttpRequest BuildEntraTokenRequest(std::string_view authorityHost, TenantIdLabel tenantId,
                                                 std::string body);

[[nodiscard]] std::optional<std::string> ReadTokenFile(const std::string& path);
// Rejects URIs that would send the system access token to an unintended endpoint (plain http, no host,
// userinfo, or a fragment). Throws std::invalid_argument.
void ValidateOidcRequestUri(std::string_view uri);

// Extracts the oidcToken member, or an empty string when the body is not the expected JSON object.
[[nodiscard]] std::string ParseOidcToken(const HttpResponse& response);
} // namespace Private

// Service principal with a client secret (OAuth 2.0 client credentials grant).
template <HttpClientLike THttp> class BasicClientSecretCredential final {
  public:
    BasicClientSecretCredential(THttp& httpClient, ClientSecretCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options)) {
        static_cast<void>(Private::NormalizeAuthorityHost(m_options.AuthorityHost));
    }
    // Shares ownership of the HTTP client, so the credential keeps it alive.
    BasicClientSecretCredential(std::shared_ptr<THttp> httpClient, ClientSecretCredentialOptions options)
        : BasicClientSecretCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        if (!Private::IsValidTenantId(m_options.TenantId) || m_options.ClientId.empty() ||
            m_options.ClientSecret.empty() || scopes.empty()) {
            co_return Private::InvalidArgument();
        }
        std::string body = "grant_type=client_credentials&client_id=" + Private::FormEncode(m_options.ClientId) +
                           "&client_secret=" + Private::FormEncode(m_options.ClientSecret) +
                           "&scope=" + Private::FormEncode(Private::JoinScopes(scopes));
        co_return co_await Private::SendTokenRequest(
            *m_httpClient,
            Private::BuildEntraTokenRequest(m_options.AuthorityHost, m_options.TenantId, std::move(body)),
            m_options.RequestOptions, m_options.Retry, false);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    ClientSecretCredentialOptions m_options;
};

using ClientSecretCredential = BasicClientSecretCredential<HttpClient>;

// Azure Workload Identity: exchanges a federated token file for an Entra ID token (client assertion grant).
template <HttpClientLike THttp> class BasicWorkloadIdentityCredential final {
  public:
    BasicWorkloadIdentityCredential(THttp& httpClient, WorkloadIdentityCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options)) {
        static_cast<void>(Private::NormalizeAuthorityHost(m_options.AuthorityHost));
    }
    // Shares ownership of the HTTP client, so the credential keeps it alive.
    BasicWorkloadIdentityCredential(std::shared_ptr<THttp> httpClient, WorkloadIdentityCredentialOptions options)
        : BasicWorkloadIdentityCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        if (!Private::IsValidTenantId(m_options.TenantId) || m_options.ClientId.empty() ||
            m_options.TokenFilePath.empty() || scopes.empty()) {
            co_return Private::InvalidArgument();
        }
        // Deliberately read inline: the file is a few hundred bytes (a projected service-account token) and the
        // credential sits behind CachingTokenCredential, so this runs about once per token lifetime. Moving it
        // off the io thread would need a client-owned thread or a new executor option, which costs more than the
        // microseconds this blocks for. Revisit if TokenFilePath may point at a slow or network filesystem.
        const std::optional<std::string> assertion = Private::ReadTokenFile(m_options.TokenFilePath);
        if (!assertion.has_value()) {
            co_return std::unexpected(std::make_error_code(std::errc::no_such_file_or_directory));
        }
        std::string body = Private::BuildClientAssertionBody(m_options.ClientId, *assertion, scopes);
        co_return co_await Private::SendTokenRequest(
            *m_httpClient,
            Private::BuildEntraTokenRequest(m_options.AuthorityHost, m_options.TenantId, std::move(body)),
            m_options.RequestOptions, m_options.Retry, false);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    WorkloadIdentityCredentialOptions m_options;
};

using WorkloadIdentityCredential = BasicWorkloadIdentityCredential<HttpClient>;

// Exchanges a caller-supplied client assertion (a signed JWT, for example an OIDC token issued by a CI system) for an
// Entra ID token. The assertion is requested again on every token request.
template <HttpClientLike THttp> class BasicClientAssertionCredential final {
  public:
    BasicClientAssertionCredential(THttp& httpClient, ClientAssertionCredentialOptions options)
        : m_httpClient(&httpClient) {
        static_cast<void>(Private::NormalizeAuthorityHost(options.AuthorityHost));
        m_options = std::make_shared<const ClientAssertionCredentialOptions>(std::move(options));
    }
    // Shares ownership of the HTTP client, so the credential (and requests it has in flight) keep it alive.
    BasicClientAssertionCredential(std::shared_ptr<THttp> httpClient, ClientAssertionCredentialOptions options)
        : BasicClientAssertionCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        // The coroutine frame keeps the shared options (and the HTTP client) alive for the whole call.
        const auto options = m_options;
        const auto owner = m_httpOwner;
        if (!Private::IsValidTenantId(options->TenantId) || options->ClientId.empty() || !options->AssertionProvider ||
            scopes.empty()) {
            co_return Private::InvalidArgument();
        }
        AssertionResult assertion = co_await options->AssertionProvider();
        if (!assertion.has_value()) {
            co_return std::unexpected(assertion.error());
        }
        if (assertion->empty()) {
            co_return Private::InvalidArgument();
        }
        std::string body = Private::BuildClientAssertionBody(options->ClientId, *assertion, scopes);
        co_return co_await Private::SendTokenRequest(
            *m_httpClient, Private::BuildEntraTokenRequest(options->AuthorityHost, options->TenantId, std::move(body)),
            options->RequestOptions, options->Retry, false);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    std::shared_ptr<const ClientAssertionCredentialOptions> m_options;
};

using ClientAssertionCredential = BasicClientAssertionCredential<HttpClient>;

// Managed identity via IMDS (VMs, VM scale sets, AKS) or the App Service identity endpoint. Requests exactly one
// scope, which is converted to a resource ("https://storage.azure.com/.default" -> "https://storage.azure.com").
template <HttpClientLike THttp> class BasicManagedIdentityCredential final {
  public:
    BasicManagedIdentityCredential(THttp& httpClient, ManagedIdentityCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options)) {}
    // Shares ownership of the HTTP client, so the credential keeps it alive.
    BasicManagedIdentityCredential(std::shared_ptr<THttp> httpClient, ManagedIdentityCredentialOptions options)
        : BasicManagedIdentityCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        const bool appService = !m_options.IdentityEndpoint.empty();
        if (scopes.size() != 1U || scopes.front().empty() ||
            (!m_options.ClientId.empty() && !m_options.ResourceId.empty()) ||
            (appService && m_options.IdentityHeader.empty()) || (!appService && m_options.ImdsEndpoint.empty()) ||
            !Private::IsAcceptableIdentityEndpoint(appService ? m_options.IdentityEndpoint : m_options.ImdsEndpoint)) {
            co_return Private::InvalidArgument();
        }
        std::string_view resource = scopes.front();
        if (constexpr std::string_view DefaultSuffix = "/.default"; resource.ends_with(DefaultSuffix)) {
            resource.remove_suffix(DefaultSuffix.size());
        }

        std::vector<std::pair<std::string, std::string>> query{
            {"api-version", appService ? "2019-08-01" : "2018-02-01"}, {"resource", std::string{resource}}};
        if (!m_options.ClientId.empty()) {
            query.emplace_back("client_id", m_options.ClientId);
        }
        if (!m_options.ResourceId.empty()) {
            query.emplace_back(appService ? "mi_res_id" : "msi_res_id", m_options.ResourceId);
        }
        const std::string& endpoint = appService ? m_options.IdentityEndpoint : m_options.ImdsEndpoint;

        HttpRequest request;
        request.SetMethod(HttpMethod::Get);
        request.SetUrl(endpoint + (!endpoint.contains('?') ? "?" : "&") + Private::BuildQueryString(query));
        if (appService) {
            request.AddHeader(HttpHeader{"X-IDENTITY-HEADER", m_options.IdentityHeader});
        } else {
            request.AddHeader(HttpHeader{"Metadata", "true"});
        }
        co_return co_await Private::SendTokenRequest(*m_httpClient, std::move(request), m_options.RequestOptions,
                                                     m_options.Retry, true);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    ManagedIdentityCredentialOptions m_options;
};

using ManagedIdentityCredential = BasicManagedIdentityCredential<HttpClient>;

struct AzurePipelinesCredentialOptions {
    std::string TenantId;
    std::string ClientId;
    std::string ServiceConnectionId;
    std::string SystemAccessToken;
    // The Azure Pipelines OIDC token endpoint; defaults to the SYSTEM_OIDCREQUESTURI environment variable.
    // Must be an https URL (the construction throws std::invalid_argument otherwise): the system access token is sent
    // to it.
    std::string OidcRequestUri;
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    AzureRequestOptions RequestOptions;
    // Retries of transient failures of the OIDC token request and of the token exchange.
    RetryOptions Retry;
};

// Authenticates as a Microsoft Entra ID application through an Azure Pipelines service connection using workload
// identity federation: the OIDC token Azure Pipelines issues for the service connection is exchanged for an access
// token with a client-assertion grant. Only the OIDC token request lives here; the exchange is
// BasicClientAssertionCredential. Missing OidcRequestUri, ServiceConnectionId or SystemAccessToken ->
// std::errc::invalid_argument; a non-2xx OIDC response -> BlobStorageErrorCode::AuthenticationFailed; a 2xx response
// without an oidcToken -> BlobStorageErrorCode::InvalidResponse.
template <HttpClientLike THttp> class BasicAzurePipelinesCredential final {
  public:
    BasicAzurePipelinesCredential(THttp& httpClient, AzurePipelinesCredentialOptions options)
        : m_exchange(httpClient, MakeExchangeOptions(httpClient, nullptr, std::move(options))) {}
    // Shares ownership of the HTTP client; the OIDC token request keeps it alive too.
    BasicAzurePipelinesCredential(std::shared_ptr<THttp> httpClient, AzurePipelinesCredentialOptions options)
        : m_exchange(httpClient,
                     MakeExchangeOptions(Private::RequireHttpClient(httpClient), httpClient, std::move(options))) {}

    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        co_return co_await m_exchange.GetTokenAsync(std::move(scopes));
    }

  private:
    static constexpr unsigned int FirstSuccess = 200;
    static constexpr unsigned int LastSuccess = 299;

    static HttpRequest BuildOidcRequest(const AzurePipelinesCredentialOptions& options) {
        const auto separator = options.OidcRequestUri.find('?') == std::string::npos ? '?' : '&';
        HttpRequest request;
        request.SetMethod(HttpMethod::Post);
        request.SetUrl(options.OidcRequestUri + separator +
                       "api-version=7.1&serviceConnectionId=" + Private::FormEncode(options.ServiceConnectionId));
        request.AddHeader(HttpHeader{"Content-Type", "application/json"});
        request.AddHeader(HttpHeader{"Authorization", "Bearer " + options.SystemAccessToken});
        request.SetBody(std::string{});
        return request;
    }

    static ClientAssertionCredentialOptions MakeExchangeOptions(THttp& httpClient, std::shared_ptr<THttp> owner,
                                                                AzurePipelinesCredentialOptions options) {
        if (options.OidcRequestUri.empty()) {
            options.OidcRequestUri = Private::GetEnvironmentValue("SYSTEM_OIDCREQUESTURI");
        }
        // The system access token is sent as a bearer token to this URI, so refuse anything but https.
        // An empty value is reported when a token is requested, as before.
        if (!options.OidcRequestUri.empty()) {
            Private::ValidateOidcRequestUri(options.OidcRequestUri);
        }

        ClientAssertionCredentialOptions exchange;
        exchange.TenantId = options.TenantId;
        exchange.ClientId = options.ClientId;
        exchange.AuthorityHost = options.AuthorityHost;
        exchange.RequestOptions = options.RequestOptions;
        exchange.Retry = options.Retry;
        // The provider owns a copy of everything it needs, so in-flight requests do not depend on the credential.
        auto shared = std::make_shared<const AzurePipelinesCredentialOptions>(std::move(options));
        exchange.AssertionProvider = [&httpClient, owner = std::move(owner),
                                      shared = std::move(shared)]() -> boost::asio::awaitable<AssertionResult> {
            return RequestOidcToken(httpClient, owner, shared);
        };
        return exchange;
    }

    // A static coroutine with by-value parameters, so its frame owns the client and options it uses.
    static boost::asio::awaitable<AssertionResult>
    RequestOidcToken(THttp& httpClient, std::shared_ptr<THttp> owner,
                     std::shared_ptr<const AzurePipelinesCredentialOptions> options) {
        if (options->OidcRequestUri.empty() || options->ServiceConnectionId.empty() ||
            options->SystemAccessToken.empty()) {
            co_return std::unexpected(std::make_error_code(std::errc::invalid_argument));
        }
        HttpResult result =
            co_await Private::SendWithRetry(httpClient, Private::RequestAuth{}, std::shared_ptr<NoTokenCredential>{},
                                            options->Retry, BuildOidcRequest(*options), options->RequestOptions, false);
        if (!result.has_value()) {
            co_return std::unexpected(result.error());
        }
        const HttpResponse& response = *result;
        if (response.GetStatus() < FirstSuccess || response.GetStatus() > LastSuccess) {
            co_return std::unexpected(make_error_code(BlobStorageErrorCode::AuthenticationFailed));
        }
        auto oidcToken = Private::ParseOidcToken(response);
        if (oidcToken.empty()) {
            co_return std::unexpected(make_error_code(BlobStorageErrorCode::InvalidResponse));
        }
        co_return std::move(oidcToken);
    }

    BasicClientAssertionCredential<THttp> m_exchange;
};

using AzurePipelinesCredential = BasicAzurePipelinesCredential<HttpClient>;
} // namespace AVEVA::AzureClient
