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
/// <summary>
/// The public Microsoft Entra ID authority that token requests go to unless another is configured.
/// </summary>
inline constexpr std::string_view DefaultAuthorityHost = "https://login.microsoftonline.com/";

/// <summary>
/// The Azure Instance Metadata Service token endpoint that managed identity uses when no identity endpoint is set.
/// </summary>
inline constexpr std::string_view DefaultImdsEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token";

/// <summary>
/// Configuration of a client secret credential.
/// </summary>
struct ClientSecretCredentialOptions {
    /// <summary>The Entra ID tenant, as a GUID or domain name.</summary>
    std::string TenantId;
    /// <summary>The application (client) id of the service principal.</summary>
    std::string ClientId;
    /// <summary>The client secret of the service principal.</summary>
    std::string ClientSecret;
    /// <summary>The Entra ID authority to request tokens from; must be an https URL with no path.</summary>
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    /// <summary>The options applied to each token request.</summary>
    AzureRequestOptions RequestOptions;
    /// <summary>Retries of transient token endpoint failures (408/429/5xx and transport errors).</summary>
    RetryOptions Retry;

    /// <summary>
    /// Reads AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and (when set) AZURE_AUTHORITY_HOST. Unset variables
    /// leave the matching field empty, which the credential rejects when asked for a token.
    /// </summary>
    /// <returns>The options built from the environment.</returns>
    [[nodiscard]] static ClientSecretCredentialOptions FromEnvironment();
};

/// <summary>
/// Configuration of a workload identity credential.
/// </summary>
struct WorkloadIdentityCredentialOptions {
    /// <summary>The Entra ID tenant, as a GUID or domain name.</summary>
    std::string TenantId;
    /// <summary>The application (client) id the federated token is exchanged for.</summary>
    std::string ClientId;
    /// <summary>File holding the federated (e.g. Kubernetes service account) token; re-read on every request.</summary>
    std::string TokenFilePath;
    /// <summary>The Entra ID authority to request tokens from; must be an https URL with no path.</summary>
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    /// <summary>The options applied to each token request.</summary>
    AzureRequestOptions RequestOptions;
    /// <summary>Retries of transient token endpoint failures (408/429/5xx and transport errors).</summary>
    RetryOptions Retry;

    /// <summary>
    /// Reads AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE and (when set) AZURE_AUTHORITY_HOST, as
    /// injected by Azure Workload Identity.
    /// </summary>
    /// <returns>The options built from the environment.</returns>
    [[nodiscard]] static WorkloadIdentityCredentialOptions FromEnvironment();
};

/// <summary>
/// The signed JWT to present as the client assertion, or the reason it could not be obtained.
/// </summary>
using AssertionResult = std::expected<std::string, std::error_code>;

/// <summary>
/// Configuration of a client assertion credential.
/// </summary>
struct ClientAssertionCredentialOptions {
    /// <summary>The Entra ID tenant, as a GUID or domain name.</summary>
    std::string TenantId;
    /// <summary>The application (client) id the assertion is exchanged for.</summary>
    std::string ClientId;
    /// <summary>
    /// Supplies a fresh client assertion for every token request. An error from it is passed through as the token
    /// error and no token request is sent. It runs in the coroutine that requests the token.
    /// </summary>
    std::function<boost::asio::awaitable<AssertionResult>()> AssertionProvider;
    /// <summary>The Entra ID authority to request tokens from; must be an https URL with no path.</summary>
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    /// <summary>The options applied to each token request.</summary>
    AzureRequestOptions RequestOptions;
    /// <summary>Retries of transient token endpoint failures (408/429/5xx and transport errors).</summary>
    RetryOptions Retry;
};

/// <summary>
/// Configuration of a managed identity credential. A user-assigned identity is selected by client id or by ARM
/// resource id (at most one); both empty selects the system-assigned identity.
/// </summary>
struct ManagedIdentityCredentialOptions {
    /// <summary>The client id of a user-assigned identity.</summary>
    std::string ClientId;
    /// <summary>The ARM resource id of a user-assigned identity.</summary>
    std::string ResourceId;
    /// <summary>
    /// App Service / Functions / Container Apps identity endpoint. When empty the Azure Instance Metadata Service
    /// (ImdsEndpoint) is used.
    /// </summary>
    std::string IdentityEndpoint;
    /// <summary>The secret header value that goes with <c>IdentityEndpoint</c>.</summary>
    std::string IdentityHeader;
    /// <summary>The Azure Instance Metadata Service token endpoint.</summary>
    std::string ImdsEndpoint = std::string{DefaultImdsEndpoint};
    /// <summary>The options applied to each token request.</summary>
    AzureRequestOptions RequestOptions;
    /// <summary>
    /// Retries of transient token endpoint failures; HTTP 404 and 410 are also retried (IMDS answers these while the
    /// identity is still being provisioned).
    /// </summary>
    RetryOptions Retry;

    /// <summary>
    /// Reads IDENTITY_ENDPOINT and IDENTITY_HEADER (set by App Service, Functions and Container Apps).
    /// </summary>
    /// <returns>The options built from the environment.</returns>
    [[nodiscard]] static ManagedIdentityCredentialOptions FromEnvironment();
};

namespace Private {
/// <summary>
/// The clock that token lifetimes are measured against.
/// </summary>
using Clock = std::chrono::system_clock;

/// <summary>
/// The shortest accepted token lifetime; values outside the accepted range are treated as an invalid response so a
/// hostile value cannot overflow the time point or force a tight refresh loop.
/// </summary>
constexpr std::int64_t MinTokenLifetimeSeconds = 1;

/// <summary>
/// The longest accepted token lifetime; a token that claims to live longer is accepted but refreshed at this age.
/// </summary>
constexpr std::int64_t MaxTokenLifetimeSeconds = 24 * 60 * 60;

/// <summary>
/// Reads an environment variable, wiping the platform copy of its value on Windows because it may be a secret.
/// </summary>
/// <param name="name">The variable's name.</param>
/// <returns>The value; empty when the variable is unset.</returns>
[[nodiscard]] std::string GetEnvironmentValue(gsl::not_null<gsl::czstring> name);

/// <summary>
/// Parses a whole text as a decimal integer.
/// </summary>
/// <param name="text">The text to parse.</param>
/// <returns>The value; empty when the text is not entirely an integer or does not fit.</returns>
[[nodiscard]] std::optional<std::int64_t> ParseInteger(std::string_view text) noexcept;

/// <summary>
/// Managed identity requests carry a secret header, so the endpoint must be https or a local/link-local
/// address (App Service uses loopback, IMDS uses 169.254.169.254).
/// </summary>
/// <param name="endpoint">The endpoint URL to check.</param>
/// <returns>Whether the endpoint may receive the secret header.</returns>
[[nodiscard]] Validity ValidityOfIdentityEndpoint(std::string_view endpoint) noexcept;

// Returns a scalar member as text (numbers are serialized) or an empty string.

/// <summary>
/// Turns a token endpoint response into a token. Entra ID returns {"access_token", "expires_in": seconds}; managed
/// identity endpoints return "expires_on" (Unix seconds) and possibly a string "expires_in".
/// </summary>
/// <param name="error">The transport error of the request, if any.</param>
/// <param name="response">The response to parse.</param>
/// <returns>
/// The token; <paramref name="error"/> when set; a <c>TokenEndpointErrorCategory</c> code for a non-2xx response;
/// otherwise <c>BlobStorageErrorCode::InvalidResponse</c> when the body lacks a usable token or lifetime.
/// </returns>
[[nodiscard]] std::expected<AccessToken, std::error_code> ParseTokenResponse(std::error_code error,
                                                                             const HttpResponse& response);

/// <summary>
/// The failure for missing or invalid credential configuration, reported before any request is sent.
/// </summary>
/// <returns>An error of <c>std::errc::invalid_argument</c>.</returns>
[[nodiscard]] inline std::unexpected<std::error_code> InvalidArgument() {
    return std::unexpected(std::make_error_code(std::errc::invalid_argument));
}

/// <summary>
/// Sends a token endpoint request (with retries) and parses the response into a token. Header names and values with
/// control characters are refused up front, so they cannot split the request.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
/// <param name="httpClient">The client to send the request with.</param>
/// <param name="request">The token request.</param>
/// <param name="requestOptions">The options applied to the request.</param>
/// <param name="retry">The retry policy for transient failures.</param>
/// <param name="notFoundRetry">Whether an HTTP 404 or 410 is retried as well.</param>
/// <returns>The token; <c>std::errc::invalid_argument</c> for a malformed header; otherwise the failure of the
/// exchange.</returns>
template <HttpClientLike THttp>
boost::asio::awaitable<TokenResult> SendTokenRequest(THttp& httpClient, HttpRequest request,
                                                     AzureRequestOptions requestOptions, RetryOptions retry,
                                                     NotFoundRetry notFoundRetry) {
    for (const HttpHeader& header : request.GetHeaders()) {
        if (Private::ControlCharactersIn(header.GetName()) == ControlCharacters::Present ||
            Private::ControlCharactersIn(header.GetValue()) == ControlCharacters::Present) {
            co_return InvalidArgument();
        }
    }
    HttpResult result =
        co_await Private::SendWithRetry(httpClient, Private::RequestAuth{}, std::shared_ptr<NoTokenCredential>{}, retry,
                                        std::move(request), std::move(requestOptions), notFoundRetry);
    if (!result.has_value()) {
        co_return std::unexpected(result.error());
    }
    co_return ParseTokenResponse(std::error_code{}, *result);
}

/// <summary>
/// Encodes a value for an application/x-www-form-urlencoded body.
/// </summary>
/// <param name="value">The raw value.</param>
/// <returns>The percent-encoded value.</returns>
[[nodiscard]] inline std::string FormEncode(std::string_view value) { return Private::UrlEncode(value, {}); }

/// <summary>
/// Joins scopes into the space-separated form the token endpoint expects.
/// </summary>
/// <param name="scopes">The scopes to join.</param>
/// <returns>The scopes separated by single spaces.</returns>
[[nodiscard]] inline std::string JoinScopes(const std::vector<std::string>& scopes) {
    return scopes | std::views::join_with(' ') | std::ranges::to<std::string>();
}

/// <summary>
/// Builds the form body of a client-credentials request that authenticates with a JWT client assertion.
/// </summary>
/// <param name="clientId">The application (client) id.</param>
/// <param name="assertion">The signed JWT to present.</param>
/// <param name="scopes">The scopes to request.</param>
/// <returns>The form-encoded body.</returns>
[[nodiscard]] inline std::string BuildClientAssertionBody(std::string_view clientId, std::string_view assertion,
                                                          const std::vector<std::string>& scopes) {
    return "grant_type=client_credentials&client_id=" + FormEncode(clientId) +
           "&client_assertion_type=" + FormEncode("urn:ietf:params:oauth:client-assertion-type:jwt-bearer") +
           "&client_assertion=" + FormEncode(assertion) + "&scope=" + FormEncode(JoinScopes(scopes));
}

/// <summary>
/// Tenant ids are GUIDs or domain names; anything else could alter the token endpoint path.
/// </summary>
/// <param name="tenantId">The tenant id to check.</param>
/// <returns>Whether the id is a plausible tenant id.</returns>
[[nodiscard]] Validity ValidityOfTenantId(std::string_view tenantId) noexcept;

/// <summary>
/// Rejects authority hosts that would send credentials to an unintended endpoint and returns
/// the host without a trailing slash.
/// </summary>
/// <param name="authorityHost">The configured authority host.</param>
/// <returns>The host without trailing slashes.</returns>
/// <exception cref="std::invalid_argument">Thrown when the host is not an https URL with a host and no userinfo,
/// path, query, fragment or zero port.</exception>
[[nodiscard]] std::string NormalizeAuthorityHost(std::string_view authorityHost);

/// <summary>
/// Tags the tenant id with its own type so it's never adjacent-and-same-type with the
/// authority host parameter.
/// </summary>
struct TenantIdTag {};

/// <summary>
/// A tenant id that cannot be confused with the authority host when passed as an argument.
/// </summary>
using TenantIdLabel = Private::StringLabel<TenantIdTag>;

/// <summary>
/// Builds the POST of a token request to the tenant's v2.0 token endpoint.
/// </summary>
/// <param name="authorityHost">The authority host; validated and normalized.</param>
/// <param name="tenantId">The tenant id; the caller has validated it.</param>
/// <param name="body">The form-encoded request body.</param>
/// <returns>The request.</returns>
/// <exception cref="std::invalid_argument">Thrown when <paramref name="authorityHost"/> is rejected.</exception>
[[nodiscard]] HttpRequest BuildEntraTokenRequest(std::string_view authorityHost, TenantIdLabel tenantId,
                                                 std::string body);

/// <summary>
/// Reads a token from a file.
/// </summary>
/// <param name="path">The file to read.</param>
/// <returns>The content without surrounding whitespace; empty when the file is unreadable, blank or holds
/// whitespace inside the token.</returns>
[[nodiscard]] std::optional<std::string> ReadTokenFile(const std::string& path);

/// <summary>
/// Rejects URIs that would send the system access token to an unintended endpoint (plain http, no host,
/// userinfo, or a fragment).
/// </summary>
/// <param name="uri">The URI to check.</param>
/// <exception cref="std::invalid_argument">Thrown when the URI is rejected.</exception>
void ValidateOidcRequestUri(std::string_view uri);

/// <summary>
/// Extracts the oidcToken member, or an empty string when the body is not the expected JSON object.
/// </summary>
/// <param name="response">The OIDC token response.</param>
/// <returns>The token text; empty when absent.</returns>
[[nodiscard]] std::string ParseOidcToken(const HttpResponse& response);
} // namespace Private

/// <summary>
/// Service principal with a client secret (OAuth 2.0 client credentials grant).
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
template <HttpClientLike THttp> class BasicClientSecretCredential final {
  public:
    BasicClientSecretCredential(THttp& httpClient, ClientSecretCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options)) {
        static_cast<void>(Private::NormalizeAuthorityHost(m_options.AuthorityHost));
    }
    /// <summary>
    /// Shares ownership of the HTTP client, so the credential keeps it alive.
    /// </summary>
    /// <param name="httpClient">The client to send token requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the client is null or the authority host is
    /// rejected.</exception>
    BasicClientSecretCredential(std::shared_ptr<THttp> httpClient, ClientSecretCredentialOptions options)
        : BasicClientSecretCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    /// <summary>
    /// Requests a token with the client secret.
    /// </summary>
    /// <param name="scopes">The scopes to request; must not be empty.</param>
    /// <returns>The token; <c>std::errc::invalid_argument</c> when the configuration or scopes are unusable (nothing
    /// is sent); otherwise the failure of the token request.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        if (Private::ValidityOfTenantId(m_options.TenantId) == Validity::Invalid || m_options.ClientId.empty() ||
            m_options.ClientSecret.empty() || scopes.empty()) {
            co_return Private::InvalidArgument();
        }
        std::string body = "grant_type=client_credentials&client_id=" + Private::FormEncode(m_options.ClientId) +
                           "&client_secret=" + Private::FormEncode(m_options.ClientSecret) +
                           "&scope=" + Private::FormEncode(Private::JoinScopes(scopes));
        co_return co_await Private::SendTokenRequest(
            *m_httpClient,
            Private::BuildEntraTokenRequest(m_options.AuthorityHost, m_options.TenantId, std::move(body)),
            m_options.RequestOptions, m_options.Retry, NotFoundRetry::Fail);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    ClientSecretCredentialOptions m_options;
};

/// <summary>
/// A client secret credential over the default HTTP client.
/// </summary>
using ClientSecretCredential = BasicClientSecretCredential<HttpClient>;

/// <summary>
/// Azure Workload Identity: exchanges a federated token file for an Entra ID token (client assertion grant).
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
template <HttpClientLike THttp> class BasicWorkloadIdentityCredential final {
  public:
    /// <summary>
    /// Uses a client the credential does not own, which must outlive it.
    /// </summary>
    /// <param name="httpClient">The client to send token requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the authority host is rejected.</exception>
    BasicWorkloadIdentityCredential(THttp& httpClient, WorkloadIdentityCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options)) {
        static_cast<void>(Private::NormalizeAuthorityHost(m_options.AuthorityHost));
    }
    /// <summary>
    /// Shares ownership of the HTTP client, so the credential keeps it alive.
    /// </summary>
    /// <param name="httpClient">The client to send token requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the client is null or the authority host is
    /// rejected.</exception>
    BasicWorkloadIdentityCredential(std::shared_ptr<THttp> httpClient, WorkloadIdentityCredentialOptions options)
        : BasicWorkloadIdentityCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    /// <summary>
    /// Reads the federated token file and exchanges its content for an access token.
    /// </summary>
    /// <param name="scopes">The scopes to request; must not be empty.</param>
    /// <returns>The token; <c>std::errc::invalid_argument</c> when the configuration or scopes are unusable (nothing
    /// is sent); <c>std::errc::no_such_file_or_directory</c> when the file cannot be read; otherwise the failure of
    /// the token request.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        if (Private::ValidityOfTenantId(m_options.TenantId) == Validity::Invalid || m_options.ClientId.empty() ||
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
            m_options.RequestOptions, m_options.Retry, NotFoundRetry::Fail);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    WorkloadIdentityCredentialOptions m_options;
};

/// <summary>
/// A workload identity credential over the default HTTP client.
/// </summary>
using WorkloadIdentityCredential = BasicWorkloadIdentityCredential<HttpClient>;

/// <summary>
/// Exchanges a caller-supplied client assertion (a signed JWT, for example an OIDC token issued by a CI system) for an
/// Entra ID token. The assertion is requested again on every token request.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
template <HttpClientLike THttp> class BasicClientAssertionCredential final {
  public:
    /// <summary>
    /// Uses a client the credential does not own, which must outlive it.
    /// </summary>
    /// <param name="httpClient">The client to send token requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the authority host is rejected.</exception>
    BasicClientAssertionCredential(THttp& httpClient, ClientAssertionCredentialOptions options)
        : m_httpClient(&httpClient) {
        static_cast<void>(Private::NormalizeAuthorityHost(options.AuthorityHost));
        m_options = std::make_shared<const ClientAssertionCredentialOptions>(std::move(options));
    }
    /// <summary>
    /// Shares ownership of the HTTP client, so the credential (and requests it has in flight) keep it alive.
    /// </summary>
    /// <param name="httpClient">The client to send token requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the client is null or the authority host is
    /// rejected.</exception>
    BasicClientAssertionCredential(std::shared_ptr<THttp> httpClient, ClientAssertionCredentialOptions options)
        : BasicClientAssertionCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    /// <summary>
    /// Obtains a fresh assertion from the provider and exchanges it for an access token.
    /// </summary>
    /// <param name="scopes">The scopes to request; must not be empty.</param>
    /// <returns>The token; <c>std::errc::invalid_argument</c> when the configuration, scopes or assertion are unusable
    /// (no token request is sent); the provider's error when it fails; otherwise the failure of the token
    /// request.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        // The coroutine frame keeps the shared options (and the HTTP client) alive for the whole call.
        const auto options = m_options;
        const auto owner = m_httpOwner;
        if (Private::ValidityOfTenantId(options->TenantId) == Validity::Invalid || options->ClientId.empty() ||
            !options->AssertionProvider || scopes.empty()) {
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
            options->RequestOptions, options->Retry, NotFoundRetry::Fail);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    std::shared_ptr<const ClientAssertionCredentialOptions> m_options;
};

/// <summary>
/// A client assertion credential over the default HTTP client.
/// </summary>
using ClientAssertionCredential = BasicClientAssertionCredential<HttpClient>;

/// <summary>
/// Managed identity via IMDS (VMs, VM scale sets, AKS) or the App Service identity endpoint. Requests exactly one
/// scope, which is converted to a resource ("https://storage.azure.com/.default" -> "https://storage.azure.com").
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
template <HttpClientLike THttp> class BasicManagedIdentityCredential final {
  public:
    /// <summary>
    /// Uses a client the credential does not own, which must outlive it.
    /// </summary>
    /// <param name="httpClient">The client to send token requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    BasicManagedIdentityCredential(THttp& httpClient, ManagedIdentityCredentialOptions options)
        : m_httpClient(&httpClient), m_options(std::move(options)) {}
    /// <summary>
    /// Shares ownership of the HTTP client, so the credential keeps it alive.
    /// </summary>
    /// <param name="httpClient">The client to send token requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the client is null.</exception>
    BasicManagedIdentityCredential(std::shared_ptr<THttp> httpClient, ManagedIdentityCredentialOptions options)
        : BasicManagedIdentityCredential(Private::RequireHttpClient(httpClient), std::move(options)) {
        m_httpOwner = std::move(httpClient);
    }

    /// <summary>
    /// Requests a token from the App Service identity endpoint when one is configured, otherwise from IMDS.
    /// </summary>
    /// <param name="scopes">Exactly one scope, converted to the resource the token is for.</param>
    /// <returns>The token; <c>std::errc::invalid_argument</c> when the configuration or scopes are unusable (nothing
    /// is sent); otherwise the failure of the token request.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        enum class IdentitySource : std::uint8_t { Imds, AppService };
        const auto source = m_options.IdentityEndpoint.empty() ? IdentitySource::Imds : IdentitySource::AppService;
        if (scopes.size() != 1U || scopes.front().empty() ||
            (!m_options.ClientId.empty() && !m_options.ResourceId.empty()) ||
            (source == IdentitySource::AppService && m_options.IdentityHeader.empty()) ||
            (source == IdentitySource::Imds && m_options.ImdsEndpoint.empty()) ||
            Private::ValidityOfIdentityEndpoint(source == IdentitySource::AppService
                                                    ? m_options.IdentityEndpoint
                                                    : m_options.ImdsEndpoint) == Validity::Invalid) {
            co_return Private::InvalidArgument();
        }
        std::string_view resource = scopes.front();
        if (constexpr std::string_view DefaultSuffix = "/.default"; resource.ends_with(DefaultSuffix)) {
            resource.remove_suffix(DefaultSuffix.size());
        }

        std::vector<std::pair<std::string, std::string>> query{
            {"api-version", source == IdentitySource::AppService ? "2019-08-01" : "2018-02-01"},
            {"resource", std::string{resource}}};
        if (!m_options.ClientId.empty()) {
            query.emplace_back("client_id", m_options.ClientId);
        }
        if (!m_options.ResourceId.empty()) {
            query.emplace_back(source == IdentitySource::AppService ? "mi_res_id" : "msi_res_id", m_options.ResourceId);
        }
        const std::string& endpoint =
            source == IdentitySource::AppService ? m_options.IdentityEndpoint : m_options.ImdsEndpoint;

        HttpRequest request;
        request.SetMethod(HttpMethod::Get);
        request.SetUrl(endpoint + (!endpoint.contains('?') ? "?" : "&") + Private::BuildQueryString(query));
        if (source == IdentitySource::AppService) {
            request.AddHeader(HttpHeader{"X-IDENTITY-HEADER", m_options.IdentityHeader});
        } else {
            request.AddHeader(HttpHeader{"Metadata", "true"});
        }
        co_return co_await Private::SendTokenRequest(*m_httpClient, std::move(request), m_options.RequestOptions,
                                                     m_options.Retry, NotFoundRetry::Retry);
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    ManagedIdentityCredentialOptions m_options;
};

/// <summary>
/// A managed identity credential over the default HTTP client.
/// </summary>
using ManagedIdentityCredential = BasicManagedIdentityCredential<HttpClient>;

/// <summary>
/// Configuration of an Azure Pipelines credential.
/// </summary>
struct AzurePipelinesCredentialOptions {
    /// <summary>The Entra ID tenant, as a GUID or domain name.</summary>
    std::string TenantId;
    /// <summary>The application (client) id of the service connection's identity.</summary>
    std::string ClientId;
    /// <summary>The id of the service connection to request an OIDC token for.</summary>
    std::string ServiceConnectionId;
    /// <summary>The pipeline's system access token, sent as the bearer token of the OIDC token request.</summary>
    std::string SystemAccessToken;
    /// <summary>
    /// The Azure Pipelines OIDC token endpoint; defaults to the SYSTEM_OIDCREQUESTURI environment variable.
    /// Must be an https URL (the construction throws std::invalid_argument otherwise): the system access token is sent
    /// to it.
    /// </summary>
    std::string OidcRequestUri;
    /// <summary>The Entra ID authority to request tokens from; must be an https URL with no path.</summary>
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    /// <summary>The options applied to each request.</summary>
    AzureRequestOptions RequestOptions;
    /// <summary>Retries of transient failures of the OIDC token request and of the token exchange.</summary>
    RetryOptions Retry;
};

/// <summary>
/// Authenticates as a Microsoft Entra ID application through an Azure Pipelines service connection using workload
/// identity federation: the OIDC token Azure Pipelines issues for the service connection is exchanged for an access
/// token with a client-assertion grant. Only the OIDC token request lives here; the exchange is
/// <c>BasicClientAssertionCredential</c>. Missing OidcRequestUri, ServiceConnectionId or SystemAccessToken ->
/// <c>std::errc::invalid_argument</c>; a non-2xx OIDC response -> <c>BlobStorageErrorCode::AuthenticationFailed</c>;
/// a 2xx response without an oidcToken -> <c>BlobStorageErrorCode::InvalidResponse</c>.
/// </summary>
/// <typeparam name="THttp">The HTTP client type.</typeparam>
template <HttpClientLike THttp> class BasicAzurePipelinesCredential final {
  public:
    /// <summary>
    /// Uses a client the credential does not own, which must outlive it.
    /// </summary>
    /// <param name="httpClient">The client to send requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the authority host or a non-empty OIDC request URI is
    /// rejected.</exception>
    BasicAzurePipelinesCredential(THttp& httpClient, AzurePipelinesCredentialOptions options)
        : m_exchange(httpClient, MakeExchangeOptions(httpClient, nullptr, std::move(options))) {}
    /// <summary>
    /// Shares ownership of the HTTP client; the OIDC token request keeps it alive too.
    /// </summary>
    /// <param name="httpClient">The client to send requests with.</param>
    /// <param name="options">The credential's configuration.</param>
    /// <exception cref="std::invalid_argument">Thrown when the client is null, or the authority host or a non-empty
    /// OIDC request URI is rejected.</exception>
    BasicAzurePipelinesCredential(std::shared_ptr<THttp> httpClient, AzurePipelinesCredentialOptions options)
        : m_exchange(httpClient,
                     MakeExchangeOptions(Private::RequireHttpClient(httpClient), httpClient, std::move(options))) {}

    /// <summary>
    /// Obtains the pipeline's OIDC token and exchanges it for an access token.
    /// </summary>
    /// <param name="scopes">The scopes to request; must not be empty.</param>
    /// <returns>The token, or the failure of the OIDC token request or of the exchange.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        co_return co_await m_exchange.GetTokenAsync(std::move(scopes));
    }

  private:
    /// <summary>
    /// Builds the POST that asks Azure Pipelines for an OIDC token for the service connection.
    /// </summary>
    /// <param name="options">Supplies the endpoint, service connection and system access token.</param>
    /// <returns>The request.</returns>
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

    /// <summary>
    /// Derives the options of the token exchange, whose assertion provider requests the OIDC token.
    /// </summary>
    /// <param name="httpClient">The client the provider sends the OIDC request with.</param>
    /// <param name="owner">Keeps the client alive for the provider when ownership is shared; may be null.</param>
    /// <param name="options">The credential's configuration; an empty OIDC request URI defaults from the
    /// environment.</param>
    /// <returns>The options of the exchange credential.</returns>
    /// <exception cref="std::invalid_argument">Thrown when a non-empty OIDC request URI is rejected.</exception>
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

    /// <summary>
    /// A static coroutine with by-value parameters, so its frame owns the client and options it uses.
    /// </summary>
    /// <param name="httpClient">The client to send the OIDC request with.</param>
    /// <param name="owner">Keeps the client alive for the call when ownership is shared; may be null.</param>
    /// <param name="options">The configuration to take the endpoint and secrets from.</param>
    /// <returns>The OIDC token; <c>std::errc::invalid_argument</c> when the configuration is incomplete;
    /// <c>BlobStorageErrorCode::AuthenticationFailed</c> for a non-2xx response;
    /// <c>BlobStorageErrorCode::InvalidResponse</c> when the response holds no oidcToken; otherwise the transport
    /// failure.</returns>
    static boost::asio::awaitable<AssertionResult>
    RequestOidcToken(THttp& httpClient, std::shared_ptr<THttp> owner,
                     std::shared_ptr<const AzurePipelinesCredentialOptions> options) {
        if (options->OidcRequestUri.empty() || options->ServiceConnectionId.empty() ||
            options->SystemAccessToken.empty()) {
            co_return std::unexpected(std::make_error_code(std::errc::invalid_argument));
        }
        HttpResult result = co_await Private::SendWithRetry(
            httpClient, Private::RequestAuth{}, std::shared_ptr<NoTokenCredential>{}, options->Retry,
            BuildOidcRequest(*options), options->RequestOptions, NotFoundRetry::Fail);
        if (!result.has_value()) {
            co_return std::unexpected(result.error());
        }
        const HttpResponse& response = *result;
        if (response.GetStatus().Class() != HttpStatusClass::Success) {
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

/// <summary>
/// An Azure Pipelines credential over the default HTTP client.
/// </summary>
using AzurePipelinesCredential = BasicAzurePipelinesCredential<HttpClient>;
} // namespace AVEVA::AzureClient
