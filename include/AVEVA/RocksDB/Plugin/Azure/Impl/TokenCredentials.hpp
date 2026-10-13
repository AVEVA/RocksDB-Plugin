// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/ChainedTokenCredential.hpp>
#include <AVEVA/AzureClient/CredentialVariant.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/value.hpp>
#include <boost/log/trivial.hpp>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
using AzureClient::ChainedTokenCredential;
using AzureClient::CredentialVariant;

/// <summary>
/// Keeps a runtime (and thus the HttpClient the wrapped credential borrows) alive until every in-flight
/// GetTokenAsync has completed, so a token refresh that outlives the filesystem cannot touch a destroyed client.
/// The runtime is released from a handler posted to its own executor rather than from inside the HTTP client's
/// completion callback, so the client is never destroyed while it is still running that callback.
/// </summary>
template <AzureClient::TokenCredentialLike TInner, AzureClient::HttpClientLike THttp = ::AVEVA::HttpClient>
class RuntimeBoundCredential final {
    std::shared_ptr<BasicClientRuntime<THttp>> m_runtime;
    std::shared_ptr<TInner> m_inner;

  public:
    RuntimeBoundCredential(std::shared_ptr<BasicClientRuntime<THttp>> runtime, std::shared_ptr<TInner> inner)
        : m_runtime(std::move(runtime)), m_inner(std::move(inner)) {}

    template <boost::asio::completion_token_for<AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<AzureClient::TokenSignature>(
            [this](auto handler, std::vector<std::string> requestedScopes) {
                m_inner->GetTokenAsync(std::move(requestedScopes),
                                       [runtime = m_runtime, inner = m_inner, handler = std::move(handler)](
                                           std::error_code error, AzureClient::AccessToken accessToken) mutable {
                                           handler(error, std::move(accessToken));
                                           auto executor = runtime->HttpClient().get_executor();
                                           // Keeps the runtime and inner credential alive until the completion above
                                           // has returned, then releases them from a fresh executor task rather than
                                           // from inside the token completion's own stack.
                                           boost::asio::post(
                                               executor, [runtime = std::move(runtime), inner = std::move(inner)]() {});
                                       });
            },
            token, std::move(scopes));
    }
};

struct AzurePipelinesCredentialOptions {
    std::string TenantId;
    std::string ClientId;
    std::string ServiceConnectionId;
    std::string SystemAccessToken;

    // The Azure Pipelines OIDC token endpoint; defaults to the SYSTEM_OIDCREQUESTURI environment variable.
    std::string OidcRequestUri;
    std::string AuthorityHost = std::string{AzureClient::DefaultAuthorityHost};
    HttpRequestOptions RequestOptions;
    AzureClient::RetryOptions Retry;
};

/// <summary>
/// Authenticates as a Microsoft Entra ID application through an Azure Pipelines service connection using
/// workload identity federation: an OIDC token issued by Azure Pipelines for the service connection is
/// exchanged for an access token with a client-assertion grant. Only the OIDC token request lives here; the
/// exchange is AzureClient::BasicClientAssertionCredential.
/// </summary>
template <AzureClient::HttpClientLike THttp> class BasicAzurePipelinesCredential final {
    AzureClient::BasicClientAssertionCredential<THttp> m_exchange;

    static constexpr unsigned int FirstSuccess = 200;
    static constexpr unsigned int LastSuccess = 299;

    static HttpRequest BuildOidcRequest(const AzurePipelinesCredentialOptions& options) {
        const auto separator = options.OidcRequestUri.find('?') == std::string::npos ? '?' : '&';
        HttpRequest request;
        request.SetMethod(HttpMethod::Post);
        request.SetUrl(options.OidcRequestUri + separator + "api-version=7.1&serviceConnectionId=" +
                       AzureClient::Private::FormEncode(options.ServiceConnectionId));
        request.AddHeader(HttpHeader{"Content-Type", "application/json"});
        request.AddHeader(HttpHeader{"Authorization", "Bearer " + options.SystemAccessToken});
        request.SetBody(std::string{});
        return request;
    }

    // Extracts the oidcToken member, or an empty string when the body is not the expected JSON object.
    static std::string ParseOidcToken(const HttpResponse& response) {
        boost::system::error_code ec;
        const boost::json::value root = boost::json::parse(response.GetBody(), ec);
        if (ec || !root.is_object()) {
            return {};
        }
        return AzureClient::Private::JsonText(root.as_object(), "oidcToken");
    }

    static AzureClient::ClientAssertionCredentialOptions MakeExchangeOptions(THttp& httpClient,
                                                                             AzurePipelinesCredentialOptions options) {
        if (options.OidcRequestUri.empty()) {
            options.OidcRequestUri = AzureClient::Private::GetEnvironmentValue("SYSTEM_OIDCREQUESTURI");
        }

        AzureClient::ClientAssertionCredentialOptions exchange;
        exchange.TenantId = options.TenantId;
        exchange.ClientId = options.ClientId;
        exchange.AuthorityHost = options.AuthorityHost;
        exchange.RequestOptions = options.RequestOptions;
        exchange.Retry = options.Retry;
        // The provider owns a copy of everything it needs, so in-flight requests do not depend on the credential.
        exchange.AssertionProvider = [&httpClient,
                                      options = std::move(options)](AzureClient::AssertionCompletion done) {
            if (options.OidcRequestUri.empty() || options.ServiceConnectionId.empty() ||
                options.SystemAccessToken.empty()) {
                done(std::make_error_code(std::errc::invalid_argument), {});
                return;
            }

            AzureClient::Private::SendWithRetryAsync(
                httpClient, AzureClient::Private::RequestAuth{}, std::shared_ptr<AzureClient::NoTokenCredential>{},
                options.Retry, BuildOidcRequest(options),
                [done = std::move(done)](std::error_code error, const HttpResponse& response) mutable {
                    if (error) {
                        done(error, {});
                        return;
                    }
                    if (response.GetStatus() < FirstSuccess || response.GetStatus() > LastSuccess) {
                        done(AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::AuthenticationFailed), {});
                        return;
                    }
                    auto oidcToken = ParseOidcToken(response);
                    if (oidcToken.empty()) {
                        done(AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::InvalidResponse), {});
                        return;
                    }
                    done(std::error_code{}, std::move(oidcToken));
                },
                options.RequestOptions, false);
        };
        return exchange;
    }

  public:
    BasicAzurePipelinesCredential(THttp& httpClient, AzurePipelinesCredentialOptions options)
        : m_exchange(httpClient, MakeExchangeOptions(httpClient, std::move(options))) {}

    template <boost::asio::completion_token_for<AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return m_exchange.GetTokenAsync(std::move(scopes), std::forward<CompletionToken>(token));
    }
};

using AzurePipelinesCredential = BasicAzurePipelinesCredential<::AVEVA::HttpClient>;

/// <summary>The credentials a single token source can be, over the HTTP transport `THttp`.</summary>
template <AzureClient::HttpClientLike THttp>
using BasicLeafCredential =
    CredentialVariant<AzureClient::BasicClientSecretCredential<THttp>,
                      AzureClient::BasicWorkloadIdentityCredential<THttp>,
                      AzureClient::BasicManagedIdentityCredential<THttp>, BasicAzurePipelinesCredential<THttp>>;

/// <summary>A leaf credential, or a chain of them.</summary>
template <AzureClient::HttpClientLike THttp>
using BasicSourceCredential =
    CredentialVariant<AzureClient::BasicClientSecretCredential<THttp>,
                      AzureClient::BasicWorkloadIdentityCredential<THttp>,
                      AzureClient::BasicManagedIdentityCredential<THttp>, BasicAzurePipelinesCredential<THttp>,
                      ChainedTokenCredential<BasicLeafCredential<THttp>>>;

/// <summary>
/// The one credential type every plugin client over `THttp` is instantiated with: any source, kept alive with the
/// runtime and cached.
/// </summary>
template <AzureClient::HttpClientLike THttp>
using BasicPluginCredential =
    AzureClient::CachingTokenCredential<RuntimeBoundCredential<BasicSourceCredential<THttp>, THttp>>;

using LeafCredential = BasicLeafCredential<::AVEVA::HttpClient>;
using SourceCredential = BasicSourceCredential<::AVEVA::HttpClient>;
using PluginCredential = BasicPluginCredential<::AVEVA::HttpClient>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
