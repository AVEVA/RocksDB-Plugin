// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
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
/// <summary>
/// A closed set of credential types behind one token-credential interface. Dispatch is a std::visit over the
/// alternatives, so no virtual calls or type erasure are involved.
/// </summary>
template <class... TCreds> class CredentialVariant {
    std::variant<std::shared_ptr<TCreds>...> m_value;

    template <class Handler> void Begin(std::vector<std::string> scopes, Handler handler) {
        std::visit([&](auto& credential) { credential->GetTokenAsync(std::move(scopes), std::move(handler)); },
                   m_value);
    }

  public:
    template <class T>
        requires(std::same_as<T, TCreds> || ...)
    explicit CredentialVariant(std::shared_ptr<T> credential) : m_value(std::move(credential)) {}

    // The dynamic type of the held credential, for diagnostics and tests.
    [[nodiscard]] const std::type_info& Type() const noexcept {
        return std::visit([](const auto& credential) -> const std::type_info& { return typeid(*credential); }, m_value);
    }

    template <boost::asio::completion_token_for<AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<AzureClient::TokenSignature>(
            [this](auto handler, std::vector<std::string> requestedScopes) {
                Begin(std::move(requestedScopes), std::move(handler));
            },
            token, std::move(scopes));
    }
};

/// <summary>
/// Tries a list of credentials in order and completes with the token of the first one that succeeds.
/// Fails with the error of the last credential when none of them succeed.
/// </summary>
template <AzureClient::TokenCredentialLike TSource>
class ChainedTokenCredential final : public std::enable_shared_from_this<ChainedTokenCredential<TSource>> {
    std::vector<std::shared_ptr<TSource>> m_sources;

    // The state of one GetTokenAsync call. It is a template on the handler so that the continuation handed to each
    // source has one fixed type however long the chain is.
    template <class Handler> struct Run : std::enable_shared_from_this<Run<Handler>> {
        std::shared_ptr<ChainedTokenCredential> Owner;
        std::vector<std::string> Scopes;
        Handler Completion;

        Run(std::shared_ptr<ChainedTokenCredential> owner, std::vector<std::string> scopes, Handler completion)
            : Owner(std::move(owner)), Scopes(std::move(scopes)), Completion(std::move(completion)) {}

        void Try(std::size_t index, std::error_code lastError) {
            if (index >= Owner->m_sources.size()) {
                Completion(lastError, AzureClient::AccessToken{});
                return;
            }

            Owner->m_sources[index]->GetTokenAsync(
                Scopes, [self = this->shared_from_this(), index](std::error_code error,
                                                                 AzureClient::AccessToken token) mutable {
                    if (!error) {
                        self->Completion(error, std::move(token));
                        return;
                    }

                    // Only the last failure is surfaced to the caller; earlier ones are routine when probing sources.
                    if (index + 1 < self->Owner->m_sources.size()) {
                        BOOST_LOG_TRIVIAL(debug) << "Token credential source #" << index
                                                 << " failed: " << error.message() << "; trying next source";
                    } else {
                        BOOST_LOG_TRIVIAL(warning) << "Token credential source #" << index
                                                   << " failed: " << error.message() << "; no more sources";
                    }
                    self->Try(index + 1, error);
                });
        }
    };

  public:
    explicit ChainedTokenCredential(std::vector<std::shared_ptr<TSource>> sources) : m_sources(std::move(sources)) {
        std::erase(m_sources, nullptr);
    }

    template <boost::asio::completion_token_for<AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<AzureClient::TokenSignature>(
            [this](auto handler, std::vector<std::string> requestedScopes) {
                using HandlerType = std::decay_t<decltype(handler)>;
                auto run = std::make_shared<Run<HandlerType>>(this->shared_from_this(), std::move(requestedScopes),
                                                              std::move(handler));
                run->Try(0, AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::AuthenticationFailed));
            },
            token, std::move(scopes));
    }
};

/// <summary>
/// Keeps a ClientRuntime (and thus the HttpClient the wrapped credential borrows) alive until every in-flight
/// GetTokenAsync has completed, so a token refresh that outlives the filesystem cannot touch a destroyed client.
/// The runtime is released from a handler posted to its own executor rather than from inside the HTTP client's
/// completion callback, so the client is never destroyed while it is still running that callback.
/// </summary>
template <AzureClient::TokenCredentialLike TInner> class RuntimeBoundCredential final {
    std::shared_ptr<ClientRuntime> m_runtime;
    std::shared_ptr<TInner> m_inner;

  public:
    RuntimeBoundCredential(std::shared_ptr<ClientRuntime> runtime, std::shared_ptr<TInner> inner)
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
    int MaxRetries = 3;
};

/// <summary>
/// Authenticates as a Microsoft Entra ID application through an Azure Pipelines service connection using
/// workload identity federation: an OIDC token issued by Azure Pipelines for the service connection is
/// exchanged for an access token with a client-assertion grant.
/// </summary>
class AzurePipelinesCredential final : public std::enable_shared_from_this<AzurePipelinesCredential> {
    // The request pipeline below is compiled once in TokenCredentials.cpp, so it takes the completion in this erased
    // form rather than as a template on the caller's handler.
    using TokenCompletion = std::move_only_function<AzureClient::TokenSignature>;

    ::AVEVA::HttpClient* m_httpClient;
    AzurePipelinesCredentialOptions m_options;

    void Begin(std::vector<std::string> scopes, TokenCompletion completion);
    void ExchangeOidcToken(const std::string& oidcToken, const std::vector<std::string>& scopes,
                           TokenCompletion completion);

  public:
    AzurePipelinesCredential(::AVEVA::HttpClient& httpClient, AzurePipelinesCredentialOptions options);

    template <boost::asio::completion_token_for<AzureClient::TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<AzureClient::TokenSignature>(
            [this](auto handler, std::vector<std::string> requestedScopes) {
                Begin(std::move(requestedScopes), TokenCompletion{std::move(handler)});
            },
            token, std::move(scopes));
    }
};

/// <summary>The credentials a single token source can be.</summary>
using LeafCredential = CredentialVariant<AzureClient::ClientSecretCredential, AzureClient::WorkloadIdentityCredential,
                                         AzureClient::ManagedIdentityCredential, AzurePipelinesCredential>;

/// <summary>A leaf credential, or a chain of them.</summary>
using SourceCredential = CredentialVariant<AzureClient::ClientSecretCredential, AzureClient::WorkloadIdentityCredential,
                                           AzureClient::ManagedIdentityCredential, AzurePipelinesCredential,
                                           ChainedTokenCredential<LeafCredential>>;

/// <summary>
/// The one credential type every plugin client is instantiated with: any source, kept alive with the
/// runtime and cached.
/// </summary>
using PluginCredential = AzureClient::CachingTokenCredential<RuntimeBoundCredential<SourceCredential>>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
