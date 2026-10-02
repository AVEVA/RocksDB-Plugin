// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/ITokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <memory>
#include <string>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Tries a list of credentials in order and completes with the token of the first one that succeeds.
/// Fails with the error of the last credential when none of them succeed.
/// </summary>
class ChainedTokenCredential final : public AzureClient::ITokenCredential,
                                     public std::enable_shared_from_this<ChainedTokenCredential> {
    std::vector<std::shared_ptr<AzureClient::ITokenCredential>> m_sources;

    void TryGetToken(std::size_t index, std::vector<std::string> scopes, GetTokenCompletionHandler completion,
                     std::error_code lastError);

  public:
    explicit ChainedTokenCredential(std::vector<std::shared_ptr<AzureClient::ITokenCredential>> sources);
    void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override;
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
class AzurePipelinesCredential final : public AzureClient::ITokenCredential,
                                       public std::enable_shared_from_this<AzurePipelinesCredential> {
    IHttpClient* m_httpClient;
    AzurePipelinesCredentialOptions m_options;

    void ExchangeOidcToken(const std::string& oidcToken, const std::vector<std::string>& scopes,
                           GetTokenCompletionHandler completion);

  public:
    AzurePipelinesCredential(IHttpClient& httpClient, AzurePipelinesCredentialOptions options);
    void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
