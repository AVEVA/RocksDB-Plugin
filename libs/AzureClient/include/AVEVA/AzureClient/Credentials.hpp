// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/ITokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <string>
#include <string_view>
#include <vector>

// Microsoft Entra ID token credentials that request tokens through an HTTP client (any HttpClientLike).
//
// Lifetime: each credential keeps a non-owning reference to the HTTP client, which must outlive the credential and
// every in-flight GetTokenAsync. Completions run on the HTTP client's executor.
//
// Every GetTokenAsync sends a token request; wrap the credential in CachingTokenCredential to reuse tokens until
// they approach expiry.
//
// Errors: missing or invalid configuration -> std::errc::invalid_argument (no request is sent); an unreadable
// federated token file -> std::errc::no_such_file_or_directory; a non-2xx token endpoint response ->
// BlobStorageErrorCode::AuthenticationFailed; a 2xx response without a usable access_token/expiry ->
// BlobStorageErrorCode::InvalidResponse; transport errors are passed through.
namespace AVEVA::AzureClient {
inline constexpr std::string_view DefaultAuthorityHost = "https://login.microsoftonline.com/";
inline constexpr std::string_view DefaultImdsEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token";

struct ClientSecretCredentialOptions {
    std::string TenantId;
    std::string ClientId;
    std::string ClientSecret;
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    HttpRequestOptions RequestOptions;
    // Retries of transient token endpoint failures (408/429/5xx and transport errors).
    RetryOptions Retry;
};

// Service principal with a client secret (OAuth 2.0 client credentials grant).
template <HttpClientLike THttp> class BasicClientSecretCredential final : public ITokenCredential {
  public:
    BasicClientSecretCredential(THttp& httpClient, ClientSecretCredentialOptions options);

    void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override;

  private:
    THttp* m_httpClient;
    ClientSecretCredentialOptions m_options;
};

using ClientSecretCredential = BasicClientSecretCredential<HttpClient>;

struct WorkloadIdentityCredentialOptions {
    std::string TenantId;
    std::string ClientId;
    // File holding the federated (e.g. Kubernetes service account) token; re-read on every request.
    std::string TokenFilePath;
    std::string AuthorityHost = std::string{DefaultAuthorityHost};
    HttpRequestOptions RequestOptions;
    // Retries of transient token endpoint failures (408/429/5xx and transport errors).
    RetryOptions Retry;

    // Reads AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE and (when set) AZURE_AUTHORITY_HOST, as
    // injected by Azure Workload Identity.
    [[nodiscard]] static WorkloadIdentityCredentialOptions FromEnvironment();
};

// Azure Workload Identity: exchanges a federated token file for an Entra ID token (client assertion grant).
template <HttpClientLike THttp> class BasicWorkloadIdentityCredential final : public ITokenCredential {
  public:
    BasicWorkloadIdentityCredential(THttp& httpClient, WorkloadIdentityCredentialOptions options);

    void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override;

  private:
    THttp* m_httpClient;
    WorkloadIdentityCredentialOptions m_options;
};

using WorkloadIdentityCredential = BasicWorkloadIdentityCredential<HttpClient>;

struct ManagedIdentityCredentialOptions {
    // A user-assigned identity, by client id or by ARM resource id (at most one); both empty selects the
    // system-assigned identity.
    std::string ClientId;
    std::string ResourceId;
    // App Service / Functions / Container Apps identity endpoint and secret header. When IdentityEndpoint is
    // empty the Azure Instance Metadata Service (ImdsEndpoint) is used.
    std::string IdentityEndpoint;
    std::string IdentityHeader;
    std::string ImdsEndpoint = std::string{DefaultImdsEndpoint};
    HttpRequestOptions RequestOptions;
    // Retries of transient token endpoint failures; HTTP 404 and 410 are also retried (IMDS answers these while the
    // identity is still being provisioned).
    RetryOptions Retry;

    // Reads IDENTITY_ENDPOINT and IDENTITY_HEADER (set by App Service, Functions and Container Apps).
    [[nodiscard]] static ManagedIdentityCredentialOptions FromEnvironment();
};

// Managed identity via IMDS (VMs, VM scale sets, AKS) or the App Service identity endpoint. Requests exactly one
// scope, which is converted to a resource ("https://storage.azure.com/.default" -> "https://storage.azure.com").
template <HttpClientLike THttp> class BasicManagedIdentityCredential final : public ITokenCredential {
  public:
    BasicManagedIdentityCredential(THttp& httpClient, ManagedIdentityCredentialOptions options);

    void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override;

  private:
    THttp* m_httpClient;
    ManagedIdentityCredentialOptions m_options;
};

using ManagedIdentityCredential = BasicManagedIdentityCredential<HttpClient>;
} // namespace AVEVA::AzureClient
