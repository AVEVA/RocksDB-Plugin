// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Credentials.hpp>

namespace AVEVA::AzureClient {
ClientSecretCredentialOptions ClientSecretCredentialOptions::FromEnvironment() {
    ClientSecretCredentialOptions options;
    options.TenantId = Private::GetEnvironmentValue("AZURE_TENANT_ID");
    options.ClientId = Private::GetEnvironmentValue("AZURE_CLIENT_ID");
    options.ClientSecret = Private::GetEnvironmentValue("AZURE_CLIENT_SECRET");
    if (std::string authorityHost = Private::GetEnvironmentValue("AZURE_AUTHORITY_HOST"); !authorityHost.empty()) {
        options.AuthorityHost = std::move(authorityHost);
    }
    return options;
}

WorkloadIdentityCredentialOptions WorkloadIdentityCredentialOptions::FromEnvironment() {
    WorkloadIdentityCredentialOptions options;
    options.TenantId = Private::GetEnvironmentValue("AZURE_TENANT_ID");
    options.ClientId = Private::GetEnvironmentValue("AZURE_CLIENT_ID");
    options.TokenFilePath = Private::GetEnvironmentValue("AZURE_FEDERATED_TOKEN_FILE");
    if (std::string authorityHost = Private::GetEnvironmentValue("AZURE_AUTHORITY_HOST");
        !authorityHost.empty()) {
        options.AuthorityHost = std::move(authorityHost);
    }
    return options;
}

ManagedIdentityCredentialOptions ManagedIdentityCredentialOptions::FromEnvironment() {
    ManagedIdentityCredentialOptions options;
    options.IdentityEndpoint = Private::GetEnvironmentValue("IDENTITY_ENDPOINT");
    options.IdentityHeader = Private::GetEnvironmentValue("IDENTITY_HEADER");
    return options;
}

template class BasicClientSecretCredential<HttpClient>;
template class BasicWorkloadIdentityCredential<HttpClient>;
template class BasicManagedIdentityCredential<HttpClient>;
} // namespace AVEVA::AzureClient
