// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include <AVEVA/AzureClient/Detail/CredentialsImpl.hpp>

namespace AVEVA::AzureClient {
WorkloadIdentityCredentialOptions WorkloadIdentityCredentialOptions::FromEnvironment() {
    WorkloadIdentityCredentialOptions options;
    options.TenantId = Private::CredentialsDetail::GetEnvironmentValue("AZURE_TENANT_ID");
    options.ClientId = Private::CredentialsDetail::GetEnvironmentValue("AZURE_CLIENT_ID");
    options.TokenFilePath = Private::CredentialsDetail::GetEnvironmentValue("AZURE_FEDERATED_TOKEN_FILE");
    if (std::string authorityHost = Private::CredentialsDetail::GetEnvironmentValue("AZURE_AUTHORITY_HOST");
        !authorityHost.empty()) {
        options.AuthorityHost = std::move(authorityHost);
    }
    return options;
}

ManagedIdentityCredentialOptions ManagedIdentityCredentialOptions::FromEnvironment() {
    ManagedIdentityCredentialOptions options;
    options.IdentityEndpoint = Private::CredentialsDetail::GetEnvironmentValue("IDENTITY_ENDPOINT");
    options.IdentityHeader = Private::CredentialsDetail::GetEnvironmentValue("IDENTITY_HEADER");
    return options;
}

template class BasicClientSecretCredential<HttpClient>;
template class BasicWorkloadIdentityCredential<HttpClient>;
template class BasicManagedIdentityCredential<HttpClient>;
} // namespace AVEVA::AzureClient
