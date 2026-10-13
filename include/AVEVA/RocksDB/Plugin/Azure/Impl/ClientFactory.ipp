// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/Credentials.hpp>

#include <boost/log/trivial.hpp>

#include <memory>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory {
template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>> MakePluginCredential(BasicSourceCredential<THttp> source) {
    // The source credential owns the HTTP client, so a token refresh still in flight keeps it alive.
    return AzureClient::MakeCachingTokenCredential(std::make_shared<BasicSourceCredential<THttp>>(std::move(source)));
}

template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreateClientSecretCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime, const std::string& tenantId,
                             const std::string& clientId, const std::string& clientSecret) {
    AzureClient::ClientSecretCredentialOptions options;
    options.TenantId = tenantId;
    options.ClientId = clientId;
    options.ClientSecret = clientSecret;
    options.Retry = CreateRetryOptions();
    return MakePluginCredential<THttp>(
        BasicSourceCredential<THttp>{std::make_shared<AzureClient::BasicClientSecretCredential<THttp>>(
            runtime->SharedHttpClient(), std::move(options))});
}

template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreatePipelinesCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime, const std::string& tenantId,
                          const std::string& clientId, const std::string& serviceConnectionId,
                          const std::string& systemAccessToken) {
    AzureClient::AzurePipelinesCredentialOptions options;
    options.TenantId = tenantId;
    options.ClientId = clientId;
    options.ServiceConnectionId = serviceConnectionId;
    options.SystemAccessToken = systemAccessToken;
    options.Retry = CreateRetryOptions();
    return MakePluginCredential<THttp>(
        BasicSourceCredential<THttp>{std::make_shared<AzureClient::BasicAzurePipelinesCredential<THttp>>(
            runtime->SharedHttpClient(), std::move(options))});
}

template <AzureClient::HttpClientLike THttp>
std::vector<std::shared_ptr<BasicLeafCredential<THttp>>>
CreateCredentialSources(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                        const Models::ChainedCredentialInfo& chainedCredential) {
    std::vector<std::shared_ptr<BasicLeafCredential<THttp>>> sources;

    // Try to use user specified credentials to try to authenticate first.
    {
        AzureClient::ClientSecretCredentialOptions options;
        options.TenantId = chainedCredential.GetTenantId();
        options.ClientId = chainedCredential.GetServicePrincipalId();
        options.ClientSecret = chainedCredential.GetServicePrincipalSecret();
        options.Retry = CreateRetryOptions();
        sources.push_back(std::make_shared<BasicLeafCredential<THttp>>(
            std::make_shared<AzureClient::BasicClientSecretCredential<THttp>>(runtime->SharedHttpClient(),
                                                                              std::move(options))));
    }

    // Managed identity is always tried: the user-assigned identity when an id is supplied, otherwise the
    // system-assigned one (the effective behavior before the SDK replacement). It only runs when the service
    // principal above failed, so off-Azure hosts pay one failed IMDS probe at most.
    {
        auto options = AzureClient::ManagedIdentityCredentialOptions::FromEnvironment();
        if (const auto managedIdentityId = chainedCredential.GetManagedIdentityId()) {
            options.ClientId = std::string(*managedIdentityId);
        }
        options.Retry = CreateRetryOptions();
        sources.push_back(std::make_shared<BasicLeafCredential<THttp>>(
            std::make_shared<AzureClient::BasicManagedIdentityCredential<THttp>>(runtime->SharedHttpClient(),
                                                                                 std::move(options))));
    }

    // Environment credential: a client secret credential constructed from environment variable values.
    {
        auto options = AzureClient::ClientSecretCredentialOptions::FromEnvironment();
        options.Retry = CreateRetryOptions();
        if (!options.TenantId.empty() && !options.ClientId.empty() && !options.ClientSecret.empty()) {
            sources.push_back(std::make_shared<BasicLeafCredential<THttp>>(
                std::make_shared<AzureClient::BasicClientSecretCredential<THttp>>(runtime->SharedHttpClient(),
                                                                                  std::move(options))));
        }
    }

    // Workload identity credential constructed from environment variables.
    {
        auto options = AzureClient::WorkloadIdentityCredentialOptions::FromEnvironment();
        options.Retry = CreateRetryOptions();
        if (!options.TenantId.empty() && !options.ClientId.empty() && !options.TokenFilePath.empty()) {
            sources.push_back(std::make_shared<BasicLeafCredential<THttp>>(
                std::make_shared<AzureClient::BasicWorkloadIdentityCredential<THttp>>(runtime->SharedHttpClient(),
                                                                                      std::move(options))));
        }
    }

    return sources;
}

template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreateChainedCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                        const Models::ChainedCredentialInfo& chainedCredential) {
    // Failures of earlier sources are routine when probing, so only the final one is worth a warning.
    const auto logFailure = [](std::size_t index, std::error_code error, bool isLast) {
        if (isLast) {
            BOOST_LOG_TRIVIAL(warning) << "Token credential source #" << index << " failed: " << error.message()
                                       << "; no more sources";
        } else {
            BOOST_LOG_TRIVIAL(debug) << "Token credential source #" << index << " failed: " << error.message()
                                     << "; trying next source";
        }
    };
    return MakePluginCredential<THttp>(
        BasicSourceCredential<THttp>{std::make_shared<ChainedTokenCredential<BasicLeafCredential<THttp>>>(
            CreateCredentialSources<THttp>(runtime, chainedCredential), logFailure)});
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateSharedKeyServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                                        const std::string& storageAccountUrl,
                                                        const std::string& storageAccountKey) {
    auto options = CreateServiceClientOptions(storageAccountUrl);
    options.SharedKey.AccountName = AccountNameFromUrl(storageAccountUrl);
    options.SharedKey.AccountKey = storageAccountKey;
    return BlobServiceClientOn<THttp>{runtime->SharedHttpClient(), std::move(options)};
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateClientSecretServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                                           const std::string& storageAccountUrl,
                                                           const std::string& tenantId, const std::string& clientId,
                                                           const std::string& clientSecret) {
    return BlobServiceClientOn<THttp>{runtime->SharedHttpClient(), CreateServiceClientOptions(storageAccountUrl),
                                      CreateClientSecretCredential<THttp>(runtime, tenantId, clientId, clientSecret)};
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreatePipelinesServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                                        const std::string& storageAccountUrl,
                                                        const std::string& tenantId, const std::string& clientId,
                                                        const std::string& serviceConnectionId,
                                                        const std::string& systemAccessToken) {
    return BlobServiceClientOn<THttp>{
        runtime->SharedHttpClient(), CreateServiceClientOptions(storageAccountUrl),
        CreatePipelinesCredential<THttp>(runtime, tenantId, clientId, serviceConnectionId, systemAccessToken)};
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                               const Models::ServicePrincipalStorageInfo& servicePrincipal) {
    return CreateClientSecretServiceClient<THttp>(
        runtime, servicePrincipal.GetStorageAccountUrl(), servicePrincipal.GetTenantId(),
        servicePrincipal.GetServicePrincipalId(), servicePrincipal.GetServicePrincipalSecret());
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                               const Models::ChainedCredentialInfo& chainedCredential) {
    return BlobServiceClientOn<THttp>{runtime->SharedHttpClient(),
                                      CreateServiceClientOptions(chainedCredential.GetStorageAccountUrl()),
                                      CreateChainedCredential<THttp>(runtime, chainedCredential)};
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory
