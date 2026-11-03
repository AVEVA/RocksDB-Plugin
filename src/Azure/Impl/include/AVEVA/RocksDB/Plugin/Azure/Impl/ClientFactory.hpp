// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include <AVEVA/AzureClient/Credentials.hpp>

#include <boost/log/trivial.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

/// Builds the Azure clients and credentials the filesystem uses. Kept apart from the blob operations so that code
/// that only reads or writes blobs does not depend on every credential type.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory {
AzureClient::RetryOptions CreateRetryOptions();
AzureClient::BlobServiceClientOptions CreateServiceClientOptions(const std::string& storageAccountUrl);
std::string AccountNameFromUrl(const std::string& storageAccountUrl);

// Everything below is generic over the HTTP transport so that tests can run it on a fake one.
// Wraps `source` so that in-flight token refreshes keep `runtime` (and its HttpClient) alive, and caches tokens.
template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>> MakePluginCredential(BasicSourceCredential<THttp> source);
template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreateClientSecretCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                             const Models::TenantId& tenantId, const Models::ClientId& clientId,
                             const Models::ClientSecret& clientSecret);
template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreatePipelinesCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime, const std::string& tenantId,
                          const std::string& clientId, const std::string& serviceConnectionId,
                          const std::string& systemAccessToken);
// The ordered credential sources behind CreateChainedCredential: service principal, managed identity
// (system-assigned when no id is given), then environment and workload identity when their variables are set.
template <AzureClient::HttpClientLike THttp>
std::vector<std::shared_ptr<BasicLeafCredential<THttp>>>
CreateCredentialSources(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                        const Models::ChainedCredentialInfo& chainedCredential);
template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreateChainedCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                        const Models::ChainedCredentialInfo& chainedCredential);
template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                               const Models::ServicePrincipalStorageInfo& servicePrincipal);
template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                               const Models::ChainedCredentialInfo& servicePrincipal);
template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateSharedKeyServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                                        const Models::AccountUrl& storageAccountUrl,
                                                        const StorageAccountKey& storageAccountKey);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory

namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory {
template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>> MakePluginCredential(BasicSourceCredential<THttp> source) {
    // The source credential owns the HTTP client, so a token refresh still in flight keeps it alive.
    return BasicPluginCredential<THttp>::Create(std::make_shared<BasicSourceCredential<THttp>>(std::move(source)));
}

template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreateClientSecretCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                             const Models::TenantId& tenantId, const Models::ClientId& clientId,
                             const Models::ClientSecret& clientSecret) {
    AzureClient::ClientSecretCredentialOptions options;
    options.TenantId = tenantId.Value();
    options.ClientId = clientId.Value();
    options.ClientSecret = clientSecret.Value();
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
        const auto tenantId = chainedCredential.GetTenantId();
        const auto clientId = chainedCredential.GetServicePrincipalId();
        const auto clientSecret = chainedCredential.GetServicePrincipalSecret();
        AzureClient::ClientSecretCredentialOptions options;
        options.TenantId = tenantId.Value();
        options.ClientId = clientId.Value();
        options.ClientSecret = clientSecret.Value();
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
            options.ClientId = managedIdentityId->Value();
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
    const auto logFailure = [](AzureClient::SourceIndex index, std::error_code error,
                               AzureClient::ChainPosition position) {
        if (position == AzureClient::ChainPosition::Last) {
            BOOST_LOG_TRIVIAL(warning) << "Token credential source #" << index.Value() << " failed: " << error.message()
                                       << "; no more sources";
        } else {
            BOOST_LOG_TRIVIAL(debug) << "Token credential source #" << index.Value() << " failed: " << error.message()
                                     << "; trying next source";
        }
    };
    return MakePluginCredential<THttp>(
        BasicSourceCredential<THttp>{ChainedTokenCredential<BasicLeafCredential<THttp>>::Create(
            CreateCredentialSources<THttp>(runtime, chainedCredential), logFailure)});
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateSharedKeyServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                                        const Models::AccountUrl& storageAccountUrl,
                                                        const StorageAccountKey& storageAccountKey) {
    auto options = CreateServiceClientOptions(storageAccountUrl.Value());
    options.SharedKey.AccountName = AccountNameFromUrl(storageAccountUrl.Value());
    options.SharedKey.AccountKey = storageAccountKey.Value();
    return BlobServiceClientOn<THttp>{runtime->SharedHttpClient(), std::move(options)};
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                               const Models::ServicePrincipalStorageInfo& servicePrincipal) {
    const auto storageAccountUrl = servicePrincipal.GetStorageAccountUrl();
    return BlobServiceClientOn<THttp>{
        runtime->SharedHttpClient(), CreateServiceClientOptions(storageAccountUrl.Value()),
        CreateClientSecretCredential<THttp>(runtime, servicePrincipal.GetTenantId(),
                                            servicePrincipal.GetServicePrincipalId(),
                                            servicePrincipal.GetServicePrincipalSecret())};
}

template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                               const Models::ChainedCredentialInfo& chainedCredential) {
    const auto storageAccountUrl = chainedCredential.GetStorageAccountUrl();
    return BlobServiceClientOn<THttp>{runtime->SharedHttpClient(),
                                      CreateServiceClientOptions(storageAccountUrl.Value()),
                                      CreateChainedCredential<THttp>(runtime, chainedCredential)};
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory

// The production instantiations are compiled once, in ClientFactory.cpp, so that every file that builds a filesystem
// over the real HTTP client does not instantiate the credential and client construction again.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory {
extern template std::shared_ptr<PluginCredential> MakePluginCredential<::AVEVA::HttpClient>(SourceCredential);
extern template std::shared_ptr<PluginCredential>
CreateClientSecretCredential<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&, const Models::TenantId&,
                                                  const Models::ClientId&, const Models::ClientSecret&);
extern template std::shared_ptr<PluginCredential>
CreatePipelinesCredential<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&, const std::string&,
                                               const std::string&, const std::string&, const std::string&);
extern template std::vector<std::shared_ptr<LeafCredential>>
CreateCredentialSources<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                             const Models::ChainedCredentialInfo&);
extern template std::shared_ptr<PluginCredential>
CreateChainedCredential<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                             const Models::ChainedCredentialInfo&);
extern template BlobServiceClient CreateServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                                                           const Models::ServicePrincipalStorageInfo&);
extern template BlobServiceClient CreateServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                                                           const Models::ChainedCredentialInfo&);
extern template BlobServiceClient
CreateSharedKeyServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&, const Models::AccountUrl&,
                                                  const StorageAccountKey&);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory
