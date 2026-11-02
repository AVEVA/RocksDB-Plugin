// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include <memory>
#include <string>
#include <vector>

/// Builds the Azure clients and credentials the filesystem uses. Kept apart from the blob operations so that code
/// that only reads or writes blobs does not depend on every credential type.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory {
AzureClient::RetryOptions CreateRetryOptions();
AzureClient::BlobServiceClientOptions CreateServiceClientOptions(const std::string& storageAccountUrl);
std::string AccountNameFromUrl(const std::string& storageAccountUrl);

// Everything below is generic over the HTTP transport so that tests can run it on a fake one; the definitions are in
// ClientFactory.ipp.
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
