// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
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
std::shared_ptr<BasicPluginCredential<THttp>>
MakePluginCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime, BasicSourceCredential<THttp> source);
template <AzureClient::HttpClientLike THttp>
std::shared_ptr<BasicPluginCredential<THttp>>
CreateClientSecretCredential(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime, const std::string& tenantId,
                             const std::string& clientId, const std::string& clientSecret);
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
                                                        const std::string& storageAccountUrl,
                                                        const std::string& storageAccountKey);
template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreateClientSecretServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                                           const std::string& storageAccountUrl,
                                                           const std::string& tenantId, const std::string& clientId,
                                                           const std::string& clientSecret);
template <AzureClient::HttpClientLike THttp>
BlobServiceClientOn<THttp> CreatePipelinesServiceClient(const std::shared_ptr<BasicClientRuntime<THttp>>& runtime,
                                                        const std::string& storageAccountUrl,
                                                        const std::string& tenantId, const std::string& clientId,
                                                        const std::string& serviceConnectionId,
                                                        const std::string& systemAccessToken);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.ipp"
