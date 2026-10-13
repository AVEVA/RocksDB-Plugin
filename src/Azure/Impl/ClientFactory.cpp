// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Environment.hpp"

#include <AVEVA/AzureClient/Credentials.hpp>

#include <boost/log/trivial.hpp>

#include <memory>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory {
AzureClient::RetryOptions CreateRetryOptions() {
    AzureClient::RetryOptions retry;
    retry.MaxRetries = Configuration::MaxClientRetries;
    retry.MaxDelay = Configuration::MaxRetryDelay;
    return retry;
}

AzureClient::BlobServiceClientOptions CreateServiceClientOptions(const std::string& storageAccountUrl) {
    AzureClient::BlobServiceClientOptions options;
    options.ServiceEndpoint = storageAccountUrl;
    options.Retry = CreateRetryOptions();
    return options;
}

std::string AccountNameFromUrl(const std::string& storageAccountUrl) {
    auto hostStart = storageAccountUrl.find("://");
    hostStart = hostStart == std::string::npos ? 0 : hostStart + 3;
    const auto hostEnd = storageAccountUrl.find_first_of("./:", hostStart);
    return storageAccountUrl.substr(hostStart, hostEnd == std::string::npos ? std::string::npos : hostEnd - hostStart);
}

template std::shared_ptr<PluginCredential>
MakePluginCredential<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&, SourceCredential);
template std::shared_ptr<PluginCredential>
CreateClientSecretCredential<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&, const std::string&,
                                                  const std::string&, const std::string&);
template std::shared_ptr<PluginCredential>
CreatePipelinesCredential<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&, const std::string&,
                                               const std::string&, const std::string&, const std::string&);
template std::vector<std::shared_ptr<LeafCredential>>
CreateCredentialSources<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                             const Models::ChainedCredentialInfo&);
template std::shared_ptr<PluginCredential>
CreateChainedCredential<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                             const Models::ChainedCredentialInfo&);
template BlobServiceClient CreateServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                                                    const Models::ServicePrincipalStorageInfo&);
template BlobServiceClient CreateServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                                                    const Models::ChainedCredentialInfo&);
template BlobServiceClient CreateSharedKeyServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                                                             const std::string&, const std::string&);
template BlobServiceClient CreateClientSecretServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                                                                const std::string&, const std::string&,
                                                                                const std::string&, const std::string&);
template BlobServiceClient CreatePipelinesServiceClient<::AVEVA::HttpClient>(const std::shared_ptr<ClientRuntime>&,
                                                                             const std::string&, const std::string&,
                                                                             const std::string&, const std::string&,
                                                                             const std::string&);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory