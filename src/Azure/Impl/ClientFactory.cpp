// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Environment.hpp"
#include "ClientFactory.ipp"

#include <AVEVA/AzureClient/Credentials.hpp>

#include <boost/log/trivial.hpp>

#include <algorithm>
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

// Real storage endpoints name the account in the host (account.blob.core.windows.net). Emulators and IP or single-label
// hosts (Azurite, containers) are path-style instead: http://127.0.0.1:10000/account.
std::string AccountNameFromUrl(const std::string& storageAccountUrl) {
    auto hostStart = storageAccountUrl.find("://");
    hostStart = hostStart == std::string::npos ? 0 : hostStart + 3;
    const auto authorityEnd = storageAccountUrl.find_first_of("/?#", hostStart);
    const auto authority = storageAccountUrl.substr(
        hostStart, authorityEnd == std::string::npos ? std::string::npos : authorityEnd - hostStart);
    // Strip any port; an IPv6 literal keeps its brackets and so is never mistaken for a label.
    const auto portStart = authority.rfind(':');
    const auto host = portStart != std::string::npos && authority.find(']', portStart) == std::string::npos
                          ? authority.substr(0, portStart)
                          : authority;

    const auto isIpv4 =
        !host.empty() && std::ranges::all_of(host, [](const char ch) { return (ch >= '0' && ch <= '9') || ch == '.'; });
    const auto isPathStyle = isIpv4 || host.starts_with('[') || host.find('.') == std::string::npos;
    if (isPathStyle && authorityEnd != std::string::npos && storageAccountUrl[authorityEnd] == '/') {
        const auto segmentStart = authorityEnd + 1;
        const auto segmentEnd = storageAccountUrl.find_first_of("/?#", segmentStart);
        const auto segment = storageAccountUrl.substr(
            segmentStart, segmentEnd == std::string::npos ? std::string::npos : segmentEnd - segmentStart);
        if (!segment.empty()) {
            return segment;
        }
    }
    return host.substr(0, host.find('.'));
}

template std::shared_ptr<PluginCredential> MakePluginCredential<::AVEVA::HttpClient>(SourceCredential);
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