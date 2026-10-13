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
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::ClientFactory