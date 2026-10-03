// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>

#include <boost/asio/use_future.hpp>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
static const std::string g_sizeMetadata = "filesize";

namespace {
std::string GetEnvironmentValue(const char* name) {
#ifdef _WIN32
    char* buffer = nullptr;
    std::size_t size = 0;
    if (_dupenv_s(&buffer, &size, name) != 0 || buffer == nullptr) {
        return {};
    }
    const std::unique_ptr<char, decltype(&std::free)> owner{buffer, &std::free};
    return std::string{buffer};
#else
    const char* value = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
    return value == nullptr ? std::string{} : std::string{value};
#endif
}

void CreateIfNotExistsWithRetry(AzureClient::BlobContainerClient& client, int maxRetries = 5) {
    int retries = 1;
    while (true) {
        auto result = client.CreateIfNotExistsAsync(boost::asio::use_future).get();
        if (result.has_value()) {
            return;
        }

        if (retries == maxRetries) {
            ThrowRequestFailed(result.error());
        }

        retries++;
        std::this_thread::sleep_for(std::chrono::seconds(retries));
    }
}
} // namespace

void BlobHelpers::SetFileSize(AzureClient::BlobClient& client, int64_t size) {
    AzureClient::SetBlobMetadataOptions options;
    options.Metadata.emplace(g_sizeMetadata, std::to_string(size));
    Unwrap(client.SetMetadataAsync(std::move(options), boost::asio::use_future).get());
}

int64_t BlobHelpers::GetFileSize(AzureClient::BlobClient& client) {
    return FileSizeFromProperties(Unwrap(client.GetPropertiesAsync(boost::asio::use_future).get()));
}

int64_t BlobHelpers::FileSizeFromProperties(const AzureClient::Models::BlobProperties& properties) {
    auto metaIter = properties.Metadata.find(g_sizeMetadata);
    return metaIter != properties.Metadata.end() ? static_cast<int64_t>(std::stoll(metaIter->second)) : 0;
}

int64_t BlobHelpers::GetBlobCapacity(AzureClient::BlobClient& client) {
    const auto props = Unwrap(client.GetPropertiesAsync(boost::asio::use_future).get());
    return static_cast<int64_t>(props.ContentLength);
}

bool BlobHelpers::CreateIfNotExists(AzureClient::PageBlobClient& client, int64_t capacity) {
    AzureClient::CreatePageBlobOptions options;
    options.Conditions.IfNoneMatch = "*";
    auto result =
        client.CreateAsync(static_cast<uint64_t>(capacity), std::move(options), boost::asio::use_future).get();
    if (result.has_value()) {
        return true;
    }

    const auto& error = result.error();
    if (error.ErrorCode == "BlobAlreadyExists" ||
        error.Code == AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::BlobAlreadyExists)) {
        return false;
    }

    ThrowRequestFailed(error);
}

std::pair<int64_t, int64_t> BlobHelpers::RoundToEndOfNearestPage(int64_t size) {
    auto [partialPageSize, roundedSize] = RoundToBeginningOfNearestPage(size);
    if (partialPageSize != 0) {
        roundedSize += Configuration::PageBlob::PageSize;
    }

    return std::make_pair(partialPageSize, roundedSize);
}

std::pair<int64_t, int64_t> BlobHelpers::RoundToBeginningOfNearestPage(int64_t size) {
    const auto partialPageSize = size % static_cast<int64_t>(Configuration::PageBlob::PageSize);
    auto pages = (size / static_cast<int64_t>(Configuration::PageBlob::PageSize));
    const auto roundedSize = pages * static_cast<int64_t>(Configuration::PageBlob::PageSize);
    return std::make_pair(partialPageSize, roundedSize);
}

AzureClient::RetryOptions BlobHelpers::CreateRetryOptions() {
    AzureClient::RetryOptions retry;
    retry.MaxRetries = Configuration::MaxClientRetries;
    return retry;
}

AzureClient::BlobServiceClientOptions BlobHelpers::CreateServiceClientOptions(const std::string& storageAccountUrl) {
    AzureClient::BlobServiceClientOptions options;
    options.ServiceEndpoint = storageAccountUrl;
    options.Retry = CreateRetryOptions();
    return options;
}

std::shared_ptr<AzureClient::ITokenCredential>
BlobHelpers::CreateClientSecretCredential(ClientRuntime& runtime, const std::string& tenantId,
                                          const std::string& clientId, const std::string& clientSecret) {
    AzureClient::ClientSecretCredentialOptions options;
    options.TenantId = tenantId;
    options.ClientId = clientId;
    options.ClientSecret = clientSecret;
    options.Retry = CreateRetryOptions();
    return std::make_shared<AzureClient::CachingTokenCredential>(
        std::make_shared<AzureClient::ClientSecretCredential>(runtime.HttpClient(), std::move(options)));
}

std::shared_ptr<AzureClient::ITokenCredential>
BlobHelpers::CreatePipelinesCredential(ClientRuntime& runtime, const std::string& tenantId, const std::string& clientId,
                                       const std::string& serviceConnectionId, const std::string& systemAccessToken) {
    AzurePipelinesCredentialOptions options;
    options.TenantId = tenantId;
    options.ClientId = clientId;
    options.ServiceConnectionId = serviceConnectionId;
    options.SystemAccessToken = systemAccessToken;
    options.MaxRetries = Configuration::MaxClientRetries;
    return std::make_shared<AzureClient::CachingTokenCredential>(
        std::make_shared<AzurePipelinesCredential>(runtime.HttpClient(), std::move(options)));
}

std::shared_ptr<AzureClient::ITokenCredential>
BlobHelpers::CreateChainedCredential(ClientRuntime& runtime, const Models::ChainedCredentialInfo& chainedCredential) {
    std::vector<std::shared_ptr<AzureClient::ITokenCredential>> sources;

    // Try to use user specified credentials to try to authenticate first.
    {
        AzureClient::ClientSecretCredentialOptions options;
        options.TenantId = chainedCredential.GetTenantId();
        options.ClientId = chainedCredential.GetServicePrincipalId();
        options.ClientSecret = chainedCredential.GetServicePrincipalSecret();
        options.Retry = CreateRetryOptions();
        sources.push_back(
            std::make_shared<AzureClient::ClientSecretCredential>(runtime.HttpClient(), std::move(options)));
    }

    if (const auto managedIdentityId = chainedCredential.GetManagedIdentityId()) {
        auto options = AzureClient::ManagedIdentityCredentialOptions::FromEnvironment();
        options.ClientId = std::string(*managedIdentityId);
        options.Retry = CreateRetryOptions();
        sources.push_back(
            std::make_shared<AzureClient::ManagedIdentityCredential>(runtime.HttpClient(), std::move(options)));
    }

    // Environment credential: a client secret credential constructed from environment variable values.
    {
        AzureClient::ClientSecretCredentialOptions options;
        options.TenantId = GetEnvironmentValue("AZURE_TENANT_ID");
        options.ClientId = GetEnvironmentValue("AZURE_CLIENT_ID");
        options.ClientSecret = GetEnvironmentValue("AZURE_CLIENT_SECRET");
        if (const auto authorityHost = GetEnvironmentValue("AZURE_AUTHORITY_HOST"); !authorityHost.empty()) {
            options.AuthorityHost = authorityHost;
        }
        options.Retry = CreateRetryOptions();
        if (!options.TenantId.empty() && !options.ClientId.empty() && !options.ClientSecret.empty()) {
            sources.push_back(
                std::make_shared<AzureClient::ClientSecretCredential>(runtime.HttpClient(), std::move(options)));
        }
    }

    // Workload identity credential constructed from environment variables.
    {
        auto options = AzureClient::WorkloadIdentityCredentialOptions::FromEnvironment();
        options.Retry = CreateRetryOptions();
        if (!options.TenantId.empty() && !options.ClientId.empty() && !options.TokenFilePath.empty()) {
            sources.push_back(
                std::make_shared<AzureClient::WorkloadIdentityCredential>(runtime.HttpClient(), std::move(options)));
        }
    }

    return std::make_shared<AzureClient::CachingTokenCredential>(
        std::make_shared<ChainedTokenCredential>(std::move(sources)));
}

std::string BlobHelpers::AccountNameFromUrl(const std::string& storageAccountUrl) {
    auto hostStart = storageAccountUrl.find("://");
    hostStart = hostStart == std::string::npos ? 0 : hostStart + 3;
    const auto hostEnd = storageAccountUrl.find_first_of("./:", hostStart);
    return storageAccountUrl.substr(hostStart, hostEnd == std::string::npos ? std::string::npos : hostEnd - hostStart);
}

AzureClient::BlobServiceClient
BlobHelpers::CreateServiceClient(ClientRuntime& runtime, const Models::ServicePrincipalStorageInfo& servicePrincipal) {
    auto options = CreateServiceClientOptions(servicePrincipal.GetStorageAccountUrl());
    options.TokenCredential =
        CreateClientSecretCredential(runtime, servicePrincipal.GetTenantId(), servicePrincipal.GetServicePrincipalId(),
                                     servicePrincipal.GetServicePrincipalSecret());
    return AzureClient::BlobServiceClient{runtime.HttpClient(), std::move(options)};
}

AzureClient::BlobServiceClient
BlobHelpers::CreateServiceClient(ClientRuntime& runtime, const Models::ChainedCredentialInfo& chainedCredential) {
    auto options = CreateServiceClientOptions(chainedCredential.GetStorageAccountUrl());
    options.TokenCredential = CreateChainedCredential(runtime, chainedCredential);
    return AzureClient::BlobServiceClient{runtime.HttpClient(), std::move(options)};
}

std::shared_ptr<AzureClient::BlobContainerClient>
BlobHelpers::GetContainerClient(const AzureClient::BlobServiceClient& blobServiceClient, const std::string& name) {
    auto blobContainerClient =
        std::make_shared<AzureClient::BlobContainerClient>(blobServiceClient.GetBlobContainerClient(name));
    CreateIfNotExistsWithRetry(*blobContainerClient);
    return blobContainerClient;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
