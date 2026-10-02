// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/ITokenCredential.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
struct BlobHelpers {
    static void SetFileSize(AzureClient::BlobClient& client, int64_t size);
    static int64_t GetFileSize(AzureClient::BlobClient& client);
    static int64_t GetBlobCapacity(AzureClient::BlobClient& client);
    // Creates the page blob with the given capacity unless it already exists. Returns true if it was created.
    static bool CreateIfNotExists(AzureClient::PageBlobClient& client, int64_t capacity);
    static std::pair<int64_t, int64_t> RoundToEndOfNearestPage(int64_t size);
    static std::pair<int64_t, int64_t> RoundToBeginningOfNearestPage(int64_t size);
    static AzureClient::RetryOptions CreateRetryOptions();
    static AzureClient::BlobServiceClientOptions CreateServiceClientOptions(const std::string& storageAccountUrl);
    static std::shared_ptr<AzureClient::ITokenCredential> CreateClientSecretCredential(ClientRuntime& runtime,
                                                                                       const std::string& tenantId,
                                                                                       const std::string& clientId,
                                                                                       const std::string& clientSecret);
    static std::shared_ptr<AzureClient::ITokenCredential>
    CreatePipelinesCredential(ClientRuntime& runtime, const std::string& tenantId, const std::string& clientId,
                              const std::string& serviceConnectionId, const std::string& systemAccessToken);
    static std::shared_ptr<AzureClient::ITokenCredential>
    CreateChainedCredential(ClientRuntime& runtime, const Models::ChainedCredentialInfo& chainedCredential);
    static std::string AccountNameFromUrl(const std::string& storageAccountUrl);
    static AzureClient::BlobServiceClient
    CreateServiceClient(ClientRuntime& runtime, const Models::ServicePrincipalStorageInfo& servicePrincipal);
    static AzureClient::BlobServiceClient CreateServiceClient(ClientRuntime& runtime,
                                                              const Models::ChainedCredentialInfo& servicePrincipal);
    static std::shared_ptr<AzureClient::BlobContainerClient>
    GetContainerClient(const AzureClient::BlobServiceClient& blobServiceClient, const std::string& name);
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
