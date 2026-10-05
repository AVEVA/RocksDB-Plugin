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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
struct BlobHelpers {
    static void SetFileSize(AzureClient::BlobClient& client, int64_t size);
    static int64_t GetFileSize(AzureClient::BlobClient& client);
    // The logical file size recorded in the blob's metadata (0 when absent). Throws std::runtime_error (naming
    // lobName when given) when the value is not a non-negative integer.
    static int64_t FileSizeFromProperties(const AzureClient::Models::BlobProperties& properties,
                                          std::string_view blobName = {});
    static int64_t GetBlobCapacity(AzureClient::BlobClient& client);
    // Creates the page blob with the given capacity unless it already exists. Returns true if it was created.
    static bool CreateIfNotExists(AzureClient::PageBlobClient& client, int64_t capacity);
    // Creates the container. Transient failures are retried by the client; this additionally retries 409 conflicts
    // (for example a container still being deleted) up to maxRetries attempts with a linearly growing, jittered delay.
    static void CreateContainerIfNotExists(AzureClient::BlobContainerClient& client, int maxRetries = 5,
                                           std::chrono::milliseconds baseDelay = std::chrono::seconds(1));
    static std::pair<int64_t, int64_t> RoundToEndOfNearestPage(int64_t size);
    static std::pair<int64_t, int64_t> RoundToBeginningOfNearestPage(int64_t size);
    static AzureClient::RetryOptions CreateRetryOptions();
    static AzureClient::BlobServiceClientOptions CreateServiceClientOptions(const std::string& storageAccountUrl);
    // Wraps `credential` so that in-flight token refreshes keep `runtime` (and its IHttpClient) alive.
    static std::shared_ptr<AzureClient::ITokenCredential>
    BindToRuntime(const std::shared_ptr<ClientRuntime>& runtime, std::shared_ptr<AzureClient::ITokenCredential> credential);
    static std::shared_ptr<AzureClient::ITokenCredential> CreateClientSecretCredential(const std::shared_ptr<ClientRuntime>& runtime,
                                                                                       const std::string& tenantId,
                                                                                       const std::string& clientId,
                                                                                       const std::string& clientSecret);
    static std::shared_ptr<AzureClient::ITokenCredential>
    CreatePipelinesCredential(const std::shared_ptr<ClientRuntime>& runtime, const std::string& tenantId, const std::string& clientId,
                              const std::string& serviceConnectionId, const std::string& systemAccessToken);
    // The ordered credential sources behind CreateChainedCredential: service principal, managed identity
    // (system-assigned when no id is given), then environment and workload identity when their variables are set.
    static std::vector<std::shared_ptr<AzureClient::ITokenCredential>>
    CreateCredentialSources(const std::shared_ptr<ClientRuntime>& runtime, const Models::ChainedCredentialInfo& chainedCredential);
    static std::shared_ptr<AzureClient::ITokenCredential>
    CreateChainedCredential(const std::shared_ptr<ClientRuntime>& runtime, const Models::ChainedCredentialInfo& chainedCredential);
    static std::string AccountNameFromUrl(const std::string& storageAccountUrl);
    static AzureClient::BlobServiceClient
    CreateServiceClient(const std::shared_ptr<ClientRuntime>& runtime, const Models::ServicePrincipalStorageInfo& servicePrincipal);
    static AzureClient::BlobServiceClient CreateServiceClient(const std::shared_ptr<ClientRuntime>& runtime,
                                                              const Models::ChainedCredentialInfo& servicePrincipal);
    static std::shared_ptr<AzureClient::BlobContainerClient>
    GetContainerClient(const AzureClient::BlobServiceClient& blobServiceClient, const std::string& name);
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
