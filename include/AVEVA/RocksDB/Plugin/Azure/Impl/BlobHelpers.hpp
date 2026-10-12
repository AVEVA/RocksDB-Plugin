// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <boost/asio/use_future.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
struct BlobHelpers {
    static constexpr std::string_view SizeMetadataKey = "filesize";

    template <typename TBlob> static void SetFileSize(TBlob& client, int64_t size) {
        AzureClient::SetBlobMetadataOptions options;
        options.Metadata.emplace(std::string(SizeMetadataKey), std::to_string(size));
        Unwrap(BlockOn(client.get_executor(), client.SetMetadataAsync(std::move(options), boost::asio::use_future)));
    }
    // Everything one GetProperties call reveals, so callers needing several values pay for a single round trip.
    struct BlobInfo {
        int64_t Size;
        int64_t Capacity;
        std::string ETag;
    };
    template <typename TBlob> static BlobInfo GetBlobInfo(TBlob& client) {
        auto properties = Unwrap(BlockOn(client.get_executor(), client.GetPropertiesAsync(boost::asio::use_future)));
        return {FileSizeFromProperties(properties), static_cast<int64_t>(properties.ContentLength),
                std::move(properties.ETag)};
    }
    template <typename TBlob> static int64_t GetFileSize(TBlob& client) {
        return FileSizeFromProperties(
            Unwrap(BlockOn(client.get_executor(), client.GetPropertiesAsync(boost::asio::use_future))));
    }
    // The logical file size recorded in the blob's metadata (0 when absent). Throws std::runtime_error (naming
    // lobName when given) when the value is not a non-negative integer.
    static int64_t FileSizeFromProperties(const AzureClient::Models::BlobProperties& properties,
                                          std::string_view blobName = {});
    template <typename TBlob> static int64_t GetBlobCapacity(TBlob& client) {
        const auto props = Unwrap(BlockOn(client.get_executor(), client.GetPropertiesAsync(boost::asio::use_future)));
        return static_cast<int64_t>(props.ContentLength);
    }
    // Creates the page blob with the given capacity unless it already exists. Returns true if it was created.
    template <typename TBlob> static bool CreateIfNotExists(TBlob& client, int64_t capacity) {
        AzureClient::CreatePageBlobOptions options;
        options.Conditions.IfNoneMatch = "*";
        auto result = BlockOn(client.get_executor(), client.CreateAsync(static_cast<uint64_t>(capacity),
                                                                        std::move(options), boost::asio::use_future));
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
    // Creates the container. Transient failures are retried by the client; this additionally retries 409 conflicts
    // (for example a container still being deleted) up to maxRetries attempts with a linearly growing, jittered delay.
    static void CreateContainerIfNotExists(Impl::BlobContainerClient& client, int maxRetries = 5,
                                           std::chrono::milliseconds baseDelay = std::chrono::seconds(1));
    static std::pair<int64_t, int64_t> RoundToEndOfNearestPage(int64_t size);
    static std::pair<int64_t, int64_t> RoundToBeginningOfNearestPage(int64_t size);
    static AzureClient::RetryOptions CreateRetryOptions();
    static AzureClient::BlobServiceClientOptions CreateServiceClientOptions(const std::string& storageAccountUrl);
    // Wraps `source` so that in-flight token refreshes keep `runtime` (and its HttpClient) alive, and caches tokens.
    static std::shared_ptr<PluginCredential> MakePluginCredential(const std::shared_ptr<ClientRuntime>& runtime,
                                                                  SourceCredential source);
    static std::shared_ptr<PluginCredential> CreateClientSecretCredential(const std::shared_ptr<ClientRuntime>& runtime,
                                                                          const std::string& tenantId,
                                                                          const std::string& clientId,
                                                                          const std::string& clientSecret);
    static std::shared_ptr<PluginCredential> CreatePipelinesCredential(const std::shared_ptr<ClientRuntime>& runtime,
                                                                       const std::string& tenantId,
                                                                       const std::string& clientId,
                                                                       const std::string& serviceConnectionId,
                                                                       const std::string& systemAccessToken);
    // The ordered credential sources behind CreateChainedCredential: service principal, managed identity
    // (system-assigned when no id is given), then environment and workload identity when their variables are set.
    static std::vector<std::shared_ptr<LeafCredential>>
    CreateCredentialSources(const std::shared_ptr<ClientRuntime>& runtime,
                            const Models::ChainedCredentialInfo& chainedCredential);
    static std::shared_ptr<PluginCredential>
    CreateChainedCredential(const std::shared_ptr<ClientRuntime>& runtime,
                            const Models::ChainedCredentialInfo& chainedCredential);
    static std::string AccountNameFromUrl(const std::string& storageAccountUrl);
    static Impl::BlobServiceClient CreateServiceClient(const std::shared_ptr<ClientRuntime>& runtime,
                                                       const Models::ServicePrincipalStorageInfo& servicePrincipal);
    static Impl::BlobServiceClient CreateServiceClient(const std::shared_ptr<ClientRuntime>& runtime,
                                                       const Models::ChainedCredentialInfo& servicePrincipal);
    static std::shared_ptr<Impl::BlobContainerClient>
    GetContainerClient(const Impl::BlobServiceClient& blobServiceClient, const std::string& name);
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
