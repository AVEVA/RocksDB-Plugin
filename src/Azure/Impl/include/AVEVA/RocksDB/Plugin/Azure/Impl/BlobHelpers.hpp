// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <AVEVA/AzureClient/BlockOn.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobHelpers {
inline constexpr std::string_view SizeMetadataKey = "filesize";

// The logical file size recorded in the blob's metadata (0 when absent). Throws std::runtime_error (naming
// the blob when given) when the value is not a non-negative integer.
int64_t FileSizeFromProperties(const AzureClient::Models::BlobProperties& properties, std::string_view blobName = {});
// Adds up to +-20% so concurrent clients retrying the same condition do not move in lock-step.
std::chrono::milliseconds WithJitter(std::chrono::milliseconds delay);
// Creates the container. Transient failures are retried by the client; this additionally retries 409 conflicts
// (for example a container still being deleted) up to maxRetries attempts with a linearly growing, jittered delay.
// The client already retries transient failures (Configuration::MaxClientRetries), so this loop only waits out
// 409 contention such as ContainerBeingDeleted, which the client treats as final. Any other failure is rethrown
// immediately; retrying here as well would multiply the worst-case latency.
template <typename TContainer>
void CreateContainerIfNotExists(TContainer& client, int maxRetries = 5,
                                std::chrono::milliseconds baseDelay = std::chrono::seconds(1)) {
    int retries = 1;
    while (true) {
        auto result = client.CreateIfNotExistsAsync(AzureClient::block_on);
        if (result.has_value()) {
            return;
        }

        const auto& error = result.error();
        if (retries >= maxRetries || error.StatusCode != HttpStatus::Conflict) {
            ThrowRequestFailed(error);
        }

        retries++;
        std::this_thread::sleep_for(WithJitter(baseDelay * retries));
    }
}
std::pair<int64_t, int64_t> RoundToEndOfNearestPage(int64_t size);
std::pair<int64_t, int64_t> RoundToBeginningOfNearestPage(int64_t size);
// Gets the container client for `name`, creating the container when it does not exist yet.
template <typename TService> auto GetContainerClient(const TService& blobServiceClient, const std::string& name) {
    std::shared_ptr<typename std::remove_cvref_t<decltype(*blobServiceClient.GetBlobContainerClient(name))>>
        blobContainerClient = blobServiceClient.GetBlobContainerClient(name);
    CreateContainerIfNotExists(*blobContainerClient);
    return blobContainerClient;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobHelpers
