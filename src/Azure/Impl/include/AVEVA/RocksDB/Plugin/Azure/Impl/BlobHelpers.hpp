// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RequestHelpers.hpp"

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
        auto result = Block(client, client.CreateIfNotExistsAsync());
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
// For reads that must fill a known range (unlike EOF-tolerant random reads): throws a RequestFailedException with
// InvalidResponse when fewer or more bytes than `expected` arrived, since a short read would leave stale buffer
// contents that a later upload writes over real data.
void RequireFullDownload(std::string_view name, int64_t offset, int64_t expected, int64_t actual);
// Both return {bytes past the page boundary, rounded size}. They throw std::invalid_argument for a negative size, and
// RoundToEndOfNearestPage also when the rounded-up size does not fit in int64_t.
[[nodiscard]] std::pair<int64_t, int64_t> RoundToEndOfNearestPage(int64_t size);
[[nodiscard]] std::pair<int64_t, int64_t> RoundToBeginningOfNearestPage(int64_t size);
// Gets the container client for `name`, creating the container when it does not exist yet.
template <typename TService> auto GetContainerClient(const TService& blobServiceClient, const std::string& name) {
    const auto blobContainerClient =
        std::make_shared<std::remove_cvref_t<decltype(blobServiceClient.GetBlobContainerClient(name))>>(
            blobServiceClient.GetBlobContainerClient(name));
    CreateContainerIfNotExists(*blobContainerClient);
    return blobContainerClient;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobHelpers
