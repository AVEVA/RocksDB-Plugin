// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <boost/asio/use_future.hpp>

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
std::chrono::milliseconds BlobHelpers::WithJitter(std::chrono::milliseconds delay) {
    thread_local std::mt19937 generator{std::random_device{}()};
    std::uniform_real_distribution<double> factor(0.8, 1.2);
    return std::chrono::milliseconds(
        static_cast<std::chrono::milliseconds::rep>(static_cast<double>(delay.count()) * factor(generator)));
}

int64_t BlobHelpers::FileSizeFromProperties(const AzureClient::Models::BlobProperties& properties,
                                            std::string_view blobName) {
    const auto metaIter = properties.Metadata.find(std::string(BlobHelpers::SizeMetadataKey));
    if (metaIter == properties.Metadata.end()) {
        return 0;
    }

    const std::string& text = metaIter->second;
    int64_t size = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), size);
    if (error != std::errc{} || end != text.data() + text.size() || size < 0) {
        std::string message =
            "Invalid '" + std::string(BlobHelpers::SizeMetadataKey) + "' metadata value '" + text + "'";
        if (!blobName.empty()) {
            message += " on blob '" + std::string(blobName) + "'";
        }
        throw std::runtime_error(message);
    }

    return size;
}

std::pair<int64_t, int64_t> BlobHelpers::RoundToEndOfNearestPage(int64_t size) {
    auto [partialPageSize, roundedSize] = RoundToBeginningOfNearestPage(size);
    if (partialPageSize != 0) {
        constexpr auto pageSize = static_cast<int64_t>(Configuration::PageBlob::PageSize);
        if (roundedSize > std::numeric_limits<int64_t>::max() - pageSize) {
            throw std::invalid_argument("Size " + std::to_string(size) + " cannot be rounded up to a whole page");
        }
        roundedSize += pageSize;
    }

    return std::make_pair(partialPageSize, roundedSize);
}

std::pair<int64_t, int64_t> BlobHelpers::RoundToBeginningOfNearestPage(int64_t size) {
    if (size < 0) {
        throw std::invalid_argument("Size must not be negative, got " + std::to_string(size));
    }

    constexpr auto pageSize = static_cast<int64_t>(Configuration::PageBlob::PageSize);
    const auto partialPageSize = size % pageSize;
    return std::make_pair(partialPageSize, size - partialPageSize);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
