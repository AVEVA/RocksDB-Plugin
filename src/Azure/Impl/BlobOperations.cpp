// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <boost/asio/use_future.hpp>

#include <cassert>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <stdexcept>
namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations {
namespace {
AzureClient::Models::BlobByteRange ToRange(int64_t offset, int64_t length) {
    return AzureClient::Models::BlobByteRange{static_cast<uint64_t>(offset), static_cast<uint64_t>(length)};
}
} // namespace

int64_t GetSize(AzureClient::PageBlobClient& client) { return BlobHelpers::GetFileSize(client); }

void SetSize(const ClientRuntime& runtime, AzureClient::PageBlobClient& client, int64_t size) {
    runtime.ThrowIfWritesFenced();
    BlobHelpers::SetFileSize(client, size);
}

int64_t GetCapacity(AzureClient::PageBlobClient& client) { return BlobHelpers::GetBlobCapacity(client); }

void SetCapacity(const ClientRuntime& runtime, AzureClient::PageBlobClient& client, int64_t capacity) {
    runtime.ThrowIfWritesFenced();
    Unwrap(BlockOn(client.get_executor(),
                   client.ResizeAsync(static_cast<uint64_t>(capacity), AzureClient::ResizePageBlobOptions{},
                                      boost::asio::use_future)));
}

std::string GetEtag(AzureClient::PageBlobClient& client) {
    auto properties = Unwrap(BlockOn(client.get_executor(), client.GetPropertiesAsync(boost::asio::use_future)));
    return std::move(properties.ETag);
}

BlobMetadata GetMetadata(AzureClient::PageBlobClient& client) {
    auto info = BlobHelpers::GetBlobInfo(client);
    return {info.Size, std::move(info.ETag)};
}

void DownloadToFile(AzureClient::PageBlobClient& client, const std::string& path, int64_t offset, int64_t length) {
    // An explicit zero-length range is rejected by the client, and there is nothing to fetch for an empty blob.
    if (length <= 0) {
        std::ofstream file(std::filesystem::path(path), std::ios::binary | std::ios::trunc);
        if (!file) {
            throw std::runtime_error("Could not create empty file '" + path + "'");
        }
        return;
    }

    AzureClient::DownloadToOptions options;
    options.Range = ToRange(offset, length);
    options.ChunkSize = Configuration::Transfer::DownloadChunkSize;
    options.Concurrency = Configuration::Transfer::DownloadConcurrency;
    Unwrap(BlockOn(client.get_executor(),
                   client.DownloadToAsync(std::filesystem::path(path), std::move(options), boost::asio::use_future)));
}

int64_t DownloadToBuffer(AzureClient::PageBlobClient& client, std::span<char> buffer, int64_t offset, int64_t length) {
    if (length <= 0) {
        return 0;
    }

    return Download(client, buffer, offset, length, std::string());
}

// A successful ranged response without Content-Range is accepted; the body length is the source of truth.
int64_t Download(AzureClient::PageBlobClient& client, std::span<char> buffer, int64_t offset, int64_t length,
                 const std::string& ifMatch) {
    AzureClient::DownloadBlobOptions options;
    options.Range = ToRange(offset, length);
    if (!ifMatch.empty()) {
        options.Conditions.IfMatch = ifMatch;
    }
    // The body is copied once, straight into the caller's buffer; a range larger than the buffer fails instead of
    // being truncated or overflowing.
    const auto result = Unwrap(BlockOn(
        client.get_executor(), client.DownloadToAsync(buffer, std::move(options), boost::asio::use_future,
                                                      RequestOptionsForTransfer(client.GetDefaultRequestOptions(),
                                                                                static_cast<uint64_t>(length)))));

    assert((!result.ContentRange.has_value() || !result.ContentRange->Length.has_value() ||
            *result.ContentRange->Length == result.BytesWritten) &&
           "Body size differs from server ContentRange");
    return static_cast<int64_t>(result.BytesWritten);
}

void UploadPages(const ClientRuntime& runtime, AzureClient::PageBlobClient& client, std::span<const char> buffer,
                 int64_t blobOffset) {
    runtime.ThrowIfWritesFenced();
    Unwrap(BlockOn(
        client.get_executor(),
        client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::as_bytes(buffer), boost::asio::use_future,
                                RequestOptionsForTransfer(client.GetDefaultRequestOptions(), buffer.size()))));
}

void UploadPagesAsync(const ClientRuntime& runtime, AzureClient::PageBlobClient& client,
                      std::shared_ptr<const std::vector<char>> data, int64_t blobOffset, UploadCallback callback) {
    try {
        runtime.ThrowIfWritesFenced();
    } catch (...) {
        callback(std::current_exception());
        return;
    }

    // The request views this buffer and keeps it alive itself, so the payload is never copied.
    const auto size = data->size();
    client.UploadPagesAsync(
        static_cast<uint64_t>(blobOffset), std::move(data),
        [callback = std::move(callback)](auto result) {
            std::exception_ptr error;
            try {
                Unwrap(std::move(result));
            } catch (...) {
                error = std::current_exception();
            }
            callback(error);
        },
        RequestOptionsForTransfer(client.GetDefaultRequestOptions(), size));
}

void DownloadAsync(AzureClient::PageBlobClient& client, int64_t blobOffset, int64_t readLength,
                   const std::string& ifMatch, std::chrono::milliseconds timeout, DownloadCallback callback) {
    AzureClient::DownloadBlobOptions options;
    options.Range = ToRange(blobOffset, readLength);
    options.Conditions.IfMatch = ifMatch;
    // The transfer-scaled default timeout can be minutes; a caller-supplied deadline only ever shortens it.
    auto requestOptions =
        RequestOptionsForTransfer(client.GetDefaultRequestOptions(), static_cast<uint64_t>(readLength));
    if (timeout.count() > 0 && timeout < requestOptions.GetTimeout()) {
        requestOptions.SetTimeout(timeout);
    }
    client.DownloadAsync(
        std::move(options),
        [callback = std::move(callback)](auto result) mutable {
            std::exception_ptr error;
            std::string content;
            try {
                content = std::move(Unwrap(std::move(result)).Content);
            } catch (...) {
                error = std::current_exception();
            }
            callback(error, std::move(content));
        },
        std::move(requestOptions));
}

void GetMetadataAsync(AzureClient::PageBlobClient& client, MetadataCallback callback) {
    client.GetPropertiesAsync([callback = std::move(callback)](auto result) mutable {
        std::exception_ptr error;
        int64_t size = 0;
        std::string etag;
        try {
            auto properties = Unwrap(std::move(result));
            size = BlobHelpers::FileSizeFromProperties(properties);
            etag = std::move(properties.ETag);
        } catch (...) {
            error = std::current_exception();
        }
        callback(error, size, std::move(etag));
    });
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations
