// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"

#include <boost/asio/use_future.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
// Chunking used when downloading a range of a blob into a local file.
const constexpr std::size_t g_downloadChunkSize = static_cast<std::size_t>(4) * 1024 * 1024;
const constexpr std::size_t g_downloadConcurrency = 4;

AzureClient::Models::BlobByteRange ToRange(int64_t offset, int64_t length) {
    return AzureClient::Models::BlobByteRange{static_cast<uint64_t>(offset), static_cast<uint64_t>(length)};
}
} // namespace

PageBlob::PageBlob(std::shared_ptr<ClientRuntime> runtime, AzureClient::PageBlobClient client)
    : m_runtime(std::move(runtime)), m_client(std::move(client)) {}

int64_t PageBlob::GetSize() { return BlobHelpers::GetFileSize(m_client); }

void PageBlob::SetSize(int64_t size) {
    m_runtime->ThrowIfWritesFenced();
    BlobHelpers::SetFileSize(m_client, size);
}

int64_t PageBlob::GetCapacity() { return BlobHelpers::GetBlobCapacity(m_client); }

void PageBlob::SetCapacity(int64_t capacity) {
    m_runtime->ThrowIfWritesFenced();
    Unwrap(
        BlockOn(m_client.get_executor(), m_client
            .ResizeAsync(static_cast<uint64_t>(capacity), AzureClient::ResizePageBlobOptions{}, boost::asio::use_future)));
}

void PageBlob::DownloadTo(const std::string& path, int64_t offset, int64_t length) {
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
    options.ChunkSize = g_downloadChunkSize;
    options.Concurrency = g_downloadConcurrency;
    Unwrap(BlockOn(m_client.get_executor(), m_client.DownloadToAsync(std::filesystem::path(path), std::move(options), boost::asio::use_future)));
}

int64_t PageBlob::DownloadTo(std::span<char> buffer, int64_t offset, int64_t length) {
    if (length <= 0) {
        return 0;
    }

    return Download(buffer, offset, length, std::string());
}

void PageBlob::UploadPages(const std::span<char> buffer, const int64_t blobOffset) {
    m_runtime->ThrowIfWritesFenced();
    Unwrap(BlockOn(m_client.get_executor(), m_client
               .UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::as_bytes(buffer), boost::asio::use_future,
                                 RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(), buffer.size()))));
}

void PageBlob::UploadPagesAsync(std::vector<char> data, const int64_t blobOffset, UploadCallback callback) {
    UploadPagesAsync(std::make_shared<const std::vector<char>>(std::move(data)), blobOffset, std::move(callback));
}

void PageBlob::UploadPagesAsync(std::shared_ptr<const std::vector<char>> owned, const int64_t blobOffset,
                                UploadCallback callback) {
    try {
        m_runtime->ThrowIfWritesFenced();
    } catch (...) {
        callback(std::current_exception());
        return;
    }

    // The request views this buffer and keeps it alive itself, so the payload is never copied.
    const auto size = owned->size();
    m_client.UploadPagesAsync(
        static_cast<uint64_t>(blobOffset), std::move(owned),
        [callback = std::move(callback)](auto result) {
            std::exception_ptr error;
            try {
                Unwrap(std::move(result));
            } catch (...) {
                error = std::current_exception();
            }
            callback(error);
        },
        RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(), size));
}

Core::BlobMetadata PageBlob::GetMetadata() {
    auto properties = Unwrap(BlockOn(m_client.get_executor(), m_client.GetPropertiesAsync(boost::asio::use_future)));
    return {BlobHelpers::FileSizeFromProperties(properties), std::move(properties.ETag)};
}

std::string PageBlob::GetEtag() {
    auto properties = Unwrap(BlockOn(m_client.get_executor(), m_client.GetPropertiesAsync(boost::asio::use_future)));
    return std::move(properties.ETag);
}

void PageBlob::DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                             DownloadCallback callback) {
    DownloadAsync(blobOffset, readLength, ifMatch, std::chrono::milliseconds::zero(), std::move(callback));
}

void PageBlob::DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                             std::chrono::milliseconds timeout, DownloadCallback callback) {
    AzureClient::DownloadBlobOptions options;
    options.Range = ToRange(blobOffset, readLength);
    options.Conditions.IfMatch = ifMatch;
    // The transfer-scaled default timeout can be minutes; a caller-supplied deadline only ever shortens it.
    auto requestOptions =
        RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(), static_cast<uint64_t>(readLength));
    if (timeout.count() > 0 && timeout < requestOptions.GetTimeout()) {
        requestOptions.SetTimeout(timeout);
    }
    m_client.DownloadAsync(
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

void PageBlob::GetMetadataAsync(MetadataCallback callback) {
    m_client.GetPropertiesAsync([callback = std::move(callback)](auto result) mutable {
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

// A successful ranged response without Content-Range is accepted; the body length is the source of truth.
int64_t PageBlob::Download(std::span<char> buffer, int64_t offset, int64_t length, const std::string& ifMatch) {
    AzureClient::DownloadBlobOptions options;
    options.Range = ToRange(offset, length);
    if (!ifMatch.empty()) {
        options.Conditions.IfMatch = ifMatch;
    }
    const auto result = Unwrap(BlockOn(m_client.get_executor(), m_client
                                   .DownloadAsync(std::move(options), boost::asio::use_future,
                                                  RequestOptionsForTransfer(m_client.GetDefaultRequestOptions(),
                                                                            static_cast<uint64_t>(length)))));

    // TODO(backlog): AzureClient only returns the body as std::string, so every download is copied once more into
    // the caller's buffer. Writing the response body straight into a caller-provided span needs support in
    // AzureClient's DownloadBlobOptions/HttpClient; track as an AzureClient enhancement.
    const auto bytesRead = std::min(result.Content.size(), buffer.size());
    std::memcpy(buffer.data(), result.Content.data(), bytesRead);

    assert((!result.ContentRange.has_value() || !result.ContentRange->Length.has_value() ||
            *result.ContentRange->Length == result.Content.size()) &&
           "Body size differs from server ContentRange");
    return static_cast<int64_t>(bytesRead);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
