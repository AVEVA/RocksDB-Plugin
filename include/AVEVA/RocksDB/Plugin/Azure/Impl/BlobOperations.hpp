// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <boost/asio/use_future.hpp>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// Every operation is a template on the page blob client type (`TBlob`): the real client in production, an in-memory
// fake in tests. Completion callbacks are templated too, so they are invoked without being type-erased.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations {
/// <summary>
/// Size and ETag of a blob, read together so that they describe the same version of the blob.
/// </summary>
struct BlobMetadata {
    int64_t Size = 0;
    std::string ETag;
};

namespace Detail {
inline AzureClient::Models::BlobByteRange ToRange(int64_t offset, int64_t length) {
    return AzureClient::Models::BlobByteRange{static_cast<uint64_t>(offset), static_cast<uint64_t>(length)};
}
} // namespace Detail

// The synchronous operations block on the calling thread, which must never be one running the host io_context.
// Mutations throw when `runtime` has fenced writes. Failures are thrown as RequestFailedException.

template <typename TBlob> [[nodiscard]] int64_t GetSize(TBlob& client) { return BlobHelpers::GetFileSize(client); }

template <typename TBlob> void SetSize(const ClientRuntime& runtime, TBlob& client, int64_t size) {
    runtime.ThrowIfWritesFenced();
    BlobHelpers::SetFileSize(client, size);
}

template <typename TBlob> [[nodiscard]] int64_t GetCapacity(TBlob& client) {
    return BlobHelpers::GetBlobCapacity(client);
}

template <typename TBlob> void SetCapacity(const ClientRuntime& runtime, TBlob& client, int64_t capacity) {
    runtime.ThrowIfWritesFenced();
    Unwrap(BlockOn(client.get_executor(),
                   client.ResizeAsync(static_cast<uint64_t>(capacity), AzureClient::ResizePageBlobOptions{},
                                      boost::asio::use_future)));
}

template <typename TBlob> [[nodiscard]] std::string GetEtag(TBlob& client) {
    auto properties = Unwrap(BlockOn(client.get_executor(), client.GetPropertiesAsync(boost::asio::use_future)));
    return std::move(properties.ETag);
}

// A single request, so the size and ETag describe the same version of the blob.
template <typename TBlob> [[nodiscard]] BlobMetadata GetMetadata(TBlob& client) {
    auto info = BlobHelpers::GetBlobInfo(client);
    return {info.Size, std::move(info.ETag)};
}

// Downloads [offset, offset + length) to a local file; a non-positive length produces an empty file.
template <typename TBlob> void DownloadToFile(TBlob& client, const std::string& path, int64_t offset, int64_t length) {
    // An explicit zero-length range is rejected by the client, and there is nothing to fetch for an empty blob.
    if (length <= 0) {
        std::ofstream file(std::filesystem::path(path), std::ios::binary | std::ios::trunc);
        if (!file) {
            throw std::runtime_error("Could not create empty file '" + path + "'");
        }
        return;
    }

    AzureClient::DownloadToOptions options;
    options.Range = Detail::ToRange(offset, length);
    options.ChunkSize = Configuration::Transfer::DownloadChunkSize;
    options.Concurrency = Configuration::Transfer::DownloadConcurrency;
    Unwrap(BlockOn(client.get_executor(),
                   client.DownloadToAsync(std::filesystem::path(path), std::move(options), boost::asio::use_future)));
}

// As DownloadToBuffer, but fails when the blob's ETag no longer matches `ifMatch` (ignored when empty). A successful
// ranged response without Content-Range is accepted; the body length is the source of truth.
template <typename TBlob>
[[nodiscard]] int64_t Download(TBlob& client, std::span<char> buffer, int64_t offset, int64_t length,
                               const std::string& ifMatch) {
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(offset, length);
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

// Returns the number of bytes written to `buffer`; a non-positive length returns 0 without a request.
template <typename TBlob>
[[nodiscard]] int64_t DownloadToBuffer(TBlob& client, std::span<char> buffer, int64_t offset, int64_t length) {
    if (length <= 0) {
        return 0;
    }

    return Download(client, buffer, offset, length, std::string());
}

template <typename TBlob>
void UploadPages(const ClientRuntime& runtime, TBlob& client, std::span<const char> buffer, int64_t blobOffset) {
    runtime.ThrowIfWritesFenced();
    Unwrap(BlockOn(
        client.get_executor(),
        client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::as_bytes(buffer), boost::asio::use_future,
                                RequestOptionsForTransfer(client.GetDefaultRequestOptions(), buffer.size()))));
}

// The asynchronous operations never block; completions run on the host io_context's threads (or inline when the
// request cannot be started) and must not block on further blob I/O.

/// <summary>`callback(std::exception_ptr error)`: an exception, or null on success.</summary>
template <typename TBlob, typename TCallback>
void UploadPagesAsync(const ClientRuntime& runtime, TBlob& client, std::shared_ptr<const std::vector<char>> data,
                      int64_t blobOffset, TCallback callback) {
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
        [callback = std::move(callback)](auto result) mutable {
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

/// <summary>
/// `callback(std::exception_ptr error, std::string data)`: an exception (null on success) and the downloaded bytes.
/// A non-zero `timeout` caps how long the request may take; it only ever shortens the transfer-scaled default.
/// </summary>
template <typename TBlob, typename TCallback>
void DownloadAsync(TBlob& client, int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                   std::chrono::milliseconds timeout, TCallback callback) {
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(blobOffset, readLength);
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

/// <summary>
/// `callback(std::exception_ptr error, int64_t size, std::string etag)`: an exception (null on success), the blob's
/// size and its ETag.
/// </summary>
template <typename TBlob, typename TCallback> void GetMetadataAsync(TBlob& client, TCallback callback) {
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
