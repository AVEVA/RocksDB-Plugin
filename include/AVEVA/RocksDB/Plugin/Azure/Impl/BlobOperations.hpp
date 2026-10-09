// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations {
/// <summary>
/// Size and ETag of a blob, read together so that they describe the same version of the blob.
/// </summary>
struct BlobMetadata {
    int64_t Size = 0;
    std::string ETag;
};

/// <summary>Completion of UploadPagesAsync: an exception, or null on success.</summary>
using UploadCallback = std::function<void(std::exception_ptr error)>;
/// <summary>Completion of DownloadAsync: an exception (null on success) and the downloaded bytes.</summary>
using DownloadCallback = std::function<void(std::exception_ptr error, std::string data)>;
/// <summary>Completion of GetMetadataAsync: an exception (null on success), the blob's size and its ETag.</summary>
using MetadataCallback = std::function<void(std::exception_ptr error, int64_t size, std::string etag)>;

// The synchronous operations block on the calling thread, which must never be one running the host io_context.
// Mutations throw when `runtime` has fenced writes. Failures are thrown as RequestFailedException.

[[nodiscard]] int64_t GetSize(AzureClient::PageBlobClient& client);
void SetSize(const ClientRuntime& runtime, AzureClient::PageBlobClient& client, int64_t size);
[[nodiscard]] int64_t GetCapacity(AzureClient::PageBlobClient& client);
void SetCapacity(const ClientRuntime& runtime, AzureClient::PageBlobClient& client, int64_t capacity);
[[nodiscard]] std::string GetEtag(AzureClient::PageBlobClient& client);
// A single request, so the size and ETag describe the same version of the blob.
[[nodiscard]] BlobMetadata GetMetadata(AzureClient::PageBlobClient& client);

// Downloads [offset, offset + length) to a local file; a non-positive length produces an empty file.
void DownloadToFile(AzureClient::PageBlobClient& client, const std::string& path, int64_t offset, int64_t length);
// Returns the number of bytes written to `buffer`; a non-positive length returns 0 without a request.
[[nodiscard]] int64_t DownloadToBuffer(AzureClient::PageBlobClient& client, std::span<char> buffer, int64_t offset,
                                       int64_t length);
// As DownloadToBuffer, but fails when the blob's ETag no longer matches `ifMatch` (ignored when empty).
[[nodiscard]] int64_t Download(AzureClient::PageBlobClient& client, std::span<char> buffer, int64_t offset,
                               int64_t length, const std::string& ifMatch);
void UploadPages(const ClientRuntime& runtime, AzureClient::PageBlobClient& client, std::span<const char> buffer,
                 int64_t blobOffset);

// The asynchronous operations never block; completions run on the host io_context's threads (or inline when the
// request cannot be started) and must not block on further blob I/O.
void UploadPagesAsync(const ClientRuntime& runtime, AzureClient::PageBlobClient& client,
                      std::shared_ptr<const std::vector<char>> data, int64_t blobOffset, UploadCallback callback);
// A non-zero `timeout` caps how long the request may take; it only ever shortens the transfer-scaled default.
void DownloadAsync(AzureClient::PageBlobClient& client, int64_t blobOffset, int64_t readLength,
                   const std::string& ifMatch, std::chrono::milliseconds timeout, DownloadCallback callback);
void GetMetadataAsync(AzureClient::PageBlobClient& client, MetadataCallback callback);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations
