// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>
/// Size and ETag of a blob, read together so that they describe the same version of the blob.
/// </summary>
struct BlobMetadata {
    int64_t Size = 0;
    std::string ETag;
};

class BlobClient {
  public:
    virtual ~BlobClient() = default;

    /// <summary>
    /// Returns the size of the blob's representable data.
    /// </summary>
    /// <returns>The size of the blob.</returns>
    virtual int64_t GetSize() = 0;

    /// <summary>
    /// Sets the size of the blob to the specified value.
    /// </summary>
    /// <param name="size">The new size to set, in bytes.</param>
    virtual void SetSize(int64_t size) = 0;

    /// <summary>
    /// Returns the capacity of the blob.
    /// </summary>
    /// <returns>The capacity value.</returns>
    virtual int64_t GetCapacity() = 0;

    /// <summary>
    /// Sets the blob's capacity to the specified value.
    /// </summary>
    /// <param name="capacity">The new capacity value to set, in bytes.</param>
    virtual void SetCapacity(int64_t capacity) = 0;

    /// <summary>
    /// Downloads a portion of data to the specified file path.
    /// </summary>
    /// <param name="path">The destination file path where the data will be saved.</param>
    /// <param name="offset">The starting position (in bytes) from which to begin downloading.</param>
    /// <param name="length">The number of bytes to download from the offset.</param>
    virtual void DownloadTo(const std::string& path, int64_t offset, int64_t length) = 0;

    /// <summary>
    /// Downloads data into the provided buffer starting at the specified offset for the given length.
    /// </summary>
    /// <param name="buffer">A span of bytes where the downloaded data will be stored.</param>
    /// <param name="blobOffset">The starting position (in bytes) in the blob from which to begin downloading.</param>
    /// <param name="length">The number of bytes to download from the offset.</param>
    /// <returns>The number of bytes actually downloaded or -1 if some problem occurred.</returns>
    virtual int64_t DownloadTo(std::span<char> buffer, int64_t blobOffset, int64_t length) = 0;

    /// <summary>
    /// Uploads a sequence of pages to a blob at the specified offset.
    /// </summary>
    /// <param name="buffer">A span containing the page data to upload.</param>
    /// <param name="blobOffset">The offset within the blob where the data should be uploaded.</param>
    virtual void UploadPages(const std::span<char> buffer, int64_t blobOffset) = 0;

    /// <summary>
    /// Completion of UploadPagesAsync: an exception, or null on success.
    /// </summary>
    using UploadCallback = std::function<void(std::exception_ptr error)>;

    /// <summary>
    /// Asynchronously uploads a sequence of pages, taking ownership of the data. The callback may run on any thread
    /// (including inline) and must not block on further blob I/O. The default implementation is synchronous.
    /// </summary>
    virtual void UploadPagesAsync(std::vector<char> data, int64_t blobOffset, UploadCallback callback) {
        std::exception_ptr error;
        try {
            UploadPages(std::span<char>(data), blobOffset);
        } catch (...) {
            error = std::current_exception();
        }
        callback(error);
    }

    /// <summary>
    /// As above, but the data is shared so implementations can send it without copying; the owner may recycle the
    /// buffer once its last reference is released. The default implementation copies into the vector overload.
    /// </summary>
    virtual void UploadPagesAsync(std::shared_ptr<const std::vector<char>> data, int64_t blobOffset,
                                  UploadCallback callback) {
        UploadPagesAsync(std::vector<char>(data->begin(), data->end()), blobOffset, std::move(callback));
    }

    /// <summary>
    /// Retrieve the current ETag of the blob.
    /// </summary>
    /// <returns>The current ETag of the blob.</returns>
    virtual std::string GetEtag() = 0;

    /// <summary>
    /// Retrieves the blob's size and ETag. Implementations should use a single request; the default calls
    /// GetSize and GetEtag, which may observe two different versions of the blob.
    /// </summary>
    virtual BlobMetadata GetMetadata() { return {GetSize(), GetEtag()}; }

    /// <summary>
    /// Downloads a portion of the blob into the provided buffer, performing an ETag match check.
    /// </summary>
    /// <param name="buffer">A span of bytes where the downloaded data will be stored.</param>
    /// <param name="blobOffset">The starting position (in bytes) in the blob from which to begin downloading.</param>
    /// <param name="readLength">The number of bytes to download from the offset.</param>
    /// <param name="ifMatch">The ETag to check against.</param>
    /// <returns>The number of bytes actually downloaded.</returns>
    virtual int64_t Download(std::span<char> buffer, int64_t blobOffset, int64_t readLength,
                             const std::string& ifMatch) = 0;

    /// <summary>
    /// Completion of DownloadAsync: an exception (null on success) and the downloaded bytes.
    /// </summary>
    using DownloadCallback = std::function<void(std::exception_ptr error, std::string data)>;

    /// <summary>
    /// Completion of GetMetadataAsync: an exception (null on success), the blob's size and its ETag.
    /// </summary>
    using MetadataCallback = std::function<void(std::exception_ptr error, int64_t size, std::string etag)>;

    /// <summary>
    /// Asynchronously downloads a portion of the blob, performing an ETag match check. The callback may run on any
    /// thread (including inline) and must not block on further blob I/O. The default implementation is synchronous.
    /// </summary>
    /// <param name="blobOffset">The starting position (in bytes) in the blob from which to begin downloading.</param>
    /// <param name="readLength">The number of bytes to download from the offset.</param>
    /// <param name="ifMatch">The ETag to check against.</param>
    /// <param name="callback">Invoked exactly once with the outcome.</param>
    virtual void DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                               DownloadCallback callback) {
        std::string data;
        std::exception_ptr error;
        try {
            data.resize(static_cast<size_t>(readLength));
            data.resize(static_cast<size_t>(Download(std::span<char>(data), blobOffset, readLength, ifMatch)));
        } catch (...) {
            error = std::current_exception();
            data.clear();
        }
        callback(error, std::move(data));
    }

    /// <summary>
    /// As above, but a non-zero `timeout` caps how long the request may take (RocksDB's IOOptions::timeout). The
    /// default implementation ignores the timeout and forwards to the overload without one.
    /// </summary>
    virtual void DownloadAsync(int64_t blobOffset, int64_t readLength, const std::string& ifMatch,
                               std::chrono::milliseconds timeout, DownloadCallback callback) {
        static_cast<void>(timeout);
        DownloadAsync(blobOffset, readLength, ifMatch, std::move(callback));
    }

    /// <summary>
    /// Asynchronously retrieves the blob's size and ETag. Same threading rules as DownloadAsync. The default
    /// implementation is synchronous.
    /// </summary>
    /// <param name="callback">Invoked exactly once with the outcome.</param>
    virtual void GetMetadataAsync(MetadataCallback callback) {
        BlobMetadata metadata;
        std::exception_ptr error;
        try {
            metadata = GetMetadata();
        } catch (...) {
            error = std::current_exception();
        }
        callback(error, metadata.Size, std::move(metadata.ETag));
    }
};
} // namespace AVEVA::RocksDB::Plugin::Core
