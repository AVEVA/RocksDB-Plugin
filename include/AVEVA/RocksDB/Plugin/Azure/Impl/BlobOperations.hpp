// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <AVEVA/AzureClient/BlockOn.hpp>

#include <boost/asio/use_future.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
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

/// <summary>
/// Everything one GetProperties call reveals, so callers needing several values pay for a single round trip.
/// </summary>
struct BlobInfo {
    int64_t Size = 0;
    int64_t Capacity = 0;
    std::string ETag;
    std::chrono::sys_seconds LastModified;
};

template <typename TBlob> [[nodiscard]] BlobInfo GetInfo(TBlob& client) {
    auto properties = Call(client, [&](auto token) { return client.GetPropertiesAsync(token); });
    return {BlobHelpers::FileSizeFromProperties(properties), static_cast<int64_t>(properties.ContentLength),
            std::move(properties.ETag), std::chrono::time_point_cast<std::chrono::seconds>(properties.LastModified)};
}

template <typename TBlob> [[nodiscard]] int64_t GetSize(TBlob& client) { return GetInfo(client).Size; }

template <typename TBlob> [[nodiscard]] int64_t GetCapacity(TBlob& client) { return GetInfo(client).Capacity; }

template <typename TBlob> [[nodiscard]] std::string GetEtag(TBlob& client) { return GetInfo(client).ETag; }

// A single request, so the size and ETag describe the same version of the blob.
template <typename TBlob> [[nodiscard]] BlobMetadata GetMetadata(TBlob& client) {
    auto info = GetInfo(client);
    return {info.Size, std::move(info.ETag)};
}

template <typename TBlob> void SetSize(const ClientRuntimeBase& runtime, TBlob& client, int64_t size) {
    runtime.ThrowIfWritesFenced();
    AzureClient::SetBlobMetadataOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), std::to_string(size));
    Call(client, [&](auto token) { return client.SetMetadataAsync(std::move(options), token); });
}

template <typename TBlob> void SetCapacity(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    runtime.ThrowIfWritesFenced();
    Call(client, [&](auto token) {
        return client.ResizeAsync(static_cast<uint64_t>(capacity), AzureClient::ResizePageBlobOptions{}, token);
    });
}

// Creates the page blob with the given capacity and a zero logical size, replacing any existing blob: Put Page
// Blob without If-None-Match overwrites, so this is one request for both the new and the existing case.
template <typename TBlob> void CreateOrReplace(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    runtime.ThrowIfWritesFenced();
    AzureClient::CreatePageBlobOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), "0");
    Call(client,
         [&](auto token) { return client.CreateAsync(static_cast<uint64_t>(capacity), std::move(options), token); });
}

// Creates the page blob with the given capacity unless it already exists. Returns true if it was created.
template <typename TBlob> bool CreateIfNotExists(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    runtime.ThrowIfWritesFenced();
    // The client suppresses BlobAlreadyExists and reports it through Response::Error() instead of failing.
    const auto response = CallResponse(
        client, [&](auto token) { return client.CreateIfNotExistsAsync(static_cast<uint64_t>(capacity), token); });
    return !response.Error().has_value();
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
    Call(client,
         [&](auto token) { return client.DownloadToAsync(std::filesystem::path(path), std::move(options), token); });
}

// A download into a caller-owned buffer that has been started but not finished; see StartDownload.
template <typename TBlob> struct PendingDownload {
    decltype(std::declval<TBlob&>().DownloadToAsync(std::span<char>{}, AzureClient::DownloadBlobOptions{},
                                                    boost::asio::use_future)) Result;
};

// Starts the download without waiting, so several can be in flight at once. The buffer and client must stay alive
// until FinishDownload returns (or the future has been waited for).
template <typename TBlob>
[[nodiscard]] PendingDownload<TBlob> StartDownload(TBlob& client, std::span<char> buffer, int64_t offset,
                                                   int64_t length, const std::string& ifMatch = {}) {
    AzureClient::ThrowIfRunningOn(client.get_executor());
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(offset, length);
    if (!ifMatch.empty()) {
        options.Conditions.IfMatch = ifMatch;
    }
    // The body is copied once, straight into the caller's buffer; a range larger than the buffer fails instead of
    // being truncated or overflowing.
    return {client.DownloadToAsync(buffer, std::move(options), boost::asio::use_future,
                                   TransferOptions(client, static_cast<uint64_t>(length)))};
}

// Waits for the download and returns the number of bytes written to the buffer.
template <typename TBlob> [[nodiscard]] int64_t FinishDownload(PendingDownload<TBlob>& pending) {
    const auto result = Unwrap(pending.Result.get());
    if (result.ContentRange.has_value() && result.ContentRange->Length.has_value() &&
        *result.ContentRange->Length != result.BytesWritten) {
        throw std::runtime_error("Downloaded " + std::to_string(result.BytesWritten) +
                                 " bytes but the server reported " + std::to_string(*result.ContentRange->Length));
    }
    return static_cast<int64_t>(result.BytesWritten);
}

// As DownloadToBuffer, but fails when the blob's ETag no longer matches `ifMatch` (ignored when empty). A successful
// ranged response without Content-Range is accepted; the body length is the source of truth.
template <typename TBlob>
[[nodiscard]] int64_t Download(TBlob& client, std::span<char> buffer, int64_t offset, int64_t length,
                               const std::string& ifMatch) {
    auto pending = StartDownload(client, buffer, offset, length, ifMatch);
    return FinishDownload(pending);
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
void UploadPages(const ClientRuntimeBase& runtime, TBlob& client, std::span<const char> buffer, int64_t blobOffset) {
    runtime.ThrowIfWritesFenced();
    Call(client, [&](auto token) {
        return client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::as_bytes(buffer), token,
                                       TransferOptions(client, buffer.size()));
    });
}

/// <summary>A run of populated bytes in a page blob.</summary>
struct PageChunk {
    int64_t Offset = 0;
    int64_t Length = 0;
};

// The parts of the first `size` bytes that hold data, split to at most Configuration::Transfer::MaxCopyChunkSize.
// Never-written and cleared pages read as zeros, so a copy need not transfer them. The listing is guarded by `etag`
// (ignored when empty), so a concurrent write fails the call instead of being silently missed.
template <typename TBlob>
[[nodiscard]] std::vector<PageChunk> PlanPageCopy(TBlob& source, int64_t size, const std::string& etag) {
    std::vector<PageChunk> chunks;
    AzureClient::GetPageRangesOptions options;
    if (!etag.empty()) {
        options.Conditions.IfMatch = etag;
    }
    do {
        auto page = Call(source, [&](auto token) { return source.GetPageRangesAsync(options, token); });
        for (const auto& range : page.Ranges) {
            auto offset = static_cast<int64_t>(range.Offset);
            const auto end = (std::min)(size, offset + static_cast<int64_t>(range.Length));
            while (offset < end) {
                const auto length = (std::min)(end - offset, Configuration::Transfer::MaxCopyChunkSize);
                chunks.push_back({offset, length});
                offset += length;
            }
        }
        options.Marker = std::move(page.NextMarker);
    } while (!options.Marker.empty());
    return chunks;
}

// Copies the populated pages of `source` into `destination`, which must already exist with enough capacity. The next
// chunk is downloaded while the current one uploads. Every request is guarded by `etag` (ignored when empty), so a
// write to the source mid-copy fails instead of producing a torn destination. A final chunk that is not page sized
// is padded with zeros; the logical size is carried separately by SetSize.
template <typename TBlob>
void CopyPages(const ClientRuntimeBase& runtime, TBlob& source, TBlob& destination, int64_t size,
               const std::string& etag) {
    const auto chunks = PlanPageCopy(source, size, etag);
    if (chunks.empty()) {
        return;
    }

    constexpr auto PageSize = static_cast<int64_t>(Configuration::PageBlob::PageSize);
    const auto paddedLength = [](int64_t length) {
        return static_cast<size_t>((length + PageSize - 1) / PageSize * PageSize);
    };
    // Two buffers sized for the largest chunk are reused for the whole copy rather than allocated per chunk.
    size_t largest = 0;
    for (const auto& chunk : chunks) {
        largest = std::max(largest, paddedLength(chunk.Length));
    }
    std::vector<char> current(largest);
    std::vector<char> next(chunks.size() > 1 ? largest : 0);
    // Zeroes the padding after a chunk's data, which would otherwise hold the previous chunk's bytes.
    const auto zeroPadding = [&](std::vector<char>& buffer, int64_t length) {
        std::fill(buffer.begin() + length, buffer.begin() + static_cast<std::ptrdiff_t>(paddedLength(length)), '\0');
    };
    std::optional<PendingDownload<TBlob>> pending;
    // A download still in flight writes into `current`/`next`, so on failure wait for it before they are destroyed.
    // Declared after the buffers so that it runs first.
    struct WaitForPending {
        std::optional<PendingDownload<TBlob>>& Pending;
        ~WaitForPending() {
            if (Pending && Pending->Result.valid()) {
                Pending->Result.wait();
            }
        }
    } waitForPending{pending};
    pending.emplace(StartDownload(source, std::span<char>(current).first(static_cast<size_t>(chunks.front().Length)),
                                  chunks.front().Offset, chunks.front().Length, etag));
    for (size_t index = 0; index < chunks.size(); ++index) {
        const auto& chunk = chunks[index];
        if (FinishDownload(*pending) != chunk.Length) {
            throw std::runtime_error("Unexpected end of blob while copying pages at offset " +
                                     std::to_string(chunk.Offset));
        }
        if (index + 1 < chunks.size()) {
            const auto& following = chunks[index + 1];
            pending.emplace(StartDownload(source, std::span<char>(next).first(static_cast<size_t>(following.Length)),
                                          following.Offset, following.Length, etag));
        }
        zeroPadding(current, chunk.Length);
        UploadPages(runtime, destination, std::span<const char>(current).first(paddedLength(chunk.Length)),
                    chunk.Offset);
        current.swap(next);
    }
}
// True when the service refused to read the copy source for lack of authorization, which says nothing about the
// destination: the caller may still copy the bytes through the client.
[[nodiscard]] inline bool IsCopySourceUnauthorized(const AzureClient::BlobStorageError& error) {
    return error.StatusCode == 401U || error.StatusCode == 403U || error.ErrorCode == "CannotVerifyCopySource";
}

// Copies the populated pages of `source` into `destination` (which must already exist with enough capacity) inside
// the storage service with Put Page From URL, so the data never crosses the network to this process. Several
// requests are kept in flight. The listing and every copied range are guarded by `etag` (ignored when empty), so a
// write to the source mid-copy fails instead of producing a torn destination. A final chunk that is not page sized
// is rounded up to a whole page; the logical size is carried separately by SetSize.
//
// The first request runs alone: if the service refuses to authorize the source (a client whose credential cannot be
// forwarded, e.g. Shared Key) nothing has been copied yet and false is returned so the caller can fall back to
// CopyPages. Any other failure throws.
template <typename TBlob>
[[nodiscard]] bool CopyPagesServerSide(const ClientRuntimeBase& runtime, TBlob& source, TBlob& destination,
                                       int64_t size, const std::string& etag) {
    runtime.ThrowIfWritesFenced();
    const auto chunks = PlanPageCopy(source, size, etag);
    if (chunks.empty()) {
        return true;
    }
    AzureClient::ThrowIfRunningOn(destination.get_executor());
    AzureClient::ThrowIfRunningOn(source.get_executor());
    constexpr auto PageSize = static_cast<int64_t>(Configuration::PageBlob::PageSize);
    const auto sourceUrl = source.Url();
    const auto start = [&](const PageChunk& chunk, auto token) {
        AzureClient::UploadPagesFromUrlOptions options;
        options.SourceIfMatch = etag;
        options.AuthorizeSourceWithClientCredential = true;
        const auto length = (chunk.Length + PageSize - 1) / PageSize * PageSize;
        return destination.UploadPagesFromUrlAsync(static_cast<uint64_t>(chunk.Offset), sourceUrl,
                                                   static_cast<uint64_t>(chunk.Offset), static_cast<uint64_t>(length),
                                                   std::move(options), token);
    };

    auto first = start(chunks.front(), AzureClient::block_on);
    if (!first.has_value()) {
        if (IsCopySourceUnauthorized(first.error())) {
            return false;
        }
        ThrowRequestFailed(first.error());
    }

    using Pending = decltype(start(chunks.front(), boost::asio::use_future));
    std::deque<Pending> inFlight;
    const auto drain = [&inFlight] {
        for (auto& pending : inFlight) {
            if (pending.valid()) {
                pending.wait();
            }
        }
    };
    try {
        for (size_t index = 1; index < chunks.size(); ++index) {
            if (inFlight.size() >= Configuration::Transfer::MaxConcurrentPageCopies) {
                Unwrap(inFlight.front().get());
                inFlight.pop_front();
            }
            runtime.ThrowIfWritesFenced();
            inFlight.push_back(start(chunks[index], boost::asio::use_future));
        }
        while (!inFlight.empty()) {
            Unwrap(inFlight.front().get());
            inFlight.pop_front();
        }
    } catch (...) {
        drain();
        throw;
    }
    return true;
}

// The asynchronous operations never block; completions run on the host io_context's threads (or inline when the
// request cannot be started) and must not block on further blob I/O.

/// <summary>`callback(std::exception_ptr error)`: an exception, or null on success.</summary>
template <typename TBlob, typename TCallback>
void UploadPagesAsync(const ClientRuntimeBase& runtime, TBlob& client, std::shared_ptr<const std::vector<char>> data,
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
        TransferOptions(client, size));
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
    auto requestOptions = TransferOptions(client, static_cast<uint64_t>(readLength));
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
