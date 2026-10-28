// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DownloadBufferPool.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TaskGroup.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Every operation is a template on the page blob client type (`TBlob`): the real client in production, an in-memory
// fake in tests.
//
// The operations are coroutines (`XxxAsync`): they `co_await` the Azure client and report failure by throwing
// RequestFailedException, so a sequence of them reads as straight-line code. A caller that is itself a coroutine
// co_awaits them directly. The only place that blocks is the plain function of the same name without the suffix
// (`GetInfo`, `SetSize`, ...), a one-line `Block` used by RocksDB's synchronous entry points; it must never run on a
// thread of the host io_context. Arguments are taken by reference, so they must outlive the operation.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations {
template <typename T> using Task = boost::asio::awaitable<T>;

/// <summary>
/// Size and ETag of a blob, read together so that they describe the same version of the blob.
/// </summary>
struct BlobMetadata {
    int64_t Size = 0;
    std::string ETag;
};

namespace Detail {
// Runs the coroutine `operation`, waits for the result and returns its value; failures throw. A stop request on
// `stopToken` cancels the operation, which then throws. Never call on a thread running the client's executor.
template <class TBlob, class Awaitable>
auto BlockStoppable(TBlob& client, std::stop_token stopToken, const char* stoppedMessage, Awaitable operation) {
    ThrowIfRunningOn(client.get_executor());
    if (stopToken.stop_requested()) {
        throw std::system_error(std::make_error_code(std::errc::operation_canceled), stoppedMessage);
    }
    return Spawn(client.get_executor(), std::move(operation), std::move(stopToken)).get();
}

inline AzureClient::Models::BlobByteRange ToRange(int64_t offset, int64_t length) {
    return AzureClient::Models::BlobByteRange{static_cast<uint64_t>(offset), static_cast<uint64_t>(length)};
}

// A body that disagrees with the server's Content-Range would otherwise surface as a silent short read.
// InvalidResponse translates to Corruption.
template <typename TResult> std::optional<RequestFailedException> DownloadLengthError(const TResult& result) {
    if (result.ContentRange.has_value() && result.ContentRange->Length.has_value() &&
        *result.ContentRange->Length != result.BytesWritten) {
        return RequestFailedException(
            200, "",
            "Downloaded " + std::to_string(result.BytesWritten) + " bytes but the server reported " +
                std::to_string(*result.ContentRange->Length),
            "", AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::InvalidResponse));
    }
    return std::nullopt;
}

template <typename TResult> void CheckDownloadLength(const TResult& result) {
    if (auto error = DownloadLengthError(result)) {
        throw *std::move(error);
    }
}
} // namespace Detail

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

template <typename TBlob> [[nodiscard]] Task<BlobInfo> GetInfoAsync(TBlob& client) {
    auto properties = Unwrap(co_await client.GetPropertiesAsync());
    co_return BlobInfo{BlobHelpers::FileSizeFromProperties(properties), static_cast<int64_t>(properties.ContentLength),
                       std::move(properties.ETag),
                       std::chrono::time_point_cast<std::chrono::seconds>(properties.LastModified)};
}

template <typename TBlob> [[nodiscard]] BlobInfo GetInfo(TBlob& client) { return Block(client, GetInfoAsync(client)); }

// As GetInfo, but a stop request cancels the call, which then throws. Lets a shutdown that is waiting for the caller's
// thread return promptly instead of waiting out a slow or hung request.
template <typename TBlob> [[nodiscard]] BlobInfo GetInfo(TBlob& client, std::stop_token stopToken) {
    return Detail::BlockStoppable(client, std::move(stopToken), "Request stopped", GetInfoAsync(client));
}

// Drops the blob's cached logical size when the enclosing mutation ends, whether or not it succeeded: a failure part
// way through may still have changed the blob.
template <typename TBlob> class SizeInvalidation {
    const ClientRuntimeBase& m_runtime;
    std::string m_url;

  public:
    SizeInvalidation(const ClientRuntimeBase& runtime, const TBlob& client)
        : m_runtime(runtime), m_url(client.ResourceUrl()) {}
    SizeInvalidation(const SizeInvalidation&) = delete;
    SizeInvalidation& operator=(const SizeInvalidation&) = delete;
    ~SizeInvalidation() { m_runtime.InvalidateBlobSize(m_url); }
};

// A single request, so the size and ETag describe the same version of the blob.
template <typename TBlob> [[nodiscard]] Task<BlobMetadata> GetMetadataAsync(TBlob& client) {
    auto info = co_await GetInfoAsync(client);
    co_return BlobMetadata{info.Size, std::move(info.ETag)};
}

template <typename TBlob> [[nodiscard]] BlobMetadata GetMetadata(TBlob& client) {
    return Block(client, GetMetadataAsync(client));
}

// The blob's logical size, served from the runtime's short-lived cache when it is fresh.
template <typename TBlob>
[[nodiscard]] Task<int64_t> GetCachedSizeAsync(const ClientRuntimeBase& runtime, TBlob& client) {
    const auto& url = client.ResourceUrl();
    if (const auto cached = runtime.CachedBlobSize(url)) {
        co_return *cached;
    }
    const auto epoch = runtime.BlobSizeEpoch();
    auto info = co_await GetInfoAsync(client);
    runtime.CacheBlobMetadata(url, info.Size, std::move(info.ETag), epoch);
    co_return info.Size;
}

template <typename TBlob> [[nodiscard]] int64_t GetCachedSize(const ClientRuntimeBase& runtime, TBlob& client) {
    return Block(client, GetCachedSizeAsync(runtime, client));
}

// Size and ETag from the runtime's cache when fresh, otherwise one properties request that then fills the cache. Used
// when a file is opened, which RocksDB does for many files.
template <typename TBlob>
[[nodiscard]] Task<ClientRuntimeBase::CachedMetadata> GetCachedMetadataAsync(const ClientRuntimeBase& runtime,
                                                                             TBlob& client) {
    const auto& url = client.ResourceUrl();
    if (auto cached = runtime.CachedBlobMetadata(url)) {
        co_return std::move(*cached);
    }
    const auto epoch = runtime.BlobSizeEpoch();
    const auto fetchedAt = runtime.MetadataNow();
    auto info = co_await GetInfoAsync(client);
    runtime.CacheBlobMetadata(url, info.Size, info.ETag, epoch);
    co_return ClientRuntimeBase::CachedMetadata{info.Size, std::move(info.ETag), fetchedAt};
}

template <typename TBlob>
[[nodiscard]] ClientRuntimeBase::CachedMetadata GetCachedMetadata(const ClientRuntimeBase& runtime, TBlob& client) {
    return Block(client, GetCachedMetadataAsync(runtime, client));
}

// Returns the blob's new ETag, which the response already carries, so callers need no follow-up properties request.
template <typename TBlob>
Task<std::string> SetSizeAsync(const ClientRuntimeBase& runtime, TBlob& client, int64_t size) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    AzureClient::SetBlobMetadataOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), std::to_string(size));
    auto result = Unwrap(co_await client.SetMetadataAsync(
        std::move(options), BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
    co_return std::move(result.ETag);
}

template <typename TBlob> std::string SetSize(const ClientRuntimeBase& runtime, TBlob& client, int64_t size) {
    return Block(client, SetSizeAsync(runtime, client, size));
}

template <typename TBlob>
Task<void> SetCapacityAsync(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    Unwrap(co_await client.ResizeAsync(static_cast<uint64_t>(capacity), AzureClient::ResizePageBlobOptions{},
                                       BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
}

template <typename TBlob> void SetCapacity(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    Block(client, SetCapacityAsync(runtime, client, capacity));
}

// Creates the page blob with the given capacity and a zero logical size, replacing any existing blob: Put Page
// Blob without If-None-Match overwrites, so this is one request for both the new and the existing case.
template <typename TBlob>
Task<void> CreateOrReplaceAsync(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    AzureClient::CreatePageBlobOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), "0");
    Unwrap(co_await client.CreateAsync(static_cast<uint64_t>(capacity), std::move(options),
                                       BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
}

template <typename TBlob> void CreateOrReplace(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    Block(client, CreateOrReplaceAsync(runtime, client, capacity));
}

// Creates the page blob with the given capacity unless it already exists. Returns true if it was created.
template <typename TBlob>
Task<bool> CreateIfNotExistsAsync(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    // The client suppresses BlobAlreadyExists and reports it through Response::Error() instead of failing.
    const auto response = UnwrapResponse(
        co_await client.CreateIfNotExistsAsync(static_cast<uint64_t>(capacity), AzureClient::CreatePageBlobOptions{},
                                               BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
    co_return !response.Error().has_value();
}

template <typename TBlob> bool CreateIfNotExists(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    return Block(client, CreateIfNotExistsAsync(runtime, client, capacity));
}

// Deletes the blob if it exists, fenced and bounded by the write deadline like every other mutation so a stale lease
// holder cannot delete after losing the lease. Returns true if the blob existed.
template <typename TBlob> Task<bool> DeleteAsync(const ClientRuntimeBase& runtime, TBlob& client) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    const auto response = UnwrapResponse(co_await client.DeleteIfExistsAsync(
        AzureClient::DeleteBlobOptions{}, BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
    co_return !response.Error().has_value();
}

template <typename TBlob> bool Delete(const ClientRuntimeBase& runtime, TBlob& client) {
    return Block(client, DeleteAsync(runtime, client));
}

// Batched Delete: fenced, and every chunk is bounded by the remaining write deadline. The caller inspects the
// per-blob errors of the result.
template <typename TContainer>
[[nodiscard]] Task<AzureClient::Models::DeleteBlobsResult>
DeleteBatchAsync(const ClientRuntimeBase& runtime, TContainer& container, std::vector<std::string> names,
                 AzureClient::DeleteBlobsOptions options) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    co_return Unwrap(co_await container.DeleteBlobsAsync(
        std::move(names), std::move(options), BoundByWriteDeadline(writeBudget, container.GetDefaultRequestOptions())));
}

template <typename TContainer>
[[nodiscard]] AzureClient::Models::DeleteBlobsResult DeleteBatch(const ClientRuntimeBase& runtime,
                                                                 TContainer& container, std::vector<std::string> names,
                                                                 AzureClient::DeleteBlobsOptions options) {
    return Block(container, DeleteBatchAsync(runtime, container, std::move(names), std::move(options)));
}

// Downloads [offset, offset + length) to a local file; a non-positive length produces an empty file. A stop request
// cancels the transfer, which then throws; the client removes its partial file, so nothing is left at `path`.
template <typename TBlob>
void DownloadToFile(TBlob& client, const std::filesystem::path& path, int64_t offset, int64_t length,
                    std::stop_token stopToken = {},
                    std::optional<AzureClient::DownloadWriteScheduler> writeScheduler = {}) {
    // An explicit zero-length range is rejected by the client, and there is nothing to fetch for an empty blob.
    if (length <= 0) {
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        if (!file) {
            throw std::runtime_error("Could not create empty file '" + path.string() + "'");
        }
        return;
    }

    AzureClient::DownloadToOptions options;
    options.Range = Detail::ToRange(offset, length);
    options.ChunkSize = Configuration::Transfer::DownloadChunkSize;
    options.Concurrency = Configuration::Transfer::DownloadConcurrency;
    options.WriteScheduler = std::move(writeScheduler);

    // The download is cancelled through the awaiting coroutine's slot; the client removes its partial file.
    Detail::BlockStoppable(
        client, std::move(stopToken), "Download stopped",
        Unwrapped(client.DownloadToAsync(path, std::move(options), client.GetDefaultRequestOptions())));
}

// Downloads into a caller-owned buffer and returns the number of bytes written. The buffer must stay alive until the
// operation completes. The ETag condition (`ifMatch`, ignored when empty) fails the call when the blob has changed. A
// successful ranged response without Content-Range is accepted; the body length is the source of truth, and a range
// larger than the buffer fails instead of being truncated or overflowing.
template <typename TBlob>
[[nodiscard]] Task<int64_t> DownloadAsync(TBlob& client, std::span<char> buffer, int64_t offset, int64_t length,
                                          std::string ifMatch = {}) {
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(offset, length);
    if (!ifMatch.empty()) {
        options.Conditions.IfMatch = std::move(ifMatch);
    }
    // The body is copied once, straight into the caller's buffer.
    const auto result = Unwrap(co_await client.DownloadToAsync(buffer, std::move(options),
                                                               TransferOptions(client, static_cast<uint64_t>(length))));
    Detail::CheckDownloadLength(result);
    co_return static_cast<int64_t>(result.BytesWritten);
}

template <typename TBlob>
[[nodiscard]] int64_t Download(TBlob& client, std::span<char> buffer, int64_t offset, int64_t length,
                               std::string ifMatch) {
    return Block(client, DownloadAsync(client, buffer, offset, length, std::move(ifMatch)));
}

// Returns the number of bytes written to `buffer`; a non-positive length returns 0 without a request.
template <typename TBlob>
[[nodiscard]] int64_t DownloadToBuffer(TBlob& client, std::span<char> buffer, int64_t offset, int64_t length) {
    if (length <= 0) {
        return 0;
    }

    return Download(client, buffer, offset, length, std::string());
}

/// <summary>Bytes downloaded by DownloadForReadAsync.</summary>
struct DownloadedBytes {
    /// Keeps the pooled buffer holding `Data` alive; hold it to retain the bytes without copying.
    std::shared_ptr<const void> Owner;
    std::string_view Data;
};

/// <summary>
/// Downloads a range into a pooled buffer, for reads that outlive the caller's stack. The bytes stay valid for as
/// long as `Owner` is held. A non-zero `timeout` caps how long the request may take, retries included; it only ever
/// shortens the transfer-scaled default. A stop request on the token the operation was started with cancels the
/// transfer, transport retries included.
/// </summary>
template <typename TBlob>
[[nodiscard]] Task<DownloadedBytes> DownloadForReadAsync(TBlob& client, int64_t blobOffset, int64_t readLength,
                                                         std::string ifMatch, std::chrono::milliseconds timeout) {
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(blobOffset, readLength);
    options.Conditions.IfMatch = std::move(ifMatch);
    // The transfer-scaled default timeout can be minutes; a caller-supplied deadline only ever shortens it.
    auto requestOptions = TransferOptions(client, static_cast<uint64_t>(readLength));
    if (timeout.count() > 0) {
        if (timeout < requestOptions.GetTimeout()) {
            requestOptions.SetTimeout(timeout);
        }
        // The timeout is per attempt; the total deadline is what stops transport retries from outliving the caller.
        requestOptions.SetTotalDeadline(timeout);
    }
    // The lease lives in this frame, which outlives the transfer, so it can never write into freed memory.
    auto buffer = DownloadBufferPool::Shared().Acquire(static_cast<size_t>(readLength));
    const auto downloaded = Unwrap(co_await client.DownloadToAsync(std::span<char>(buffer->Data(), buffer->Size()),
                                                                   std::move(options), std::move(requestOptions)));
    Detail::CheckDownloadLength(downloaded);
    const std::string_view data(buffer->Data(), std::min(static_cast<size_t>(downloaded.BytesWritten), buffer->Size()));
    co_return DownloadedBytes{std::shared_ptr<const void>(std::move(buffer)), data};
}

// Views `buffer`, so the caller must keep it alive until the operation completes; prefer the shared-buffer overload
// when ownership can be handed over.
template <typename TBlob>
[[nodiscard]] Task<void> UploadPagesAsync(const ClientRuntimeBase& runtime, TBlob& client, std::span<const char> buffer,
                                          int64_t blobOffset) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    Unwrap(co_await client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::as_bytes(buffer),
                                            AzureClient::UploadPagesOptions{},
                                            BoundByWriteDeadline(writeBudget, TransferOptions(client, buffer.size()))));
}

template <typename TBlob>
void UploadPages(const ClientRuntimeBase& runtime, TBlob& client, std::span<const char> buffer, int64_t blobOffset) {
    Block(client, UploadPagesAsync(runtime, client, buffer, blobOffset));
}

// The request views `data` and keeps it alive itself, so the payload is never copied.
template <typename TBlob>
[[nodiscard]] Task<void> UploadPagesAsync(const ClientRuntimeBase& runtime, TBlob& client,
                                          std::shared_ptr<const std::vector<char>> data, int64_t blobOffset) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const auto size = data->size();
    Unwrap(co_await client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::move(data),
                                            AzureClient::UploadPagesOptions{},
                                            BoundByWriteDeadline(writeBudget, TransferOptions(client, size))));
}

template <typename TBlob>
void UploadPages(const ClientRuntimeBase& runtime, TBlob& client, std::shared_ptr<const std::vector<char>> data,
                 int64_t blobOffset) {
    Block(client, UploadPagesAsync(runtime, client, std::move(data), blobOffset));
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
[[nodiscard]] Task<std::vector<PageChunk>> PlanPageCopyAsync(TBlob& source, int64_t size, std::string etag) {
    std::vector<PageChunk> chunks;
    AzureClient::GetPageRangesOptions options;
    if (!etag.empty()) {
        options.Conditions.IfMatch = etag;
    }
    do {
        auto page = Unwrap(co_await source.GetPageRangesAsync(options));
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
    co_return chunks;
}

// Copies the populated pages of `source` into `destination`, which must already exist with enough capacity. Chunks
// are downloaded one after another while earlier ones upload (up to MaxConcurrentPageCopies), so a large copy costs
// one upload latency in total rather than per chunk. The listing and every download are guarded by `etag` (ignored
// when empty), so a write to the source mid-copy fails instead of producing a torn destination. A final chunk that is
// not page sized is padded with zeros; the logical size is carried separately by SetSize.
template <typename TBlob>
Task<void> CopyPagesAsync(const ClientRuntimeBase& runtime, TBlob& source, TBlob& destination, int64_t size,
                          std::string etag) {
    const auto chunks = co_await PlanPageCopyAsync(source, size, etag);
    if (chunks.empty()) {
        co_return;
    }

    constexpr auto PageSize = static_cast<int64_t>(Configuration::PageBlob::PageSize);
    const auto paddedLength = [](int64_t length) {
        return static_cast<size_t>((length + PageSize - 1) / PageSize * PageSize);
    };

    using Buffer = std::shared_ptr<std::vector<char>>;
    // Every buffer ever made, declared before the uploads so it outlives them. One that only this list references is
    // idle; a running upload holds its own copy. Reusing idle ones avoids allocating a chunk-sized buffer per chunk.
    std::vector<Buffer> buffers;
    const auto acquire = [&buffers](size_t bufferSize) {
        for (auto& buffer : buffers) {
            if (buffer.use_count() == 1) {
                // Orders the relaxed use_count() read before reuse of memory the last other owner wrote.
                std::atomic_thread_fence(std::memory_order_acquire);
                buffer->resize(bufferSize);
                return buffer;
            }
        }
        return buffers.emplace_back(std::make_shared<std::vector<char>>(bufferSize));
    };

    TaskGroup uploads(co_await boost::asio::this_coro::executor, Configuration::Transfer::MaxConcurrentPageCopies);
    co_await uploads.Scope([&](TaskGroup& running) -> Task<void> {
        for (const auto& chunk : chunks) {
            Buffer buffer = acquire(paddedLength(chunk.Length));
            const auto downloaded =
                co_await DownloadAsync(source, std::span<char>(*buffer).first(static_cast<size_t>(chunk.Length)),
                                       chunk.Offset, chunk.Length, etag);
            if (downloaded != chunk.Length) {
                throw std::runtime_error("Unexpected end of blob while copying pages at offset " +
                                         std::to_string(chunk.Offset));
            }
            // Zeroes the padding after the data, which would otherwise hold a previous chunk's bytes.
            std::fill(buffer->begin() + chunk.Length, buffer->end(), '\0');
            co_await running.Spawn(UploadPagesAsync(runtime, destination, std::move(buffer), chunk.Offset));
        }
    });
}

template <typename TBlob>
void CopyPages(const ClientRuntimeBase& runtime, TBlob& source, TBlob& destination, int64_t size,
               const std::string& etag) {
    Block(source, CopyPagesAsync(runtime, source, destination, size, etag));
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
[[nodiscard]] Task<bool> CopyPagesServerSideAsync(const ClientRuntimeBase& runtime, TBlob& source, TBlob& destination,
                                                  int64_t size, std::string etag) {
    runtime.ThrowIfWritesFenced();
    // A destination that cannot forward a bearer token cannot authorize a SAS-less source either; the service would
    // refuse the copy, so skip the doomed request and let the caller copy the bytes through the client.
    if (!destination.CanForwardCredential() && !source.HasSasToken()) {
        co_return false;
    }
    const auto chunks = co_await PlanPageCopyAsync(source, size, etag);
    if (chunks.empty()) {
        co_return true;
    }
    constexpr auto PageSize = static_cast<int64_t>(Configuration::PageBlob::PageSize);
    const auto sourceUrl = source.Url();
    const auto copyRange =
        [&](PageChunk chunk) -> Task<AzureClient::BlobResult<AzureClient::Models::UploadPagesResult>> {
        // Planning and earlier requests consume the lease budget, so it is taken afresh for every request; a relative
        // duration captured earlier would let this one outlive the write deadline.
        const auto writeBudget = runtime.ThrowIfWritesFenced();
        AzureClient::UploadPagesFromUrlOptions options;
        options.SourceIfMatch = etag;
        options.AuthorizeSourceWithClientCredential = true;
        const auto length = (chunk.Length + PageSize - 1) / PageSize * PageSize;
        co_return co_await destination.UploadPagesFromUrlAsync(
            static_cast<uint64_t>(chunk.Offset), sourceUrl, static_cast<uint64_t>(chunk.Offset),
            static_cast<uint64_t>(length), std::move(options),
            BoundByWriteDeadline(writeBudget, destination.GetDefaultRequestOptions()));
    };
    const auto copyRangeChecked = [&](PageChunk chunk) -> Task<void> { Unwrap(co_await copyRange(chunk)); };

    const auto first = co_await copyRange(chunks.front());
    if (!first.has_value()) {
        if (IsCopySourceUnauthorized(first.error())) {
            co_return false;
        }
        ThrowRequestFailed(first.error());
    }

    TaskGroup copies(co_await boost::asio::this_coro::executor, Configuration::Transfer::MaxConcurrentPageCopies);
    co_await copies.Scope([&](TaskGroup& running) -> Task<void> {
        for (size_t index = 1; index < chunks.size(); ++index) {
            co_await running.Spawn(copyRangeChecked(chunks[index]));
        }
    });
    co_return true;
}

template <typename TBlob>
[[nodiscard]] bool CopyPagesServerSide(const ClientRuntimeBase& runtime, TBlob& source, TBlob& destination,
                                       int64_t size, const std::string& etag) {
    return Block(source, CopyPagesServerSideAsync(runtime, source, destination, size, etag));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations
