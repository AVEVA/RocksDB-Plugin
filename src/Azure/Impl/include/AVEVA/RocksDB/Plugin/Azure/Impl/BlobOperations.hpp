// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DownloadBufferPool.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SignedBytes.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TaskGroup.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/HttpClient/HttpBody.hpp>

#include <gsl/pointers>

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
// thread of the host io_context. Clients and buffers are taken by reference, so they must outlive the operation; the
// runtime is shared, so a coroutine frame (and the SizeInvalidation in it) never outlives it.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations {
/// <summary>Whether CreateIfNotExists made the blob or found it already there.</summary>
enum class CreateOutcome : std::uint8_t { AlreadyExisted, Created };

/// <summary>Whether the storage service copied the pages or declined, leaving the copy to the caller.</summary>
enum class ServerSideCopy : std::uint8_t { Declined, Done };

template <typename T> using Task = boost::asio::awaitable<T>;

/// <summary>The blob a copy reads from; distinct from Destination so the two cannot be swapped.</summary>
template <typename TBlob> class Source {
    TBlob* m_blob;

  public:
    explicit Source(TBlob& blob) noexcept : m_blob(&blob) {}
    [[nodiscard]] TBlob& Blob() const noexcept { return *m_blob; }
};

/// <summary>The blob a copy writes to; distinct from Source so the two cannot be swapped.</summary>
template <typename TBlob> class Destination {
    TBlob* m_blob;

  public:
    explicit Destination(TBlob& blob) noexcept : m_blob(&blob) {}
    [[nodiscard]] TBlob& Blob() const noexcept { return *m_blob; }
};

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

inline AzureClient::Models::BlobByteRange ToRange(const Core::ByteOffset offset, const Core::ByteCount length) {
    return AzureClient::Models::BlobByteRange{offset.Value(), length.Value()};
}

// A body that disagrees with the server's Content-Range would otherwise surface as a silent short read.
// InvalidResponse translates to Corruption.
template <typename TResult> std::optional<RequestFailedException> DownloadLengthError(const TResult& result) {
    if (result.ContentRange.has_value() && result.ContentRange->Length.has_value() &&
        *result.ContentRange->Length != result.BytesWritten) {
        return RequestFailedException(
            HttpStatusCode{200}, ServiceErrorCode{""},
            ErrorMessage{"Downloaded " + std::to_string(result.BytesWritten) + " bytes but the server reported " +
                         std::to_string(*result.ContentRange->Length)},
            ServiceRequestId{""}, AzureClient::make_error_code(AzureClient::BlobStorageErrorCode::InvalidResponse));
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
    std::shared_ptr<const ClientRuntimeBase> m_runtime;
    std::string m_url;

  public:
    SizeInvalidation(std::shared_ptr<const ClientRuntimeBase> runtime, const TBlob& client)
        : m_runtime(std::move(runtime)), m_url(client.ResourceUrl()) {}
    SizeInvalidation(const SizeInvalidation&) = delete;
    SizeInvalidation& operator=(const SizeInvalidation&) = delete;
    SizeInvalidation(SizeInvalidation&&) = delete;
    SizeInvalidation& operator=(SizeInvalidation&&) = delete;
    ~SizeInvalidation() { m_runtime->MetadataCache().Invalidate(m_url); }
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
[[nodiscard]] Task<int64_t> GetCachedSizeAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client) {
    const auto& url = client.ResourceUrl();
    if (const auto cached = runtime->MetadataCache().Size(url)) {
        co_return *cached;
    }
    const auto epoch = runtime->MetadataCache().SizeEpoch();
    auto info = co_await GetInfoAsync(client);
    runtime->MetadataCache().StoreMetadata(url, info.Size, std::move(info.ETag), epoch);
    co_return info.Size;
}

template <typename TBlob>
[[nodiscard]] int64_t GetCachedSize(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client) {
    return Block(client, GetCachedSizeAsync(runtime, client));
}

// Size and ETag from the runtime's cache when fresh, otherwise one properties request that then fills the cache. Used
// when a file is opened, which RocksDB does for many files.
template <typename TBlob>
[[nodiscard]] Task<BlobMetadataCache::CachedMetadata>
GetCachedMetadataAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client) {
    const auto& url = client.ResourceUrl();
    if (auto cached = runtime->MetadataCache().Metadata(url)) {
        co_return std::move(*cached);
    }
    const auto epoch = runtime->MetadataCache().SizeEpoch();
    const auto fetchedAt = runtime->MetadataCache().Now();
    auto info = co_await GetInfoAsync(client);
    runtime->MetadataCache().StoreMetadata(url, info.Size, info.ETag, epoch);
    co_return BlobMetadataCache::CachedMetadata{info.Size, std::move(info.ETag), fetchedAt};
}

template <typename TBlob>
[[nodiscard]] BlobMetadataCache::CachedMetadata
GetCachedMetadata(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client) {
    return Block(client, GetCachedMetadataAsync(runtime, client));
}

// Returns the blob's new ETag, which the response already carries, so callers need no follow-up properties request.
template <typename TBlob>
Task<std::string> SetSizeAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client,
                               const Core::ByteCount size) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    AzureClient::SetBlobMetadataOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), std::to_string(size.Value()));
    auto result = Unwrap(co_await client.SetMetadataAsync(
        std::move(options), BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
    co_return std::move(result.ETag);
}

template <typename TBlob>
std::string SetSize(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client,
                    const Core::ByteCount size) {
    return Block(client, SetSizeAsync(runtime, client, size));
}

template <typename TBlob>
Task<void> SetCapacityAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client,
                            const Core::ByteCount capacity) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    Unwrap(co_await client.ResizeAsync(AzureClient::PageBlobSize{capacity.Value()},
                                       AzureClient::ResizePageBlobOptions{},
                                       BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
}

template <typename TBlob>
void SetCapacity(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client,
                 const Core::ByteCount capacity) {
    Block(client, SetCapacityAsync(runtime, client, capacity));
}

// Creates the page blob with the given capacity and a zero logical size, replacing any existing blob: Put Page
// Blob without If-None-Match overwrites, so this is one request for both the new and the existing case.
template <typename TBlob>
Task<void> CreateOrReplaceAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client,
                                const Core::ByteCount capacity) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    AzureClient::CreatePageBlobOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), "0");
    Unwrap(co_await client.CreateAsync(AzureClient::PageBlobSize{capacity.Value()}, std::move(options),
                                       BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
}

template <typename TBlob>
void CreateOrReplace(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client,
                     const Core::ByteCount capacity) {
    Block(client, CreateOrReplaceAsync(runtime, client, capacity));
}

// Creates the page blob with the given capacity unless it already exists.
template <typename TBlob>
Task<CreateOutcome> CreateIfNotExistsAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client,
                                           const Core::ByteCount capacity) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    // The client suppresses BlobAlreadyExists and reports it through Response::Error() instead of failing.
    const auto response = UnwrapResponse(co_await client.CreateIfNotExistsAsync(
        AzureClient::PageBlobSize{capacity.Value()}, AzureClient::CreatePageBlobOptions{},
        BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
    co_return response.Error().has_value() ? CreateOutcome::AlreadyExisted : CreateOutcome::Created;
}

template <typename TBlob>
CreateOutcome CreateIfNotExists(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client,
                                const Core::ByteCount capacity) {
    return Block(client, CreateIfNotExistsAsync(runtime, client, capacity));
}

// Deletes the blob if it exists, fenced and bounded by the write deadline like every other mutation so a stale lease
// holder cannot delete after losing the lease. Reports whether the blob was there to delete.
template <typename TBlob>
Task<AzureClient::PriorState> DeleteAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    const auto response = UnwrapResponse(co_await client.DeleteIfExistsAsync(
        AzureClient::DeleteBlobOptions{}, BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions())));
    co_return response.Error().has_value() ? AzureClient::PriorState::AlreadyAbsent : AzureClient::PriorState::Existed;
}

template <typename TBlob>
AzureClient::PriorState Delete(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client) {
    return Block(client, DeleteAsync(runtime, client));
}

// Batched Delete: fenced, and every chunk is bounded by the remaining write deadline. The caller inspects the
// per-blob errors of the result.
template <typename TContainer>
[[nodiscard]] Task<AzureClient::Models::DeleteBlobsResult>
DeleteBatchAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TContainer& container,
                 std::vector<std::string> names, AzureClient::DeleteBlobsOptions options) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    co_return Unwrap(co_await container.DeleteBlobsAsync(
        std::move(names), std::move(options), BoundByWriteDeadline(writeBudget, container.GetDefaultRequestOptions())));
}

template <typename TContainer>
[[nodiscard]] AzureClient::Models::DeleteBlobsResult
DeleteBatch(const std::shared_ptr<const ClientRuntimeBase>& runtime, TContainer& container,
            std::vector<std::string> names, AzureClient::DeleteBlobsOptions options) {
    return Block(container, DeleteBatchAsync(runtime, container, std::move(names), std::move(options)));
}

// Downloads into a caller-owned buffer and returns the number of bytes written. The buffer must stay alive until the
// operation completes. The ETag condition (`ifMatch`, ignored when empty) fails the call when the blob has changed. A
// successful ranged response without Content-Range is accepted; the body length is the source of truth, and a range
// larger than the buffer fails instead of being truncated or overflowing.
template <typename TBlob>
[[nodiscard]] Task<Core::ByteCount> DownloadAsync(TBlob& client, std::span<char> buffer, const Core::ByteOffset offset,
                                                  const Core::ByteCount length, std::string ifMatch = {}) {
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(offset, length);
    if (!ifMatch.empty()) {
        options.Conditions.IfMatch = std::move(ifMatch);
    }
    // The body is copied once, straight into the caller's buffer.
    const auto result =
        Unwrap(co_await client.DownloadToAsync(buffer, std::move(options), TransferOptions(client, length.Value())));
    Detail::CheckDownloadLength(result);
    co_return Core::ByteCount{result.BytesWritten};
}

template <typename TBlob>
[[nodiscard]] Core::ByteCount Download(TBlob& client, std::span<char> buffer, const Core::ByteOffset offset,
                                       const Core::ByteCount length, std::string ifMatch) {
    return Block(client, DownloadAsync(client, buffer, offset, length, std::move(ifMatch)));
}

// Returns the number of bytes written to `buffer`; a zero length returns 0 without a request.
template <typename TBlob>
[[nodiscard]] Core::ByteCount DownloadToBuffer(TBlob& client, std::span<char> buffer, const Core::ByteOffset offset,
                                               const Core::ByteCount length) {
    if (length == Core::ByteCount{}) {
        return Core::ByteCount{};
    }

    return Download(client, buffer, offset, length, std::string());
}

namespace Detail {
// Fills `bytes` bytes of `wave` at `at` from the blob at `from + at`; a short body fails instead of leaving a hole.
template <typename TBlob>
Task<void> DownloadChunkAsync(TBlob& client, std::shared_ptr<std::vector<char>> wave, const size_t at,
                              const size_t bytes, const Core::ByteOffset from) {
    const auto blobOffset = from.Value() + at;
    const auto received = co_await DownloadAsync(client, std::span<char>(*wave).subspan(at, bytes),
                                                 Core::ByteOffset{blobOffset}, Core::ByteCount{bytes});
    if (received != Core::ByteCount{bytes}) {
        throw std::runtime_error("Unexpected end of blob while downloading at offset " + std::to_string(blobOffset));
    }
}

// Fills `wave` from the blob range starting at `blobOffset`, a chunk per request with up to DownloadConcurrency in
// flight. Any short chunk fails the wave, so a file is never published with a hole in it.
template <typename TBlob>
Task<void> DownloadWaveAsync(TBlob& client, std::shared_ptr<std::vector<char>> wave,
                             const Core::ByteOffset blobOffset) {
    const auto chunkSize = Configuration::Transfer::DownloadChunkSize.Value();
    TaskGroup downloads(co_await boost::asio::this_coro::executor,
                        ConcurrencyLimit{Configuration::Transfer::DownloadConcurrency});
    co_await downloads.Scope([&](TaskGroup& running) -> Task<void> {
        for (size_t start = 0; start < wave->size(); start += chunkSize) {
            const auto length = std::min(chunkSize, wave->size() - start);
            co_await running.Spawn(DownloadChunkAsync(client, wave, start, length, blobOffset));
        }
    });
}

// Removes a download's destination unless the download completed, so a failure never leaves a truncated file.
class PartialFile {
  public:
    enum class Outcome : std::uint8_t { Incomplete, Complete };

    explicit PartialFile(std::filesystem::path path) : m_path(std::move(path)) {}
    PartialFile(const PartialFile&) = delete;
    PartialFile& operator=(const PartialFile&) = delete;
    PartialFile(PartialFile&&) = delete;
    PartialFile& operator=(PartialFile&&) = delete;
    ~PartialFile() {
        if (m_outcome == Outcome::Incomplete) {
            std::error_code ignored;
            std::filesystem::remove(m_path, ignored);
        }
    }
    void MarkComplete() noexcept { m_outcome = Outcome::Complete; }

  private:
    std::filesystem::path m_path;
    Outcome m_outcome = Outcome::Incomplete;
};
} // namespace Detail

// Downloads [offset, offset + length) to a local file; a zero length produces an empty file without a request. Each
// wave of chunks is fetched on the client's executor and written to disk here, on the caller's thread, so the plugin
// needs no thread of its own for the blocking writes. A stop request cancels the transfer, which then throws; a failed
// download removes its partial file, so nothing is left at `path`.
template <typename TBlob>
void DownloadToFile(TBlob& client, const std::filesystem::path& path, const Core::ByteOffset offset,
                    const Core::ByteCount length, std::stop_token stopToken = {}) {
    Detail::PartialFile partial(path);
    {
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        if (!file) {
            throw std::runtime_error("Could not create file '" + path.string() + "'");
        }
        const auto waveBytes =
            Configuration::Transfer::DownloadChunkSize.Value() * Configuration::Transfer::DownloadConcurrency;
        auto wave = std::make_shared<std::vector<char>>();
        for (uint64_t done = 0; done < length.Value();) {
            const auto bytes = std::min<uint64_t>(waveBytes, length.Value() - done);
            wave->resize(bytes);
            Detail::BlockStoppable(client, stopToken, "Download stopped",
                                   Detail::DownloadWaveAsync(client, wave, Core::ByteOffset{offset.Value() + done}));
            file.write(wave->data(), static_cast<std::streamsize>(wave->size()));
            if (!file) {
                throw std::runtime_error("Could not write file '" + path.string() + "'");
            }
            done += bytes;
        }
        file.close();
        if (!file) {
            throw std::runtime_error("Could not finish writing file '" + path.string() + "'");
        }
    }
    partial.MarkComplete();
}

/// <summary>Bytes downloaded by DownloadForReadAsync.</summary>
struct DownloadedBytes {
    /// Keeps the pooled buffer holding `Data` alive; hold it to retain the bytes without copying.
    std::shared_ptr<const void> Owner;
    std::string_view Data;
};

/// <summary>
/// Downloads a range into a pooled buffer, for reads that outlive the caller's stack. The bytes stay valid for as
/// long as `Owner` is held. A stop request on the token the operation was started with cancels the transfer,
/// transport retries included; that is how a caller's deadline is enforced.
/// </summary>
template <typename TBlob>
[[nodiscard]] Task<DownloadedBytes> DownloadForReadAsync(TBlob& client, const Core::ByteOffset blobOffset,
                                                         const Core::ByteCount readLength, std::string ifMatch) {
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(blobOffset, readLength);
    options.Conditions.IfMatch = std::move(ifMatch);
    auto requestOptions = TransferOptions(client, readLength.Value());
    // The lease lives in this frame, which outlives the transfer, so it can never write into freed memory.
    auto buffer = DownloadBufferPool::Shared().Acquire(readLength.Value());
    const auto downloaded = Unwrap(co_await client.DownloadToAsync(std::span<char>(buffer->Data(), buffer->Size()),
                                                                   std::move(options), std::move(requestOptions)));
    Detail::CheckDownloadLength(downloaded);
    const std::string_view data(buffer->Data(), std::min(downloaded.BytesWritten, buffer->Size()));
    co_return DownloadedBytes{std::shared_ptr<const void>(std::move(buffer)), data};
}

// Views `buffer`, so the caller must keep it alive until the operation completes; prefer the shared-buffer overload
// when ownership can be handed over.
template <typename TBlob>
[[nodiscard]] Task<void> UploadPagesAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client,
                                          std::span<const char> buffer, const Core::ByteOffset blobOffset) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    Unwrap(co_await client.UploadPagesAsync(
        AzureClient::PageOffset{blobOffset.Value()}, ::AVEVA::HttpBody{std::string(buffer.begin(), buffer.end())},
        AzureClient::UploadPagesOptions{}, BoundByWriteDeadline(writeBudget, TransferOptions(client, buffer.size()))));
}

template <typename TBlob>
void UploadPages(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client, std::span<const char> buffer,
                 const Core::ByteOffset blobOffset) {
    Block(client, UploadPagesAsync(runtime, client, buffer, blobOffset));
}

// The request views `data` and keeps it alive itself, so the payload is never copied.
template <typename TBlob>
[[nodiscard]] Task<void> UploadPagesAsync(std::shared_ptr<const ClientRuntimeBase> runtime, TBlob& client,
                                          std::shared_ptr<const std::vector<char>> data,
                                          const Core::ByteOffset blobOffset) {
    const auto writeBudget = runtime->Fence().ThrowIfFenced();
    const auto size = data->size();
    const auto bytes = std::as_bytes(std::span(*data));
    Unwrap(co_await client.UploadPagesAsync(
        AzureClient::PageOffset{blobOffset.Value()},
        ::AVEVA::HttpBody(bytes, gsl::not_null<std::shared_ptr<const void>>(std::move(data))),
        AzureClient::UploadPagesOptions{}, BoundByWriteDeadline(writeBudget, TransferOptions(client, size))));
}

template <typename TBlob>
void UploadPages(const std::shared_ptr<const ClientRuntimeBase>& runtime, TBlob& client,
                 std::shared_ptr<const std::vector<char>> data, const Core::ByteOffset blobOffset) {
    Block(client, UploadPagesAsync(runtime, client, std::move(data), blobOffset));
}

/// <summary>A run of populated bytes in a page blob.</summary>
struct PageChunk {
    Core::ByteOffset Offset;
    Core::ByteCount Length;
};

// The parts of the first `size` bytes that hold data, split to at most Configuration::Transfer::MaxCopyChunkSize.
// Never-written and cleared pages read as zeros, so a copy need not transfer them. The listing is guarded by `etag`
// (ignored when empty), so a concurrent write fails the call instead of being silently missed.
template <typename TBlob>
[[nodiscard]] Task<std::vector<PageChunk>> PlanPageCopyAsync(TBlob& source, const Core::ByteCount size,
                                                             std::string etag) {
    std::vector<PageChunk> chunks;
    AzureClient::GetPageRangesOptions options;
    if (!etag.empty()) {
        options.Conditions.IfMatch = etag;
    }
    do {
        auto page = Unwrap(co_await source.GetPageRangesAsync(options));
        for (const auto& range : page.Ranges) {
            auto offset = range.Offset;
            const auto end = (std::min)(size.Value(), offset + range.Length);
            while (offset < end) {
                const auto length = (std::min)(end - offset, Configuration::Transfer::MaxCopyChunkSize.Value());
                chunks.push_back({Core::ByteOffset{offset}, Core::ByteCount{length}});
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
Task<void> CopyPagesAsync(std::shared_ptr<const ClientRuntimeBase> runtime, const Source<TBlob> from,
                          const Destination<TBlob> to, const Core::ByteCount size, std::string etag) {
    auto& source = from.Blob();
    auto& destination = to.Blob();
    const auto chunks = co_await PlanPageCopyAsync(source, size, etag);
    if (chunks.empty()) {
        co_return;
    }

    constexpr auto PageSize = Configuration::PageBlob::PageSize.Value();
    const auto paddedLength = [](const Core::ByteCount length) {
        return (length.Value() + PageSize - 1) / PageSize * PageSize;
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

    TaskGroup uploads(co_await boost::asio::this_coro::executor,
                      ConcurrencyLimit{Configuration::Transfer::MaxConcurrentPageCopies});
    co_await uploads.Scope([&](TaskGroup& running) -> Task<void> {
        for (const auto& chunk : chunks) {
            Buffer buffer = acquire(paddedLength(chunk.Length));
            const auto dataLength = chunk.Length.Value();
            const auto downloaded = co_await DownloadAsync(source, std::span<char>(*buffer).first(dataLength),
                                                           chunk.Offset, chunk.Length, etag);
            if (downloaded != chunk.Length) {
                throw std::runtime_error("Unexpected end of blob while copying pages at offset " +
                                         std::to_string(chunk.Offset.Value()));
            }
            // Zeroes the padding after the data, which would otherwise hold a previous chunk's bytes.
            std::fill(buffer->begin() + static_cast<std::ptrdiff_t>(dataLength), buffer->end(), '\0');
            co_await running.Spawn(UploadPagesAsync(runtime, destination, std::move(buffer), chunk.Offset));
        }
    });
}

template <typename TBlob>
void CopyPages(const std::shared_ptr<const ClientRuntimeBase>& runtime, const Source<TBlob> source,
               const Destination<TBlob> destination, const Core::ByteCount size, const std::string& etag) {
    Block(source.Blob(), CopyPagesAsync(runtime, source, destination, size, etag));
}

enum class CopySourceAccess : std::uint8_t { Permitted, Unauthorized };

// Unauthorized when the service refused to read the copy source for lack of authorization, which says nothing about
// the destination: the caller may still copy the bytes through the client.
[[nodiscard]] inline CopySourceAccess CopySourceAccessOf(const AzureClient::BlobStorageError& error) {
    return error.StatusCode.Value() == HttpStatus::Unauthorized.Value() ||
                   error.StatusCode.Value() == HttpStatus::Forbidden.Value() ||
                   error.ErrorCode == "CannotVerifyCopySource"
               ? CopySourceAccess::Unauthorized
               : CopySourceAccess::Permitted;
}

// Copies the populated pages of `source` into `destination` (which must already exist with enough capacity) inside
// the storage service with Put Page From URL, so the data never crosses the network to this process. Several
// requests are kept in flight. The listing and every copied range are guarded by `etag` (ignored when empty), so a
// write to the source mid-copy fails instead of producing a torn destination. A final chunk that is not page sized
// is rounded up to a whole page; the logical size is carried separately by SetSize.
//
// The first request runs alone: if the service refuses to authorize the source (a client whose credential cannot be
// forwarded, e.g. Shared Key) nothing has been copied yet and Declined is returned so the caller can fall back to
// CopyPages. Any other failure throws.
template <typename TBlob>
[[nodiscard]] Task<ServerSideCopy> CopyPagesServerSideAsync(std::shared_ptr<const ClientRuntimeBase> runtime,
                                                            const Source<TBlob> from, const Destination<TBlob> to,
                                                            const Core::ByteCount size, std::string etag) {
    auto& source = from.Blob();
    auto& destination = to.Blob();
    // Fails fast before the listing when writes are already fenced. The budget is not carried over: every copy
    // request below takes it afresh and bounds itself with BoundByWriteDeadline, because the listing consumes part of
    // it.
    static_cast<void>(runtime->Fence().ThrowIfFenced());
    // A destination that cannot forward a bearer token cannot authorize a SAS-less source either; the service would
    // refuse the copy, so skip the doomed request and let the caller copy the bytes through the client.
    if (destination.ForwardableCredential() == AzureClient::CredentialPresence::Absent &&
        source.SasToken() == AzureClient::CredentialPresence::Absent) {
        co_return ServerSideCopy::Declined;
    }
    const auto chunks = co_await PlanPageCopyAsync(source, size, etag);
    if (chunks.empty()) {
        co_return ServerSideCopy::Done;
    }
    constexpr auto PageSize = Configuration::PageBlob::PageSize.Value();
    const auto sourceUrl = source.Url();
    const auto copyRange =
        [&](PageChunk chunk) -> Task<AzureClient::BlobResult<AzureClient::Models::UploadPagesResult>> {
        // Planning and earlier requests consume the lease budget, so it is taken afresh for every request; a relative
        // duration captured earlier would let this one outlive the write deadline.
        const auto writeBudget = runtime->Fence().ThrowIfFenced();
        AzureClient::UploadPagesFromUrlOptions options;
        options.SourceIfMatch = etag;
        options.SourceAuth = AzureClient::SourceAuthorization::ClientCredential;
        const auto length = (chunk.Length.Value() + PageSize - 1) / PageSize * PageSize;
        co_return co_await destination.UploadPagesFromUrlAsync(
            AzureClient::PageOffset{chunk.Offset.Value()}, sourceUrl,
            AzureClient::SourcePageOffset{chunk.Offset.Value()}, AzureClient::PageLength{length}, std::move(options),
            BoundByWriteDeadline(writeBudget, destination.GetDefaultRequestOptions()));
    };
    const auto copyRangeChecked = [&](PageChunk chunk) -> Task<void> { Unwrap(co_await copyRange(chunk)); };

    const auto first = co_await copyRange(chunks.front());
    if (!first.has_value()) {
        if (CopySourceAccessOf(first.error()) == CopySourceAccess::Unauthorized) {
            co_return ServerSideCopy::Declined;
        }
        ThrowRequestFailed(first.error());
    }

    TaskGroup copies(co_await boost::asio::this_coro::executor,
                     ConcurrencyLimit{Configuration::Transfer::MaxConcurrentPageCopies});
    co_await copies.Scope([&](TaskGroup& running) -> Task<void> {
        for (size_t index = 1; index < chunks.size(); ++index) {
            co_await running.Spawn(copyRangeChecked(chunks[index]));
        }
    });
    co_return ServerSideCopy::Done;
}

template <typename TBlob>
[[nodiscard]] ServerSideCopy CopyPagesServerSide(const std::shared_ptr<const ClientRuntimeBase>& runtime,
                                                 const Source<TBlob> source, const Destination<TBlob> destination,
                                                 const Core::ByteCount size, const std::string& etag) {
    return Block(source.Blob(), CopyPagesServerSideAsync(runtime, source, destination, size, etag));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations
