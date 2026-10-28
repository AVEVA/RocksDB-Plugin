// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DownloadBufferPool.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockOn.hpp>

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/wait_traits.hpp>
#include <boost/scope/scope_exit.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
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
// Owns the cancellation signal handed to a client request. The client installs its handler into the signal's slot
// while initiating, and boost::asio signals are not thread-safe, so installation and emission share one mutex.
// Posting the emission to the client's executor does not achieve that: another thread running the same io_context
// can emit while the handler is being installed. A stop that arrives before installation is remembered and emitted as
// soon as the handler is in place, so it is not lost.
//
// Request runs inside a std::stop_callback, where an escaping exception terminates the process, so it must not
// allocate. The executor hop is therefore prepared by Install, before the I/O is accepted: a wait on a timer that
// never expires is left outstanding on the executor, and Request only cancels it, which completes the wait there.
// Whoever accepted the I/O must call Retire once the operation has completed, which completes the wait unemitted.
template <class Executor> class StopBridge {
    using Wake = boost::asio::basic_waitable_timer<std::chrono::steady_clock,
                                                   boost::asio::wait_traits<std::chrono::steady_clock>, Executor>;

    std::mutex m_mutex;
    boost::asio::cancellation_signal m_signal;
    // Guards only the timer, and is never held while initiating, so Retire can run from a completion that races
    // with an initiation still holding m_mutex.
    std::mutex m_wakeMutex;
    Wake m_wake;
    bool m_installed = false;
    bool m_emitted = false;
    bool m_requested = false;
    std::atomic<bool> m_retired = false;

    struct Forwarder {
        std::shared_ptr<StopBridge> Bridge;
        void operator()() const noexcept { StopBridge::Request(Bridge); }
    };
    std::mutex m_linkMutex;
    std::optional<std::stop_callback<Forwarder>> m_registration;
    bool m_released = false;

    void EmitLocked() {
        m_emitted = true;
        m_signal.emit(boost::asio::cancellation_type::terminal);
    }

    void ArmWake(const std::shared_ptr<StopBridge>& self) {
        const std::lock_guard lock(m_wakeMutex);
        m_wake.expires_at(std::chrono::steady_clock::time_point::max());
        m_wake.async_wait([self](const boost::system::error_code& error) {
            if (error != boost::asio::error::operation_aborted || self->m_retired.load(std::memory_order_acquire)) {
                return;
            }
            const std::lock_guard lock(self->m_mutex);
            self->m_signal.emit(boost::asio::cancellation_type::terminal);
        });
    }

    // Cancelling an outstanding wait cannot fail on the supported platforms; it does not allocate.
    void CancelWake() noexcept {
        const std::lock_guard lock(m_wakeMutex);
        m_wake.cancel();
    }

  public:
    explicit StopBridge(const Executor& executor) : m_wake(executor) {}

    [[nodiscard]] boost::asio::cancellation_slot Slot() noexcept { return m_signal.slot(); }

    // Runs `initiate`, which must install the handler synchronously, and returns its result. Emission is excluded
    // for the duration, so it cannot observe a half-installed handler. Throws, with nothing left outstanding, if
    // the delivery resources cannot be prepared or `initiate` throws.
    template <class Initiate> decltype(auto) Install(const std::shared_ptr<StopBridge>& self, Initiate&& initiate) {
        ArmWake(self);
        try {
            const std::lock_guard lock(m_mutex);
            const auto settle = [this] {
                m_installed = true;
                if (m_requested && !m_emitted) {
                    EmitLocked();
                }
            };
            if constexpr (std::is_void_v<std::invoke_result_t<Initiate>>) {
                std::forward<Initiate>(initiate)();
                settle();
            } else {
                auto result = std::forward<Initiate>(initiate)();
                settle();
                return result;
            }
        } catch (...) {
            Retire();
            throw;
        }
    }

    // Called from the stopping thread; allocation-free. The emission itself runs on the client's executor so the
    // handler never runs on the stopping thread.
    static void Request(const std::shared_ptr<StopBridge>& self) noexcept {
        {
            const std::lock_guard lock(self->m_mutex);
            self->m_requested = true;
            if (!self->m_installed || self->m_emitted) {
                return;
            }
            self->m_emitted = true;
        }
        self->CancelWake();
    }

    // The operation has completed: releases the outstanding wait without emitting.
    void Retire() noexcept {
        m_retired.store(true, std::memory_order_release);
        CancelWake();
    }

    // Registers `token` to request a stop for the life of one transfer, held inline so the bridge is the only
    // allocation. Arm and Release may race: whichever comes second wins, so a transfer that finished before arming
    // never registers, and a registration is never left behind. The registration owns a reference to the bridge
    // until Release, which the completion must call. Does not throw: moving a stop_token or shared_ptr and
    // registering a stop_callback never allocate.
    void Arm(std::stop_token token, const std::shared_ptr<StopBridge>& self) noexcept {
        const std::lock_guard lock(m_linkMutex);
        if (!m_released) {
            m_registration.emplace(std::move(token), Forwarder{self});
        }
    }

    // Destroying the registration waits for a running Forwarder, which only takes m_mutex. Initiation no longer
    // holds that by the time of Arm, and Release before Arm finds no registration, so holding m_linkMutex is safe.
    // The caller holds its own reference, so dropping the Forwarder's cannot destroy the bridge here.
    void Release() noexcept {
        const std::lock_guard lock(m_linkMutex);
        m_released = true;
        m_registration.reset();
    }
};

// Starts `start(token, requestOptions)` with a cancellation slot that `stopToken` can fire, waits for the result and
// returns its value; failures throw. Never call on a thread running the client's executor.
template <class TBlob, class Start>
auto CallStoppable(TBlob& client, std::stop_token stopToken, const char* stoppedMessage, Start&& start) {
    AzureClient::ThrowIfRunningOn(client.get_executor());
    using Bridge = StopBridge<decltype(client.get_executor())>;
    auto bridge = std::make_shared<Bridge>(client.get_executor());
    auto requestOptions = client.GetDefaultRequestOptions();
    requestOptions.SetCancellationSlot(bridge->Slot());
    const std::stop_callback onStop(stopToken, [bridge]() noexcept { Bridge::Request(bridge); });
    if (stopToken.stop_requested()) {
        throw std::system_error(std::make_error_code(std::errc::operation_canceled), stoppedMessage);
    }
    auto pending = bridge->Install(
        bridge, [&] { return std::forward<Start>(start)(boost::asio::use_future, std::move(requestOptions)); });
    const auto retire = boost::scope::make_scope_exit([&bridge]() noexcept { bridge->Retire(); });
    return Unwrap(pending.get());
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
    auto properties = Call([&](auto token) { return client.GetPropertiesAsync(token); });
    return {BlobHelpers::FileSizeFromProperties(properties), static_cast<int64_t>(properties.ContentLength),
            std::move(properties.ETag), std::chrono::time_point_cast<std::chrono::seconds>(properties.LastModified)};
}

// As GetInfo, but a stop request cancels the call, which then throws. Lets a shutdown that is waiting for the caller's
// thread return promptly instead of waiting out a slow or hung request.
template <typename TBlob> [[nodiscard]] BlobInfo GetInfo(TBlob& client, std::stop_token stopToken) {
    auto properties =
        Detail::CallStoppable(client, std::move(stopToken), "Request stopped", [&](auto token, auto options) {
            return client.GetPropertiesAsync(token, std::move(options));
        });
    return {BlobHelpers::FileSizeFromProperties(properties), static_cast<int64_t>(properties.ContentLength),
            std::move(properties.ETag), std::chrono::time_point_cast<std::chrono::seconds>(properties.LastModified)};
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
template <typename TBlob> [[nodiscard]] BlobMetadata GetMetadata(TBlob& client) {
    auto info = GetInfo(client);
    return {info.Size, std::move(info.ETag)};
}

// The blob's logical size, served from the runtime's short-lived cache when it is fresh.
template <typename TBlob> [[nodiscard]] int64_t GetCachedSize(const ClientRuntimeBase& runtime, TBlob& client) {
    const auto& url = client.ResourceUrl();
    if (const auto cached = runtime.CachedBlobSize(url)) {
        return *cached;
    }
    const auto epoch = runtime.BlobSizeEpoch();
    auto info = GetInfo(client);
    runtime.CacheBlobMetadata(url, info.Size, std::move(info.ETag), epoch);
    return info.Size;
}

// Size and ETag from the runtime's cache when fresh, otherwise one properties request that then fills the cache. Used
// when a file is opened, which RocksDB does for many files.
template <typename TBlob>
[[nodiscard]] ClientRuntimeBase::CachedMetadata GetCachedMetadata(const ClientRuntimeBase& runtime, TBlob& client) {
    const auto& url = client.ResourceUrl();
    if (auto cached = runtime.CachedBlobMetadata(url)) {
        return std::move(*cached);
    }
    const auto epoch = runtime.BlobSizeEpoch();
    const auto fetchedAt = runtime.MetadataNow();
    auto info = GetInfo(client);
    runtime.CacheBlobMetadata(url, info.Size, info.ETag, epoch);
    return {info.Size, std::move(info.ETag), fetchedAt};
}

// Returns the blob's new ETag, which the response already carries, so callers need no follow-up properties request.
template <typename TBlob> std::string SetSize(const ClientRuntimeBase& runtime, TBlob& client, int64_t size) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    AzureClient::SetBlobMetadataOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), std::to_string(size));
    return Call([&](auto token) {
               return client.SetMetadataAsync(std::move(options), token,
                                              BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions()));
           })
        .ETag;
}

template <typename TBlob> void SetCapacity(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    Call([&](auto token) {
        return client.ResizeAsync(static_cast<uint64_t>(capacity), AzureClient::ResizePageBlobOptions{}, token,
                                  BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions()));
    });
}

// Creates the page blob with the given capacity and a zero logical size, replacing any existing blob: Put Page
// Blob without If-None-Match overwrites, so this is one request for both the new and the existing case.
template <typename TBlob> void CreateOrReplace(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    AzureClient::CreatePageBlobOptions options;
    options.Metadata.emplace(std::string(BlobHelpers::SizeMetadataKey), "0");
    Call([&](auto token) {
        return client.CreateAsync(static_cast<uint64_t>(capacity), std::move(options), token,
                                  BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions()));
    });
}

// Creates the page blob with the given capacity unless it already exists. Returns true if it was created.
template <typename TBlob> bool CreateIfNotExists(const ClientRuntimeBase& runtime, TBlob& client, int64_t capacity) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    // The client suppresses BlobAlreadyExists and reports it through Response::Error() instead of failing.
    const auto response = CallResponse([&](auto token) {
        return client.CreateIfNotExistsAsync(static_cast<uint64_t>(capacity), token,
                                             BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions()));
    });
    return !response.Error().has_value();
}

// Deletes the blob if it exists, fenced and bounded by the write deadline like every other mutation so a stale lease
// holder cannot delete after losing the lease. Returns true if the blob existed.
template <typename TBlob> bool Delete(const ClientRuntimeBase& runtime, TBlob& client) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const SizeInvalidation<TBlob> invalidate(runtime, client);
    const auto response = CallResponse([&](auto token) {
        return client.DeleteIfExistsAsync(token, BoundByWriteDeadline(writeBudget, client.GetDefaultRequestOptions()));
    });
    return !response.Error().has_value();
}

// Batched Delete: fenced, and every chunk is bounded by the remaining write deadline. The caller inspects the
// per-blob errors of the result.
template <typename TContainer>
[[nodiscard]] auto DeleteBatch(const ClientRuntimeBase& runtime, TContainer& container, std::vector<std::string> names,
                               AzureClient::DeleteBlobsOptions options) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    return Call([&](auto token) {
        return container.DeleteBlobsAsync(std::move(names), std::move(options), token,
                                          BoundByWriteDeadline(writeBudget, container.GetDefaultRequestOptions()));
    });
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

    // The client may install its cancellation handler after this call has returned the future (file preparation is
    // queued), which the bridge cannot serialize; see tasks A05.
    Detail::CallStoppable(client, std::move(stopToken), "Download stopped", [&](auto token, auto requestOptions) {
        return client.DownloadToAsync(path, std::move(options), token, std::move(requestOptions));
    });
}

// A download into a caller-owned buffer that has been started but not finished; see StartDownload.
template <typename TBlob> struct PendingDownload {
    decltype(std::declval<TBlob&>().DownloadToAsync(std::span<char>{}, AzureClient::DownloadBlobOptions{},
                                                    boost::asio::use_future)) Result;
    decltype(std::declval<TBlob&>().get_executor()) Executor;
};

// Starts the download without waiting, so several can be in flight at once. The buffer and client must stay alive
// until FinishDownload returns (or the future has been waited for).
template <typename TBlob>
[[nodiscard]] PendingDownload<TBlob> StartDownload(TBlob& client, std::span<char> buffer, int64_t offset,
                                                   int64_t length, std::string ifMatch = {}) {
    AzureClient::ThrowIfRunningOn(client.get_executor());
    AzureClient::DownloadBlobOptions options;
    options.Range = Detail::ToRange(offset, length);
    if (!ifMatch.empty()) {
        options.Conditions.IfMatch = std::move(ifMatch);
    }
    // The body is copied once, straight into the caller's buffer; a range larger than the buffer fails instead of
    // being truncated or overflowing.
    return {client.DownloadToAsync(buffer, std::move(options), boost::asio::use_future,
                                   TransferOptions(client, static_cast<uint64_t>(length))),
            client.get_executor()};
}

// Waits for the download and returns the number of bytes written to the buffer.
template <typename TBlob> [[nodiscard]] int64_t FinishDownload(PendingDownload<TBlob>& pending) {
    // Re-checked at the blocking site, not only at Start, so a Pending handed across threads cannot deadlock.
    AzureClient::ThrowIfRunningOn(pending.Executor);
    const auto result = Unwrap(pending.Result.get());
    Detail::CheckDownloadLength(result);
    return static_cast<int64_t>(result.BytesWritten);
}

// As DownloadToBuffer, but fails when the blob's ETag no longer matches `ifMatch` (ignored when empty). A successful
// ranged response without Content-Range is accepted; the body length is the source of truth.
template <typename TBlob>
[[nodiscard]] int64_t Download(TBlob& client, std::span<char> buffer, int64_t offset, int64_t length,
                               std::string ifMatch) {
    auto pending = StartDownload(client, buffer, offset, length, std::move(ifMatch));
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

// Copies `buffer` into the request body; prefer the shared-buffer overload when the caller can hand over ownership.
template <typename TBlob>
void UploadPages(const ClientRuntimeBase& runtime, TBlob& client, std::span<const char> buffer, int64_t blobOffset) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    Call([&](auto token) {
        return client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::as_bytes(buffer), token,
                                       BoundByWriteDeadline(writeBudget, TransferOptions(client, buffer.size())));
    });
}

// The request views `data` and keeps it alive itself, so the payload is not copied.
template <typename TBlob>
void UploadPages(const ClientRuntimeBase& runtime, TBlob& client, std::shared_ptr<const std::vector<char>> data,
                 int64_t blobOffset) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    const auto size = data->size();
    Call([&](auto token) {
        return client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), data, token,
                                       BoundByWriteDeadline(writeBudget, TransferOptions(client, size)));
    });
}

// An upload that has been started but not finished; see StartUploadPages.
template <typename TFuture, typename TExecutor> struct PendingUpload {
    TFuture Result;
    TExecutor Executor;
};

// Starts the upload without waiting, so several can be in flight at once. The request views the shared buffer (no
// copy) and keeps it alive; the client must stay alive until FinishUploadPages returns.
template <typename TBlob>
[[nodiscard]] auto StartUploadPages(const ClientRuntimeBase& runtime, TBlob& client,
                                    std::shared_ptr<const std::vector<char>> buffer, int64_t blobOffset) {
    const auto writeBudget = runtime.ThrowIfWritesFenced();
    AzureClient::ThrowIfRunningOn(client.get_executor());
    const auto size = buffer->size();
    auto result = client.UploadPagesAsync(static_cast<uint64_t>(blobOffset), std::move(buffer), boost::asio::use_future,
                                          BoundByWriteDeadline(writeBudget, TransferOptions(client, size)));
    return PendingUpload<decltype(result), decltype(client.get_executor())>{std::move(result), client.get_executor()};
}

// Waits for the upload, throwing RequestFailedException on failure.
template <typename TFuture, typename TExecutor> void FinishUploadPages(PendingUpload<TFuture, TExecutor>& pending) {
    AzureClient::ThrowIfRunningOn(pending.Executor);
    Unwrap(pending.Result.get());
}

/// <summary>A run of populated bytes in a page blob.</summary>
struct PageChunk {
    int64_t Offset = 0;
    int64_t Length = 0;
};

// Hands an already started operation to `container`. Growing the container can throw after the operation was
// accepted, and a use_future future does not wait when destroyed, so the operation is drained (`drain(item)`) before
// the exception propagates: whatever it reads or writes must stay alive until it completes, and the container's
// cleanup cannot see an operation that never made it in.
template <typename TContainer, typename TItem, typename TDrain>
void PushAccepted(TContainer& container, TItem&& item, TDrain&& drain) {
    try {
        container.push_back(std::move(item));
    } catch (...) {
        drain(item); // push_back's strong guarantee leaves `item` intact
        throw;
    }
}

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
        auto page = Call([&](auto token) { return source.GetPageRangesAsync(options, token); });
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
// chunk is downloaded while earlier ones upload (up to MaxConcurrentPageCopies in flight). Every request is guarded by
// `etag` (ignored when empty), so a write to the source mid-copy fails instead of producing a torn destination. A final
// chunk that is not page sized is padded with zeros; the logical size is carried separately by SetSize.
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

    using Buffer = std::shared_ptr<std::vector<char>>;
    // Every buffer ever made. One that only this list references is idle; uploads and the download in progress hold
    // their own copy. Reusing idle ones avoids allocating a chunk-sized buffer per chunk.
    std::vector<Buffer> buffers;
    const auto acquire = [&buffers](size_t size) {
        for (auto& buffer : buffers) {
            if (buffer.use_count() == 1) {
                // Orders the relaxed use_count() read before reuse of memory the last other owner wrote.
                std::atomic_thread_fence(std::memory_order_acquire);
                buffer->resize(size);
                return buffer;
            }
        }
        return buffers.emplace_back(std::make_shared<std::vector<char>>(size));
    };

    // Declared after the buffers so that it is destroyed first. A download still in flight writes into `downloading`,
    // so on failure wait for it before the buffer is released.
    Buffer downloading;
    std::optional<PendingDownload<TBlob>> pending;
    const boost::scope::scope_exit waitForPending{[&pending] {
        if (pending && pending->Result.valid()) {
            pending->Result.wait();
        }
    }};

    const auto startDownload = [&](const PageChunk& chunk) {
        downloading = acquire(paddedLength(chunk.Length));
        pending.emplace(StartDownload(source, std::span<char>(*downloading).first(static_cast<size_t>(chunk.Length)),
                                      chunk.Offset, chunk.Length, etag));
    };

    // Uploads run concurrently so a large copy costs one upload latency in total rather than per chunk. They are
    // finished in order and, on failure, all waited for because they use the destination client.
    using Upload = decltype(StartUploadPages(runtime, destination, Buffer{}, 0));
    std::deque<Upload> uploads;
    const auto drainUploads = [&uploads] {
        for (auto& upload : uploads) {
            if (upload.Result.valid()) {
                upload.Result.wait();
            }
        }
    };
    try {
        startDownload(chunks.front());
        for (size_t index = 0; index < chunks.size(); ++index) {
            const auto& chunk = chunks[index];
            if (FinishDownload(*pending) != chunk.Length) {
                throw std::runtime_error("Unexpected end of blob while copying pages at offset " +
                                         std::to_string(chunk.Offset));
            }
            const Buffer filled = std::move(downloading);
            if (index + 1 < chunks.size()) {
                startDownload(chunks[index + 1]);
            }
            // Zeroes the padding after the data, which would otherwise hold a previous chunk's bytes.
            std::fill(filled->begin() + chunk.Length, filled->end(), '\0');
            if (uploads.size() >= Configuration::Transfer::MaxConcurrentPageCopies) {
                FinishUploadPages(uploads.front());
                uploads.pop_front();
            }
            PushAccepted(uploads, StartUploadPages(runtime, destination, filled, chunk.Offset),
                         [](Upload& upload) { upload.Result.wait(); });
        }
        while (!uploads.empty()) {
            FinishUploadPages(uploads.front());
            uploads.pop_front();
        }
    } catch (...) {
        drainUploads();
        throw;
    }
} // True when the service refused to read the copy source for lack of authorization, which says nothing about the
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
    // A destination that cannot forward a bearer token cannot authorize a SAS-less source either; the service would
    // refuse the copy, so skip the doomed request and let the caller copy the bytes through the client.
    if (!destination.CanForwardCredential() && !source.HasSasToken()) {
        return false;
    }
    const auto chunks = PlanPageCopy(source, size, etag);
    if (chunks.empty()) {
        return true;
    }
    AzureClient::ThrowIfRunningOn(destination.get_executor());
    AzureClient::ThrowIfRunningOn(source.get_executor());
    constexpr auto PageSize = static_cast<int64_t>(Configuration::PageBlob::PageSize);
    const auto sourceUrl = source.Url();
    const auto start = [&](const PageChunk& chunk, auto token) {
        // Planning and earlier requests consume the lease budget, so it is taken afresh for every request; a relative
        // duration captured earlier would let this one outlive the write deadline.
        const auto writeBudget = runtime.ThrowIfWritesFenced();
        AzureClient::UploadPagesFromUrlOptions options;
        options.SourceIfMatch = etag;
        options.AuthorizeSourceWithClientCredential = true;
        const auto length = (chunk.Length + PageSize - 1) / PageSize * PageSize;
        return destination.UploadPagesFromUrlAsync(
            static_cast<uint64_t>(chunk.Offset), sourceUrl, static_cast<uint64_t>(chunk.Offset),
            static_cast<uint64_t>(length), std::move(options), token,
            BoundByWriteDeadline(writeBudget, destination.GetDefaultRequestOptions()));
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
            PushAccepted(inFlight, start(chunks[index], boost::asio::use_future),
                         [](Pending& pending) { pending.wait(); });
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
    std::optional<std::chrono::steady_clock::duration> writeBudget;
    try {
        writeBudget = runtime.ThrowIfWritesFenced();
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
            if (!result.has_value()) {
                error = std::make_exception_ptr(MakeRequestFailed(result.error()));
            }
            callback(error);
        },
        BoundByWriteDeadline(writeBudget, TransferOptions(client, size)));
}

/// <summary>
/// `callback(std::exception_ptr error, unsigned int httpStatus, std::string_view data, std::shared_ptr<const void>
/// owner)`: an exception (null on
/// success), the HTTP status carried by a RequestFailedException (0 on success or when no response was received) so
/// callers can classify failures without rethrowing, and the downloaded bytes.
/// The bytes live in a pooled buffer that is valid until the callback returns, or for as long as `owner` is held;
/// keep `owner` to retain the bytes without copying.
/// A non-zero `timeout` caps how long the request may take, retries included; it only ever shortens the
/// transfer-scaled default.
/// A stop request on `stopToken` cancels the transfer, transport retries included; the callback then receives the
/// cancellation error. The request is not cancelled by anything else, so a shared download simply passes no token.
/// </summary>
template <typename TBlob, typename TCallback>
void DownloadAsync(TBlob& client, int64_t blobOffset, int64_t readLength, std::string ifMatch,
                   std::chrono::milliseconds timeout, TCallback callback, std::stop_token stopToken = {}) {
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
    // An already-abandoned transfer completes without allocating a buffer or a bridge.
    if (stopToken.stop_requested()) {
        callback(
            std::make_exception_ptr(RequestFailedException(0, "ReadAbandoned", "Read abandoned before it completed", "",
                                                           std::make_error_code(std::errc::operation_canceled))),
            0U, std::string_view{}, std::shared_ptr<const void>{});
        return;
    }
    // The lease is owned by the completion, so the transfer can never write into freed memory.
    auto buffer = DownloadBufferPool::Shared().Acquire(static_cast<size_t>(readLength));
    const std::span<char> destination(buffer->Data(), buffer->Size());
    // The stop registration is armed after the request is initiated, so destroying it (by the completion, possibly
    // on another thread) never waits on the bridge mutex that initiation holds. A stop requested before arming
    // fires on arming.
    using Bridge = Detail::StopBridge<decltype(client.get_executor())>;
    const auto bridge = stopToken.stop_possible() ? std::make_shared<Bridge>(client.get_executor()) : nullptr;
    if (bridge) {
        requestOptions.SetCancellationSlot(bridge->Slot());
    }
    auto start = [&] {
        client.DownloadToAsync(
            destination, std::move(options),
            [buffer = std::move(buffer), callback = std::move(callback), bridge](auto result) mutable {
                if (bridge) {
                    bridge->Release();
                    bridge->Retire();
                }
                std::exception_ptr error;
                unsigned int status = 0;
                size_t written = 0;
                if (!result.has_value()) {
                    status = result.error().StatusCode;
                    error = std::make_exception_ptr(MakeRequestFailed(result.error()));
                } else if (const auto& downloaded = std::move(*result).Value();
                           auto lengthError = Detail::DownloadLengthError(downloaded)) {
                    status = lengthError->StatusCode;
                    error = std::make_exception_ptr(*std::move(lengthError));
                } else {
                    written = static_cast<size_t>(downloaded.BytesWritten);
                }
                const std::string_view data(buffer->Data(), std::min(written, buffer->Size()));
                callback(error, status, data, std::shared_ptr<const void>(std::move(buffer)));
            },
            std::move(requestOptions));
    };
    if (bridge) {
        bridge->Install(bridge, start);
        bridge->Arm(std::move(stopToken), bridge);
    } else {
        start();
    }
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
        if (!result.has_value()) {
            error = std::make_exception_ptr(MakeRequestFailed(result.error()));
        } else {
            try {
                auto properties = std::move(*result).Value();
                size = BlobHelpers::FileSizeFromProperties(properties);
                etag = std::move(properties.ETag);
            } catch (...) {
                error = std::current_exception();
            }
        }
        callback(error, size, std::move(etag));
    });
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::BlobOperations
