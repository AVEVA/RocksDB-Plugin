// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlockOn.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#ifdef _WIN32
// The target is built with _WIN32_WINNT=0x0A00 (exported publicly) and Boost.Asio's ABI depends on it.
#if defined(_WIN32_WINNT) && _WIN32_WINNT < 0x0A00
#error "aveva-rocksdb-plugin-azure-impl requires _WIN32_WINNT >= 0x0A00 (Windows 10)."
#endif
// Boost.Asio pulls in <windows.h>, whose macros clash with RocksDB method names (mirrors rocksdb/env.h). These
// headers are internal (not installed), so the macros never reach consumers of the public headers.
#undef DeleteFile
#undef GetCurrentTime
#undef GetFreeSpace
#undef LoadLibrary
#endif
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>Returns the time left until the earliest lease renewal deadline, or empty when no lease is held.</summary>
using WriteDeadlineSource = std::function<std::optional<std::chrono::steady_clock::duration>()>;

/// <summary>
/// What every file, lock and blob operation needs from the runtime, independent of the HTTP transport: the write
/// fence shared by every file, lock and directory of a filesystem. Kept non-template so those classes need not know
/// which transport they run on.
/// </summary>
class ClientRuntimeBase {
    std::atomic<bool> m_writesFenced{false};
    mutable std::mutex m_deadlineMutex;
    WriteDeadlineSource m_deadlineSource;
    mutable std::once_flag m_writerOnce;
    mutable std::unique_ptr<boost::asio::thread_pool> m_writerPool;

  public:
    ClientRuntimeBase() = default;
    virtual ~ClientRuntimeBase();
    ClientRuntimeBase(const ClientRuntimeBase&) = delete;
    ClientRuntimeBase& operator=(const ClientRuntimeBase&) = delete;
    ClientRuntimeBase(ClientRuntimeBase&&) = delete;
    ClientRuntimeBase& operator=(ClientRuntimeBase&&) = delete;

    /// <summary>
    /// A scheduler for the blocking disk writes of file downloads, backed by a small pool created on first use and
    /// owned by this runtime, so a download never starts a thread per write batch. Valid while the runtime lives.
    /// </summary>
    [[nodiscard]] std::function<void(std::move_only_function<void()>)> DownloadWriter() const;

    /// <summary>
    /// Makes every later mutation through this runtime's blobs fail. Called once the filesystem can no longer
    /// prove it still holds its locks, so files opened earlier stop writing too. Irreversible.
    /// </summary>
    void FenceWrites() noexcept;
    [[nodiscard]] bool WritesFenced() const noexcept;
    /// <summary>
    /// Throws std::runtime_error when writes are fenced or the earliest lease renewal deadline has passed. The
    /// deadline check closes the window before the renewal loop gives up, in which another writer may already own
    /// the lease. Returns the time left (empty when no deadline applies) so the caller can bound its request without
    /// querying the deadline source a second time.
    /// </summary>
    std::optional<std::chrono::steady_clock::duration> ThrowIfWritesFenced() const;

    /// <summary>
    /// Installs the source of the time left until the earliest lease renewal deadline (empty result: no lease is
    /// held). Pass an empty function to remove it. The source is called under an internal lock, so removing it
    /// guarantees it is no longer running.
    /// </summary>
    void SetWriteDeadlineSource(WriteDeadlineSource source);
    /// <summary>Time left before mutations must stop; empty when no lease deadline applies.</summary>
    [[nodiscard]] std::optional<std::chrono::steady_clock::duration> WriteTimeRemaining() const;

    /// <summary>
    /// A short-lived cache of blobs' logical sizes (keyed by blob URL) for FileSystem::GetFileSize, which RocksDB
    /// calls repeatedly for the same files. Every operation that can change a blob's size invalidates its entry
    /// when it finishes, and a fetch that overlapped any invalidation is never stored (see BlobSizeEpoch), so only
    /// another process's writes can be missed, and only for Configuration::MetadataCacheTtl.
    /// </summary>
    [[nodiscard]] std::optional<int64_t> CachedBlobSize(const std::string& blobUrl) const;
    /// <summary>Read before fetching a size; pass it to CacheBlobSize so a racing invalidation wins.</summary>
    [[nodiscard]] uint64_t BlobSizeEpoch() const noexcept;
    void CacheBlobSize(const std::string& blobUrl, int64_t size, uint64_t epoch) const;
    void InvalidateBlobSize(const std::string& blobUrl) const;
    /// <summary>For operations that touch many blobs at once (directory delete, rename).</summary>
    void InvalidateAllBlobSizes() const;
    /// <summary>
    /// The time source of the size and metadata caches (Configuration::MetadataCacheTtl). Tests replace it with a
    /// manual clock, set before any file is created, so expiry never depends on sleeping.
    /// </summary>
    [[nodiscard]] std::chrono::steady_clock::time_point MetadataNow() const;
    void SetMetadataClockForTesting(std::function<std::chrono::steady_clock::time_point()> clock);

  private:
    struct CachedSize {
        int64_t Size = 0;
        std::chrono::steady_clock::time_point FetchedAt;
    };
    mutable std::mutex m_sizeMutex;
    mutable std::unordered_map<std::string, CachedSize> m_sizes;
    mutable std::atomic<uint64_t> m_sizeEpoch{0};
    std::function<std::chrono::steady_clock::time_point()> m_metadataClock;
};

/// <summary>
/// Caps a mutating request's timeout and total deadline (retries included) at the time left before the lease
/// renewal deadline, so the request cannot land after another writer may have taken over.
/// </summary>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions
BoundByWriteDeadline(const ClientRuntimeBase& runtime, AVEVA::AzureClient::AzureRequestOptions options);

/// <summary>Same, using a remaining time already obtained from ThrowIfWritesFenced.</summary>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions
BoundByWriteDeadline(std::optional<std::chrono::steady_clock::duration> remaining,
                     AVEVA::AzureClient::AzureRequestOptions options);

/// <summary>
/// How a transport is created on the host's io_context. Any transport constructible from an io_context works as is;
/// the production one is specialised (in ClientRuntime.cpp) to configure connection pooling and trust roots.
/// </summary>
template <AzureClient::HttpClientLike THttp> struct HttpClientFactory {
    static std::shared_ptr<THttp> Create(boost::asio::io_context& context) { return std::make_shared<THttp>(context); }
};
template <> struct HttpClientFactory<::AVEVA::HttpClient> {
    static std::shared_ptr<::AVEVA::HttpClient> Create(boost::asio::io_context& context);
};

/// <summary>
/// Holds the HTTP client that every Azure Blob Storage client of a filesystem uses, bound to an io_context
/// owned and run by the host application. The runtime never runs, stops or destroys that io_context: the host
/// must keep it alive and running on at least one thread for as long as the runtime exists.
/// The HTTP client is shared with the Azure clients and credentials, which keep it alive for as long as they exist.
/// Operations are started with boost::asio::use_future and waited for on the caller's thread, which
/// must never be one of the threads running the io_context.
/// </summary>
template <AzureClient::HttpClientLike THttp> class BasicClientRuntime : public ClientRuntimeBase {
    std::shared_ptr<THttp> m_httpClient;

  public:
    /// <summary>    Shares an already constructed transport (the way tests inject a fake).</summary>
    explicit BasicClientRuntime(std::shared_ptr<THttp> httpClient) : m_httpClient(std::move(httpClient)) {}
    /// <summary>Creates the transport on the host's io_context through HttpClientFactory.</summary>
    explicit BasicClientRuntime(boost::asio::io_context& context)
        : m_httpClient(HttpClientFactory<THttp>::Create(context)) {}

    [[nodiscard]] THttp& HttpClient() const noexcept { return *m_httpClient; }
    /// <summary>The owning handle to pass to Azure clients and credentials.</summary>
    [[nodiscard]] const std::shared_ptr<THttp>& SharedHttpClient() const noexcept { return m_httpClient; }
};

/// <summary>The production runtime, over the real HTTP transport.</summary>
using ClientRuntime = BasicClientRuntime<::AVEVA::HttpClient>;

[[noreturn]] void ThrowRequestFailed(const AzureClient::BlobStorageError& error);

/// <summary>
/// Request options for a request transferring `bytes` of payload: raises the response body limit so the
/// payload fits, and extends the timeout so that large transfers on slow links are not cut short.
/// </summary>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions
RequestOptionsForTransfer(const AVEVA::AzureClient::AzureRequestOptions& defaults, uint64_t bytes);

/// Request options for moving `bytes` through `client`: the client's defaults with the timeout and response limit
/// scaled to the transfer size.
template <typename Client>
[[nodiscard]] AVEVA::AzureClient::AzureRequestOptions TransferOptions(const Client& client, uint64_t bytes) {
    return RequestOptionsForTransfer(client.GetDefaultRequestOptions(), bytes);
}

/// <summary>
/// Returns the full response of a completed operation, or throws RequestFailedException on failure.
/// Use this for operations whose Response::Error() carries information (e.g. DeleteIfExists).
/// </summary>
template <class T>
AzureClient::Response<T> UnwrapResponse(std::expected<AzureClient::Response<T>, AzureClient::BlobStorageError> result) {
    if (!result.has_value()) {
        ThrowRequestFailed(result.error());
    }

    return std::move(*result);
}

/// <summary>
/// Returns the value of a completed operation, or throws RequestFailedException on failure.
/// </summary>
template <class T> T Unwrap(std::expected<AzureClient::Response<T>, AzureClient::BlobStorageError> result) {
    return std::move(UnwrapResponse(std::move(result))).Value();
}

/// <summary>
/// Starts an operation, waits for it on the calling thread and returns its value, throwing RequestFailedException on
/// failure. The library's block_on token checks the thread before the request starts. `start` receives the completion
/// token, e.g.
/// `Call([&](auto token) { return c.GetPropertiesAsync(token); })`.
/// </summary>
template <class Start> auto Call(Start&& start) { return Unwrap(std::forward<Start>(start)(AzureClient::block_on)); }

/// <summary>As Call, but returns the full response (for operations whose Response::Error() carries
/// information).</summary>
template <class Start> auto CallResponse(Start&& start) {
    return UnwrapResponse(std::forward<Start>(start)(AzureClient::block_on));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
