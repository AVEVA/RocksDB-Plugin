// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobMetadataCache.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RequestHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteFence.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>

#include <boost/asio/thread_pool.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
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
/// <summary>
/// What every file, lock and blob operation needs from the runtime, independent of the HTTP transport: the write
/// fence and the blob metadata cache shared by every file, lock and directory of a filesystem, and the download
/// writer pool. Kept non-template so those classes need not know which transport they run on. Not meant to be
/// deleted through a base pointer; share it through the concrete runtime's shared_ptr.
/// </summary>
class ClientRuntimeBase {
    WriteFence m_fence;
    BlobMetadataCache m_metadata;
    mutable std::once_flag m_writerOnce;
    mutable std::unique_ptr<boost::asio::thread_pool> m_writerPool;

  public:
    ClientRuntimeBase() = default;
    ~ClientRuntimeBase();
    ClientRuntimeBase(const ClientRuntimeBase&) = delete;
    ClientRuntimeBase& operator=(const ClientRuntimeBase&) = delete;
    ClientRuntimeBase(ClientRuntimeBase&&) = delete;
    ClientRuntimeBase& operator=(ClientRuntimeBase&&) = delete;

    /// <summary>
    /// A scheduler for the blocking disk writes of file downloads, backed by a small pool created on first use and
    /// owned by this runtime, so a download never starts a thread per write batch. Valid while the runtime lives.
    /// </summary>
    [[nodiscard]] boost::asio::thread_pool::executor_type DownloadWriter() const;

    [[nodiscard]] WriteFence& Fence() noexcept { return m_fence; }
    [[nodiscard]] const WriteFence& Fence() const noexcept { return m_fence; }
    [[nodiscard]] BlobMetadataCache& MetadataCache() noexcept { return m_metadata; }
    [[nodiscard]] const BlobMetadataCache& MetadataCache() const noexcept { return m_metadata; }

    // Forwarding functions to Fence() and MetadataCache(), kept so callers can migrate gradually.
    void FenceWrites() noexcept { m_fence.Fence(); }
    [[nodiscard]] bool WritesFenced() const noexcept { return m_fence.Fenced(); }
    std::optional<std::chrono::steady_clock::duration> ThrowIfWritesFenced() const { return m_fence.ThrowIfFenced(); }
    void SetLeaseDeadline(const void* owner, std::optional<std::chrono::steady_clock::time_point> deadline) {
        m_fence.SetLeaseDeadline(owner, deadline);
    }
    [[nodiscard]] std::optional<std::chrono::steady_clock::duration> WriteTimeRemaining() const {
        return m_fence.TimeRemaining();
    }

    using CachedMetadata = BlobMetadataCache::CachedMetadata;
    [[nodiscard]] std::optional<int64_t> CachedBlobSize(std::string_view blobUrl) const {
        return m_metadata.Size(blobUrl);
    }
    [[nodiscard]] uint64_t BlobSizeEpoch() const noexcept { return m_metadata.SizeEpoch(); }
    void CacheBlobSize(std::string_view blobUrl, int64_t size, uint64_t epoch) const {
        m_metadata.StoreSize(blobUrl, size, epoch);
    }
    [[nodiscard]] std::optional<CachedMetadata> CachedBlobMetadata(std::string_view blobUrl) const {
        return m_metadata.Metadata(blobUrl);
    }
    void CacheBlobMetadata(std::string_view blobUrl, int64_t size, std::string etag, uint64_t epoch) const {
        m_metadata.StoreMetadata(blobUrl, size, std::move(etag), epoch);
    }
    void InvalidateBlobSize(std::string_view blobUrl) const { m_metadata.Invalidate(blobUrl); }
    void InvalidateAllBlobSizes() const { m_metadata.InvalidateAll(); }
    [[nodiscard]] std::chrono::steady_clock::time_point MetadataNow() const { return m_metadata.Now(); }
    void SetMetadataClockForTesting(const std::atomic<int64_t>* manualClockNanos) noexcept {
        m_metadata.SetClockForTesting(manualClockNanos);
    }
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
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
