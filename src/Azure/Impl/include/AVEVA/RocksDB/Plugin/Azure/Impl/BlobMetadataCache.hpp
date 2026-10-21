// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// A short-lived cache of blobs' logical sizes and ETags (keyed by blob URL) for FileSystem::GetFileSize and file
/// opens, which RocksDB repeats for the same files. Every operation that can change a blob's size invalidates its
/// entry when it finishes, and a fetch that overlapped an invalidation of its own blob is never stored (see
/// SizeEpoch), so only another process's writes can be missed, and only for Configuration::MetadataCacheTtl.
/// </summary>
class BlobMetadataCache {
  public:
    /// <summary>A cached (size, ETag) pair and when it was fetched.</summary>
    struct CachedMetadata {
        int64_t Size = 0;
        std::string ETag;
        std::chrono::steady_clock::time_point FetchedAt;
    };

    [[nodiscard]] std::optional<int64_t> Size(std::string_view blobUrl) const;
    /// <summary>Read before fetching a size; pass it to StoreSize so a racing invalidation wins.</summary>
    [[nodiscard]] uint64_t SizeEpoch() const noexcept;
    void StoreSize(std::string_view blobUrl, int64_t size, uint64_t epoch) const;
    /// <summary>
    /// Lets opening a file skip its properties round trip. The entry shares the size cache's lifetime and
    /// invalidation; a stale ETag is harmless because ETag-conditioned reads refresh on a 412.
    /// </summary>
    [[nodiscard]] std::optional<CachedMetadata> Metadata(std::string_view blobUrl) const;
    void StoreMetadata(std::string_view blobUrl, int64_t size, std::string etag, uint64_t epoch) const;
    void Invalidate(std::string_view blobUrl) const;
    /// <summary>For operations that touch many blobs at once (directory delete, rename).</summary>
    void InvalidateAll() const;
    /// <summary>
    /// The time source of the cache (Configuration::MetadataCacheTtl). Tests replace it with a manual clock
    /// (nanoseconds since the steady_clock epoch, owned by the test and outliving the cache), set before any entry is
    /// stored, so expiry never depends on sleeping.
    /// </summary>
    [[nodiscard]] std::chrono::steady_clock::time_point Now() const;
    void SetClockForTesting(const std::atomic<int64_t>* manualClockNanos) noexcept;

  private:
    struct Entry {
        int64_t Size = 0;
        std::optional<std::string> ETag;
        std::chrono::steady_clock::time_point FetchedAt;
    };
    // Transparent hashing lets string_view lookups avoid building a std::string.
    struct UrlHash {
        using is_transparent = void;
        std::size_t operator()(const std::string_view url) const noexcept { return std::hash<std::string_view>{}(url); }
    };
    using EntryMap = std::unordered_map<std::string, Entry, UrlHash, std::equal_to<>>;

    void Store(std::string_view blobUrl, int64_t size, std::optional<std::string> etag, uint64_t epoch) const;
    // Locks, drops the entry if it expired, and returns project(entry) (empty when there is none), so a caller that
    // needs only the size never copies the ETag.
    template <typename TProject> [[nodiscard]] auto WithFreshEntry(std::string_view blobUrl, TProject project) const;

    mutable std::mutex m_mutex;
    mutable EntryMap m_sizes;
    mutable std::atomic<uint64_t> m_epoch{0};
    // Per-URL invalidation epochs: a fetch is rejected only if its own URL was invalidated after it started, or it
    // started before the watermark (raised whenever tombstones are dropped wholesale).
    mutable std::unordered_map<std::string, uint64_t, UrlHash, std::equal_to<>> m_tombstones;
    mutable uint64_t m_tombstoneWatermark = 0;
    mutable std::chrono::steady_clock::time_point m_nextSweep{};
    const std::atomic<int64_t>* m_manualClock = nullptr;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
