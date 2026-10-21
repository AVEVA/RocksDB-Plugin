// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobMetadataCache.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <algorithm>
#include <type_traits>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TProject> auto BlobMetadataCache::WithFreshEntry(std::string_view blobUrl, TProject project) const {
    using Result = std::invoke_result_t<TProject, const Entry&>;
    const std::scoped_lock lock(m_mutex);
    const auto it = m_sizes.find(blobUrl);
    if (it == m_sizes.end()) {
        return Result{};
    }
    if (Now() - it->second.FetchedAt >= Configuration::MetadataCacheTtl) {
        m_sizes.erase(it);
        return Result{};
    }
    return project(it->second);
}

std::chrono::steady_clock::time_point BlobMetadataCache::Now() const {
    if (m_manualClock) {
        return std::chrono::steady_clock::time_point(std::chrono::nanoseconds(m_manualClock->load()));
    }
    return std::chrono::steady_clock::now();
}

void BlobMetadataCache::SetClockForTesting(const std::atomic<int64_t>* manualClockNanos) noexcept {
    m_manualClock = manualClockNanos;
}

std::optional<int64_t> BlobMetadataCache::Size(std::string_view blobUrl) const {
    return WithFreshEntry(blobUrl, [](const Entry& entry) { return std::optional<int64_t>(entry.Size); });
}

uint64_t BlobMetadataCache::SizeEpoch() const noexcept { return m_epoch.load(); }

std::optional<BlobMetadataCache::CachedMetadata> BlobMetadataCache::Metadata(std::string_view blobUrl) const {
    return WithFreshEntry(blobUrl, [](const Entry& entry) -> std::optional<CachedMetadata> {
        if (!entry.ETag) {
            return std::nullopt;
        }
        return CachedMetadata{entry.Size, *entry.ETag, entry.FetchedAt};
    });
}

void BlobMetadataCache::StoreSize(std::string_view blobUrl, const int64_t size, const uint64_t epoch) const {
    Store(blobUrl, size, std::nullopt, epoch);
}

void BlobMetadataCache::StoreMetadata(std::string_view blobUrl, const int64_t size, std::string etag,
                                      const uint64_t epoch) const {
    Store(blobUrl, size, std::move(etag), epoch);
}

void BlobMetadataCache::Store(std::string_view blobUrl, const int64_t size, std::optional<std::string> etag,
                              const uint64_t epoch) const {
    constexpr std::size_t MaxCachedSizes = 4096;
    const auto now = Now();
    const std::scoped_lock lock(m_mutex);
    // Invalidations bump the epoch under this lock, so a fetch that started before an invalidation of its own URL
    // (or before the watermark) can never be stored.
    if (epoch < m_tombstoneWatermark) {
        return;
    }
    if (const auto tomb = m_tombstones.find(blobUrl); tomb != m_tombstones.end() && tomb->second > epoch) {
        return;
    }
    if (m_sizes.size() >= MaxCachedSizes) {
        // The full sweep is O(n), so it runs at most once per TTL; otherwise a single entry is evicted.
        if (now >= m_nextSweep) {
            std::erase_if(m_sizes, [&](const auto& entry) {
                return now - entry.second.FetchedAt >= Configuration::MetadataCacheTtl;
            });
            m_nextSweep = now + Configuration::MetadataCacheTtl;
        }
        if (m_sizes.size() >= MaxCachedSizes && !m_sizes.contains(blobUrl)) {
            m_sizes.erase(m_sizes.begin());
        }
    }
    if (const auto it = m_sizes.find(blobUrl); it != m_sizes.end()) {
        it->second = Entry{size, std::move(etag), now};
    } else {
        m_sizes.emplace(blobUrl, Entry{size, std::move(etag), now});
    }
}

void BlobMetadataCache::Invalidate(std::string_view blobUrl) const {
    const std::scoped_lock lock(m_mutex);
    constexpr std::size_t MaxTombstones = 4096;
    const auto invalidatedAt = ++m_epoch;
    if (const auto it = m_sizes.find(blobUrl); it != m_sizes.end()) {
        m_sizes.erase(it);
    }
    if (m_tombstones.size() >= MaxTombstones && !m_tombstones.contains(blobUrl)) {
        // Dropping tombstones must not let an older in-flight fetch through, so raise the watermark past them.
        m_tombstoneWatermark = invalidatedAt;
        m_tombstones.clear();
    }
    m_tombstones.insert_or_assign(std::string(blobUrl), invalidatedAt);
}

void BlobMetadataCache::InvalidateAll() const {
    const std::scoped_lock lock(m_mutex);
    m_tombstoneWatermark = ++m_epoch;
    m_tombstones.clear();
    m_sizes.clear();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
