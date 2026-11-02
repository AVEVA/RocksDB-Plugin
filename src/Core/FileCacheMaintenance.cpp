// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

// Eviction, size accounting checks and deferred file release for FileCacheImpl. Every *Unsafe member here expects the
// caller to hold m_mutex; ReleaseOnBackgroundThread and TryDeleteCachedFile must be called with it released.

#include "FileCacheImpl.hpp"
#include <boost/log/trivial.hpp>
#include <cassert>
#include <exception>
#include <string>
#include <vector>
using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Core {
namespace {
enum class ListPosition : std::uint8_t { Interior, Front };
constexpr size_t MaxUndownloadedStaleEntries = 2048;
} // namespace

// Second chance: an entry relinked within the last size/4 accesses is already near the front, so hot files do not
// churn the list on every read. Small caches (< 4 entries) always relink, keeping exact LRU order.
void FileCacheImpl::EntryAccessedUnsafe(FileCacheEntry& file, const RelinkPolicy policy) {
    ++m_accessEpoch;
    if (policy == RelinkPolicy::Lazy && file.LinkState() == ListLink::Linked &&
        m_accessEpoch.Value() - file.LastRelinkEpoch.Value() < m_cache.size() / 4) {
        return;
    }
    file.LastRelinkEpoch = m_accessEpoch;
    file.unlink();
    m_entryList.push_front(file);
}

size_t FileCacheImpl::PruneUndownloadedStaleEntriesUnsafe() {
    if (m_undownloadedStaleCount <= MaxUndownloadedStaleEntries) {
        return 0;
    }

    // Oldest first. Removing an entry unlinks it from m_metadataOnlyList, so the list shrinks on every iteration.
    size_t pruned = 0;
    while (m_undownloadedStaleCount > MaxUndownloadedStaleEntries && !m_metadataOnlyList.empty()) {
        const std::string staleEntry = m_metadataOnlyList.back().GetFilePath();
        if (m_metadataOnlyList.back().Disk == DiskPresence::KnownAbsent) {
            // Nothing was ever downloaded, so there is no file or handle to release: drop it without queueing a delete.
            DropNeverDownloadedEntryUnsafe(staleEntry);
        } else if (RemoveFileUnsafe(staleEntry) == RemovalOutcome::NotRemoved) {
            break;
        }
        ++pruned;
    }
    return pruned;
}

FileCacheImpl::EvictionOutcome FileCacheImpl::EvictToFit(const ByteCount bytes) {
    if (bytes > m_maxSize) {
        // No point in evicting everything from the cache if we can't fit the new file.
        BOOST_LOG_SEV(*m_logger, debug) << "Skipping eviction from file cache because " << bytes.Value()
                                        << " bytes is greater than the maximum of " << m_maxSize.Value();
        return EvictionOutcome::Insufficient;
    }
    return EvictBytesUnsafe(bytes);
}

FileCacheImpl::EvictionOutcome FileCacheImpl::ShrinkToMaxUnsafe() {
    const auto size = GetCurrentSizeUnsafe();
    if (size <= m_maxSize) {
        return EvictionOutcome::Sufficient;
    }
    (void)EvictBytesUnsafe(size - m_maxSize);
    const auto remaining = GetCurrentSizeUnsafe();
    if (remaining > m_maxSize) {
        BOOST_LOG_SEV(*m_logger, warning)
            << "File cache is still " << remaining.Value() << " bytes after shrinking to the maximum of "
            << m_maxSize.Value() << " bytes; the rest is not evictable yet";
        return EvictionOutcome::Insufficient;
    }
    return EvictionOutcome::Sufficient;
}

FileCacheImpl::EvictionOutcome FileCacheImpl::EvictBytesUnsafe(const ByteCount bytes) {
    BOOST_LOG_SEV(*m_logger, debug) << "Attempting to evict " << bytes.Value() << " (bytes) from the file cache.";
    ByteCount bytesEvicted;
    size_t evictedCount = 0;
    if (m_entryList.empty()) {
        return bytesEvicted >= bytes ? EvictionOutcome::Sufficient : EvictionOutcome::Insufficient;
    }

    // Walk from the least recently used entry towards the front. The next candidate is captured before the current
    // one is removed, and the front entry is processed too (the loop ends after it).
    auto tail = m_entryList.s_iterator_to(m_entryList.back());
    while (bytesEvicted < bytes) {
        const auto current = tail;
        const auto position = current == m_entryList.begin() ? ListPosition::Front : ListPosition::Interior;
        if (position == ListPosition::Interior) {
            --tail;
        }

        // Don't evict downloading files. They could be files that we are making space for.
        const auto state = current->GetState();
        if (state == FileCacheEntry::State::Downloading || state == FileCacheEntry::State::QueuedForDownload) {
            BOOST_LOG_SEV(*m_logger, debug) << "Skipping eviction of '" << current->GetFilePath()
                                            << "'. It is currently downloading or queued for download";
        } else if (state == FileCacheEntry::State::Stale && current->GetSize() == ByteCount{}) {
            // Frees nothing, and removing it would discard its access history; PruneUndownloadedStaleEntriesUnsafe
            // bounds these.
        } else {
            const auto fileSize = current->GetSize();
            if (RemoveFileUnsafe(current->GetFilePath()) == RemovalOutcome::Removed) {
                bytesEvicted += fileSize;
                ++evictedCount;
            }
        }

        if (position == ListPosition::Front) {
            break;
        }
    }

    BOOST_LOG_SEV(*m_logger, debug) << "Evicted " << evictedCount << " file(s), " << bytesEvicted.Value() << " of "
                                    << bytes.Value() << " (bytes) requested, from the file cache";
    return bytesEvicted >= bytes ? EvictionOutcome::Sufficient : EvictionOutcome::Insufficient;
}

// Runs on the background thread with no lock held. Closing a handle waits for any reader still using it, and only
// then is the file deleted, which matters on Windows where an open file cannot be removed.
void FileCacheImpl::ReleaseOnBackgroundThread(std::vector<PendingRelease>& releases) {
    for (auto& release : releases) {
        if (release.Handle) {
            release.Handle->Close();
        }
        if (release.Fate == DiskFate::Delete && TryDeleteCachedFile(release.FilePath) == DeleteOutcome::Failed) {
            std::scoped_lock lock(m_mutex);
            m_failedDeletes.push_back(release.FilePath);
        }
    }
    releases.clear();
}

// A file that cannot be deleted is logged and remembered for a later retry; its bytes have already left the
// cache's accounting, so the only cost of a failure is disk space until the retry succeeds. The path is built inside
// the try so an allocation failure is reported as a failed delete rather than abandoning the release mid-way.
FileCacheImpl::DeleteOutcome FileCacheImpl::TryDeleteCachedFile(const std::string& filePath) {
    try {
        const auto cachedFilePath = m_cachePath / filePath;
        if (m_filesystem->DeleteFile(cachedFilePath) == FilesystemResult::Succeeded) {
            return DeleteOutcome::Deleted;
        }
        BOOST_LOG_SEV(*m_logger, warning)
            << "Could not delete cached file '" << cachedFilePath.string() << "'. It will be retried.";
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, warning)
            << "Deleting cached file '" << filePath << "' failed: " << e.what() << ". It will be retried.";
    }
    return DeleteOutcome::Failed;
}

ByteCount FileCacheImpl::GetCurrentSizeUnsafe() const noexcept { return m_activeBytes; }

// Debug-only cross-check of the running totals against a full scan of the entries.
void FileCacheImpl::ValidateCountersUnsafe() const {
#ifndef NDEBUG
    ByteCount bytes;
    size_t stale = 0;
    size_t linked = 0;
    for (const auto& entry : m_entryList) {
        linked += entry.MetadataOnly.is_linked() ? size_t{1} : size_t{0};
        const auto state = entry.GetState();
        if (state != FileCacheEntry::State::Downloading && state != FileCacheEntry::State::QueuedForDownload) {
            bytes += entry.GetSize();
        }
        if (state == FileCacheEntry::State::Stale && entry.GetSize() == ByteCount{}) {
            ++stale;
        }
    }
    assert(bytes == m_activeBytes && "Active byte total should match the entries");
    assert(stale == m_undownloadedStaleCount && "Stale entry count should match the entries");
    assert(linked == stale && "Every metadata-only stale entry should be in the prune list");
#endif
}
} // namespace AVEVA::RocksDB::Plugin::Core
