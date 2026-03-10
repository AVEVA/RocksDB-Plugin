// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/LruFileIndex.hpp"

#include <filesystem>
#include <limits>
#include <string>

namespace AVEVA::RocksDB::Plugin::Core
{
    namespace
    {
        /// <summary>
        /// Returns the sharded path string for a cache entry file.
        /// Entries are bucketed into one of 256 subdirectories named by the first two
        /// hex digits of the filename (i.e. the hex encoding of the first byte of the
        /// cache key) so that no single directory accumulates an unbounded number of
        /// entries, which degrades readdir performance on many filesystems.
        ///
        /// Output format (normal case):
        ///   <cacheDir> / <shard> / <filename>
        ///
        /// Example: cacheDir="C:\cache", key first byte=0x4A → filename="4aff..."
        ///   → "C:\cache\4a\4aff..."
        /// </summary>
        std::string ShardedPathStr(const std::string& cacheDirStr, std::string_view filename)
        {
            // '\\' on Windows, '/' on POSIX.
            constexpr char kSep = static_cast<char>(std::filesystem::path::preferred_separator);
            std::string result;
            if (filename.size() >= 2)
            {
                // Reserve exactly: cacheDir + sep + 2-char shard + sep + full filename.
                result.reserve(cacheDirStr.size() + 1 + 2 + 1 + filename.size());
                result += cacheDirStr;
                result += kSep;
                result.append(filename.data(), 2); // shard dir  = first two hex chars
                result += kSep;
                result.append(filename.data(), filename.size()); // filename = all hex chars
            }
            else
            {
                // Degenerate fallback: a single-character filename cannot be sharded.
                // In practice unreachable — every key byte produces two hex chars —
                // but handled defensively for zero-length or single-char edge cases.
                result.reserve(cacheDirStr.size() + 1 + filename.size());
                result += cacheDirStr;
                result += kSep;
                result.append(filename.data(), filename.size());
            }
            return result;
        }
    }

    LruFileIndex::LruFileIndex(std::string cacheDirStr, size_t capacity)
        : m_cacheDirStr(std::move(cacheDirStr)), m_capacity(capacity)
    {
    }

    std::string LruFileIndex::MakePath(std::string_view filename) const
    {
        return ShardedPathStr(m_cacheDirStr, filename);
    }

    std::shared_lock<std::shared_mutex> LruFileIndex::AcquireShared() const
    {
        return std::shared_lock<std::shared_mutex>(m_mutex);
    }

    bool LruFileIndex::ContainsLocked(std::string_view filename) const noexcept
    {
        return m_index.find(filename) != m_index.end();
    }

    std::optional<LruFileIndex::EvictList> LruFileIndex::ReserveCapacity(
        std::string_view filename, size_t storedSize, bool force_insert)
    {
        EvictList evictPairs;
        std::lock_guard lock(m_mutex);

        auto indexIt = m_index.find(filename);
        const size_t existingSize = (indexIt != m_index.end()) ? (*indexIt)->size : 0;

        // When not forced, skip the write rather than evicting a potentially more
        // valuable entry.  Net-change accounting ensures a same-key update is never
        // skipped even when the cache is exactly full.
        if (!force_insert && m_currentSize - existingSize + storedSize > m_capacity)
            return std::nullopt;

        // Pin the existing entry at MRU so EvictUntilSizeLocked cannot evict it,
        // and so the temporary m_currentSize adjustment below avoids double-counting.
        if (indexIt != m_index.end())
            m_lruList.splice(m_lruList.begin(), m_lruList, *indexIt);

        // Temporarily discount the existing entry so eviction is sized against the
        // net new bytes required.  Restored before returning so m_currentSize stays
        // consistent on any failure path after this method returns.
        m_currentSize -= existingSize;
        if (m_currentSize + storedSize > m_capacity)
            EvictUntilSizeLocked(m_capacity >= storedSize ? m_capacity - storedSize : 0, evictPairs);
        m_currentSize += existingSize;

        return evictPairs;
    }

    LruFileIndex::EvictList LruFileIndex::RegisterEntry(
        std::string_view filename, size_t storedSize)
    {
        EvictList evictPairs;
        std::lock_guard lock(m_mutex);

        // Remove any existing entry for this key — either the one preserved by
        // ReserveCapacity or a newer one written by a concurrent thread between the
        // disk write and now.  The file on disk was atomically replaced, so no
        // separate file deletion is needed for the displaced entry.
        auto existingIt = m_index.find(filename);
        if (existingIt != m_index.end())
        {
            m_currentSize -= (*existingIt)->size;
            m_lruList.erase(*existingIt);
            m_index.erase(existingIt);
        }

        Entry newEntry{};
        newEntry.filename.assign(filename.data(), filename.size());
        newEntry.size = storedSize;
        m_lruList.push_front(std::move(newEntry));
        m_index.insert(m_lruList.begin());
        m_currentSize += storedSize;

        // Correct any capacity overshoot from concurrent writes for different keys
        // that each independently passed ReserveCapacity's admission check.
        EvictUntilSizeLocked(m_capacity, evictPairs);

        return evictPairs;
    }

    bool LruFileIndex::SpliceOrErase(
        std::string_view filename, bool advise_erase,
        std::pair<std::string, std::string>& advisedPair, bool& erased)
    {
        std::lock_guard lock(m_mutex);
        auto indexIt = m_index.find(filename);
        if (indexIt == m_index.end())
            return false;
        if (advise_erase)
        {
            advisedPair = RemoveEntryLocked(*indexIt);
            erased = true;
        }
        else
        {
            m_lruList.splice(m_lruList.begin(), m_lruList, *indexIt);
            erased = false;
        }
        return true;
    }

    std::pair<std::string, std::string> LruFileIndex::Remove(
        std::string_view filename) noexcept
    {
        try
        {
            std::lock_guard lock(m_mutex);
            auto it = m_index.find(filename);
            if (it != m_index.end())
                return RemoveEntryLocked(*it);
        }
        catch (...) {}
        return {};
    }

    LruFileIndex::EvictList LruFileIndex::SetCapacity(size_t capacity)
    {
        EvictList evictPairs;
        std::lock_guard lock(m_mutex);
        m_capacity = capacity;
        EvictUntilSizeLocked(m_capacity, evictPairs);
        return evictPairs;
    }

    LruFileIndex::EvictList LruFileIndex::Deflate(size_t decrease)
    {
        EvictList evictPairs;
        std::lock_guard lock(m_mutex);
        m_capacity -= std::min(decrease, m_capacity);
        EvictUntilSizeLocked(m_capacity, evictPairs);
        return evictPairs;
    }

    void LruFileIndex::Inflate(size_t increase)
    {
        std::lock_guard lock(m_mutex);
        // Saturating add: clamp to size_t max rather than wrapping silently.
        const size_t remaining = std::numeric_limits<size_t>::max() - m_capacity;
        m_capacity += (increase > remaining) ? remaining : increase;
    }

    size_t LruFileIndex::GetCapacity() const noexcept
    {
        std::shared_lock lock(m_mutex);
        return m_capacity;
    }

    size_t LruFileIndex::GetUsage() const noexcept
    {
        std::shared_lock lock(m_mutex);
        return m_currentSize;
    }

    uint64_t LruFileIndex::GetEvictedCount() const noexcept
    {
        return m_evictedCount.load(std::memory_order_relaxed);
    }

    void LruFileIndex::EvictUntilSizeLocked(size_t targetSize, EvictList& evictPairs)
    {
        while (m_currentSize > targetSize && !m_lruList.empty())
        {
            evictPairs.push_back(RemoveEntryLocked(std::prev(m_lruList.end())));
            m_evictedCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    std::pair<std::string, std::string>
        LruFileIndex::RemoveEntryLocked(LruIterator it)
    {
        // Build the orig and graveyard path strings while under the lock so that the
        // unique graveyard name is reserved before any concurrent writer can claim the
        // same sequence number.  No filesystem operation is performed here; the caller
        // must commit the returned pair after releasing the lock.  Deferring the rename
        // outside the lock allows bulk evictions to release the mutex sooner, at the
        // cost of a narrow TOCTOU window where a concurrent insert for the same key
        // could have its file moved to the graveyard (causing one cache miss; the
        // phantom index entry is then cleaned up by Lookup).
        const std::string origPathStr = ShardedPathStr(m_cacheDirStr, it->filename);
        const std::string graveyardPathStr =
            origPathStr + "." +
            std::to_string(m_seq.fetch_add(1, std::memory_order_relaxed)) + ".del";
        m_currentSize -= it->size;
        m_index.erase(it);
        m_lruList.erase(it);
        return { origPathStr, graveyardPathStr };
    }
}
