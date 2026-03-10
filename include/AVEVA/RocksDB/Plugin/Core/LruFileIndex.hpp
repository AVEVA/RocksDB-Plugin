// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/static_string.hpp>

#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>
    /// Thread-safe LRU index for a file-based cache. Tracks named entries (filename
    /// + size), enforces a capacity limit with an LRU eviction policy, and produces
    /// rename+delete path pairs for deferred filesystem cleanup.
    /// </summary>
    /// <remarks>
    /// Callers are responsible for committing the eviction pairs returned by mutating
    /// operations (via the filesystem) after the call returns.
    ///
    /// Locking model:
    ///   shared_lock  — concurrent readers: GetCapacity, GetUsage,
    ///                  and callers that hold the guard returned by AcquireShared().
    ///   unique_lock  — brief exclusive sections: all index mutations.
    /// </remarks>
    class LruFileIndex
    {
    public:
        /// <summary>Maximum hex-encoded key length supported inline (covers 32-byte / 256-bit keys).</summary>
        static constexpr size_t kMaxFilenameLen = 64;

        /// <summary>Pending file-rename+delete pairs produced by mutating operations.
        /// Each element is {originalPath, graveyardPath}; callers must commit them
        /// after the call returns.</summary>
        using EvictList = std::vector<std::pair<std::string, std::string>>;

        explicit LruFileIndex(std::string cacheDirStr, size_t capacity);

        LruFileIndex(const LruFileIndex&) = delete;
        LruFileIndex& operator=(const LruFileIndex&) = delete;
        LruFileIndex(LruFileIndex&&) = delete;
        LruFileIndex& operator=(LruFileIndex&&) = delete;

        /// <summary>Returns the sharded on-disk path string for a cache entry file.</summary>
        [[nodiscard]] std::string MakePath(std::string_view filename) const;

        /// <summary>Returns a shared lock on the index, suitable for holding across I/O
        /// that must not be interrupted by concurrent evictions (e.g. Lookup phase 1).</summary>
        [[nodiscard]] std::shared_lock<std::shared_mutex> AcquireShared() const;

        /// <summary>Returns true if <paramref name="filename"/> is present in the index.
        /// Must be called while a lock returned by AcquireShared() (or the exclusive lock)
        /// is held.</summary>
        [[nodiscard]] bool ContainsLocked(std::string_view filename) const noexcept;

        /// <summary>Admission control and eviction scheduling (WriteEntry phase 1).
        /// Returns std::nullopt when force_insert is false and the cache is over capacity.
        /// Otherwise returns eviction pairs the caller must commit after the call.</summary>
        [[nodiscard]] std::optional<EvictList> ReserveCapacity(
            std::string_view filename, size_t storedSize, bool force_insert);

        /// <summary>Registers the newly-written entry and corrects capacity overshoot
        /// from concurrent writers (WriteEntry phase 3).
        /// Returns eviction pairs the caller must commit after the call.</summary>
        [[nodiscard]] EvictList RegisterEntry(std::string_view filename, size_t storedSize);

        /// <summary>Lookup phase 2: splices the entry to MRU or schedules it for erasure.
        /// Returns false when the entry disappeared between phase 1 and phase 2.
        /// Sets <paramref name="erased"/> and fills <paramref name="advisedPair"/> on removal.</summary>
        [[nodiscard]] bool SpliceOrErase(std::string_view filename, bool advise_erase,
            std::pair<std::string, std::string>& advisedPair, bool& erased);

        /// <summary>Removes a single entry from the index.
        /// Returns {origPath, graveyardPath}; both strings are empty if the entry was not found.
        /// The caller must commit the pair after the call returns.</summary>
        [[nodiscard]] std::pair<std::string, std::string> Remove(
            std::string_view filename) noexcept;

        /// <summary>Sets the capacity limit and evicts LRU entries as needed.
        /// Returns eviction pairs the caller must commit after the call.</summary>
        [[nodiscard]] EvictList SetCapacity(size_t capacity);

        /// <summary>Reduces capacity by <paramref name="decrease"/> bytes and evicts as needed.
        /// Returns eviction pairs the caller must commit after the call.</summary>
        [[nodiscard]] EvictList Deflate(size_t decrease);

        /// <summary>Increases capacity by <paramref name="increase"/> bytes (saturating).</summary>
        void Inflate(size_t increase);

        [[nodiscard]] size_t GetCapacity() const noexcept;
        [[nodiscard]] size_t GetUsage() const noexcept;
        [[nodiscard]] uint64_t GetEvictedCount() const noexcept;

    private:
        /// <summary>Single data record held by both the hash index and the sequenced LRU list.</summary>
        struct Entry
        {
            boost::static_string<kMaxFilenameLen> filename{};
            size_t size{0};
        };

        using LruList     = std::list<Entry>;
        using LruIterator = LruList::iterator;

        /// <summary>Hashes and compares LruList iterators by their filename, enabling
        /// heterogeneous lookup from a plain std::string_view.</summary>
        struct IteratorHash
        {
            using is_transparent = void;
            size_t operator()(LruIterator it) const noexcept { return StringHash{}(it->filename); }
            size_t operator()(std::string_view sv) const noexcept  { return StringHash{}(sv); }
        };
        struct IteratorEqual
        {
            using is_transparent = void;
            bool operator()(LruIterator a, LruIterator b)    const noexcept { return a->filename == b->filename; }
            bool operator()(std::string_view sv, LruIterator it) const noexcept { return sv == it->filename; }
            bool operator()(LruIterator it, std::string_view sv) const noexcept { return it->filename == sv; }
        };

        using Index = boost::unordered::unordered_flat_set<LruIterator, IteratorHash, IteratorEqual>;

        /// <summary>Evicts LRU entries from the back of the list until m_currentSize &lt;= targetSize.
        /// Must be called with m_mutex held.</summary>
        void EvictUntilSizeLocked(size_t targetSize, EvictList& evictPairs);

        /// <summary>Removes a single entry from the in-memory index and returns {origPath, graveyardPath}.
        /// No filesystem operation is performed; the caller must commit the returned pair.
        /// Must be called with m_mutex held.</summary>
        [[nodiscard]] std::pair<std::string, std::string> RemoveEntryLocked(LruIterator it);

        std::string m_cacheDirStr;
        size_t m_capacity;
        size_t m_currentSize{0};

        // shared_mutex used as a two-level reader-writer lock:
        //   shared_lock  — concurrent readers: GetCapacity, GetUsage, AcquireShared callers.
        //   unique_lock  — brief exclusive sections: all index mutations.
        mutable std::shared_mutex m_mutex;

        /// <summary>Sequenced LRU list: front = MRU, back = LRU.</summary>
        LruList m_lruList;
        /// <summary>Hash index: filename → iterator into m_lruList for O(1) lookup.</summary>
        Index   m_index;

        /// <summary>Monotonically increasing counter for unique graveyard file names.</summary>
        std::atomic<uint64_t> m_seq{0};
        /// <summary>Total entries evicted due to capacity pressure since construction.</summary>
        std::atomic<uint64_t> m_evictedCount{0};
    };
}
