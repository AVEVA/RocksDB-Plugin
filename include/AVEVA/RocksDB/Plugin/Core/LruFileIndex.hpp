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
    ///   shared_lock  — concurrent readers: GetCapacity, GetUsage.
    ///   unique_lock  — brief exclusive sections: all index mutations, TryPin, Unpin.
    /// </remarks>
    class LruFileIndex
    {
    public:
        /// <summary>
        /// Maximum hex-encoded key length supported inline
        /// (covers 32-byte / 256-bit keys).
        /// </summary>
        static constexpr size_t kMaxFilenameLen = 64;

        /// <summary>
        /// Pending file-rename+delete pairs produced by mutating operations.
        /// Each element is {originalPath, graveyardPath}; callers must commit them
        /// after the call returns.
        /// </summary>
        using EvictList = std::vector<std::pair<std::string, std::string>>;

        explicit LruFileIndex(std::string cacheDirStr, size_t capacity);
        LruFileIndex(const LruFileIndex&) = delete;
        LruFileIndex& operator=(const LruFileIndex&) = delete;
        LruFileIndex(LruFileIndex&&) = delete;
        LruFileIndex& operator=(LruFileIndex&&) = delete;

        /// <returns>
        /// The sharded on-disk path string for a cache entry file.
        /// </returns>
        [[nodiscard]] std::string MakePath(std::string_view filename) const;

        /// <summary>
        /// RAII guard that prevents the pinned entry from being evicted while I/O is
        /// in progress.  Constructed only by TryPin(); releases the pin on destruction.
        /// </summary>
        class ScopedPin
        {
        public:
            ScopedPin() = default;
            ~ScopedPin() noexcept { if (m_owner) m_owner->Unpin(m_filename); }

            ScopedPin(const ScopedPin&) = delete;
            ScopedPin& operator=(const ScopedPin&) = delete;

            ScopedPin(ScopedPin&& o) noexcept
                : m_owner(std::exchange(o.m_owner, nullptr))
                , m_filename(std::move(o.m_filename))
            {}
            ScopedPin& operator=(ScopedPin&& o) noexcept
            {
                if (this != &o)
                {
                    if (m_owner) m_owner->Unpin(m_filename);
                    m_owner    = std::exchange(o.m_owner, nullptr);
                    m_filename = std::move(o.m_filename);
                }
                return *this;
            }

        private:
            friend class LruFileIndex;
            ScopedPin(LruFileIndex* owner, std::string_view filename)
                : m_owner(owner)
            {
                m_filename.assign(filename.data(), filename.size());
            }
            LruFileIndex* m_owner{nullptr};
            boost::static_string<kMaxFilenameLen> m_filename{};
        };

        /// <summary>
        /// Pins an entry to prevent it being evicted while the caller performs I/O.
        /// Returns a ScopedPin RAII guard that releases the pin on destruction,
        /// or std::nullopt if the entry is not in the index.
        /// </summary>
        [[nodiscard]] std::optional<ScopedPin> TryPin(std::string_view filename);

        /// <summary>
        /// Admission control and eviction scheduling (WriteEntry phase 1).
        /// Returns std::nullopt when force_insert is false and the cache is over capacity.
        /// Otherwise returns eviction pairs the caller must commit after the call.
        /// </summary>
        [[nodiscard]] std::optional<EvictList> ReserveCapacity(
            std::string_view filename, size_t storedSize, bool force_insert);

        /// <summary>
        /// Registers the newly-written entry and corrects capacity overshoot
        /// from concurrent writers.
        /// </summary>
        /// 
        /// <returns>
        /// Eviction pairs the caller must commit after the call.
        /// </returns>
        [[nodiscard]] EvictList RegisterEntry(std::string_view filename, size_t storedSize);

        /// <summary>
        /// Splices the entry to most recently used or schedules it for erasure.
        /// </summary>
        /// 
        /// <returns>
        /// false when the entry disappeared.
        /// Sets <paramref name="erased"/> and fills <paramref name="advisedPair"/> on removal.
        /// </returns>
        [[nodiscard]] bool SpliceOrErase(std::string_view filename,
            bool advise_erase,
            std::pair<std::string, std::string>& advisedPair, bool& erased);

        /// <summary>
        /// Removes a single entry from the index.
        /// </summary>
        /// 
        /// <returns>
        /// {origPath, graveyardPath}; both strings are empty if the entry was not found.
        /// The caller must commit the pair after the call returns.
        /// </returns>
        [[nodiscard]] std::pair<std::string, std::string> Remove(
            std::string_view filename) noexcept;

        /// <summary>
        /// Sets the capacity limit and evicts LRU entries as needed.
        /// </summary>
        /// 
        /// <returns>
        /// Eviction pairs the caller must commit after the call.
        /// </returns>
        [[nodiscard]] EvictList SetCapacity(size_t capacity);

        /// <summary>
        /// Reduces capacity by <paramref name="decrease"/> bytes and evicts as needed.
        /// </summary>
        /// 
        /// <returns>
        /// Eviction pairs the caller must commit after the call.
        /// </returns>
        [[nodiscard]] EvictList Deflate(size_t decrease);

        /// <summary>
        /// Increases capacity by <paramref name="increase"/> bytes (saturating).
        /// </summary>
        void Inflate(size_t increase);

        /// <summary>
        /// Gets the capacity of the container.
        /// </summary>
        /// <returns>The current capacity.</returns>
        [[nodiscard]] size_t GetCapacity() const noexcept;

        /// <summary>
        /// Gets the current usage in bytes.
        /// </summary>
        /// <returns>The current usage value.</returns>
        [[nodiscard]] size_t GetUsage() const noexcept;

        /// <summary>
        /// Gets the count of evicted items.
        /// </summary>
        /// <returns>The number of items that have been evicted since this object's inception.</returns>
        [[nodiscard]] uint64_t GetEvictedCount() const noexcept;

    private:
        /// <summary>
        /// Single data record held by both the hash index and the sequenced LRU list.
        /// </summary>
        struct Entry
        {
            boost::static_string<kMaxFilenameLen> filename{};
            size_t size{0};
            uint32_t pinCount{0};
        };

        using LruList = std::list<Entry>;
        using LruIterator = LruList::iterator;

        /// <summary>
        /// Hashes and compares LruList iterators by their filename, enabling
        /// heterogeneous lookup from a plain std::string_view.
        /// </summary>
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

        /// <summary>
        /// Evicts LRU entries from the back of the list until m_currentSize &lt;= targetSize.
        /// Must be called with m_mutex held.
        /// </summary>
        void EvictUntilSizeLocked(size_t targetSize, EvictList& evictPairs);

        /// <summary>
        /// Removes a single entry from the in-memory index and returns {origPath, graveyardPath}.
        /// No filesystem operation is performed; the caller must commit the returned pair.
        /// Must be called with m_mutex held.
        /// </summary>
        [[nodiscard]] std::pair<std::string, std::string> RemoveEntryLocked(LruIterator it);

        std::string m_cacheDirStr;
        size_t m_capacity;
        size_t m_currentSize{0};

        /// <summary>
        /// Decrements the pin count of the named entry.  Called by ScopedPin on destruction.
        /// </summary>
        void Unpin(std::string_view filename) noexcept;

        // shared_mutex used as a two-level reader-writer lock:
        //   shared_lock  — concurrent readers: GetCapacity, GetUsage.
        //   unique_lock  — brief exclusive sections: all index mutations, TryPin, Unpin.
        mutable std::shared_mutex m_mutex;

        /// <summary>Sequenced LRU list: front = MRU, back = LRU.</summary>
        LruList m_lruList;

        /// <summary>
        /// Hash index: filename → iterator into m_lruList for O(1) lookup.
        /// </summary>
        Index m_index;

        /// <summary>
        /// Monotonically increasing counter for unique graveyard file names.
        /// </summary>
        std::atomic<uint64_t> m_seq{0};

        /// <summary>
        /// Total entries evicted due to capacity pressure since construction.
        /// </summary>
        std::atomic<uint64_t> m_evictedCount{0};
    };
}
