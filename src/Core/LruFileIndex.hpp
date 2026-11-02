// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <boost/static_string.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core {
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
class LruFileIndex {
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

    /// <summary>Whether an insert may be refused when the cache is over capacity.</summary>
    enum class InsertMode : std::uint8_t {
        IfRoom,
        Force,
    };

    /// <summary>Outcome of promoting an entry to most-recently-used.</summary>
    enum class TouchResult : std::uint8_t {
        Touched,
        NotFound,
    };

    explicit LruFileIndex(std::string cacheDirStr, ByteCount capacity);
    LruFileIndex(const LruFileIndex&) = delete;
    LruFileIndex& operator=(const LruFileIndex&) = delete;
    LruFileIndex(LruFileIndex&&) = delete;
    LruFileIndex& operator=(LruFileIndex&&) = delete;

    /// <returns>
    /// The on-disk path string for a cache entry file.
    /// </returns>
    [[nodiscard]] std::string MakePath(std::string_view filename) const;

    /// <summary>
    /// RAII guard that prevents the pinned entry from being evicted while I/O is
    /// in progress.  Constructed only by TryPin(); releases the pin on destruction.
    /// </summary>
    class ScopedPin {
      public:
        ScopedPin() = default;
        ~ScopedPin() noexcept {
            if (m_owner)
                m_owner->Unpin(m_filename, m_id);
        }

        ScopedPin(const ScopedPin&) = delete;
        ScopedPin& operator=(const ScopedPin&) = delete;

        ScopedPin(ScopedPin&& o) noexcept
            : m_owner(std::exchange(o.m_owner, nullptr)), m_id(o.m_id), m_filename(std::move(o.m_filename)) {}
        ScopedPin& operator=(ScopedPin&& o) noexcept {
            if (this != &o) {
                if (m_owner)
                    m_owner->Unpin(m_filename, m_id);
                m_owner = std::exchange(o.m_owner, nullptr);
                m_id = o.m_id;
                m_filename = std::move(o.m_filename);
            }
            return *this;
        }

      private:
        friend class LruFileIndex;
        ScopedPin(LruFileIndex* owner, std::string_view filename, Generation id) : m_owner(owner), m_id(id) {
            m_filename.assign(filename.data(), filename.size());
        }
        LruFileIndex* m_owner{nullptr};
        // Identifies the entry that was pinned, so a pin on a replaced entry never touches its successor.
        Generation m_id;
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
    /// Returns std::nullopt when mode is IfRoom and the cache is over capacity.
    /// Otherwise returns eviction pairs the caller must commit after the call.
    /// </summary>
    [[nodiscard]] std::optional<EvictList> ReserveCapacity(std::string_view filename, ByteCount storedSize,
                                                           InsertMode mode);

    /// <summary>
    /// Registers the newly-written entry and corrects capacity overshoot
    /// from concurrent writers.
    /// </summary>
    ///
    /// <returns>
    /// Eviction pairs the caller must commit after the call.
    /// </returns>
    [[nodiscard]] EvictList RegisterEntry(std::string_view filename, ByteCount storedSize);

    /// <summary>
    /// Promotes the entry to most-recently-used.
    /// </summary>
    /// <returns>NotFound when the entry is not in the index.</returns>
    [[nodiscard]] TouchResult Touch(std::string_view filename);

    /// <summary>
    /// Removes a single entry from the index.
    /// </summary>
    ///
    /// <returns>
    /// {origPath, graveyardPath}, or std::nullopt if the entry was not found.
    /// The caller must commit the pair after the call returns.
    /// </returns>
    [[nodiscard]] std::optional<std::pair<std::string, std::string>> Remove(std::string_view filename) noexcept;

    /// <summary>
    /// Sets the capacity limit and evicts LRU entries as needed.
    /// </summary>
    ///
    /// <returns>
    /// Eviction pairs the caller must commit after the call.
    /// </returns>
    [[nodiscard]] EvictList SetCapacity(ByteCount capacity);

    /// <summary>
    /// Reduces capacity by <paramref name="decrease"/> bytes and evicts as needed.
    /// </summary>
    ///
    /// <returns>
    /// Eviction pairs the caller must commit after the call.
    /// </returns>
    [[nodiscard]] EvictList Deflate(ByteCount decrease);

    /// <summary>
    /// Increases capacity by <paramref name="increase"/> bytes (saturating).
    /// </summary>
    void Inflate(ByteCount increase);

    /// <summary>
    /// Gets the capacity of the container.
    /// </summary>
    /// <returns>The current capacity.</returns>
    [[nodiscard]] ByteCount GetCapacity() const noexcept;

    /// <summary>
    /// Gets the current usage in bytes.
    /// </summary>
    /// <returns>The current usage value.</returns>
    [[nodiscard]] ByteCount GetUsage() const noexcept;

  private:
    /// <summary>
    /// Single data record held by both the hash index and the sequenced LRU list.
    /// </summary>
    struct Entry {
        boost::static_string<kMaxFilenameLen> filename{};
        ByteCount size;
        uint32_t pinCount{0};
        Generation id;
    };

    using LruList = std::list<Entry>;
    using LruIterator = LruList::iterator;

    /// <summary>
    /// Hashes and compares LruList iterators by their filename, enabling
    /// heterogeneous lookup from a plain std::string_view.
    /// </summary>
    struct IteratorHash {
        using is_transparent = void;
        size_t operator()(LruIterator it) const noexcept { return StringHash{}(it->filename); }
        size_t operator()(std::string_view sv) const noexcept { return StringHash{}(sv); }
    };
    struct IteratorEqual {
        using is_transparent = void;
        bool operator()(LruIterator a, LruIterator b) const noexcept { return a->filename == b->filename; }
        bool operator()(std::string_view sv, LruIterator it) const noexcept { return sv == it->filename; }
        bool operator()(LruIterator it, std::string_view sv) const noexcept { return it->filename == sv; }
    };

    using Index = boost::unordered::unordered_flat_set<LruIterator, IteratorHash, IteratorEqual>;

    /// <summary>
    /// Evicts LRU entries from the back of the list until m_currentSize &lt;= targetSize.
    /// Must be called with m_mutex held.
    /// </summary>
    void EvictUntilSizeLocked(ByteCount targetSize, EvictList& evictPairs);

    /// <summary>
    /// Removes a single entry from the in-memory index and returns {origPath, graveyardPath}.
    /// No filesystem operation is performed; the caller must commit the returned pair.
    /// Must be called with m_mutex held.
    /// </summary>
    [[nodiscard]] std::pair<std::string, std::string> RemoveEntryLocked(LruIterator it);

    /// <summary>Builds {origPath, graveyardPath} for an entry without mutating the index.</summary>
    [[nodiscard]] std::pair<std::string, std::string> BuildEvictionPair(const Entry& entry) const;

    /// <summary>Drops an entry from the index, list and byte count.  Cannot throw; m_mutex must be held.</summary>
    void EraseEntryLocked(LruIterator it) noexcept;

    std::string m_cacheDirStr;
    ByteCount m_capacity;
    ByteCount m_currentSize;
    Generation m_nextEntryId{1};

    /// <summary>
    /// Decrements the pin count of the named entry.  Called by ScopedPin on destruction.
    /// </summary>
    void Unpin(std::string_view filename, Generation id) noexcept;

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
};
} // namespace AVEVA::RocksDB::Plugin::Core
