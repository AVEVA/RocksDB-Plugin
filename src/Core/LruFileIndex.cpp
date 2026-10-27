// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "LruFileIndex.hpp"

#include <boost/container/small_vector.hpp>
#include <boost/scope/scope_exit.hpp>

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <limits>
#include <string>

namespace AVEVA::RocksDB::Plugin::Core {
LruFileIndex::LruFileIndex(std::string cacheDirStr, size_t capacity)
    : m_cacheDirStr(std::move(cacheDirStr)), m_capacity(capacity) {}

std::string LruFileIndex::MakePath(std::string_view filename) const {
    constexpr char kSep = static_cast<char>(std::filesystem::path::preferred_separator);
    return m_cacheDirStr + kSep + std::string(filename.data(), filename.size());
}

std::optional<LruFileIndex::ScopedPin> LruFileIndex::TryPin(std::string_view filename) {
    std::lock_guard lock(m_mutex);
    const auto it = m_index.find(filename);
    if (it == m_index.end())
        return std::nullopt;
    ++(*it)->pinCount;
    return ScopedPin(this, filename, (*it)->id);
}

void LruFileIndex::Unpin(std::string_view filename, const uint64_t id) noexcept {
    try {
        std::lock_guard lock(m_mutex);
        const auto it = m_index.find(filename);
        if (it != m_index.end() && (*it)->id == id && (*it)->pinCount > 0)
            --(*it)->pinCount;
    } catch (...) {
    }
}

std::optional<LruFileIndex::EvictList> LruFileIndex::ReserveCapacity(const std::string_view filename,
                                                                     const size_t storedSize, const bool forceInsert) {
    EvictList evictPairs;
    std::lock_guard lock(m_mutex);

    auto indexIt = m_index.find(filename);
    size_t existingSize;
    if (indexIt != m_index.end()) {
        existingSize = (*indexIt)->size;
    } else {
        existingSize = 0;
    }

    // When not forced, skip the write rather than evicting a potentially more
    // valuable entry.  Net-change accounting means a same-key update is skipped
    // only when the new entry is strictly larger than the existing one and the
    // delta would exceed capacity; equal-or-smaller replacements always proceed.
    if (!forceInsert && m_currentSize - existingSize + storedSize > m_capacity) {
        return std::nullopt;
    }

    // Move the existing entry to MRU and pin it for the eviction below: its size is discounted from
    // m_currentSize, so evicting it would subtract that size a second time.
    if (indexIt != m_index.end()) {
        m_lruList.splice(m_lruList.begin(), m_lruList, *indexIt);
        ++(*indexIt)->pinCount;
    }

    // Temporarily discount the existing entry so eviction is sized against the
    // net new bytes required.  The guard restores the discount and the pin on every
    // exit (including a throw from eviction) while the mutex is still held; the
    // existing entry is never evicted meanwhile, so its iterator stays valid.
    m_currentSize -= existingSize;
    const bool pinned = indexIt != m_index.end();
    const LruIterator pinnedEntry = pinned ? *indexIt : LruIterator{};
    const boost::scope::scope_exit restore{[this, pinned, pinnedEntry, existingSize] {
        if (pinned)
            --pinnedEntry->pinCount;
        m_currentSize += existingSize;
    }};

    // If a single entry is larger than the entire capacity it can never fit,
    // even after evicting everything.
    if (storedSize > m_capacity) {
        return std::nullopt;
    }

    if (m_currentSize + storedSize > m_capacity) {
        EvictUntilSizeLocked(m_capacity - storedSize, evictPairs);
    }

    return evictPairs;
}

LruFileIndex::EvictList LruFileIndex::RegisterEntry(std::string_view filename, size_t storedSize) {
    EvictList evictPairs;
    std::lock_guard lock(m_mutex);

    // Stage every fallible step (list node, index growth) before touching the existing entry or the byte count, so a
    // throw leaves the index exactly as it was.  After the reserve, the final insert cannot allocate.
    Entry newEntry{};
    newEntry.filename.assign(filename.data(), filename.size());
    newEntry.size = storedSize;
    newEntry.id = m_nextEntryId;
    m_lruList.push_front(std::move(newEntry));
    const auto newIt = m_lruList.begin();
    try {
        m_index.reserve(m_index.size() + 1);
    } catch (...) {
        m_lruList.erase(newIt);
        throw;
    }

    // Remove any existing entry for this key — either the one preserved by
    // ReserveCapacity or a newer one written by a concurrent thread between the
    // disk write and now.  The file on disk was atomically replaced, so no
    // separate file deletion is needed for the displaced entry.
    const auto existingIt = m_index.find(filename);
    if (existingIt != m_index.end()) {
        const auto displaced = *existingIt;
        m_currentSize -= displaced->size;
        m_index.erase(existingIt);
        m_lruList.erase(displaced);
    }

    ++m_nextEntryId;
    m_index.insert(newIt);
    m_currentSize += storedSize;
    // Correct any capacity overshoot from concurrent writes for different keys
    // that each independently passed ReserveCapacity's admission check.
    EvictUntilSizeLocked(m_capacity, evictPairs);

    return evictPairs;
}

bool LruFileIndex::Touch(std::string_view filename) {
    std::lock_guard lock(m_mutex);
    const auto it = m_index.find(filename);
    if (it == m_index.end()) {
        return false;
    }

    m_lruList.splice(m_lruList.begin(), m_lruList, *it);
    return true;
}

std::pair<std::string, std::string> LruFileIndex::Remove(std::string_view filename) noexcept {
    try {
        std::lock_guard lock(m_mutex);
        auto it = m_index.find(filename);
        if (it != m_index.end())
            return RemoveEntryLocked(*it);
    } catch (...) {
    }
    return {};
}

LruFileIndex::EvictList LruFileIndex::SetCapacity(size_t capacity) {
    EvictList evictPairs;
    std::lock_guard lock(m_mutex);
    m_capacity = capacity;
    EvictUntilSizeLocked(m_capacity, evictPairs);
    return evictPairs;
}

LruFileIndex::EvictList LruFileIndex::Deflate(size_t decrease) {
    EvictList evictPairs;
    std::lock_guard lock(m_mutex);
    m_capacity -= std::min(decrease, m_capacity);
    EvictUntilSizeLocked(m_capacity, evictPairs);
    return evictPairs;
}

void LruFileIndex::Inflate(size_t increase) {
    std::lock_guard lock(m_mutex);
    // Saturating add: clamp to size_t max rather than wrapping silently.
    const size_t remaining = std::numeric_limits<size_t>::max() - m_capacity;
    m_capacity += (increase > remaining) ? remaining : increase;
}

size_t LruFileIndex::GetCapacity() const noexcept {
    std::shared_lock lock(m_mutex);
    return m_capacity;
}

size_t LruFileIndex::GetUsage() const noexcept {
    std::shared_lock lock(m_mutex);
    return m_currentSize;
}

void LruFileIndex::EvictUntilSizeLocked(const size_t targetSize, EvictList& evictPairs) {
    if (m_currentSize <= targetSize)
        return;

    // Phase 1 selects victims and builds all their paths without mutating the index, so a throw (e.g. bad_alloc while
    // building the path of a later victim) cannot strand earlier victims: they are still tracked, not half-removed.
    const size_t pairsBefore = evictPairs.size();
    // The result vector grows geometrically instead of reserving room for every entry, so evicting one victim does not
    // allocate O(index size); growth is safe because nothing has been mutated yet.
    boost::container::small_vector<LruIterator, 4> victims;
    try {
        size_t projectedSize = m_currentSize;
        // The cursor only moves towards the front, so entries found pinned are skipped once per pass instead of once
        // per eviction.
        auto cursor = m_lruList.end();
        while (projectedSize > targetSize) {
            const std::reverse_iterator<LruIterator> scanEnd{m_lruList.begin()};
            const auto found = std::find_if(std::reverse_iterator<LruIterator>{cursor}, scanEnd,
                                            [](const Entry& entry) { return entry.pinCount == 0; });
            if (found == scanEnd)
                break; // all remaining entries are pinned; cannot evict further
            const LruIterator candidate = std::prev(found.base());
            cursor = candidate; // nothing is erased yet, so resume scanning before this victim
            victims.push_back(candidate);
            evictPairs.push_back(BuildEvictionPair(*candidate));
            projectedSize -= candidate->size;
        }
    } catch (...) {
        evictPairs.resize(pairsBefore);
        throw;
    }

    // Phase 2 cannot throw.
    for (const auto victim : victims)
        EraseEntryLocked(victim);
}

std::pair<std::string, std::string> LruFileIndex::RemoveEntryLocked(LruIterator it) {
    auto pair = BuildEvictionPair(*it);
    EraseEntryLocked(it);
    return pair;
}

std::pair<std::string, std::string> LruFileIndex::BuildEvictionPair(const Entry& entry) const {
    // Build the orig and graveyard path strings while under the lock.
    // No filesystem operation is performed here; the caller must commit the returned
    // pair after releasing the lock.  Deferring the rename outside the lock allows
    // bulk evictions to release the mutex sooner, at the cost of a narrow TOCTOU
    // window where a concurrent insert for the same key could have its file moved to
    // the graveyard (causing one cache miss; the phantom index entry is then cleaned
    // up by Lookup).
    constexpr char kSep = static_cast<char>(std::filesystem::path::preferred_separator);
    std::string origPathStr;
    origPathStr.reserve(m_cacheDirStr.size() + 1 + entry.filename.size());
    origPathStr.append(m_cacheDirStr).push_back(kSep);
    origPathStr.append(entry.filename.data(), entry.filename.size());
    std::string graveyardPathStr;
    graveyardPathStr.reserve(origPathStr.size() + 4);
    graveyardPathStr.append(origPathStr).append(".del");
    return {std::move(origPathStr), std::move(graveyardPathStr)};
}

void LruFileIndex::EraseEntryLocked(LruIterator it) noexcept {
    m_currentSize -= it->size;
    m_index.erase(it);
    m_lruList.erase(it);
}
} // namespace AVEVA::RocksDB::Plugin::Core
