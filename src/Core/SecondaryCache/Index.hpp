// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "MemoryBudget.hpp"
#include "RecordFormat.hpp"

#include <cstdint>
#include <optional>
#include <unordered_map>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
/// <summary>
/// Where one encoded record lives: four 32-bit fields, totaling 16 bytes. Offsets and lengths use kRecordAlign
/// units; the location may refer to buffered or device-backed bytes.
/// </summary>
struct Location {
    /// Index into RegionManager's region ring.
    uint32_t regionId{0};
    /// Record start, in kRecordAlign units from the start of the region.
    uint32_t offsetInAlignUnits{0};
    /// Encoded record length, in kRecordAlign units.
    uint32_t lengthInAlignUnits{0};
    /// Region generation at write time; distinguishes copies written at the same offset in different fills.
    uint32_t generation{0};
    bool operator==(const Location&) const = default;
};
static_assert(sizeof(Location) == 16);

/// <param name="location">Location to measure.</param>
/// <returns>The encoded record length in bytes.</returns>
inline size_t LocationLengthBytes(const Location& location) noexcept {
    return size_t{location.lengthInAlignUnits} * kRecordAlign;
}

/// <summary>
/// Maps a 64-bit fingerprint to its newest Location (at most one per hash, even for colliding keys). Requires
/// external synchronization, supplied by RegionManager. The map is created lazily and charged to the metadata
/// budget. Remove can leave an empty map with retained buckets; Clear destroys it and releases its allocations.
/// </summary>
class Index {
    using Map = std::unordered_map<uint64_t, Location, std::hash<uint64_t>, std::equal_to<uint64_t>,
                                   BudgetAllocator<std::pair<const uint64_t, Location>>>;
    /// Metadata budget charged for nodes and buckets.
    MemoryBudget& m_budget;
    /// Empty until the first Upsert; reset by Clear to release its buckets.
    std::optional<Map> m_map;

  public:
    /// <param name="budget">Metadata budget; must outlive the index.</param>
    explicit Index(MemoryBudget& budget) : m_budget(budget) {}

    /// <param name="hash">Key fingerprint.</param>
    /// <returns>The mapped location, or nullopt.</returns>
    std::optional<Location> Find(const uint64_t hash) const {
        if (!m_map) {
            return std::nullopt;
        }
        const auto it = m_map->find(hash);
        return it == m_map->end() ? std::nullopt : std::optional<Location>(it->second);
    }

    /// <summary>Inserts or replaces the mapping for hash.</summary>
    /// <param name="hash">Key fingerprint.</param>
    /// <param name="location">New location.</param>
    /// <returns>The replaced location, or nullopt if hash was not mapped.</returns>
    /// <exception cref="BudgetExceeded">Growth exceeds the budget; mappings are unchanged.</exception>
    std::optional<Location> Upsert(const uint64_t hash, const Location location) {
        const auto previous = Find(hash);
        if (!m_map) {
            m_map.emplace(BudgetAllocator<Map::value_type>(m_budget));
        }
        m_map->insert_or_assign(hash, location);
        return previous;
    }

    /// <summary>Removes the mapping for hash, if any.</summary>
    /// <param name="hash">Key fingerprint.</param>
    /// <returns>The removed location, or nullopt if hash was not mapped.</returns>
    std::optional<Location> Remove(const uint64_t hash) {
        const auto previous = Find(hash);
        if (previous) {
            m_map->erase(hash);
        }
        return previous;
    }

    /// <returns>Number of mapped hashes.</returns>
    size_t Size() const noexcept { return m_map ? m_map->size() : 0; }

    /// <summary>Drops all mappings and releases the map's memory.</summary>
    void Clear() noexcept { m_map.reset(); }
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
