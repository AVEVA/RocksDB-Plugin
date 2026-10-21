// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "MemoryBudget.hpp"
#include "RecordFormat.hpp"

#include <cstdint>
#include <optional>
#include <unordered_map>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
struct Location {
    uint32_t regionId{0};
    uint32_t offsetInAlignUnits{0};
    uint32_t lengthInAlignUnits{0};
    uint32_t generation{0};
    bool operator==(const Location&) const = default;
};
static_assert(sizeof(Location) == 16);

inline size_t LocationLengthBytes(const Location& location) noexcept {
    return size_t{location.lengthInAlignUnits} * kRecordAlign;
}

// All operations require RegionManager's state mutex; no independent lock domain.
class Index {
    using Map = std::unordered_map<uint64_t, Location, std::hash<uint64_t>, std::equal_to<uint64_t>,
                                   BudgetAllocator<std::pair<const uint64_t, Location>>>;
    MemoryBudget& m_budget;
    std::optional<Map> m_map;

  public:
    explicit Index(MemoryBudget& budget) : m_budget(budget) {}
    std::optional<Location> Find(const uint64_t hash) const {
        if (!m_map) {
            return std::nullopt;
        }
        const auto it = m_map->find(hash);
        return it == m_map->end() ? std::nullopt : std::optional<Location>(it->second);
    }
    std::optional<Location> Upsert(const uint64_t hash, const Location location) {
        const auto previous = Find(hash);
        if (!m_map) {
            m_map.emplace(BudgetAllocator<Map::value_type>(m_budget));
        }
        m_map->insert_or_assign(hash, location);
        return previous;
    }
    std::optional<Location> Remove(const uint64_t hash) {
        const auto previous = Find(hash);
        if (previous) {
            m_map->erase(hash);
        }
        return previous;
    }
    size_t Size() const noexcept { return m_map ? m_map->size() : 0; }
    void Clear() noexcept { m_map.reset(); }
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
