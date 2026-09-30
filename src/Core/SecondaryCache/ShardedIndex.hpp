// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "RecordFormat.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
enum class LocationFlags : uint8_t {
    kNone = 0,
    kGhost = 1u << 0,
};

struct Location {
    uint32_t regionId{0};
    uint32_t offsetInAlignUnits{0};
    uint32_t lengthInAlignUnits{0};
    uint16_t generation{0};
    uint8_t hitCount{0};
    uint8_t flags{0};
};
static_assert(sizeof(Location) == 16);

[[nodiscard]] inline size_t LocationLengthBytes(const Location& location) noexcept {
    return static_cast<size_t>(location.lengthInAlignUnits) * kRecordAlign;
}

class ShardedIndex {
  public:
    explicit ShardedIndex(uint32_t shardCount = 64) : m_shards(std::max<uint32_t>(1, shardCount)) {}

    [[nodiscard]] bool FindAndTouch(const uint64_t keyHash, Location& out) noexcept {
        auto& shard = ShardFor(keyHash);
        std::lock_guard lock(shard.mu);
        const auto it = shard.map.find(keyHash);
        if (it == shard.map.end()) {
            return false;
        }
        if (it->second.hitCount < std::numeric_limits<uint8_t>::max()) {
            ++it->second.hitCount;
        }
        out = it->second;
        return true;
    }

    [[nodiscard]] std::optional<Location> Peek(const uint64_t keyHash) const noexcept {
        auto& shard = ShardFor(keyHash);
        std::lock_guard lock(shard.mu);
        const auto it = shard.map.find(keyHash);
        if (it == shard.map.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    [[nodiscard]] std::optional<Location> Upsert(const uint64_t keyHash, const Location& loc) noexcept {
        auto& shard = ShardFor(keyHash);
        std::lock_guard lock(shard.mu);
        auto [it, inserted] = shard.map.insert_or_assign(keyHash, loc);
        if (inserted) {
            m_liveEntries.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }
        return it->second;
    }

    [[nodiscard]] std::optional<Location> Remove(const uint64_t keyHash) noexcept {
        auto& shard = ShardFor(keyHash);
        std::lock_guard lock(shard.mu);
        const auto it = shard.map.find(keyHash);
        if (it == shard.map.end()) {
            return std::nullopt;
        }
        const Location removed = it->second;
        shard.map.erase(it);
        m_liveEntries.fetch_sub(1, std::memory_order_relaxed);
        return removed;
    }

    [[nodiscard]] std::optional<Location> RemoveIfInRegion(const uint64_t keyHash, const uint32_t regionId,
                                                           const uint16_t generation) noexcept {
        auto& shard = ShardFor(keyHash);
        std::lock_guard lock(shard.mu);
        const auto it = shard.map.find(keyHash);
        if (it == shard.map.end() || it->second.regionId != regionId || it->second.generation != generation) {
            return std::nullopt;
        }
        const Location removed = it->second;
        shard.map.erase(it);
        m_liveEntries.fetch_sub(1, std::memory_order_relaxed);
        return removed;
    }

    [[nodiscard]] size_t RemoveAllInRegion(const uint32_t regionId, const uint16_t generation) noexcept {
        size_t removed = 0;
        for (auto& shard : m_shards) {
            std::lock_guard lock(shard.mu);
            for (auto it = shard.map.begin(); it != shard.map.end();) {
                if (it->second.regionId == regionId && it->second.generation == generation) {
                    it = shard.map.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
        }
        m_liveEntries.fetch_sub(removed, std::memory_order_relaxed);
        return removed;
    }

    [[nodiscard]] size_t LiveEntries() const noexcept { return m_liveEntries.load(std::memory_order_relaxed); }

    [[nodiscard]] size_t ApproximateMemoryUsage() const noexcept {
        return LiveEntries() * (sizeof(uint64_t) + sizeof(Location) + sizeof(void*) * 2);
    }

  private:
    struct Shard {
        mutable std::mutex mu;
        std::unordered_map<uint64_t, Location> map;
    };

    [[nodiscard]] static uint64_t Mix(const uint64_t value) noexcept {
        uint64_t x = value + 0x9e3779b97f4a7c15ull;
        x = (x ^ (x >> 30u)) * 0xbf58476d1ce4e5b9ull;
        x = (x ^ (x >> 27u)) * 0x94d049bb133111ebull;
        return x ^ (x >> 31u);
    }

    [[nodiscard]] Shard& ShardFor(const uint64_t keyHash) noexcept {
        return m_shards[Mix(keyHash) % m_shards.size()];
    }

    [[nodiscard]] const Shard& ShardFor(const uint64_t keyHash) const noexcept {
        return m_shards[Mix(keyHash) % m_shards.size()];
    }

    std::vector<Shard> m_shards;
    std::atomic<size_t> m_liveEntries{0};
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
