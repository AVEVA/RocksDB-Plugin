// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "BlockDevice.hpp"
#include "CacheStats.hpp"
#include "Index.hpp"

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"

#include <functional>
#include <mutex>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
class RegionManager {
  public:
    using DeviceFactory = std::function<std::unique_ptr<BlockDevice>(MemoryBudget&)>;
    struct Identity {
        Location location;
        uint64_t incarnation{0};
    };
    enum class ReadStatus { kOk, kMiss, kIoError };

    RegionManager(DeviceFactory factory, FileBasedSecondaryCacheOptions options, CacheStats& stats);
    static void Validate(const FileBasedSecondaryCacheOptions& options);
    rocksdb::Status Append(uint64_t hash, std::span<const std::byte> record, size_t payloadSize);
    ReadStatus Read(uint64_t hash, RecordBytes& record, Identity& identity);
    bool Remove(uint64_t hash, const Identity& identity);
    bool Contains(uint64_t hash, const Identity& identity) const;
    void Erase(uint64_t hash);
    rocksdb::Status SetCapacity(size_t capacity);
    size_t CapacityBytes() const;
    size_t UsageBytes() const;
    FileBasedSecondaryCacheStats Snapshot() const;

  private:
    struct Region {
        explicit Region(MemoryBudget& budget) : hashes(BudgetAllocator<uint64_t>(budget)) {}
        uint32_t generation{1};
        size_t cursor{0};
        std::vector<uint64_t, BudgetAllocator<uint64_t>> hashes;
    };
    void ConfigureLocked(size_t capacity);
    void InvalidateLocked();
    void ReclaimLocked(uint32_t region);
    bool FlushLocked();
    bool SelectRegionLocked(size_t length);
    size_t RegionBudget(uint32_t region) const noexcept;
    void RemoveLocationLocked(uint64_t hash);

    // The only state lock: index, layout, reverse lists, buffer, device, usage.
    mutable std::mutex m_mutex;
    FileBasedSecondaryCacheOptions m_options;
    CacheStats& m_stats;
    MemoryBudget m_metadata;
    Index m_index;
    std::vector<Region, BudgetAllocator<Region>> m_regions;
    std::unique_ptr<BlockDevice> m_device;
    std::vector<std::byte> m_buffer;
    uint32_t m_current{UINT32_MAX};
    uint64_t m_incarnation{0};
    size_t m_capacity{0};
    size_t m_usage{0};
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
