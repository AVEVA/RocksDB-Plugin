// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "BlockDevice.hpp"
#include "CacheStats.hpp"
#include "ShardedIndex.hpp"

#include <rocksdb/status.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
struct RegionManagerOptions {
    size_t capacityBytes{0};
    size_t regionSizeBytes{64ULL << 20};
    size_t flushBlockSizeBytes{1ULL << 20};
    size_t maxEntrySizeBytes{16ULL << 20};
};

class RegionManager {
  public:
    struct Reservation {
        uint32_t regionId{0};
        uint16_t generation{0};
        uint64_t offset{0};
        uint32_t length{0};
        uint64_t keyHash{0};
    };

    enum class ReadStatus : uint8_t {
        kOk,
        kRegionReclaimed,
        kIoError,
    };

    RegionManager(std::unique_ptr<BlockDevice> device, ShardedIndex& index, CacheStats& stats, RegionManagerOptions options);

    [[nodiscard]] bool Reserve(uint64_t keyHash, uint32_t recordSize, bool forceInsert, Reservation& out) noexcept;
    rocksdb::Status Publish(const Reservation& reservation, std::span<const std::byte> record,
                            std::optional<Location> replaced) noexcept;

    [[nodiscard]] ReadStatus Read(const Location& location, std::vector<std::byte>& recordOut) noexcept;

    rocksdb::Status SetCapacity(size_t capacityBytes) noexcept;
    [[nodiscard]] size_t CapacityBytes() const noexcept;
    [[nodiscard]] size_t UsageBytes() const noexcept;
    void AdjustLiveBytes(int64_t delta) noexcept;
    void ClearAll() noexcept;

  private:
    struct FooterEntry {
        uint64_t keyHash{0};
        uint32_t offset{0};
        uint32_t length{0};
    };
    static_assert(sizeof(FooterEntry) == 16);

    struct Region {
        enum class State : uint8_t { kOpen, kSealed, kReclaiming };

        mutable std::mutex mu;
        std::atomic<uint32_t> activeReaders{0};
        State state{State::kSealed};
        uint16_t generation{0};
        uint64_t writeCursor{0};
        uint64_t bufferedOffset{0};
        std::vector<std::byte> writeBuffer;
        bool footerOverflow{false};
        std::vector<FooterEntry> footerEntries;
    };

    [[nodiscard]] uint32_t RegionCountFor(size_t capacityBytes) const noexcept;
    [[nodiscard]] uint64_t RegionBudgetBytes(uint32_t regionId) const noexcept;
    [[nodiscard]] bool EnsureWritableRegion(uint32_t recordSize, bool forceInsert) noexcept;
    bool SealRegion(uint32_t regionId) noexcept;
    bool ReclaimRegion(uint32_t regionId) noexcept;
    void FlushOpenRegionLocked(Region& region, uint32_t regionId) noexcept;
    [[nodiscard]] std::vector<FooterEntry> ScanRegionEntries(uint32_t regionId, uint64_t budgetBytes) noexcept;
    [[nodiscard]] bool TryLoadFooter(uint32_t regionId, uint16_t generation, uint64_t budgetBytes,
                                     std::vector<FooterEntry>& entriesOut, bool& overflowOut) noexcept;

    std::unique_ptr<BlockDevice> m_device;
    ShardedIndex& m_index;
    CacheStats& m_stats;
    RegionManagerOptions m_options;
    size_t m_footerSizeBytes;
    uint64_t m_usableRegionBytes;
    std::vector<Region> m_regions;
    std::deque<uint32_t> m_sealedOrder;
    uint32_t m_currentOpenRegion{0};
    uint32_t m_nextUnusedRegion{1};
    std::atomic<size_t> m_usageBytes{0};
    mutable std::mutex m_managerMutex;
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
