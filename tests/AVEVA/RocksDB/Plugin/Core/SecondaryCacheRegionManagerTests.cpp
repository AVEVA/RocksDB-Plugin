// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "SecondaryCache/BlockDevice.hpp"
#include "SecondaryCache/CacheStats.hpp"
#include "SecondaryCache/RecordFormat.hpp"
#include "SecondaryCache/RegionManager.hpp"
#include "SecondaryCache/ShardedIndex.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;

namespace {
uint64_t HashForTest(const std::string& value) {
    uint64_t hash = 14695981039346656037ull;
    for (const unsigned char ch : value) {
        hash ^= ch;
        hash *= 1099511628211ull;
    }
    return hash;
}

Secondary::Location InsertRecord(Secondary::RegionManager& manager, Secondary::ShardedIndex& index, const std::string& key,
                                 const std::string& payload, bool forceInsert = true) {
    const uint64_t hash = HashForTest(key);
    const size_t recordSize = Secondary::RecordSize(key.size(), payload.size());
    Secondary::RegionManager::Reservation reservation{};
    if (!manager.Reserve(hash, static_cast<uint32_t>(recordSize), forceInsert, reservation)) {
        return {};
    }

    std::vector<std::byte> encoded(recordSize);
    Secondary::EncodeRecord(
        encoded,
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(key.data()), key.size()),
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(payload.data()), payload.size()),
        rocksdb::CompressionType::kNoCompression, rocksdb::CacheTier::kVolatileTier, hash);
    if (!manager.Publish(reservation, encoded, std::nullopt).ok()) {
        return {};
    }

    Secondary::Location location{reservation.regionId, static_cast<uint32_t>(reservation.offset / Secondary::kRecordAlign),
                                 static_cast<uint32_t>(reservation.length / Secondary::kRecordAlign),
                                 reservation.generation, 0, 0};
    const auto ignored = index.Upsert(hash, location);
    static_cast<void>(ignored);
    return location;
}
} // namespace

TEST(SecondaryCacheRegionManagerTests, OpenSealReclaimCycleInvalidatesOldestRegion) {
    auto rawDevice = new Secondary::MemoryBlockDevice(2, (1 << 20) + (256 << 10));
    auto device = std::unique_ptr<Secondary::BlockDevice>(rawDevice);
    Secondary::ShardedIndex index(8);
    Secondary::CacheStats stats;
    Secondary::RegionManager manager(std::move(device), index, stats,
                                     Secondary::RegionManagerOptions{512 * 1024, (1 << 20) + (256 << 10), 64 * 1024,
                                                                     64 * 1024});

    const auto first = InsertRecord(manager, index, "k1", std::string(60 * 1024, 'a'));
    for (int i = 2; i <= 12; ++i) {
        InsertRecord(manager, index, "k" + std::to_string(i), std::string(60 * 1024, static_cast<char>('a' + i)));
    }

    std::vector<std::byte> record;
    const auto status = manager.Read(first, record);
    EXPECT_EQ(status, Secondary::RegionManager::ReadStatus::kRegionReclaimed);
}

TEST(SecondaryCacheRegionManagerTests, CorruptedFooterFallsBackToSafeScan) {
    auto rawDevice = new Secondary::MemoryBlockDevice(1, (1 << 20) + (256 << 10));
    auto device = std::unique_ptr<Secondary::BlockDevice>(rawDevice);
    Secondary::ShardedIndex index(4);
    Secondary::CacheStats stats;
    Secondary::RegionManager manager(std::move(device), index, stats,
                                     Secondary::RegionManagerOptions{256 * 1024, (1 << 20) + (256 << 10), 64 * 1024,
                                                                     64 * 1024});

    const auto location = InsertRecord(manager, index, "footer-key", std::string(60 * 1024, 'x'));
    ASSERT_TRUE(manager.SetCapacity(0).ok());
    auto& region = rawDevice->MutableRegion(location.regionId);
    if (!region.empty()) {
        region[region.size() - 1] ^= std::byte{0x1};
    }

    ASSERT_TRUE(manager.SetCapacity(256 * 1024).ok());
    EXPECT_TRUE(manager.SetCapacity(0).ok());
}
