// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "SecondaryCache/BlockDevice.hpp"
#include "SecondaryCache/CacheStats.hpp"
#include "SecondaryCache/RecordFormat.hpp"
#include "SecondaryCache/RegionManager.hpp"
#include "SecondaryCache/ShardedIndex.hpp"

#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <vector>

namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;

namespace {
uint64_t HashForCorruptionTest(const std::string& value) {
    uint64_t hash = 14695981039346656037ull;
    for (const unsigned char ch : value) {
        hash ^= ch;
        hash *= 1099511628211ull;
    }
    return hash;
}

struct FixtureState {
    Secondary::MemoryBlockDevice* device{};
    std::unique_ptr<Secondary::BlockDevice> ownedDevice;
    Secondary::ShardedIndex index{8};
    Secondary::CacheStats stats{};
    std::unique_ptr<Secondary::RegionManager> manager;
    Secondary::Location location{};
    uint64_t keyHash{};
    std::string key{"corruption-key"};
};

std::unique_ptr<FixtureState> MakeFixture() {
    auto state = std::make_unique<FixtureState>();
    state->device = new Secondary::MemoryBlockDevice(1, (1 << 20) + (256 << 10));
    state->ownedDevice.reset(state->device);
    state->manager = std::make_unique<Secondary::RegionManager>(
        std::move(state->ownedDevice), state->index, state->stats,
        Secondary::RegionManagerOptions{256 * 1024, (1 << 20) + (256 << 10), 4 * 1024, 64 * 1024});

    state->keyHash = HashForCorruptionTest(state->key);
    const std::string payload(8 * 1024, 'z');
    const size_t recordSize = Secondary::RecordSize(state->key.size(), payload.size());
    Secondary::RegionManager::Reservation reservation{};
    EXPECT_TRUE(state->manager->Reserve(state->keyHash, static_cast<uint32_t>(recordSize), true, reservation));

    std::vector<std::byte> encoded(recordSize);
    Secondary::EncodeRecord(
        std::span<std::byte>(encoded.data(), encoded.size()),
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(state->key.data()), state->key.size()),
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(payload.data()), payload.size()),
        rocksdb::CompressionType::kNoCompression, rocksdb::CacheTier::kVolatileTier, state->keyHash);
    EXPECT_TRUE(state->manager->Publish(reservation, std::span<const std::byte>(encoded.data(), encoded.size()),
                                        std::nullopt)
                    .ok());

    state->location = Secondary::Location{reservation.regionId,
                                          static_cast<uint32_t>(reservation.offset / Secondary::kRecordAlign),
                                          static_cast<uint32_t>(reservation.length / Secondary::kRecordAlign),
                                          reservation.generation, 0, 0};
    const auto ignored = state->index.Upsert(state->keyHash, state->location);
    static_cast<void>(ignored);
    return state;
}

void ExpectDecodeFailureAndCleanup(FixtureState& fixture, const Secondary::DecodeResult expected,
                                   const bool removeIndex, const std::string_view lookupKey = {}) {
    std::vector<std::byte> bytes;
    ASSERT_EQ(fixture.manager->Read(fixture.location, bytes), Secondary::RegionManager::ReadStatus::kOk);
    Secondary::DecodedRecordView decoded{};
    const auto keyView =
        lookupKey.empty() ? std::string_view{fixture.key} : lookupKey;
    const auto result = Secondary::DecodeRecord(
        bytes, std::span<const std::byte>(reinterpret_cast<const std::byte*>(keyView.data()), keyView.size()),
        decoded);
    EXPECT_EQ(result, expected);
    if (removeIndex) {
        EXPECT_TRUE(fixture.index.Remove(fixture.keyHash).has_value());
    } else {
        Secondary::Location found{};
        EXPECT_TRUE(fixture.index.FindAndTouch(fixture.keyHash, found));
    }
}
} // namespace

TEST(SecondaryCacheRegionCorruptionTests, HeaderAndPayloadCorruptionAreRejected) {
    {
        auto fixture = MakeFixture();
        auto& region = fixture->device->MutableRegion(0);
        region[0] = std::byte{0};
        ExpectDecodeFailureAndCleanup(*fixture, Secondary::DecodeResult::kBadMagic, true);
    }
    {
        auto fixture = MakeFixture();
        auto& region = fixture->device->MutableRegion(0);
        region[4] = std::byte{9};
        ExpectDecodeFailureAndCleanup(*fixture, Secondary::DecodeResult::kBadVersion, true);
    }
    {
        auto fixture = MakeFixture();
        auto* header = reinterpret_cast<Secondary::RecordHeader*>(fixture->device->MutableRegion(0).data());
        header->keyLen = 0xFFFFFFFFu;
        ExpectDecodeFailureAndCleanup(*fixture, Secondary::DecodeResult::kBadLength, true);
    }
    {
        auto fixture = MakeFixture();
        auto* header = reinterpret_cast<Secondary::RecordHeader*>(fixture->device->MutableRegion(0).data());
        header->payloadLen = 0xFFFFFFFFu;
        ExpectDecodeFailureAndCleanup(*fixture, Secondary::DecodeResult::kBadLength, true);
    }
    {
        auto fixture = MakeFixture();
        auto& region = fixture->device->MutableRegion(0);
        region[sizeof(Secondary::RecordHeader)] ^= std::byte{0x1};
        ExpectDecodeFailureAndCleanup(*fixture, Secondary::DecodeResult::kBadHeaderCrc, true);
    }
    {
        auto fixture = MakeFixture();
        auto& region = fixture->device->MutableRegion(0);
        region[sizeof(Secondary::RecordHeader) + fixture->key.size()] ^= std::byte{0x1};
        ExpectDecodeFailureAndCleanup(*fixture, Secondary::DecodeResult::kBadPayloadCrc, true);
    }
    {
        auto fixture = MakeFixture();
        ExpectDecodeFailureAndCleanup(*fixture, Secondary::DecodeResult::kKeyMismatch, false, "wrong-key");
    }
}
