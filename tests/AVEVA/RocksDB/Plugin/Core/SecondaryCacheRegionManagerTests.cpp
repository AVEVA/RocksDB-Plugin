// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "SecondaryCache/RegionManager.hpp"
#include <gtest/gtest.h>

#include <barrier>
#include <thread>

namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;
using namespace AVEVA::RocksDB::Plugin::Core;

namespace {
class SecondaryCacheRegionManagerTests : public testing::Test {
  protected:
    Secondary::CacheStats stats;
    Secondary::MemoryBudget transient{1 << 20};
    Secondary::MemoryBlockDevice* device{};
    FileBasedSecondaryCacheOptions options;
    std::unique_ptr<Secondary::RegionManager> manager;
    void SetUp() override {
        options.engine = FileBasedSecondaryCacheEngine::kRegion;
        options.capacity = 1024;
        options.regionSize = 512;
        options.maxEntrySize = 256;
        options.flushBlockSize = 128;
        manager = std::make_unique<Secondary::RegionManager>(
            [&](Secondary::MemoryBudget&) {
                auto result = std::make_unique<Secondary::MemoryBlockDevice>();
                device = result.get();
                return result;
            },
            options, stats);
    }
    rocksdb::Status Append(uint64_t hash, size_t length = 192) {
        std::vector<std::byte> record(length, std::byte{0x53});
        return manager->Append(hash, record, length - 32);
    }
    Secondary::RecordBytes Bytes() { return Secondary::RecordBytes{Secondary::BudgetAllocator<std::byte>(transient)}; }
};
} // namespace

TEST_F(SecondaryCacheRegionManagerTests, FifoReclaimsWithoutReadingCorruptDisk) {
    ASSERT_TRUE(Append(1).ok());
    ASSERT_TRUE(Append(2).ok());
    std::fill(device->MutableRegion(0).begin(), device->MutableRegion(0).end(), std::byte{0xff});
    device->SetFaultHook(
        [](auto operation, auto, auto, auto) { return operation == Secondary::DeviceOperation::kRead; });
    for (uint64_t key = 3; key < 7; ++key) {
        ASSERT_TRUE(Append(key).ok());
    }
    auto bytes = Bytes();
    Secondary::RegionManager::Identity identity;
    EXPECT_EQ(manager->Read(1, bytes, identity), Secondary::RegionManager::ReadStatus::kMiss);
    EXPECT_EQ(manager->Read(2, bytes, identity), Secondary::RegionManager::ReadStatus::kMiss);
    const auto snapshot = manager->Snapshot();
    EXPECT_EQ(snapshot.liveEntries, 4u);
    EXPECT_EQ(snapshot.liveBytes, 4u * 192);
    EXPECT_EQ(snapshot.reclaims, 1u);
}

TEST_F(SecondaryCacheRegionManagerTests, ResizeAndStaleIdentityCannotEraseReplacement) {
    ASSERT_TRUE(Append(1).ok());
    auto bytes = Bytes();
    Secondary::RegionManager::Identity identity;
    ASSERT_EQ(manager->Read(1, bytes, identity), Secondary::RegionManager::ReadStatus::kOk);
    ASSERT_TRUE(Append(1).ok());
    EXPECT_FALSE(manager->Remove(1, identity));
    ASSERT_TRUE(manager->SetCapacity(2048).ok());
    ASSERT_TRUE(Append(1).ok());
    EXPECT_FALSE(manager->Remove(1, identity));
    for (uint64_t key = 2; key < 30; ++key) {
        ASSERT_TRUE(Append(key).ok());
    }
    EXPECT_LE(manager->Snapshot().liveBytes, 2048u);
    ASSERT_TRUE(manager->SetCapacity(64).ok());
    EXPECT_EQ(manager->Snapshot().liveEntries, 0u);
    ASSERT_TRUE(manager->SetCapacity(0).ok());
    EXPECT_TRUE(Append(2).ok());
    EXPECT_EQ(manager->Snapshot().liveEntries, 0u);
    ASSERT_TRUE(manager->SetCapacity(1024).ok());
    EXPECT_TRUE(Append(3).ok());
}

TEST_F(SecondaryCacheRegionManagerTests, BufferedClearDoesNotRequireDeviceRead) {
    ASSERT_TRUE(Append(1, 64).ok());
    EXPECT_EQ(manager->Snapshot().deviceWriteBytes, 0u);
    ASSERT_TRUE(manager->SetCapacity(0).ok());
    const auto snapshot = manager->Snapshot();
    EXPECT_EQ(snapshot.liveEntries, 0u);
    EXPECT_EQ(snapshot.liveBytes, 0u);
    EXPECT_EQ(snapshot.activeBufferBytes, 0u);
}

TEST_F(SecondaryCacheRegionManagerTests, FailedFlushInvalidatesAllMappingsAndAllowsRetry) {
    ASSERT_TRUE(Append(1, 128).ok());
    device->SetFaultHook([](auto op, auto, auto, auto) { return op == Secondary::DeviceOperation::kWrite; });
    EXPECT_TRUE(Append(2, 128).IsIOError());
    EXPECT_EQ(manager->Snapshot().liveEntries, 0u);
    EXPECT_EQ(manager->Snapshot().liveBytes, 0u);
    EXPECT_EQ(manager->Snapshot().ioErrors, 1u);
    device->SetFaultHook({});
    ASSERT_TRUE(Append(3, 192).ok());
    auto bytes = Bytes();
    Secondary::RegionManager::Identity identity;
    ASSERT_EQ(manager->Read(3, bytes, identity), Secondary::RegionManager::ReadStatus::kOk);
    EXPECT_EQ(bytes, Secondary::RecordBytes(192, std::byte{0x53}, bytes.get_allocator()));
}

TEST_F(SecondaryCacheRegionManagerTests, ReadFailureRemovesMapping) {
    ASSERT_TRUE(Append(1).ok());
    device->SetFaultHook([](auto op, auto, auto, auto) { return op == Secondary::DeviceOperation::kRead; });
    auto bytes = Bytes();
    Secondary::RegionManager::Identity identity;
    EXPECT_EQ(manager->Read(1, bytes, identity), Secondary::RegionManager::ReadStatus::kIoError);
    EXPECT_EQ(manager->Snapshot().liveBytes, 0u);
}

TEST_F(SecondaryCacheRegionManagerTests, FailedResizeLeavesDisabledAndCanBeReenabled) {
    ASSERT_TRUE(Append(1).ok());
    device->SetFaultHook([](auto op, auto, auto, auto) { return op == Secondary::DeviceOperation::kConfigure; });
    EXPECT_THROW(manager->SetCapacity(4096), std::runtime_error);
    EXPECT_EQ(manager->Snapshot().capacityBytes, 0u);
    EXPECT_EQ(manager->Snapshot().liveEntries, 0u);
    device->SetFaultHook({});
    ASSERT_TRUE(manager->SetCapacity(512).ok());
    EXPECT_TRUE(Append(3).ok());
}

TEST_F(SecondaryCacheRegionManagerTests, ConcurrentSameKeyPublicationAndRecyclingMaintainExactUsage) {
    std::barrier start(5);
    std::vector<std::thread> writers;
    for (uint64_t writer = 0; writer < 4; ++writer) {
        writers.emplace_back([&, writer] {
            start.arrive_and_wait();
            for (size_t i = 0; i < 500; ++i) {
                EXPECT_TRUE(Append(writer, 192).ok());
            }
        });
    }
    start.arrive_and_wait();
    for (auto& writer : writers) {
        writer.join();
    }
    const auto snapshot = manager->Snapshot();
    EXPECT_GT(snapshot.reclaims, 0u);
    EXPECT_EQ(snapshot.liveBytes, snapshot.liveEntries * 192u);
    EXPECT_LE(snapshot.liveEntries, 4u);
    EXPECT_LE(snapshot.liveBytes, snapshot.capacityBytes);
}
