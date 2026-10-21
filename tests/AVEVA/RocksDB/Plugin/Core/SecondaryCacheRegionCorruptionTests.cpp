// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "RegionBasedSecondaryCacheTestHelpers.hpp"
#include "SecondaryCache/RecordFormat.hpp"

#include <fstream>

TEST_F(RegionBasedSecondaryCacheTests, CorruptDiskRecordNeverCallsCreateAndRemovesIndexEntry) {
    m_cache.reset();
    auto options = MakeOptions(8 * 1024 * 1024, 64 * 1024, 4 * 1024);
    options.maxEntrySize = 64 * 1024;
    m_cache = std::make_unique<RegionBasedSecondaryCache>(m_cacheDir, options, MakeNullLogger());
    TestPayload payload{std::string(32 * 1024, 'x')}; // Larger than flush block: direct disk write.
    for (const size_t offset : {size_t{0}, size_t{4}, size_t{16}, size_t{28}, size_t{40}}) {
        ASSERT_TRUE(m_cache->SetCapacity(options.capacity).ok());
        ASSERT_TRUE(m_cache->Insert("key", &payload, &m_helper, false).ok());
        // Close the first CRT handle through the bounded pool before corrupting
        // its file. The mapping remains live, so this exercises a real disk miss.
        for (size_t i = 0; i < 65; ++i) {
            ASSERT_TRUE(m_cache->Insert("fill-" + std::to_string(i), &payload, &m_helper, false).ok());
        }
        const auto file = m_cacheDir / "ssd-secondary-cache-v1" / "region_0.dat";
        std::fstream stream(file, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(stream.is_open());
        stream.seekp(static_cast<std::streamoff>(offset));
        stream.put('\xff');
        stream.close();
        auto helper = m_helper;
        helper.create_cb = [](const rocksdb::Slice&, rocksdb::CompressionType, rocksdb::CacheTier,
                              rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*, rocksdb::Cache::ObjectPtr*,
                              size_t*) {
            ADD_FAILURE() << "create callback called for corrupt record";
            return rocksdb::Status::Corruption();
        };
        bool kept = true;
        EXPECT_EQ(m_cache->Lookup("key", &helper, nullptr, false, false, nullptr, kept), nullptr);
        EXPECT_FALSE(kept);
        AVEVA::RocksDB::Plugin::Core::RegionBasedSecondaryCacheStats stats;
        ASSERT_TRUE(m_cache->GetStats(stats).ok());
        EXPECT_EQ(stats.liveEntries, 65u);
        EXPECT_EQ(m_cache->Lookup("key", &helper, nullptr, false, false, nullptr, kept), nullptr);
        EXPECT_GT(stats.corruptions, 0u);
    }
}
