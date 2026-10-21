// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "RegionBasedSecondaryCacheTestHelpers.hpp"

#include <thread>

TEST_F(RegionBasedSecondaryCacheTests, InsertAndLookupRoundTrip) {
    TestPayload payload{"region-engine payload"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("region-key"), &payload, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("region-key"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(kept);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, payload.data);
    delete result;
}

TEST_F(RegionBasedSecondaryCacheTests, EraseRemovesEntry) {
    TestPayload payload{"erase me"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("erase-key"), &payload, &m_helper, true).ok());
    m_cache->Erase(MakeKey("erase-key"));

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("erase-key"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
}

TEST_F(RegionBasedSecondaryCacheTests, OverwriteExistingKeyReturnsNewestValue) {
    TestPayload original{"old"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("overwrite-key"), &original, &m_helper, true).ok());
    TestPayload updated{"new"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("overwrite-key"), &updated, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("overwrite-key"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, "new");
    delete result;
}

TEST_F(RegionBasedSecondaryCacheTests, CapacityReclaimsOldestRegion) {
    m_cache.reset();
    m_cache = std::make_unique<RegionBasedSecondaryCache>(m_cacheDir, MakeOptions(512 * 1024, 256 * 1024, 64 * 1024),
                                                          MakeNullLogger());

    const std::string firstKey = "oldest-region-entry";
    TestPayload payload{std::string(60 * 1024, 'a')};
    ASSERT_TRUE(m_cache->Insert(MakeKey(firstKey), &payload, &m_helper, true).ok());
    for (int i = 0; i < 8; ++i) {
        TestPayload fill{std::string(60 * 1024, static_cast<char>('b' + i))};
        ASSERT_TRUE(m_cache->Insert(MakeKey("fill-" + std::to_string(i)), &fill, &m_helper, true).ok());
    }

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey(firstKey), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
}

TEST_F(RegionBasedSecondaryCacheTests, DeflateInflateDoNotRemoveDiskEntries) {
    TestPayload payload{"keep me"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("deflate-key"), &payload, &m_helper, true).ok());

    size_t originalCapacity = 0;
    ASSERT_TRUE(m_cache->GetCapacity(originalCapacity).ok());
    ASSERT_TRUE(m_cache->Deflate(originalCapacity).IsNotSupported());
    ASSERT_TRUE(m_cache->Inflate(originalCapacity).IsNotSupported());

    size_t finalCapacity = 0;
    ASSERT_TRUE(m_cache->GetCapacity(finalCapacity).ok());
    EXPECT_EQ(finalCapacity, originalCapacity);

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("deflate-key"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}

TEST_F(RegionBasedSecondaryCacheTests, CacheDirectoryContainsOnlyRegionFiles) {
    TestPayload payload{"disk layout"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("layout-key"), &payload, &m_helper, true).ok());
    for (const auto& entry : std::filesystem::directory_iterator(m_cacheDir / "ssd-secondary-cache-v1")) {
        const auto name = entry.path().filename().string();
        EXPECT_TRUE(name == "owner" || name.starts_with("region_"));
    }
}

TEST_F(RegionBasedSecondaryCacheTests, ConcurrentReadersAndWritersDoNotCrash) {
    std::atomic<bool> stop{false};
    std::thread writer([&] {
        for (int i = 0; i < 200; ++i) {
            TestPayload payload{"value-" + std::to_string(i)};
            m_cache->Insert(MakeKey("key-" + std::to_string(i % 16)), &payload, &m_helper, true);
        }
        stop.store(true, std::memory_order_release);
    });

    std::thread reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            for (int i = 0; i < 16; ++i) {
                bool kept = false;
                auto handle = m_cache->Lookup(MakeKey("key-" + std::to_string(i)), &m_helper, nullptr, true, false,
                                              nullptr, kept);
                if (handle) {
                    delete static_cast<TestPayload*>(handle->Value());
                }
            }
        }
    });

    writer.join();
    reader.join();

    TestPayload finalPayload{"final"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("final-key"), &finalPayload, &m_helper, true).ok());
}
