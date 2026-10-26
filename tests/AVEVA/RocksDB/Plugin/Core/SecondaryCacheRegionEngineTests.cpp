// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

#include <thread>

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, InsertAndLookupRoundTrip) {
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

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, EraseRemovesEntry) {
    TestPayload payload{"erase me"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("erase-key"), &payload, &m_helper, true).ok());
    m_cache->Erase(MakeKey("erase-key"));

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("erase-key"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, OverwriteExistingKeyReturnsNewestValue) {
    // Dedupe would keep the first copy, so this opts out to exercise the replace path.
    auto options = MakeOptions();
    options.skipInsertIfPresent = false;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir / "overwrite", options, MakeNullLogger());

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

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, CapacityReclaimsOldestRegion) {
    m_cache.reset();
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(
        m_cacheDir, MakeOptions(512 * 1024, (1 << 20) + (256 << 10), 64 * 1024), MakeNullLogger());

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

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, DeflateInflateDoNotRemoveDiskEntries) {
    TestPayload payload{"keep me"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("deflate-key"), &payload, &m_helper, true).ok());

    size_t originalCapacity = 0;
    ASSERT_TRUE(m_cache->GetCapacity(originalCapacity).ok());
    ASSERT_TRUE(m_cache->Deflate(originalCapacity).ok());
    ASSERT_TRUE(m_cache->Inflate(originalCapacity).ok());

    size_t finalCapacity = 0;
    ASSERT_TRUE(m_cache->GetCapacity(finalCapacity).ok());
    EXPECT_EQ(finalCapacity, originalCapacity);

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("deflate-key"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, CacheDirectoryContainsOnlyRegionFiles) {
    TestPayload payload{"disk layout"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("layout-key"), &payload, &m_helper, true).ok());
    for (const auto& entry : std::filesystem::directory_iterator(m_cacheDir)) {
        EXPECT_TRUE(entry.path().filename().string().starts_with("region_"));
        EXPECT_NE(entry.path().extension().string(), ".del");
    }
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ConcurrentReadersAndWritersDoNotCrash) {
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

namespace {
rocksdb::CacheTier g_capturedTier = rocksdb::CacheTier::kNonVolatileBlockTier;

rocksdb::Status TierCapturingCreateCb(const rocksdb::Slice& data, rocksdb::CompressionType type,
                                      rocksdb::CacheTier source, rocksdb::Cache::CreateContext* ctx,
                                      rocksdb::MemoryAllocator* alloc, rocksdb::Cache::ObjectPtr* outObj,
                                      size_t* outCharge) {
    g_capturedTier = source;
    return TestCreateCb(data, type, source, ctx, alloc, outObj, outCharge);
}

size_t UsageOf(FileBasedCompressedSecondaryCache& cache) {
    size_t usage = 0;
    EXPECT_TRUE(cache.GetUsage(usage).ok());
    return usage;
}
} // namespace

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, LookupPassesVolatileTierToCreateCb) {
    rocksdb::Cache::CacheItemHelper helper{rocksdb::CacheEntryRole::kDataBlock,
                                           TestDeleteCb,
                                           TestSizeCb,
                                           TestSaveToCb,
                                           TierCapturingCreateCb,
                                           &m_helperNoSec};
    TestPayload payload{"tier"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("tier-key"), &payload, &helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("tier-key"), &helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(g_capturedTier, rocksdb::CacheTier::kVolatileTier);
    delete static_cast<TestPayload*>(handle->Value());
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, InsertSavedWithNonVolatileSourceStillHitsUnderStrictCreateCb) {
    // RocksDB's typed cache helpers reject every tier but kVolatileTier in create_cb.
    rocksdb::Cache::CacheItemHelper strictHelper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        TestSaveToCb,
        [](const rocksdb::Slice& data, rocksdb::CompressionType type, rocksdb::CacheTier source,
           rocksdb::Cache::CreateContext* ctx, rocksdb::MemoryAllocator* alloc, rocksdb::Cache::ObjectPtr* outObj,
           size_t* outCharge) -> rocksdb::Status {
            if (source != rocksdb::CacheTier::kVolatileTier) {
                return rocksdb::Status::InvalidArgument("unexpected tier");
            }
            return TestCreateCb(data, type, source, ctx, alloc, outObj, outCharge);
        },
        &m_helperNoSec};

    const std::string payload = "pre-serialised block";
    ASSERT_TRUE(m_cache
                    ->InsertSaved(MakeKey("saved-nv"), rocksdb::Slice(payload),
                                  rocksdb::CompressionType::kNoCompression, rocksdb::CacheTier::kNonVolatileBlockTier)
                    .ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("saved-nv"), &strictHelper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ReinsertOfPresentKeyDoesNotConsumeLogSpace) {
    TestPayload payload{std::string(1000, 'x')};
    ASSERT_TRUE(m_cache->Insert(MakeKey("dup"), &payload, &m_helper, false).ok());
    const size_t single = UsageOf(*m_cache);

    // Far more re-inserts than the log could hold if each appended a copy.
    for (int i = 0; i < 5000; ++i) {
        ASSERT_TRUE(m_cache->Insert(MakeKey("dup"), &payload, &m_helper, false).ok());
    }
    EXPECT_EQ(UsageOf(*m_cache), single);

    // Erasing makes the key absent again, so a later insert is admitted.
    m_cache->Erase(MakeKey("dup"));
    ASSERT_TRUE(m_cache->Insert(MakeKey("dup"), &payload, &m_helper, false).ok());
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("dup"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, RepeatedOverwritesDoNotInflateUsage) {
    TestPayload payload{std::string(1000, 'x')};
    ASSERT_TRUE(m_cache->Insert(MakeKey("same"), &payload, &m_helper, true).ok());
    const size_t single = UsageOf(*m_cache);
    ASSERT_GT(single, 0U);

    for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(m_cache->Insert(MakeKey("same"), &payload, &m_helper, true).ok());
    }
    EXPECT_EQ(UsageOf(*m_cache), single);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, EraseAndAdviseEraseReleaseUsage) {
    TestPayload payload{std::string(1000, 'x')};
    ASSERT_TRUE(m_cache->Insert(MakeKey("a"), &payload, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("b"), &payload, &m_helper, true).ok());

    m_cache->Erase(MakeKey("a"));
    const size_t afterErase = UsageOf(*m_cache);

    bool kept = true;
    auto handle = m_cache->Lookup(MakeKey("b"), &m_helper, nullptr, true, true, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_FALSE(kept);
    delete static_cast<TestPayload*>(handle->Value());
    EXPECT_LT(UsageOf(*m_cache), afterErase);
    EXPECT_EQ(UsageOf(*m_cache), 0U);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ConcurrentSameKeyInsertsAccountUsageOnce) {
    TestPayload payload{std::string(1000, 'x')};
    ASSERT_TRUE(m_cache->Insert(MakeKey("hot"), &payload, &m_helper, true).ok());
    const size_t single = UsageOf(*m_cache);

    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < 100; ++i) {
                m_cache->Insert(MakeKey("hot"), &payload, &m_helper, true);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(UsageOf(*m_cache), single);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, SetCapacityCannotGrowPastPreallocatedRegions) {
    size_t initial = 0;
    ASSERT_TRUE(m_cache->GetCapacity(initial).ok());
    ASSERT_TRUE(m_cache->SetCapacity(initial * 64).ok());

    size_t after = 0;
    ASSERT_TRUE(m_cache->GetCapacity(after).ok());
    EXPECT_LE(after, initial + ((1 << 20) + (256 << 10)));

    for (int i = 0; i < 200; ++i) {
        TestPayload payload{std::string(100 * 1024, static_cast<char>('a' + i % 26))};
        ASSERT_TRUE(m_cache->Insert(MakeKey("grow-" + std::to_string(i)), &payload, &m_helper, true).ok());
    }
    EXPECT_LE(UsageOf(*m_cache), after);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ShrinkThenGrowReusesReclaimedRegions) {
    m_cache.reset();
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(
        m_cacheDir, MakeOptions(1024 * 1024, (1 << 20) + (256 << 10), 64 * 1024), MakeNullLogger());

    const auto fill = [&](const std::string& prefix, int count) {
        for (int i = 0; i < count; ++i) {
            TestPayload payload{std::string(60 * 1024, 'z')};
            ASSERT_TRUE(m_cache->Insert(MakeKey(prefix + std::to_string(i)), &payload, &m_helper, true).ok());
        }
    };
    fill("a", 16);

    ASSERT_TRUE(m_cache->SetCapacity(256 * 1024).ok());
    EXPECT_LE(UsageOf(*m_cache), 256U * 1024U);

    ASSERT_TRUE(m_cache->SetCapacity(1024 * 1024).ok());
    fill("b", 16);
    EXPECT_GT(UsageOf(*m_cache), 256U * 1024U);
    EXPECT_LE(UsageOf(*m_cache), 1024U * 1024U);

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("b15"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ZeroCapacityDropsInsertsAndRecoversWhenRestored) {
    size_t initial = 0;
    ASSERT_TRUE(m_cache->GetCapacity(initial).ok());
    ASSERT_TRUE(m_cache->SetCapacity(0).ok());

    TestPayload payload{"dropped"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("zero"), &payload, &m_helper, true).ok());
    EXPECT_EQ(UsageOf(*m_cache), 0U);

    ASSERT_TRUE(m_cache->SetCapacity(initial).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("back"), &payload, &m_helper, true).ok());
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("back"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}
