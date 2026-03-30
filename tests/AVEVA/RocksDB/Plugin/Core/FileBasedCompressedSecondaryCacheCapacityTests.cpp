// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

// --------------------------------------------------------------------------
// Inserting more data than the capacity evicts the oldest (LRU) entry
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CapacityEvictsLruEntry)
{
    // Capacity = 2 × (kFileHeaderSize + 10) bytes; holds exactly two 10-byte entries.
    const size_t capacity = 2 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    const std::string key1 = "key_lru_1";
    const std::string key2 = "key_lru_2";
    const std::string key3 = "key_lru_3";

    TestPayload p1{"0123456789"}; // 10 bytes
    TestPayload p2{"abcdefghij"}; // 10 bytes
    TestPayload p3{"ABCDEFGHIJ"}; // 10 bytes — should evict p1

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(key3), &p3, &m_helper, true).ok());

    bool kept = false;

    // key1 should have been evicted
    EXPECT_EQ(m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept), nullptr);

    // key2 and key3 should still be present
    auto h2 = m_cache->Lookup(MakeKey(key2), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h2, nullptr);
    delete static_cast<TestPayload*>(h2->Value());

    auto h3 = m_cache->Lookup(MakeKey(key3), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h3, nullptr);
    delete static_cast<TestPayload*>(h3->Value());
}

// --------------------------------------------------------------------------
// Lookup promotes an entry to MRU; subsequent eviction targets the true LRU
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, LookupPromotesToMru)
{
    const size_t capacity = 2 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    const std::string key1 = "promo_key1";
    const std::string key2 = "promo_key2";
    const std::string key3 = "promo_key3";

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    TestPayload p3{"ABCDEFGHIJ"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok()); // LRU: [key1]
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok()); // LRU: [key2, key1]

    // Lookup key1 promotes it to MRU.  LRU: [key1, key2]
    bool kept = false;
    auto h1 = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h1, nullptr);
    delete static_cast<TestPayload*>(h1->Value());

    // Inserting key3 must evict the LRU entry, which is now key2 (not key1).
    ASSERT_TRUE(m_cache->Insert(MakeKey(key3), &p3, &m_helper, true).ok());

    EXPECT_EQ(m_cache->Lookup(MakeKey(key2), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr) << "key2 was LRU after promotion, must be evicted";

    auto h1b = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h1b, nullptr) << "key1 was promoted to MRU, must still be present";
    delete static_cast<TestPayload*>(h1b->Value());

    auto h3 = m_cache->Lookup(MakeKey(key3), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h3, nullptr);
    delete static_cast<TestPayload*>(h3->Value());
}

// --------------------------------------------------------------------------
// advise_erase=true removes the entry from the secondary cache after Lookup
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, LookupWithAdviseEraseRemovesEntry)
{
    const std::string keyStr = "advise_erase_key";
    TestPayload payload{"ephemeral data"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, /*advise_erase=*/true, nullptr, kept);

    ASSERT_NE(handle, nullptr);
    EXPECT_FALSE(kept); // entry removed from secondary cache

    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, payload.data);
    delete result;

    // A subsequent Lookup should find nothing.
    EXPECT_EQ(m_cache->Lookup(MakeKey(keyStr), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr);
}

// --------------------------------------------------------------------------
// force_insert=false skips insertion without evicting when cache is full
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ForceInsertFalse_WhenCacheFull_SkipsWithoutEvicting)
{
    // Fill the cache exactly with two 10-byte entries.
    const size_t capacity = 2 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    const std::string key1 = "fi_false_key1";
    const std::string key2 = "fi_false_key2";
    const std::string key3 = "fi_false_key3";

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    TestPayload p3{"ABCDEFGHIJ"}; // should be skipped

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());

    // Non-forced insert while full should succeed (return OK) but not modify the cache.
    ASSERT_TRUE(m_cache->Insert(MakeKey(key3), &p3, &m_helper, /*force_insert=*/false).ok());

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey(key3), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr) << "key3 must not have been inserted";

    auto h1 = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h1, nullptr);
    delete static_cast<TestPayload*>(h1->Value());

    auto h2 = m_cache->Lookup(MakeKey(key2), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h2, nullptr);
    delete static_cast<TestPayload*>(h2->Value());
}

// --------------------------------------------------------------------------
// force_insert=true evicts as normal when cache is full
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ForceInsertTrue_WhenCacheFull_Evicts)
{
    const size_t capacity = 2 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    TestPayload p3{"ABCDEFGHIJ"};

    ASSERT_TRUE(m_cache->Insert(MakeKey("fi_true_key1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("fi_true_key2"), &p2, &m_helper, true).ok());

    // Forced insert must evict the LRU entry (key1).
    ASSERT_TRUE(m_cache->Insert(MakeKey("fi_true_key3"), &p3, &m_helper, /*force_insert=*/true).ok());

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("fi_true_key1"), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr) << "key1 must be evicted";

    auto h3 = m_cache->Lookup(MakeKey("fi_true_key3"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h3, nullptr);
    delete static_cast<TestPayload*>(h3->Value());
}

// --------------------------------------------------------------------------
// force_insert=false on an existing key succeeds even when the cache is full
// (net capacity change = 0 because the old entry is replaced, not added)
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ForceInsertFalse_SameKey_WhenFull_UpdatesData)
{
    const size_t capacity = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    const std::string keyStr = "same_key_full";
    TestPayload original{"0123456789"}; // 10 bytes — fills capacity exactly
    TestPayload updated{"9876543210"};  // 10 bytes

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &original, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &updated, &m_helper, /*force_insert=*/false).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, updated.data);
    delete result;
}

// --------------------------------------------------------------------------
// SetCapacity triggers eviction of LRU entries
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, SetCapacityTriggersEviction)
{
    TestPayload p1{"1234567890"}; // 10 bytes
    TestPayload p2{"abcdefghij"}; // 10 bytes

    ASSERT_TRUE(m_cache->Insert(MakeKey("set_cap_key1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("set_cap_key2"), &p2, &m_helper, true).ok());

    // Shrink capacity to one on-disk entry, which should evict key1 (LRU).
    ASSERT_TRUE(m_cache->SetCapacity(FileBasedCompressedSecondaryCache::kFileHeaderSize + 10).ok());

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("set_cap_key1"), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr);

    auto h2 = m_cache->Lookup(MakeKey("set_cap_key2"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h2, nullptr);
    delete static_cast<TestPayload*>(h2->Value());

    size_t cap = 0;
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
}

// --------------------------------------------------------------------------
// SetCapacity(0) evicts every entry
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, SetCapacityZeroEvictsAll)
{
    TestPayload p1{"payload one"};
    TestPayload p2{"payload two"};

    ASSERT_TRUE(m_cache->Insert(MakeKey("zero_cap_k1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("zero_cap_k2"), &p2, &m_helper, true).ok());

    ASSERT_TRUE(m_cache->SetCapacity(0).ok());

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("zero_cap_k1"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    EXPECT_EQ(m_cache->Lookup(MakeKey("zero_cap_k2"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);

    size_t cap = 999;
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, 0u);
}

// --------------------------------------------------------------------------
// Deflate reduces capacity and triggers eviction; Inflate restores it
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, DeflateAndInflateCapacity)
{
    constexpr size_t kEntryStoredSize = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    const size_t initialCapacity = 2 * kEntryStoredSize;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, initialCapacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};

    ASSERT_TRUE(m_cache->Insert(MakeKey("deflate_key1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("deflate_key2"), &p2, &m_helper, true).ok());

    // Deflate by one entry: key1 (LRU) must be evicted.
    ASSERT_TRUE(m_cache->Deflate(kEntryStoredSize).ok());

    size_t cap = 0;
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, kEntryStoredSize);

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("deflate_key1"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    auto h2 = m_cache->Lookup(MakeKey("deflate_key2"), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h2, nullptr);
    delete static_cast<TestPayload*>(h2->Value());

    // Inflate restores capacity; new insertions succeed.
    ASSERT_TRUE(m_cache->Inflate(kEntryStoredSize).ok());
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, initialCapacity);

    TestPayload p3{"ABCDEFGHIJ"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("deflate_key3"), &p3, &m_helper, true).ok());
}

// --------------------------------------------------------------------------
// Inflate with SIZE_MAX must clamp to SIZE_MAX (saturating add), not wrap
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, Inflate_SaturationAtSizeMax)
{
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, /*capacity=*/1024, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    ASSERT_TRUE(m_cache->Inflate(std::numeric_limits<size_t>::max()).ok());

    size_t cap = 0;
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, std::numeric_limits<size_t>::max())
        << "Inflate(SIZE_MAX) must saturate at SIZE_MAX, not wrap";

    // A further Inflate from SIZE_MAX must be a no-op — already saturated.
    ASSERT_TRUE(m_cache->Inflate(1).ok());
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, std::numeric_limits<size_t>::max());
}

// --------------------------------------------------------------------------
// Deflate by more than the current capacity must floor at zero, not underflow
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, DeflateByMoreThanCapacity_ClampsToZero)
{
    ASSERT_TRUE(m_cache->Deflate(std::numeric_limits<size_t>::max()).ok());

    size_t cap = 999;
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, 0u)
        << "Deflate(SIZE_MAX) must floor capacity at zero, not underflow";
}

// --------------------------------------------------------------------------
// A single large insert evicts multiple smaller entries at once
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, SingleInsertEvictsMultipleEntries)
{
    constexpr size_t kSmallPayloadSize = 10;
    constexpr size_t kSmallEntrySize   = FileBasedCompressedSecondaryCache::kFileHeaderSize + kSmallPayloadSize;
    constexpr size_t kLargePayloadSize = 30;

    // Capacity fits 3 small entries (96 bytes) but NOT 3 small + 1 large.
    const size_t capacity = 3 * kSmallEntrySize;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    TestPayload p1{std::string(kSmallPayloadSize, '1')};
    TestPayload p2{std::string(kSmallPayloadSize, '2')};
    TestPayload p3{std::string(kSmallPayloadSize, '3')};
    TestPayload pLarge{std::string(kLargePayloadSize, 'L')};

    ASSERT_TRUE(m_cache->Insert(MakeKey("multi_evict_k1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("multi_evict_k2"), &p2, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("multi_evict_k3"), &p3, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("multi_evict_large"), &pLarge, &m_helper, true).ok());

    EXPECT_GE(m_cache->GetEvictedCount(), 2u)
        << "A single large insert must evict multiple smaller LRU entries";

    bool kept = false;
    auto hLarge = m_cache->Lookup(MakeKey("multi_evict_large"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(hLarge, nullptr);
    auto* resultLarge = static_cast<TestPayload*>(hLarge->Value());
    ASSERT_NE(resultLarge, nullptr);
    EXPECT_EQ(resultLarge->data, pLarge.data);
    delete resultLarge;

    EXPECT_EQ(m_cache->Lookup(MakeKey("multi_evict_k1"), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr) << "key1 (LRU) must be evicted";
    EXPECT_EQ(m_cache->Lookup(MakeKey("multi_evict_k2"), &m_helper, nullptr, true, false, nullptr, kept),
              nullptr) << "key2 must be evicted to make room for the large entry";

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_LE(usage, capacity);
}

// --------------------------------------------------------------------------
// Constructing with capacity=0: all inserts are silently dropped
// — force_insert=false is skipped in Phase 1 (capacity gate);
//   force_insert=true writes the file and immediately evicts it in Phase 3
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ZeroCapacityAtConstruction_AllInsertsDropped)
{
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, /*capacity=*/0, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    TestPayload p{"some data"};

    // force_insert=false: Phase 1 capacity gate returns OK immediately.
    ASSERT_TRUE(m_cache->Insert(MakeKey("zero_cap_noforce"), &p, &m_helper, /*force_insert=*/false).ok());

    // force_insert=true: file is written then immediately evicted in Phase 3;
    // GetEvictedCount must reflect exactly one capacity-driven eviction.
    ASSERT_TRUE(m_cache->Insert(MakeKey("zero_cap_force"), &p, &m_helper, /*force_insert=*/true).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 1u)
        << "force_insert=true into a zero-capacity cache must count as one eviction";

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u) << "Zero-capacity cache must track no bytes regardless of force_insert";

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("zero_cap_noforce"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    EXPECT_EQ(m_cache->Lookup(MakeKey("zero_cap_force"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
}

// --------------------------------------------------------------------------
// GetEvictedCount starts at zero for a freshly constructed cache
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, GetEvictedCount_StartsAtZero)
{
    EXPECT_EQ(m_cache->GetEvictedCount(), 0u);
}

// --------------------------------------------------------------------------
// GetEvictedCount increments once per entry expelled by capacity pressure
// from Insert, and again from SetCapacity
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, GetEvictedCount_IncrementsOnCapacityEviction)
{
    constexpr size_t kEntryStoredSize = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, kEntryStoredSize, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};

    ASSERT_TRUE(m_cache->Insert(MakeKey("evc_k1"), &p1, &m_helper, true).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 0u) << "No eviction yet";

    ASSERT_TRUE(m_cache->Insert(MakeKey("evc_k2"), &p2, &m_helper, true).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 1u) << "One entry evicted by capacity pressure";

    ASSERT_TRUE(m_cache->SetCapacity(0).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 2u) << "Second entry evicted by SetCapacity";
}

// --------------------------------------------------------------------------
// GetEvictedCount increments when Deflate reduces capacity below current usage
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, GetEvictedCount_IncrementsOnDeflate)
{
    constexpr size_t kEntryStoredSize = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, 2 * kEntryStoredSize, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("defl_k1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("defl_k2"), &p2, &m_helper, true).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 0u);

    ASSERT_TRUE(m_cache->Deflate(kEntryStoredSize).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 1u);
}

// --------------------------------------------------------------------------
// GetEvictedCount is NOT incremented by an explicit Erase call
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, GetEvictedCount_NotIncrementedByErase)
{
    TestPayload p{"erase payload"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("erase_evc_k"), &p, &m_helper, true).ok());
    m_cache->Erase(MakeKey("erase_evc_k"));

    EXPECT_EQ(m_cache->GetEvictedCount(), 0u)
        << "Explicit Erase must not increment the eviction counter";
}

// --------------------------------------------------------------------------
// GetEvictedCount is NOT incremented when an entry is removed by advise_erase
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, GetEvictedCount_NotIncrementedByAdviseErase)
{
    TestPayload p{"advise erase payload"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("advise_evc_k"), &p, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("advise_evc_k"), &m_helper,
                                  nullptr, true, /*advise_erase=*/true, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());

    EXPECT_EQ(m_cache->GetEvictedCount(), 0u)
        << "advise_erase removal must not increment the eviction counter";
}
