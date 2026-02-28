// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>
#include <rocksdb/status.h>

#include <gtest/gtest.h>

#include <boost/algorithm/hex.hpp>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache;

namespace
{
    // A simple heap-allocated payload object used by the test helper callbacks.
    struct TestPayload
    {
        std::string data;
    };

    size_t TestSizeCb(rocksdb::Cache::ObjectPtr obj)
    {
        return static_cast<TestPayload*>(obj)->data.size();
    }

    rocksdb::Status TestSaveToCb(rocksdb::Cache::ObjectPtr obj,
                                  size_t from_offset,
                                  size_t length,
                                  char* out_buf)
    {
        const auto* payload = static_cast<TestPayload*>(obj);
        if (from_offset + length > payload->data.size())
        {
            return rocksdb::Status::InvalidArgument("out of range");
        }
        std::memcpy(out_buf, payload->data.data() + from_offset, length);
        return rocksdb::Status::OK();
    }

    rocksdb::Status TestCreateCb(const rocksdb::Slice& data,
                                  rocksdb::CompressionType /*type*/,
                                  rocksdb::CacheTier /*source*/,
                                  rocksdb::Cache::CreateContext* /*ctx*/,
                                  rocksdb::MemoryAllocator* /*alloc*/,
                                  rocksdb::Cache::ObjectPtr* out_obj,
                                  size_t* out_charge)
    {
        auto* payload = new TestPayload{std::string(data.data(), data.size())};
        *out_obj = payload;
        *out_charge = payload->data.size();
        return rocksdb::Status::OK();
    }

    void TestDeleteCb(rocksdb::Cache::ObjectPtr obj, rocksdb::MemoryAllocator*)
    {
        delete static_cast<TestPayload*>(obj);
    }

    // Returns a temporary directory that is removed when the test fixture tears down.
    std::filesystem::path MakeTempDir(const std::string& suffix)
    {
        auto base = std::filesystem::temp_directory_path() /
                    ("aveva_sec_cache_test_" + suffix);
        std::filesystem::remove_all(base);
        std::filesystem::create_directories(base);
        return base;
    }

    // Counts .del graveyard files remaining under cacheDir.
    // RemoveEntryLocked renames evicted files to .del under the lock and the
    // caller deletes them after releasing it.  By the time the mutating
    // operation (Insert, Erase, SetCapacity, …) returns, this must be zero.
    size_t CountGraveyardFiles(const std::filesystem::path& cacheDir)
    {
        size_t count = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(cacheDir))
            if (entry.is_regular_file() && entry.path().extension() == ".del")
                ++count;
        return count;
    }
}

class FileBasedCompressedSecondaryCacheTests : public ::testing::Test
{
protected:
    std::filesystem::path m_cacheDir;
    std::unique_ptr<FileBasedCompressedSecondaryCache> m_cache;

    // A helper without secondary-cache support, required as the
    // without_secondary_compat back-pointer for m_helper.
    rocksdb::Cache::CacheItemHelper m_helperNoSec{
        rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};

    // A helper with secondary-cache support.  without_secondary_compat must
    // point to a valid no-secondary helper with the same role and deleter.
    rocksdb::Cache::CacheItemHelper m_helper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        TestSaveToCb,
        TestCreateCb,
        &m_helperNoSec};

    void SetUp() override
    {
        m_cacheDir = MakeTempDir(::testing::UnitTest::GetInstance()->current_test_info()->name());
        m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir);
    }

    void TearDown() override
    {
        m_cache.reset();
        std::filesystem::remove_all(m_cacheDir);
    }

    // Creates a key Slice backed by the provided string (must outlive the Slice).
    static rocksdb::Slice MakeKey(const std::string& s) { return rocksdb::Slice(s); }
};

// --------------------------------------------------------------------------
// Insert then Lookup round-trip
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertAndLookup)
{
    const std::string keyStr = "testkey0001";
    TestPayload payload{"hello, secondary cache"};

    auto s = m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, /*force_insert=*/true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    bool keptInSecCache = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  /*create_context=*/nullptr,
                                  /*wait=*/true,
                                  /*advise_erase=*/false,
                                  /*stats=*/nullptr,
                                  keptInSecCache);

    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(handle->IsReady());
    EXPECT_TRUE(keptInSecCache);

    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, payload.data);
    EXPECT_EQ(handle->Size(), payload.data.size());

    delete result;
}

// --------------------------------------------------------------------------
// InsertSaved then Lookup round-trip
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSavedAndLookup)
{
    const std::string keyStr = "testkey0002";
    const std::string raw = "pre-serialised block data";
    const rocksdb::Slice saved(raw);

    auto s = m_cache->InsertSaved(MakeKey(keyStr), saved,
                                   rocksdb::CompressionType::kNoCompression,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok()) << s.ToString();

    bool keptInSecCache = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr,
                                  keptInSecCache);

    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(keptInSecCache);

    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}

// --------------------------------------------------------------------------
// Lookup on a key that was never inserted returns nullptr
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, LookupMissReturnsNull)
{
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("not_inserted"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr);
    EXPECT_FALSE(kept);
}

// --------------------------------------------------------------------------
// Erase removes an entry so a subsequent Lookup returns nullptr
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EraseRemovesEntry)
{
    const std::string keyStr = "testkey0003";
    TestPayload payload{"data to erase"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    m_cache->Erase(MakeKey(keyStr));

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr);
    EXPECT_FALSE(kept);
}

// --------------------------------------------------------------------------
// Inserting more data than the capacity evicts the oldest (LRU) entry
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CapacityEvictsLruEntry)
{
    // Capacity = 2 × (kFileHeaderSize + 10) bytes; holds exactly two 10-byte entries.
    const size_t capacity = 2 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

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
    auto h1 = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    EXPECT_EQ(h1, nullptr);

    // key2 and key3 should still be present
    auto h2 = m_cache->Lookup(MakeKey(key2), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h2, nullptr);
    delete static_cast<TestPayload*>(h2->Value());

    auto h3 = m_cache->Lookup(MakeKey(key3), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h3, nullptr);
    delete static_cast<TestPayload*>(h3->Value());
}

// --------------------------------------------------------------------------
// SetCapacity triggers eviction of LRU entries
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, SetCapacityTriggersEviction)
{
    const std::string key1 = "set_cap_key1";
    const std::string key2 = "set_cap_key2";

    TestPayload p1{"1234567890"}; // 10 bytes
    TestPayload p2{"abcdefghij"}; // 10 bytes

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());

    // Shrink capacity to one on-disk entry, which should evict key1 (LRU).
    ASSERT_TRUE(m_cache->SetCapacity(FileBasedCompressedSecondaryCache::kFileHeaderSize + 10).ok());

    bool kept = false;
    auto h1 = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    EXPECT_EQ(h1, nullptr);

    auto h2 = m_cache->Lookup(MakeKey(key2), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h2, nullptr);
    delete static_cast<TestPayload*>(h2->Value());

    size_t cap = 0;
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
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
                                  nullptr, true, /*advise_erase=*/true,
                                  nullptr, kept);

    ASSERT_NE(handle, nullptr);
    EXPECT_FALSE(kept); // entry removed from secondary cache

    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, payload.data);
    delete result;

    // A subsequent Lookup should find nothing.
    auto handle2 = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                   nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle2, nullptr);
}

// --------------------------------------------------------------------------
// Insert compresses compressible data — the on-disk file must be smaller
// than the uncompressed payload plus the 21-byte file header
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertCompressesCompressibleData)
{
    const std::string keyStr = "compress_size_key";
    // 1 KiB of identical bytes — highly compressible.
    const std::string raw(1024, 'A');
    TestPayload payload{raw};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // 8 magic + 1 version + 1 compressionType + 8 dataSize + 4 checksum = 22 bytes
    constexpr std::uintmax_t kFileHeaderSize = 8 + 1 + 1 + 8 + 4;
    const std::uintmax_t fileSize = std::filesystem::file_size(filePath);

    EXPECT_LT(fileSize, kFileHeaderSize + raw.size())
        << "Expected compressed file (" << fileSize << " B) to be smaller than "
        << "header + uncompressed payload (" << kFileHeaderSize + raw.size() << " B)";

    // Round-trip must still produce the original data.
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}

// --------------------------------------------------------------------------
// force_insert=false skips insertion without evicting when cache is full
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ForceInsertFalse_WhenCacheFull_SkipsWithoutEvicting)
{
    // Fill the cache exactly with two 10-byte entries.
    const size_t capacity = 2 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

    const std::string key1 = "fi_false_key1";
    const std::string key2 = "fi_false_key2";
    const std::string key3 = "fi_false_key3";

    TestPayload p1{"0123456789"}; // 10 bytes
    TestPayload p2{"abcdefghij"}; // 10 bytes
    TestPayload p3{"ABCDEFGHIJ"}; // 10 bytes — should be skipped

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());

    // Non-forced insert while full should succeed (return OK) but not modify the cache.
    ASSERT_TRUE(m_cache->Insert(MakeKey(key3), &p3, &m_helper, /*force_insert=*/false).ok());

    bool kept = false;

    // key3 must not have been inserted.
    auto h3 = m_cache->Lookup(MakeKey(key3), &m_helper, nullptr, true, false, nullptr, kept);
    EXPECT_EQ(h3, nullptr);

    // key1 and key2 must still be present (nothing was evicted).
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
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

    const std::string key1 = "fi_true_key1";
    const std::string key2 = "fi_true_key2";
    const std::string key3 = "fi_true_key3";

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    TestPayload p3{"ABCDEFGHIJ"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());

    // Forced insert must evict the LRU entry (key1).
    ASSERT_TRUE(m_cache->Insert(MakeKey(key3), &p3, &m_helper, /*force_insert=*/true).ok());

    bool kept = false;

    auto h1 = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    EXPECT_EQ(h1, nullptr); // evicted

    auto h3 = m_cache->Lookup(MakeKey(key3), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h3, nullptr);
    delete static_cast<TestPayload*>(h3->Value());
}

// --------------------------------------------------------------------------
// Corrupting the data bytes on disk causes Lookup to return nullptr
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CorruptedDataRejectedOnLookup)
{
    const std::string keyStr = "corrupt_test_key";
    TestPayload payload{"data that will be silently corrupted on disk"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    // Reconstruct the on-disk filename using the same hex encoding as KeyToFilename.
    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // Flip one byte in the payload section (past the 22-byte header:
    // 8 magic + 1 version + 1 compressionType + 8 dataSize + 4 checksum).
    constexpr std::streamoff kFileHeaderSize = 8 + 1 + 1 + 8 + 4;
    {
        std::fstream f(filePath, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(f.is_open()) << "Cache file not found: " << filePath;
        f.seekg(kFileHeaderSize);
        char b = 0;
        f.read(&b, 1);
        f.seekp(kFileHeaderSize);
        f.put(static_cast<char>(~static_cast<unsigned char>(b)));
    }

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr);
    EXPECT_FALSE(kept);
}

// --------------------------------------------------------------------------
// Lookup promotes an entry to MRU; subsequent eviction targets the true LRU
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, LookupPromotesToMru)
{
    // capacity = 2 × (kFileHeaderSize + 10) bytes; each payload is 10 bytes.
    const size_t capacity = 2 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + 10);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

    const std::string key1 = "promo_key1";
    const std::string key2 = "promo_key2";
    const std::string key3 = "promo_key3";

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    TestPayload p3{"ABCDEFGHIJ"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok()); // LRU list: [key1]
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok()); // LRU list: [key2, key1]

    // Lookup key1 promotes it to MRU.  LRU list: [key1, key2]
    bool kept = false;
    auto h1 = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h1, nullptr);
    delete static_cast<TestPayload*>(h1->Value());

    // Inserting key3 must evict the LRU entry, which is now key2 (not key1).
    ASSERT_TRUE(m_cache->Insert(MakeKey(key3), &p3, &m_helper, true).ok());

    auto h2 = m_cache->Lookup(MakeKey(key2), &m_helper, nullptr, true, false, nullptr, kept);
    EXPECT_EQ(h2, nullptr); // key2 was LRU after promotion, must be evicted

    auto h1b = m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h1b, nullptr); // key1 was promoted to MRU, must still be present
    delete static_cast<TestPayload*>(h1b->Value());

    auto h3 = m_cache->Lookup(MakeKey(key3), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(h3, nullptr);
    delete static_cast<TestPayload*>(h3->Value());
}

// --------------------------------------------------------------------------
// Re-inserting the same key replaces the stored value
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, OverwriteExistingKeyReturnsNewData)
{
    const std::string keyStr = "overwrite_key";
    TestPayload original{"original payload data"};
    TestPayload updated{"updated payload data!"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &original, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &updated, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, updated.data);
    delete result;
}

// --------------------------------------------------------------------------
// force_insert=false on an existing key succeeds even when the cache is full
// (net capacity change = 0 because the old entry is replaced, not added)
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ForceInsertFalse_SameKey_WhenFull_UpdatesData)
{
    // Capacity exactly fits one 10-byte entry.
    const size_t capacity = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

    const std::string keyStr = "same_key_full";
    TestPayload original{"0123456789"}; // 10 bytes — fills capacity exactly
    TestPayload updated{"9876543210"};  // 10 bytes

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &original, &m_helper, true).ok());

    // force_insert=false, but replacing an existing key has zero net size change —
    // must succeed and return the updated data.
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
// Insert with a null helper returns OK without writing anything
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertWithNullHelper)
{
    TestPayload payload{"should not be stored"};
    auto s = m_cache->Insert(MakeKey("null_helper_k"), &payload, /*helper=*/nullptr, true);
    ASSERT_TRUE(s.ok());

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("null_helper_k"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
}

// --------------------------------------------------------------------------
// Insert with a non-secondary-cache-compatible helper returns OK without
// writing anything
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertWithIncompatibleHelper)
{
    TestPayload payload{"should not be stored"};
    // m_helperNoSec has no size_cb/saveto_cb/create_cb callbacks.
    auto s = m_cache->Insert(MakeKey("incompat_helper_k"), &payload, &m_helperNoSec, true);
    ASSERT_TRUE(s.ok());

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("incompat_helper_k"), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
}

// --------------------------------------------------------------------------
// Lookup with a null helper returns nullptr without touching the cache
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, LookupWithNullHelper)
{
    const std::string keyStr = "null_lookup_key";
    TestPayload payload{"real data"};
    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    bool kept = true; // deliberately true to confirm it is overwritten
    auto handle = m_cache->Lookup(MakeKey(keyStr), /*helper=*/nullptr,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr);
    EXPECT_FALSE(kept);

    // The entry must still be in the cache for a subsequent valid lookup.
    auto handle2 = m_cache->Lookup(MakeKey(keyStr), &m_helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle2, nullptr);
    delete static_cast<TestPayload*>(handle2->Value());
}

// --------------------------------------------------------------------------
// After a corrupt file is rejected on Lookup, the entry is removed from the
// index (second Lookup also returns nullptr) and the file is deleted from disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CorruptedEntryRemovedFromIndexAfterLookup)
{
    const std::string keyStr = "corrupt_cleanup_key";
    TestPayload payload{"data to be corrupted"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    constexpr std::streamoff kFileHeaderSize = 8 + 1 + 1 + 8 + 4;
    {
        std::fstream f(filePath, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(f.is_open()) << "Cache file not found: " << filePath;
        f.seekg(kFileHeaderSize);
        char b = 0;
        f.read(&b, 1);
        f.seekp(kFileHeaderSize);
        f.put(static_cast<char>(~static_cast<unsigned char>(b)));
    }

    // First lookup rejects the corrupt entry.
    bool kept = false;
    auto h1 = m_cache->Lookup(MakeKey(keyStr), &m_helper, nullptr, true, false, nullptr, kept);
    EXPECT_EQ(h1, nullptr);
    EXPECT_FALSE(kept);

    // Second lookup must also miss — the entry must have been removed from the index.
    auto h2 = m_cache->Lookup(MakeKey(keyStr), &m_helper, nullptr, true, false, nullptr, kept);
    EXPECT_EQ(h2, nullptr);
    EXPECT_FALSE(kept);

    // The corrupt file must have been deleted from disk.
    EXPECT_FALSE(std::filesystem::exists(filePath));
}

// --------------------------------------------------------------------------
// When an entry is evicted, its file is deleted from disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EvictedEntryFileIsDeletedFromDisk)
{
    const size_t capacity = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

    const std::string key1 = "evict_disk_k1";
    const std::string key2 = "evict_disk_k2";

    TestPayload p1{"0123456789"}; // 10 bytes — fills capacity
    TestPayload p2{"abcdefghij"}; // 10 bytes — evicts key1

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());

    std::string hex1;
    boost::algorithm::hex_lower(key1.begin(), key1.end(), std::back_inserter(hex1));
    const auto filePath1 = m_cacheDir / hex1.substr(0, 2) / hex1;
    ASSERT_TRUE(std::filesystem::exists(filePath1)) << "key1 file was not written";

    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());

    EXPECT_FALSE(std::filesystem::exists(filePath1)) << "Evicted file should be deleted from disk";
}

// --------------------------------------------------------------------------
// The constructor pre-creates all 256 shard subdirectories (00–ff)
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ShardDirectoriesPreCreated)
{
    const char* kHex = "0123456789abcdef";
    for (int i = 0; i < 256; ++i)
    {
        const char shard[3] = { kHex[i >> 4], kHex[i & 0xf], '\0' };
        EXPECT_TRUE(std::filesystem::is_directory(m_cacheDir / shard))
            << "Missing shard directory: " << shard;
    }
}

// --------------------------------------------------------------------------
// A successful Lookup increments the RocksDB hit statistics counters
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, LookupRecordsHitStatistics)
{
    const std::string keyStr = "stats_key";
    TestPayload payload{"statistics test payload"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    auto stats = rocksdb::CreateDBStatistics();

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, stats.get(), kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());

    EXPECT_EQ(stats->getTickerCount(rocksdb::SECONDARY_CACHE_HITS), 1u);
    EXPECT_EQ(stats->getTickerCount(rocksdb::SECONDARY_CACHE_DATA_HITS), 1u);
}

// --------------------------------------------------------------------------
// An exception thrown by a user callback is caught; Insert returns a non-OK
// status and the cache remains fully operational for subsequent calls
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ExceptionInSaveToCb_Insert_ReturnsError)
{
    // Non-capturing lambda decays to a function pointer.
    static rocksdb::Cache::CacheItemHelper throwingHelperNoSec{
        rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};
    static rocksdb::Cache::CacheItemHelper throwingHelper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        [](rocksdb::Cache::ObjectPtr, size_t, size_t, char*) -> rocksdb::Status {
            throw std::runtime_error("deliberate exception from saveto_cb");
        },
        TestCreateCb,
        &throwingHelperNoSec};

    TestPayload payload{"payload data"};
    auto s = m_cache->Insert(MakeKey("throw_cb_key"), &payload, &throwingHelper, true);
    EXPECT_FALSE(s.ok()) << "Insert must catch the callback exception and return non-OK";

    // Cache must remain operational: a normal insert and lookup must succeed.
    TestPayload p2{"post-throw data"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("post_throw_key"), &p2, &m_helper, true).ok());
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("post_throw_key"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}

// --------------------------------------------------------------------------
// An exception thrown by create_cb during Lookup is caught; nullptr is
// returned and the cache remains fully operational for subsequent calls
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ExceptionInCreateCb_Lookup_ReturnsNull)
{
    static rocksdb::Cache::CacheItemHelper throwingHelperNoSec{
        rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};
    static rocksdb::Cache::CacheItemHelper throwingHelper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        TestSaveToCb,
        [](const rocksdb::Slice&, rocksdb::CompressionType, rocksdb::CacheTier,
           rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*,
           rocksdb::Cache::ObjectPtr*, size_t*) -> rocksdb::Status {
            throw std::runtime_error("deliberate exception from create_cb");
        },
        &throwingHelperNoSec};

    const std::string keyStr = "create_throw_key";
    TestPayload payload{"data for throwing create_cb"};
    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &throwingHelper, true).ok());

    bool kept = true;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &throwingHelper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr) << "Lookup must catch the create_cb exception and return nullptr";
    EXPECT_FALSE(kept);

    // Cache must remain operational after the caught exception.
    TestPayload p2{"post-throw data"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("post_create_throw"), &p2, &m_helper, true).ok());
    auto handle2 = m_cache->Lookup(MakeKey("post_create_throw"), &m_helper,
                                   nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle2, nullptr);
    delete static_cast<TestPayload*>(handle2->Value());
}

// --------------------------------------------------------------------------
// GetUsage reflects the number of bytes currently tracked on disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, GetUsageReflectsCurrentSize)
{
    size_t usage = 999;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u) << "Empty cache must report zero usage";

    // 1 KiB of identical bytes — definitely compressible by zstd.
    const std::string data(1024, 'Z');
    TestPayload p1{data};
    ASSERT_TRUE(m_cache->Insert(MakeKey("usage_k1"), &p1, &m_helper, true).ok());

    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_GT(usage, 0u) << "Usage must be non-zero after an insert";
    EXPECT_LT(usage, data.size()) << "On-disk usage (compressed data + header) must be less than raw payload size for compressible data";

    m_cache->Erase(MakeKey("usage_k1"));

    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u) << "Usage must return to zero after erasing the only entry";
}

// --------------------------------------------------------------------------
// InsertSaved with kNoCompression compresses compressible data — on-disk file
// must be smaller than header + raw payload size
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSavedWithNoCompressionCompressesCompressibleData)
{
    const std::string keyStr = "insertsaved_compress_key";
    // 1 KiB of identical bytes — highly compressible.
    const std::string raw(1024, 'B');
    const rocksdb::Slice saved(raw);

    auto s = m_cache->InsertSaved(MakeKey(keyStr), saved,
                                   rocksdb::CompressionType::kNoCompression,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // 8 magic + 1 version + 1 compressionType + 8 dataSize + 4 checksum = 22 bytes
    constexpr std::uintmax_t kFileHeaderSize = 8 + 1 + 1 + 8 + 4;
    const std::uintmax_t fileSize = std::filesystem::file_size(filePath);

    EXPECT_LT(fileSize, kFileHeaderSize + raw.size())
        << "InsertSaved with kNoCompression should compress compressible data; "
        << "file (" << fileSize << " B) must be smaller than header + raw payload ("
        << kFileHeaderSize + raw.size() << " B)";

    // Round-trip must still produce the original data.
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}

// --------------------------------------------------------------------------
// Concurrent same-key inserts must not double-count m_currentSize.
// Each WriteEntry call uses a unique staging path, so concurrent writes for
// the same key no longer clobber each other's in-progress file.  After all
// threads join, the key must be present and usage must reflect a single entry.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConcurrentSameKeyInsert_UsageAccountedOnce)
{
    const std::string sharedKey = "concurrent_same_key";
    const std::string data(1024, 'C');

    constexpr int kThreadCount = 8;
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (int i = 0; i < kThreadCount; ++i)
    {
        threads.emplace_back([&] {
            TestPayload p{data};
            m_cache->Insert(MakeKey(sharedKey), &p, &m_helper, /*force_insert=*/true);
        });
    }
    for (auto& t : threads)
        t.join();

    // The key must be present: unique staging paths prevent file corruption.
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(sharedKey), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr) << "sharedKey must be present after concurrent same-key inserts";
    delete static_cast<TestPayload*>(handle->Value());

    // Usage must reflect at most one entry, not kThreadCount copies.
    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_LE(usage, FileBasedCompressedSecondaryCache::kFileHeaderSize + data.size())
        << "m_currentSize must not exceed one entry after concurrent same-key inserts "
        << "(usage=" << usage << " B, max=" << FileBasedCompressedSecondaryCache::kFileHeaderSize + data.size() << " B)";
}

// --------------------------------------------------------------------------
// Fix #1 regression: concurrent inserts of different keys must not leave
// m_currentSize above capacity.  Before the fix, each thread could pass
// Phase 1's capacity check independently; Phase 3 now calls
// EvictToCapacityLocked() to correct any such overshoot.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConcurrentDifferentKeyInserts_UsageWithinCapacity)
{
    // 10-byte payloads: at this size zstd adds overhead so entries are stored
    // uncompressed, giving a predictable on-disk size of (kFileHeaderSize + 10) bytes each.
    constexpr size_t kEntrySize = 10;
    constexpr int    kThreadCount = 16;

    // Capacity allows exactly 3 entries; the remaining 13 must be evicted.
    const size_t capacity = 3 * (FileBasedCompressedSecondaryCache::kFileHeaderSize + kEntrySize);
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (int i = 0; i < kThreadCount; ++i)
    {
        threads.emplace_back([&, i] {
            // Each thread gets a distinct 10-character payload.
            TestPayload p{std::string(kEntrySize, static_cast<char>('A' + (i % 26)))};
            m_cache->Insert(MakeKey("cc_diff_" + std::to_string(i)), &p, &m_helper, true);
        });
    }
    for (auto& t : threads)
        t.join();

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_LE(usage, capacity)
        << "m_currentSize must never exceed capacity after concurrent inserts "
        << "(usage=" << usage << " B, capacity=" << capacity << " B)";
}

// --------------------------------------------------------------------------
// Fix #2 regression: concurrent Lookup calls on distinct keys must all
// return correct data without deadlocking or corrupting the LRU state.
// Before the fix, concurrent Lookups serialised on the upgrade_lock; now
// Phase 1 uses shared_lock so all readers proceed simultaneously.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConcurrentLookups_AllReturnCorrectData)
{
    constexpr int kEntryCount  = 8;
    constexpr int kReaderCount = 4; // reader threads per entry

    // Pre-populate the cache with kEntryCount distinct entries.
    std::vector<std::string> keys(kEntryCount);
    std::vector<std::string> expectedData(kEntryCount);
    for (int i = 0; i < kEntryCount; ++i)
    {
        keys[i]         = "conc_lookup_key_" + std::to_string(i);
        expectedData[i] = "payload_" + std::to_string(i) + "_data";
        TestPayload p{expectedData[i]};
        ASSERT_TRUE(m_cache->Insert(MakeKey(keys[i]), &p, &m_helper, true).ok());
    }

    // Each reader thread looks up every key and records hits/misses.
    std::atomic<int> hits{0};
    std::atomic<int> dataErrors{0};

    std::vector<std::thread> threads;
    threads.reserve(kEntryCount * kReaderCount);
    for (int r = 0; r < kReaderCount; ++r)
    {
        for (int i = 0; i < kEntryCount; ++i)
        {
            threads.emplace_back([&, i] {
                bool kept = false;
                auto handle = m_cache->Lookup(MakeKey(keys[i]), &m_helper,
                                              nullptr, true, false, nullptr, kept);
                if (handle)
                {
                    hits.fetch_add(1, std::memory_order_relaxed);
                    auto* result = static_cast<TestPayload*>(handle->Value());
                    if (!result || result->data != expectedData[i])
                        dataErrors.fetch_add(1, std::memory_order_relaxed);
                    delete result;
                }
            });
        }
    }
    for (auto& t : threads)
        t.join();

    EXPECT_EQ(dataErrors.load(), 0)
        << "No concurrent Lookup must return incorrect data";
    EXPECT_GT(hits.load(), 0)
        << "At least some concurrent Lookups must succeed";
}

// --------------------------------------------------------------------------
// InsertSaved with a non-kNoCompression type stores data as-is (fast path)
// and Lookup returns the original bytes via create_cb
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSavedWithPreCompressedData)
{
    const std::string keyStr = "presaved_precompressed_key";
    const std::string raw = "data already in some compressed form";
    const rocksdb::Slice saved(raw);

    // A non-kNoCompression type triggers the fast path in InsertSaved:
    // no re-compression is attempted; data is stored exactly as supplied.
    auto s = m_cache->InsertSaved(MakeKey(keyStr), saved,
                                   rocksdb::CompressionType::kZSTD,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok()) << s.ToString();

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(kept);
    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}

// --------------------------------------------------------------------------
// Erase on a key that was never inserted must be a no-op: no crash,
// no change to usage, and subsequent operations work correctly
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EraseNonExistentKeyIsNoOp)
{
    m_cache->Erase(MakeKey("never_inserted_key"));

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u);

    // Cache remains operational after the no-op erase.
    TestPayload p{"data after no-op erase"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("post_noop_erase_key"), &p, &m_helper, true).ok());
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("post_noop_erase_key"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
}

// --------------------------------------------------------------------------
// WaitAll is a no-op: must not crash on an empty or non-empty handle list
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, WaitAllIsNoOp)
{
    // Empty vector.
    std::vector<rocksdb::SecondaryCacheResultHandle*> noHandles;
    m_cache->WaitAll(noHandles);

    // Vector with a null sentinel (simulates a handle that is already ready).
    std::vector<rocksdb::SecondaryCacheResultHandle*> withNull{nullptr};
    m_cache->WaitAll(withNull);
}

// --------------------------------------------------------------------------
// SupportForceErase returns true: advise_erase is honoured
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, SupportForceEraseReturnsTrue)
{
    EXPECT_TRUE(m_cache->SupportForceErase());
}

// --------------------------------------------------------------------------
// A key whose hex encoding exceeds Entry::kMaxFilenameLen (64 chars = 32 bytes)
// must return InvalidArgument from Insert and InsertSaved; Lookup and Erase
// must silently treat it as a miss/no-op
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, OverlongKeyReturnsInvalidArgument)
{
    // 33 bytes → 66 hex chars > kMaxFilenameLen(64).
    const std::string longKey(33, 'k');

    TestPayload p{"payload"};
    auto sInsert = m_cache->Insert(MakeKey(longKey), &p, &m_helper, true);
    EXPECT_FALSE(sInsert.ok());
    EXPECT_TRUE(sInsert.IsInvalidArgument()) << sInsert.ToString();

    auto sSaved = m_cache->InsertSaved(MakeKey(longKey),
                                        rocksdb::Slice("data"),
                                        rocksdb::CompressionType::kNoCompression,
                                        rocksdb::CacheTier::kVolatileTier);
    EXPECT_FALSE(sSaved.ok());
    EXPECT_TRUE(sSaved.IsInvalidArgument()) << sSaved.ToString();

    // Lookup and Erase must not crash.
    bool kept = true;
    auto handle = m_cache->Lookup(MakeKey(longKey), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr);
    EXPECT_FALSE(kept);

    m_cache->Erase(MakeKey(longKey)); // must be a no-op, not a crash

    // Cache must be fully operational after all the above calls.
    ASSERT_TRUE(m_cache->Insert(MakeKey("normal_key_after_overlong"), &p, &m_helper, true).ok());
}

// --------------------------------------------------------------------------
// Deflate reduces capacity and triggers eviction; Inflate restores it
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, DeflateAndInflateCapacity)
{
    // Fill with two entries of 10 bytes each (stored size = kFileHeaderSize + 10 = 32).
    constexpr size_t kEntryStoredSize = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    const size_t initialCapacity = 2 * kEntryStoredSize; // 64 B: holds both
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, initialCapacity);

    const std::string key1 = "deflate_key1";
    const std::string key2 = "deflate_key2";
    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());

    // Deflate by one entry: key1 (LRU) must be evicted.
    ASSERT_TRUE(m_cache->Deflate(kEntryStoredSize).ok());

    size_t cap = 0;
    ASSERT_TRUE(m_cache->GetCapacity(cap).ok());
    EXPECT_EQ(cap, kEntryStoredSize);

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey(key1), &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    auto h2 = m_cache->Lookup(MakeKey(key2), &m_helper, nullptr, true, false, nullptr, kept);
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
// Name() returns the expected compile-time string
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, Name_ReturnsExpectedString)
{
    EXPECT_STREQ(m_cache->Name(), "FileBasedCompressedSecondaryCache");
}

// --------------------------------------------------------------------------
// InsertSaved with a zero-size slice returns OK without writing any file
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSavedZeroSize_ReturnsOkWithoutInserting)
{
    auto s = m_cache->InsertSaved(MakeKey("zero_size_key"),
                                   rocksdb::Slice{},
                                   rocksdb::CompressionType::kNoCompression,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok());

    bool kept = false;
    EXPECT_EQ(m_cache->Lookup(MakeKey("zero_size_key"), &m_helper,
                               nullptr, true, false, nullptr, kept), nullptr);

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u);
}

// --------------------------------------------------------------------------
// Inflate with SIZE_MAX must clamp to SIZE_MAX (saturating add), not wrap
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, Inflate_SaturationAtSizeMax)
{
    constexpr size_t kInitialCapacity = 1024;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, kInitialCapacity);

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
// After eviction-driven removal, no .del graveyard files must remain on disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EvictedEntryLeavesNoGraveyardFile)
{
    const size_t capacity = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, capacity);

    TestPayload p1{"0123456789"}; // fills capacity exactly
    TestPayload p2{"abcdefghij"}; // evicts p1 via RemoveEntryLocked → rename → delete

    ASSERT_TRUE(m_cache->Insert(MakeKey("grv_evict_k1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("grv_evict_k2"), &p2, &m_helper, true).ok());

    EXPECT_EQ(CountGraveyardFiles(m_cacheDir), 0u)
        << ".del graveyard files must be deleted before Insert returns";
}

// --------------------------------------------------------------------------
// After Erase, no .del graveyard files must remain on disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EraseFileLeavesNoGraveyardFile)
{
    TestPayload p{"data to erase"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("grv_erase_key"), &p, &m_helper, true).ok());

    m_cache->Erase(MakeKey("grv_erase_key"));

    EXPECT_EQ(CountGraveyardFiles(m_cacheDir), 0u)
        << "Erase must not leave .del graveyard files on disk";
}

// --------------------------------------------------------------------------
// After advise_erase Lookup, no .del graveyard files must remain on disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, AdviseEraseLeavesNoGraveyardFile)
{
    TestPayload p{"ephemeral"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("grv_advise_key"), &p, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("grv_advise_key"), &m_helper,
                                   nullptr, true, /*advise_erase=*/true, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());

    EXPECT_EQ(CountGraveyardFiles(m_cacheDir), 0u)
        << "advise_erase must not leave .del graveyard files on disk";
}

// --------------------------------------------------------------------------
// After SetCapacity triggers eviction, no .del graveyard files must remain
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, SetCapacityLeavesNoGraveyardFiles)
{
    constexpr size_t kEntryStoredSize = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("grv_setcap_k1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("grv_setcap_k2"), &p2, &m_helper, true).ok());

    // Shrink to one entry, triggering eviction of the LRU entry.
    ASSERT_TRUE(m_cache->SetCapacity(kEntryStoredSize).ok());

    EXPECT_EQ(CountGraveyardFiles(m_cacheDir), 0u)
        << "SetCapacity must not leave .del graveyard files on disk";
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
    // Capacity holds exactly one 10-byte entry.
    constexpr size_t kEntryStoredSize = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, kEntryStoredSize);

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};

    ASSERT_TRUE(m_cache->Insert(MakeKey("evc_k1"), &p1, &m_helper, true).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 0u) << "No eviction yet";

    // Inserting a second entry must evict the first.
    ASSERT_TRUE(m_cache->Insert(MakeKey("evc_k2"), &p2, &m_helper, true).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 1u) << "One entry evicted by capacity pressure";

    // Shrinking capacity to zero must evict the remaining entry.
    ASSERT_TRUE(m_cache->SetCapacity(0).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 2u) << "Second entry evicted by SetCapacity";
}

// --------------------------------------------------------------------------
// GetEvictedCount increments when Deflate reduces capacity below current usage
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, GetEvictedCount_IncrementsOnDeflate)
{
    constexpr size_t kEntryStoredSize = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, 2 * kEntryStoredSize);

    TestPayload p1{"0123456789"};
    TestPayload p2{"abcdefghij"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("defl_k1"), &p1, &m_helper, true).ok());
    ASSERT_TRUE(m_cache->Insert(MakeKey("defl_k2"), &p2, &m_helper, true).ok());
    EXPECT_EQ(m_cache->GetEvictedCount(), 0u);

    // Deflate by exactly one entry's worth; LRU entry must be evicted.
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

// --------------------------------------------------------------------------
// A non-default zstd level is accepted and produces a correct round-trip
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CustomZstdLevel_RoundTrips)
{
    // Use level 9 (high compression) instead of the default level 1.
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(
        m_cacheDir,
        FileBasedCompressedSecondaryCache::kDefaultCapacity,
        /*zstdLevel=*/9);

    // 1 KiB of repeated bytes — highly compressible, exercises the compression path.
    const std::string raw(1024, 'X');
    TestPayload payload{raw};

    ASSERT_TRUE(m_cache->Insert(MakeKey("zstd_level9_key"), &payload, &m_helper, true).ok());

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("zstd_level9_key"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(kept);

    auto* result = static_cast<TestPayload*>(handle->Value());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->data, raw);
    delete result;
}

