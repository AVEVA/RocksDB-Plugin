// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

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
                                  nullptr, true, false, nullptr, keptInSecCache);

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
// Erase on a key that was never inserted must be a no-op: no crash,
// no change to usage, and subsequent operations work correctly
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EraseNonExistentKeyIsNoOp)
{
    m_cache->Erase(MakeKey("never_inserted_key"));

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u);

    TestPayload p{"data after no-op erase"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("post_noop_erase_key"), &p, &m_helper, true).ok());
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey("post_noop_erase_key"), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<TestPayload*>(handle->Value());
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
// InsertSaved with a non-kNoCompression type stores data as-is (fast path)
// and Lookup returns the original bytes via create_cb.
// The round-trip working proves the data was not mistakenly decompressed.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSavedWithPreCompressedData)
{
    const std::string keyStr = "presaved_precompressed_key";
    const std::string raw = "data already in some compressed form";
    const rocksdb::Slice saved(raw);

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
// InsertSaved with a non-kNoCompression type passes the original compression
// type to create_cb unchanged — the cache must not strip or reclassify it
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSaved_PreCompressed_CreateCbReceivesOriginalCompressionType)
{
    const std::string keyStr = "presaved_type_passthrough_key";
    const std::string raw = "bytes that represent already-compressed data";

    auto s = m_cache->InsertSaved(MakeKey(keyStr), rocksdb::Slice(raw),
                                   rocksdb::CompressionType::kLZ4Compression,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok()) << s.ToString();

    g_capturedCompressionType = rocksdb::CompressionType::kNoCompression; // reset sentinel

    static rocksdb::Cache::CacheItemHelper capturingHelperNoSec{
        rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};
    static rocksdb::Cache::CacheItemHelper capturingHelper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        TestSaveToCb,
        CapturingCreateCb,
        &capturingHelperNoSec};

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &capturingHelper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr) << "Lookup must succeed for a pre-compressed InsertSaved entry";
    delete static_cast<TestPayload*>(handle->Value());

    EXPECT_EQ(g_capturedCompressionType, rocksdb::CompressionType::kLZ4Compression)
        << "create_cb must receive the same compression type that was passed to InsertSaved";
}
