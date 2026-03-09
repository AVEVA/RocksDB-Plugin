// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

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
