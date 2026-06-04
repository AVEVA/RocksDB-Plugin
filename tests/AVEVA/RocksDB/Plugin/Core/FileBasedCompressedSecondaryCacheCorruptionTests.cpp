// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

// --------------------------------------------------------------------------
// When an entry is evicted, its file is deleted from disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EvictedEntryFileIsDeletedFromDisk)
{
    const size_t capacity = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, MakeNullLogger());

    const std::string key1 = "evict_disk_k1";
    const std::string key2 = "evict_disk_k2";

    TestPayload p1{"0123456789"}; // 10 bytes — fills capacity
    TestPayload p2{"abcdefghij"}; // 10 bytes — evicts key1

    ASSERT_TRUE(m_cache->Insert(MakeKey(key1), &p1, &m_helper, true).ok());

    std::string hex1;
    boost::algorithm::hex_lower(key1.begin(), key1.end(), std::back_inserter(hex1));
    const auto filePath1 = m_cacheDir / hex1;
    ASSERT_TRUE(std::filesystem::exists(filePath1)) << "key1 file was not written";

    ASSERT_TRUE(m_cache->Insert(MakeKey(key2), &p2, &m_helper, true).ok());

    EXPECT_FALSE(std::filesystem::exists(filePath1)) << "Evicted file should be deleted from disk";
}

// --------------------------------------------------------------------------
// After eviction-driven removal, no .del graveyard files must remain on disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EvictedEntryLeavesNoGraveyardFile)
{
    const size_t capacity = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, MakeNullLogger());

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
// An empty file (0 bytes) cannot provide the 1-byte type prefix; Lookup
// must return nullptr and remove the entry from the index
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, TruncatedFile_RejectedOnLookup)
{
    const std::string keyStr = "truncated_file_key";
    TestPayload payload{"data that will be truncated on disk"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex;

    // Truncate the file to zero bytes so the 1-byte type prefix is missing.
    {
        std::ofstream f(filePath, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(f.is_open());
    }

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr) << "Empty file must cause Lookup to return nullptr";
    EXPECT_FALSE(kept);

    // Entry must have been removed from the index.
    auto handle2 = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                   nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle2, nullptr);
}
