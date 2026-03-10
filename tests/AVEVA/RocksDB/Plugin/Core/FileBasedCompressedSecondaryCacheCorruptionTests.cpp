// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

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
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

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
// After eviction-driven removal, no .del graveyard files must remain on disk
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, EvictedEntryLeavesNoGraveyardFile)
{
    const size_t capacity = FileBasedCompressedSecondaryCache::kFileHeaderSize + 10;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs, capacity, FileBasedCompressedSecondaryCache::kDefaultZstdLevel, MakeNullLogger());

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
// Corrupting the magic bytes in the file header causes Lookup to return
// nullptr and removes the entry from the index
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CorruptMagicNumber_RejectedOnLookup)
{
    const std::string keyStr = "corrupt_magic_key";
    TestPayload payload{"data for magic corruption test"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // Flip a byte in the magic field (offset 0).
    {
        std::fstream f(filePath, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(f.is_open()) << "Cache file not found: " << filePath;
        f.seekg(0);
        char b = 0;
        f.read(&b, 1);
        f.seekp(0);
        f.put(static_cast<char>(~static_cast<unsigned char>(b)));
    }

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr) << "Corrupt magic must cause Lookup to return nullptr";
    EXPECT_FALSE(kept);

    // Entry must have been removed from the index.
    auto handle2 = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                   nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle2, nullptr);
}

// --------------------------------------------------------------------------
// Corrupting the version byte in the file header causes Lookup to return
// nullptr and removes the entry from the index
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CorruptVersionByte_RejectedOnLookup)
{
    const std::string keyStr = "corrupt_version_key";
    TestPayload payload{"data for version corruption test"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // The version byte is at offset 8 (after the 8-byte magic).
    constexpr std::streamoff kVersionOffset = 8;
    {
        std::fstream f(filePath, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(f.is_open()) << "Cache file not found: " << filePath;
        f.seekp(kVersionOffset);
        // Write an invalid version (0xFF) instead of the expected version (1).
        f.put(static_cast<char>(0xFF));
    }

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr) << "Corrupt version must cause Lookup to return nullptr";
    EXPECT_FALSE(kept);

    // Entry must have been removed from the index.
    auto handle2 = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                   nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle2, nullptr);
}

// --------------------------------------------------------------------------
// A truncated file (contains only the header, no payload data) causes
// Lookup to return nullptr and removes the entry from the index
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, TruncatedFile_RejectedOnLookup)
{
    const std::string keyStr = "truncated_file_key";
    TestPayload payload{"data that will be truncated on disk"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;

    // Read the original 22-byte header from the valid file.
    constexpr std::uintmax_t kHeaderSize = 22;
    std::array<char, kHeaderSize> headerBuf{};
    {
        std::ifstream f(filePath, std::ios::binary);
        ASSERT_TRUE(f.is_open());
        f.read(headerBuf.data(), kHeaderSize);
        ASSERT_TRUE(f.good());
    }

    // Rewrite the file with only the header (no payload).
    // The header's dataSize field still claims a non-zero payload, which will
    // fail the (dataSize + headerSize > mappedSize) validation in Lookup.
    {
        std::ofstream f(filePath, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(f.is_open());
        f.write(headerBuf.data(), kHeaderSize);
    }

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr) << "Truncated file must cause Lookup to return nullptr";
    EXPECT_FALSE(kept);

    // Entry must have been removed from the index.
    auto handle2 = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                   nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle2, nullptr);
}
