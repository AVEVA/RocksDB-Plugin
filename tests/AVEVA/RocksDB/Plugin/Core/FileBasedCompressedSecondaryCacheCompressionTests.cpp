// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

// --------------------------------------------------------------------------
// Insert stores data as-is: on-disk file size must equal kFileHeaderSize + payload
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, Insert_StoresDataAsIs)
{
    const std::string keyStr = "store_asis_key";
    const std::string raw(1024, 'A');
    TestPayload payload{raw};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex;

    const std::uintmax_t fileSize = std::filesystem::file_size(filePath);
    EXPECT_EQ(fileSize, FileBasedCompressedSecondaryCache::kFileHeaderSize + raw.size())
        << "File must be exactly kFileHeaderSize + payload bytes; no compression is applied";
}

// --------------------------------------------------------------------------
// InsertSaved stores data as-is regardless of the supplied CompressionType
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, InsertSaved_StoresDataAsIs)
{
    const std::string keyStr = "insertsaved_asis_key";
    const std::string raw(1024, 'B');
    const rocksdb::Slice saved(raw);

    auto s = m_cache->InsertSaved(MakeKey(keyStr), saved,
                                   rocksdb::CompressionType::kNoCompression,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex;

    const std::uintmax_t fileSize = std::filesystem::file_size(filePath);
    EXPECT_EQ(fileSize, FileBasedCompressedSecondaryCache::kFileHeaderSize + raw.size())
        << "InsertSaved must store data as-is; no compression is applied by the cache";
}

// --------------------------------------------------------------------------
// The 1-byte compression type prefix is preserved across a round-trip.
// InsertSaved with kSnappyCompression must hand the same type back to create_cb.
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, CompressionType_PreservedOnRoundTrip)
{
    const std::string keyStr = "compression_type_key";
    const std::string raw(64, 'C');
    const rocksdb::Slice saved(raw);

    auto s = m_cache->InsertSaved(MakeKey(keyStr), saved,
                                   rocksdb::CompressionType::kSnappyCompression,
                                   rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // Use CapturingCreateCb to observe what compression type is passed to create_cb.
    g_capturedCompressionType = rocksdb::CompressionType::kNoCompression;
    rocksdb::Cache::CacheItemHelper capturingHelperNoSec{rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};
    rocksdb::Cache::CacheItemHelper capturingHelper{
        rocksdb::CacheEntryRole::kDataBlock,
        TestDeleteCb,
        TestSizeCb,
        TestSaveToCb,
        CapturingCreateCb,
        &capturingHelperNoSec};

    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &capturingHelper,
                                  nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(g_capturedCompressionType, rocksdb::CompressionType::kSnappyCompression)
        << "The compression type stored in the 1-byte prefix must be handed to create_cb unchanged";
    delete static_cast<TestPayload*>(handle->Value());
}
