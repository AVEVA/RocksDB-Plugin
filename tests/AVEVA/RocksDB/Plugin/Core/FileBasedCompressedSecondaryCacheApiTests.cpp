// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"

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
// Name() returns the expected compile-time string
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, Name_ReturnsExpectedString)
{
    EXPECT_STREQ(m_cache->Name(), "FileBasedCompressedSecondaryCache");
}

// --------------------------------------------------------------------------
// Constructing a new cache over an existing directory with stale files
// removes them and starts fresh
// --------------------------------------------------------------------------
TEST_F(FileBasedCompressedSecondaryCacheTests, ConstructorCleansStaleDirectory)
{
    const std::string keyStr = "stale_dir_key";
    TestPayload payload{"stale data"};

    ASSERT_TRUE(m_cache->Insert(MakeKey(keyStr), &payload, &m_helper, true).ok());

    // Verify the file exists on disk.
    std::string hex;
    boost::algorithm::hex_lower(keyStr.begin(), keyStr.end(), std::back_inserter(hex));
    const auto filePath = m_cacheDir / hex.substr(0, 2) / hex;
    ASSERT_TRUE(std::filesystem::exists(filePath)) << "Entry file must exist before re-creation";

    // Re-construct the cache over the same directory.
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, m_fs,
        FileBasedCompressedSecondaryCache::kDefaultCapacity,
        FileBasedCompressedSecondaryCache::kDefaultZstdLevel,
        MakeNullLogger());

    // The stale file must have been removed by the constructor.
    EXPECT_FALSE(std::filesystem::exists(filePath))
        << "Constructor must remove stale files from a previous cache instance";

    // The new cache must be empty.
    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    EXPECT_EQ(usage, 0u);

    // Lookup for the old key must miss.
    bool kept = false;
    auto handle = m_cache->Lookup(MakeKey(keyStr), &m_helper,
                                  nullptr, true, false, nullptr, kept);
    EXPECT_EQ(handle, nullptr);

    // The new cache must be fully operational.
    TestPayload p2{"fresh data"};
    ASSERT_TRUE(m_cache->Insert(MakeKey("fresh_key"), &p2, &m_helper, true).ok());
    auto handle2 = m_cache->Lookup(MakeKey("fresh_key"), &m_helper,
                                   nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle2, nullptr);
    delete static_cast<TestPayload*>(handle2->Value());
}
