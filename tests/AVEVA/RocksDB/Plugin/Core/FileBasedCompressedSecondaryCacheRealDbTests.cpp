// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// End-to-end: a real RocksDB instance on the local filesystem (no Azure dependency)
// whose block cache is backed by FileBasedCompressedSecondaryCache. Unlike
// FileBasedCompressedSecondaryCacheRoundTripTests.cpp (which exercises Insert/Lookup
// directly against a test-provided CacheItemHelper), this test drives the cache through
// RocksDB's real block CacheItemHelper (cache/typed_cache.h), which is the only way to
// catch bugs in the CacheTier value passed to create_cb during Lookup().

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>

#include <gtest/gtest.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

using AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache;
using AVEVA::RocksDB::Plugin::Core::LocalFilesystem;

namespace {
constexpr int kNumKeys = 500;
constexpr size_t kValueSize = 1000;

std::string Key(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key%08d", i);
    return buf;
}

std::string Value(int i) {
    std::string value(kValueSize, static_cast<char>('a' + i % 26));
    return value;
}
} // namespace

class FileBasedCompressedSecondaryCacheRealDbTests : public ::testing::Test {
  protected:
    std::filesystem::path m_root;
    std::shared_ptr<rocksdb::Statistics> m_stats;
    std::unique_ptr<rocksdb::DB> m_db;

    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        m_root = std::filesystem::temp_directory_path() / (std::string("filebased_sc_realdb_") + info->name());
        std::filesystem::remove_all(m_root);
        std::filesystem::create_directories(m_root);

        auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
        auto secondaryCache = std::make_shared<FileBasedCompressedSecondaryCache>(
            m_root / "cache", std::make_shared<LocalFilesystem>(logger),
            /*capacity=*/64ULL * 1024 * 1024, logger);

        // A primary cache much smaller than the data set forces evictions into the secondary cache.
        rocksdb::LRUCacheOptions cacheOpts;
        cacheOpts.capacity = 64 * 1024;
        cacheOpts.num_shard_bits = 0;
        cacheOpts.secondary_cache = std::move(secondaryCache);

        rocksdb::BlockBasedTableOptions tableOpts;
        tableOpts.block_cache = rocksdb::NewLRUCache(cacheOpts);
        tableOpts.block_size = 1024; // small blocks for fine-grained eviction
        tableOpts.cache_index_and_filter_blocks = false;

        rocksdb::Options opts;
        opts.create_if_missing = true;
        opts.compression = rocksdb::kNoCompression;
        opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOpts));
        m_stats = rocksdb::CreateDBStatistics();
        opts.statistics = m_stats;

        ASSERT_TRUE(rocksdb::DB::Open(opts, (m_root / "db").string(), &m_db).ok());

        for (int i = 0; i < kNumKeys; ++i) {
            ASSERT_TRUE(m_db->Put({}, Key(i), Value(i)).ok());
        }
        ASSERT_TRUE(m_db->Flush({}).ok());
    }

    void TearDown() override {
        if (m_db) {
            (void)m_db->Close();
            m_db.reset();
        }
        std::filesystem::remove_all(m_root);
    }

    void ReadAll() {
        for (int i = 0; i < kNumKeys; ++i) {
            std::string value;
            ASSERT_TRUE(m_db->Get({}, Key(i), &value).ok()) << i;
            ASSERT_EQ(value, Value(i)) << i;
        }
    }
};

// Reads are correct (always true, even with a broken Lookup(), since RocksDB falls back to
// re-reading the SST on a secondary-cache miss) AND are actually served from the secondary
// cache (only true once Lookup()'s create_cb reports the tier RocksDB's block helper accepts).
TEST_F(FileBasedCompressedSecondaryCacheRealDbTests, ReadsAreCorrectAndServedFromSecondaryCache) {
    ReadAll(); // warms the primary cache; small capacity pushes entries into the secondary cache
    const auto hitsAfterFirstPass = m_stats->getTickerCount(rocksdb::SECONDARY_CACHE_HITS);

    ReadAll();
    ReadAll();

    EXPECT_GT(m_stats->getTickerCount(rocksdb::SECONDARY_CACHE_HITS), hitsAfterFirstPass);
}
