// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RegionBasedSecondaryCache.hpp"
#include "IntegrationTestHelpers.hpp"

#include <rocksdb/cache.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/secondary_cache.h>
#include <rocksdb/table.h>

#include <filesystem>
#include <memory>
#include <string>

using AVEVA::RocksDB::Plugin::Azure::Plugin;
using AVEVA::RocksDB::Plugin::Azure::Impl::Configuration;
using AVEVA::RocksDB::Plugin::Azure::Impl::Testing::AzureIntegrationTestBase;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------
class PluginSecondaryCacheIntegrationTests : public AzureIntegrationTestBase {
  protected:
    std::filesystem::path m_cacheDir;
    rocksdb::Env* m_env = nullptr;
    std::shared_ptr<rocksdb::Env> m_envGuard;
    std::string m_dbPath;

    std::string GetBlobNamePrefix() const override { return "plugin-sc-test"; }

    void SetUp() override {
        AzureIntegrationTestBase::SetUp();
        if (!m_credentials)
            return; // already GTEST_SKIP'd by the base

        m_cacheDir = std::filesystem::temp_directory_path() / ("aveva_plugin_sc_test_" + m_credentials->GetDbName());
        m_dbPath = m_containerPrefix + "/" + m_blobName;

        std::filesystem::create_directories(m_cacheDir);

        rocksdb::ConfigOptions configOptions;
        auto status =
            Plugin::Register(configOptions, &m_env, &m_envGuard, *m_credentials, std::nullopt, m_logger,
                             Configuration::PageBlob::DefaultBufferSize, Configuration::PageBlob::DefaultSize);

        if (!status.ok())
            GTEST_SKIP() << "Plugin::Register failed (Azure may be unavailable): " << status.ToString();
    }

    void TearDown() override {
        if (m_env) {
            rocksdb::Options destroyOpts;
            destroyOpts.env = m_env;
            rocksdb::DestroyDB(m_dbPath, destroyOpts);
        }
        std::filesystem::remove_all(m_cacheDir);
        AzureIntegrationTestBase::TearDown();
    }

    // Opens a RocksDB database at m_dbPath using:
    //   • the Azure plugin env from Plugin::Register
    //   • a fresh region-based secondary cache
    //   • a 4 KiB primary block cache (forces eviction to the secondary cache)
    std::unique_ptr<rocksdb::DB> OpenDb(bool createIfMissing = true) {
        AVEVA::RocksDB::Plugin::Core::RegionBasedSecondaryCacheOptions secondaryOptions;
        secondaryOptions.capacity = 64ULL * 1024 * 1024;
        auto secondaryCache = std::make_shared<AVEVA::RocksDB::Plugin::Core::RegionBasedSecondaryCache>(
            m_cacheDir, secondaryOptions, m_logger);

        // 4 KiB primary block cache — small enough to evict SST blocks to secondary.
        rocksdb::LRUCacheOptions cacheOpts;
        cacheOpts.capacity = 4 * 1024;
        cacheOpts.secondary_cache = std::move(secondaryCache);

        rocksdb::BlockBasedTableOptions tableOpts;
        tableOpts.block_cache = rocksdb::NewLRUCache(cacheOpts);
        tableOpts.block_size = 1024; // 1 KiB blocks for fine-grained eviction

        rocksdb::Options opts;
        opts.env = m_env;
        opts.create_if_missing = createIfMissing;
        opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOpts));

        std::unique_ptr<rocksdb::DB> db;
        rocksdb::DB::Open(opts, m_dbPath, &db);
        return db;
    }
};

// ---------------------------------------------------------------------------
// A RocksDB database can be opened with the Azure plugin env and a secondary
// cache; Put/Flush/Get round-trips succeed end-to-end.
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, OpenDb_PutFlushGet_RoundTrips) {
    auto db = OpenDb();
    ASSERT_NE(db, nullptr) << "Failed to open RocksDB with Azure plugin + secondary cache";

    ASSERT_TRUE(db->Put({}, "k1", "value_one").ok());
    ASSERT_TRUE(db->Put({}, "k2", "value_two").ok());
    ASSERT_TRUE(db->Flush({}).ok());

    std::string val;
    ASSERT_TRUE(db->Get({}, "k1", &val).ok());
    EXPECT_EQ(val, "value_one");

    ASSERT_TRUE(db->Get({}, "k2", &val).ok());
    EXPECT_EQ(val, "value_two");
}

// ---------------------------------------------------------------------------
// A RegionBasedSecondaryCache writes region files under m_cacheDir
// when InsertSaved is called — the path used when RocksDB hands pre-serialised
// block data to the secondary tier.
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, SecondaryCache_InsertSaved_WritesRegionFilesToCacheDirectory) {
    AVEVA::RocksDB::Plugin::Core::RegionBasedSecondaryCacheOptions secondaryOptions;
    secondaryOptions.capacity = 64ULL * 1024 * 1024;
    secondaryOptions.flushBlockSize = 64; // Force this record to disk instead of the append buffer.
    auto secondaryCache = std::make_shared<AVEVA::RocksDB::Plugin::Core::RegionBasedSecondaryCache>(
        m_cacheDir, secondaryOptions, m_logger);

    const std::string keyData(16, '\x42');
    const std::string payload(256, 'X');

    auto status =
        secondaryCache->InsertSaved(rocksdb::Slice(keyData), rocksdb::Slice(payload),
                                    rocksdb::CompressionType::kNoCompression, rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(status.ok()) << status.ToString();

    size_t fileCount = 0;
    for (const auto& entry : std::filesystem::directory_iterator(m_cacheDir / "ssd-secondary-cache-v1")) {
        if (entry.is_regular_file() && entry.path().filename().string().starts_with("region_") &&
            entry.path().extension() == ".dat" && entry.file_size() > 0) {
            ++fileCount;
        }
    }

    EXPECT_GT(fileCount, 0u) << "Expected region files in the secondary cache directory after InsertSaved; "
                             << "cache dir: " << m_cacheDir;
}

// ---------------------------------------------------------------------------
// Data written and flushed remains fully readable after the DB is closed and
// reopened with a new, cold secondary cache.
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, DataReadable_AfterDbReopenWithColdSecondaryCache) {
    const std::string expectedVal(512, 'Y');

    // First open: write 20 entries and flush them to SST.
    {
        auto db = OpenDb(/*createIfMissing=*/true);
        ASSERT_NE(db, nullptr);

        for (int i = 0; i < 20; ++i)
            ASSERT_TRUE(db->Put({}, "reopen_key_" + std::to_string(i), expectedVal).ok());

        ASSERT_TRUE(db->Flush({}).ok());
    }

    // Both cache tiers start cold; reads fall back to the SST on Azure.
    {
        auto db = OpenDb(/*createIfMissing=*/false);
        ASSERT_NE(db, nullptr) << "Failed to reopen RocksDB after initial write";

        for (int i = 0; i < 20; ++i) {
            std::string val;
            ASSERT_TRUE(db->Get({}, "reopen_key_" + std::to_string(i), &val).ok())
                << "Key not found after reopen: reopen_key_" << i;
            EXPECT_EQ(val, expectedVal) << "Data mismatch for reopen_key_" << i;
        }
    }
}
