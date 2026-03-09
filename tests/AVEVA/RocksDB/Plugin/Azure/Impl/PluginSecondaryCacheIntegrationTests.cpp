// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
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
class PluginSecondaryCacheIntegrationTests : public AzureIntegrationTestBase
{
protected:
    std::filesystem::path m_cacheDir;
    std::string           m_pluginName;
    rocksdb::ConfigOptions m_configOptions;
    rocksdb::Env*                    m_env = nullptr;
    std::shared_ptr<rocksdb::Env>    m_envGuard;
    std::string m_dbPath;

    std::string GetBlobNamePrefix() const override { return "plugin-sc-test"; }

    void SetUp() override
    {
        AzureIntegrationTestBase::SetUp();
        if (!m_credentials)
            return; // already GTEST_SKIP'd by the base

        m_cacheDir   = std::filesystem::temp_directory_path() /
                       ("aveva_plugin_sc_test_" + m_credentials->GetDbName());
        m_dbPath     = m_containerPrefix + "/" + m_blobName;
        m_pluginName = std::string(Plugin::Name) + m_credentials->GetDbName();

        std::filesystem::create_directories(m_cacheDir);

        auto status = Plugin::Register(
            m_configOptions,
            &m_env,
            &m_envGuard,
            *m_credentials,
            std::nullopt,
            m_logger,
            Configuration::PageBlob::DefaultBufferSize,
            Configuration::PageBlob::DefaultSize,
            m_cacheDir.string(),
            /*maxCacheSize=*/64ULL * 1024 * 1024);

        if (!status.ok())
            GTEST_SKIP() << "Plugin::Register failed (Azure may be unavailable): "
                         << status.ToString();
    }

    void TearDown() override
    {
        if (m_env)
        {
            rocksdb::Options destroyOpts;
            destroyOpts.env = m_env;
            rocksdb::DestroyDB(m_dbPath, destroyOpts);
        }
        std::filesystem::remove_all(m_cacheDir);
        AzureIntegrationTestBase::TearDown();
    }

    // Opens a RocksDB database at m_dbPath using:
    //   • the Azure plugin env from Plugin::Register
    //   • a fresh FileBasedCompressedSecondaryCache created via CreateFromString
    //   • a 4 KiB primary block cache (forces eviction to the secondary cache)
    rocksdb::DB* OpenDb(bool createIfMissing = true)
    {
        std::shared_ptr<rocksdb::SecondaryCache> secondaryCache;
        auto status = rocksdb::SecondaryCache::CreateFromString(
            m_configOptions, m_pluginName, &secondaryCache);
        if (!status.ok() || !secondaryCache)
            return nullptr;

        // 4 KiB primary block cache — small enough to evict SST blocks to secondary.
        rocksdb::LRUCacheOptions cacheOpts;
        cacheOpts.capacity        = 4 * 1024;
        cacheOpts.secondary_cache = std::move(secondaryCache);

        rocksdb::BlockBasedTableOptions tableOpts;
        tableOpts.block_cache = rocksdb::NewLRUCache(cacheOpts);
        tableOpts.block_size  = 1024; // 1 KiB blocks for fine-grained eviction

        rocksdb::Options opts;
        opts.env               = m_env;
        opts.create_if_missing = createIfMissing;
        opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOpts));

        rocksdb::DB* db = nullptr;
        rocksdb::DB::Open(opts, m_dbPath, &db);
        return db;
    }
};

// ---------------------------------------------------------------------------
// Plugin::Register with a cache path registers a SecondaryCache factory that
// CreateFromString can locate under the plugin name.
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, Register_WithCachePath_RegistersSecondaryCacheFactory)
{
    std::shared_ptr<rocksdb::SecondaryCache> cache;
    auto status = rocksdb::SecondaryCache::CreateFromString(
        m_configOptions, m_pluginName, &cache);

    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(cache, nullptr);
    EXPECT_STREQ(cache->Name(), "FileBasedCompressedSecondaryCache");
}

// ---------------------------------------------------------------------------
// A name that was never registered must not produce a SecondaryCache instance.
// Note: ConfigOptions::ignore_unsupported_options defaults to true, so RocksDB
// returns Status::OK with a null pointer rather than a non-OK status when no
// factory matches.  The meaningful invariant is that cache remains nullptr.
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, CreateFromString_UnknownName_Fails)
{
    std::shared_ptr<rocksdb::SecondaryCache> cache;
    rocksdb::SecondaryCache::CreateFromString(
        m_configOptions, m_pluginName + "_unknown_suffix", &cache);

    EXPECT_EQ(cache, nullptr);
}

// ---------------------------------------------------------------------------
// A RocksDB database can be opened with the Azure plugin env and a secondary
// cache; Put/Flush/Get round-trips succeed end-to-end.
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, OpenDb_PutFlushGet_RoundTrips)
{
    auto* db = OpenDb();
    ASSERT_NE(db, nullptr) << "Failed to open RocksDB with Azure plugin + secondary cache";

    ASSERT_TRUE(db->Put({}, "k1", "value_one").ok());
    ASSERT_TRUE(db->Put({}, "k2", "value_two").ok());
    ASSERT_TRUE(db->Flush({}).ok());

    std::string val;
    ASSERT_TRUE(db->Get({}, "k1", &val).ok());
    EXPECT_EQ(val, "value_one");

    ASSERT_TRUE(db->Get({}, "k2", &val).ok());
    EXPECT_EQ(val, "value_two");

    delete db;
}

// ---------------------------------------------------------------------------
// The secondary cache factory registered by the plugin creates a
// FileBasedCompressedSecondaryCache that writes entry files to m_cacheDir
// when InsertSaved is called — the path used when RocksDB hands pre-serialised
// block data to the secondary tier.
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, SecondaryCache_InsertSaved_WritesFilesToCacheDirectory)
{
    std::shared_ptr<rocksdb::SecondaryCache> secondaryCache;
    auto status = rocksdb::SecondaryCache::CreateFromString(
        m_configOptions, m_pluginName, &secondaryCache);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(secondaryCache, nullptr);

    // Key must be ≤ 32 bytes so its hex representation fits within
    // FileBasedCompressedSecondaryCache::Entry::kMaxFilenameLen (64 chars).
    const std::string keyData(16, '\x42');
    const std::string payload(256, 'X');

    status = secondaryCache->InsertSaved(
        rocksdb::Slice(keyData),
        rocksdb::Slice(payload),
        rocksdb::CompressionType::kNoCompression,
        rocksdb::CacheTier::kVolatileTier);
    ASSERT_TRUE(status.ok()) << status.ToString();

    size_t fileCount = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(m_cacheDir))
        if (entry.is_regular_file())
            ++fileCount;

    EXPECT_GT(fileCount, 0u)
        << "Expected entry files in the secondary cache directory after InsertSaved; "
        << "cache dir: " << m_cacheDir;
}

// ---------------------------------------------------------------------------
// Data written and flushed remains fully readable after the DB is closed and
// reopened with the same secondary cache (primary block cache is cold on reopen).
// ---------------------------------------------------------------------------
TEST_F(PluginSecondaryCacheIntegrationTests, DataReadable_AfterDbReopenWithSameSecondaryCache)
{
    const std::string expectedVal(512, 'Y');

    // First open: write 20 entries and flush them to SST.
    {
        auto* db = OpenDb(/*createIfMissing=*/true);
        ASSERT_NE(db, nullptr);

        for (int i = 0; i < 20; ++i)
            ASSERT_TRUE(db->Put({}, "reopen_key_" + std::to_string(i), expectedVal).ok());

        ASSERT_TRUE(db->Flush({}).ok());
        delete db;
    }

    // Second open: primary cache is cold; reads that miss the primary cache are
    // served from the secondary cache before falling back to the SST on Azure.
    {
        auto* db = OpenDb(/*createIfMissing=*/false);
        ASSERT_NE(db, nullptr) << "Failed to reopen RocksDB after initial write";

        for (int i = 0; i < 20; ++i)
        {
            std::string val;
            ASSERT_TRUE(db->Get({}, "reopen_key_" + std::to_string(i), &val).ok())
                << "Key not found after reopen: reopen_key_" << i;
            EXPECT_EQ(val, expectedVal) << "Data mismatch for reopen_key_" << i;
        }
        delete db;
    }
}
