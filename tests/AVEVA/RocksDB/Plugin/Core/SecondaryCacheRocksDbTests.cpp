// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// End-to-end: a real RocksDB instance on the local filesystem whose block cache
// is backed by each available secondary cache backend.

#include "SecondaryCacheTestSupport.hpp"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

using namespace AVEVA::RocksDB::Plugin::Core;
using namespace AVEVA::RocksDB::Plugin::Core::Testing;

namespace {
constexpr int kNumKeys = 2000;
constexpr size_t kValueSize = 1000;

std::string Key(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key%08d", i);
    return buf;
}

std::string Value(int i) {
    std::string value(kValueSize, static_cast<char>('a' + i % 26));
    std::memcpy(value.data(), Key(i).data(), Key(i).size());
    return value;
}

struct PrimaryCacheKind {
    const char* name;
    bool hyperClock;
};

// Stable printing keeps ctest-discovered names independent of pointer values.
[[maybe_unused]] void PrintTo(const PrimaryCacheKind& kind, std::ostream* os) { *os << kind.name; }

class SecondaryCacheRocksDbTests
    : public ::testing::TestWithParam<std::tuple<SecondaryCacheBackend, PrimaryCacheKind>> {
  protected:
    std::filesystem::path m_root;
    std::shared_ptr<rocksdb::SecondaryCache> m_secondary;
    std::shared_ptr<rocksdb::Cache> m_primary;
    std::shared_ptr<rocksdb::Statistics> m_stats;
    std::unique_ptr<rocksdb::DB> m_db;

    void SetUp() override {
        m_root = MakeUniqueTempDir("sec_cache_e2e");
        m_secondary = CreateSecondaryCache(MakeTestOptions(std::get<0>(GetParam()), m_root / "cache"));

        // A primary cache much smaller than the data set forces evictions into the secondary cache.
        constexpr size_t kPrimaryCapacity = 256 * 1024;
        if (std::get<1>(GetParam()).hyperClock) {
            rocksdb::HyperClockCacheOptions options(kPrimaryCapacity, 0, 0);
            options.secondary_cache = m_secondary;
            m_primary = options.MakeSharedCache();
        } else {
            rocksdb::LRUCacheOptions options;
            options.capacity = kPrimaryCapacity;
            options.num_shard_bits = 0;
            options.secondary_cache = m_secondary;
            m_primary = options.MakeSharedCache();
        }

        rocksdb::BlockBasedTableOptions table;
        table.block_cache = m_primary;
        table.block_size = 4096;
        table.cache_index_and_filter_blocks = false;

        rocksdb::Options options;
        options.create_if_missing = true;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
        options.compression = rocksdb::kNoCompression;
        m_stats = rocksdb::CreateDBStatistics();
        options.statistics = m_stats;

        ASSERT_TRUE(rocksdb::DB::Open(options, (m_root / "db").string(), &m_db).ok());

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
        m_primary.reset();
        m_secondary.reset();
        std::filesystem::remove_all(m_root);
    }

    void ReadAll() {
        for (int i = 0; i < kNumKeys; ++i) {
            std::string value;
            ASSERT_TRUE(m_db->Get({}, Key(i), &value).ok()) << i;
            ASSERT_EQ(value, Value(i)) << i;
        }
    }

    uint64_t Ticker(rocksdb::Tickers ticker) const { return m_stats->getTickerCount(ticker); }
};

TEST_P(SecondaryCacheRocksDbTests, ReadsAreCorrectAndServedFromSecondaryCache) {
    if (std::get<0>(GetParam()) == SecondaryCacheBackend::FileBased) {
        // FileBasedCompressedSecondaryCache::Lookup passes kNonVolatileBlockTier to create_cb, which RocksDB's
        // block helpers reject (typed_cache.h), so it never produces a hit. Tracked as a follow-up issue.
        ReadAll();
        GTEST_SKIP() << "FileBased backend never produces RocksDB block hits (known issue)";
    }
    ReadAll();
    Settle(*m_secondary);
    const auto hitsAfterFirstPass = Ticker(rocksdb::SECONDARY_CACHE_HITS);

    ReadAll();
    Settle(*m_secondary);
    ReadAll();

    EXPECT_GT(Ticker(rocksdb::SECONDARY_CACHE_HITS), hitsAfterFirstPass);
}

TEST_P(SecondaryCacheRocksDbTests, MultiGetAsyncReturnsCorrectValues) {
    ReadAll();
    Settle(*m_secondary);

    constexpr size_t kBatch = 64;
    for (size_t start = 0; start < static_cast<size_t>(kNumKeys); start += kBatch) {
        const size_t count = std::min(kBatch, static_cast<size_t>(kNumKeys) - start);
        std::vector<std::string> keyStorage;
        std::vector<rocksdb::Slice> keys;
        keyStorage.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            keyStorage.push_back(Key(static_cast<int>(start + i)));
        }
        for (const auto& key : keyStorage) {
            keys.emplace_back(key);
        }
        std::vector<rocksdb::PinnableSlice> values(count);
        std::vector<rocksdb::Status> statuses(count);
        rocksdb::ReadOptions readOptions;
        readOptions.async_io = true;
        m_db->MultiGet(readOptions, m_db->DefaultColumnFamily(), count, keys.data(), values.data(), statuses.data());
        for (size_t i = 0; i < count; ++i) {
            ASSERT_TRUE(statuses[i].ok()) << statuses[i].ToString();
            ASSERT_EQ(values[i].ToString(), Value(static_cast<int>(start + i)));
        }
    }
}

TEST_P(SecondaryCacheRocksDbTests, SecondaryCacheHitsAreNotDoubleCounted) {
    ReadAll();
    Settle(*m_secondary);
    ReadAll();

    // The adapter records SECONDARY_CACHE_HITS; a backend that also records it
    // makes the per-role breakdown exceed the total.
    const auto total = Ticker(rocksdb::SECONDARY_CACHE_HITS);
    const auto byRole = Ticker(rocksdb::SECONDARY_CACHE_DATA_HITS) + Ticker(rocksdb::SECONDARY_CACHE_INDEX_HITS) +
                        Ticker(rocksdb::SECONDARY_CACHE_FILTER_HITS);
    if (std::get<0>(GetParam()) == SecondaryCacheBackend::CacheLib) {
        EXPECT_EQ(byRole, total);
    } else {
        EXPECT_GE(byRole, total);
    }
}

TEST_P(SecondaryCacheRocksDbTests, SecondaryCacheIsNotRewrittenOnReHit) {
    if (std::get<0>(GetParam()) != SecondaryCacheBackend::CacheLib) {
        GTEST_SKIP() << "Only CacheLib reports insert counters";
    }
    ReadAll();
    Settle(*m_secondary);
    ReadAll();
    Settle(*m_secondary);
    const auto inserts = CacheLibCounter(*m_secondary, "navy_inserts");
    ReadAll();
    Settle(*m_secondary);
    // Blocks already on SSD are kept there, so a further pass writes nothing new.
    EXPECT_EQ(CacheLibCounter(*m_secondary, "navy_inserts"), inserts);
}

std::vector<std::tuple<SecondaryCacheBackend, PrimaryCacheKind>> Params() {
    std::vector<std::tuple<SecondaryCacheBackend, PrimaryCacheKind>> params;
    for (auto backend : AvailableBackends()) {
        params.emplace_back(backend, PrimaryCacheKind{"LRU", false});
        params.emplace_back(backend, PrimaryCacheKind{"HyperClock", true});
    }
    return params;
}

std::string ParamName(const ::testing::TestParamInfo<std::tuple<SecondaryCacheBackend, PrimaryCacheKind>>& info) {
    return BackendName({std::get<0>(info.param), info.index}) + "_" + std::get<1>(info.param).name;
}

INSTANTIATE_TEST_SUITE_P(Backends, SecondaryCacheRocksDbTests, ::testing::ValuesIn(Params()), ParamName);
} // namespace
