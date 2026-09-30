// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Behavior specific to CacheLibSecondaryCache. Only built with AVEVA_ROCKSDB_WITH_CACHELIB.

#include "SecondaryCacheTestSupport.hpp"

#include "AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp"

#include <rocksdb/statistics.h>

#include <gtest/gtest.h>

#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

using namespace AVEVA::RocksDB::Plugin::Core;
using namespace AVEVA::RocksDB::Plugin::Core::Testing;

namespace {
struct Payload {
    std::string data;
};

size_t SizeCb(rocksdb::Cache::ObjectPtr obj) { return static_cast<Payload*>(obj)->data.size(); }

rocksdb::Status SaveToCb(rocksdb::Cache::ObjectPtr obj, size_t offset, size_t length, char* out) {
    std::memcpy(out, static_cast<Payload*>(obj)->data.data() + offset, length);
    return rocksdb::Status::OK();
}

thread_local rocksdb::CacheTier t_lastSource = rocksdb::CacheTier::kVolatileTier;
thread_local bool t_failCreate = false;

rocksdb::Status CreateCb(const rocksdb::Slice& data, rocksdb::CompressionType, rocksdb::CacheTier source,
                         rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*, rocksdb::Cache::ObjectPtr* out,
                         size_t* charge) {
    t_lastSource = source;
    if (t_failCreate) {
        return rocksdb::Status::Corruption("injected");
    }
    auto* payload = new Payload{data.ToString()};
    *out = payload;
    *charge = payload->data.size();
    return rocksdb::Status::OK();
}

void DeleteCb(rocksdb::Cache::ObjectPtr obj, rocksdb::MemoryAllocator*) { delete static_cast<Payload*>(obj); }

class CacheLibSecondaryCacheTests : public ::testing::Test {
  protected:
    std::filesystem::path m_dir;
    std::unique_ptr<CacheLibSecondaryCache> m_cache;
    rocksdb::Cache::CacheItemHelper m_helperNoSec{rocksdb::CacheEntryRole::kDataBlock, DeleteCb};
    rocksdb::Cache::CacheItemHelper m_helper{rocksdb::CacheEntryRole::kDataBlock, DeleteCb, SizeCb, SaveToCb,
                                             CreateCb, &m_helperNoSec};

    void SetUp() override {
        m_dir = MakeUniqueTempDir("cachelib");
        t_failCreate = false;
    }

    void TearDown() override {
        m_cache.reset();
        std::filesystem::remove_all(m_dir);
    }

    void Open(size_t capacity = 16ULL * 1024 * 1024) {
        m_cache = std::make_unique<CacheLibSecondaryCache>(
            MakeTestOptions(SecondaryCacheBackend::CacheLib, m_dir, capacity));
    }

    rocksdb::Status Put(const std::string& key, const std::string& value) {
        Payload payload{value};
        return m_cache->Insert(key, &payload, &m_helper, false);
    }

    bool Has(const std::string& key) {
        bool kept = false;
        auto handle = m_cache->Lookup(key, &m_helper, nullptr, true, false, nullptr, kept);
        if (!handle) {
            return false;
        }
        delete static_cast<Payload*>(handle->Value());
        return true;
    }

    double Counter(std::string_view name) const { return CacheLibCounter(*m_cache, name); }
};
} // namespace

TEST_F(CacheLibSecondaryCacheTests, NameAndPrintableOptions) {
    Open();
    EXPECT_STREQ(m_cache->Name(), "CacheLibSecondaryCache");
    EXPECT_NE(m_cache->GetPrintableOptions().find("navyConfig::"), std::string::npos);
}

// The file based cache stops admitting new entries once full; CacheLib evicts
// the oldest region instead.
TEST_F(CacheLibSecondaryCacheTests, KeepsAdmittingAfterFull) {
    Open(8ULL * 1024 * 1024);
    const std::string value(64 * 1024, 'x');
    for (int i = 0; i < 1024; ++i) {
        ASSERT_TRUE(Put("fill" + std::to_string(i), value).ok());
        // Navy drops inserts when writes outrun region flushes; pace the burst.
        if (i % 8 == 7) {
            m_cache->Drain();
        }
    }
    m_cache->Drain();

    EXPECT_FALSE(Has("fill0"));
    EXPECT_TRUE(Has("fill1023"));

    size_t usage = 0;
    ASSERT_TRUE(m_cache->GetUsage(usage).ok());
    size_t capacity = 0;
    ASSERT_TRUE(m_cache->GetCapacity(capacity).ok());
    EXPECT_GT(usage, 0U);
    EXPECT_LE(usage, capacity);
}

TEST_F(CacheLibSecondaryCacheTests, DuplicateInsertIsSkipped) {
    Open();
    ASSERT_TRUE(Put("dup", "first").ok());
    m_cache->Drain();
    const double before = Counter("navy_inserts");
    ASSERT_TRUE(Put("dup", "second").ok());
    m_cache->Drain();
    EXPECT_EQ(Counter("navy_inserts"), before);
}

TEST_F(CacheLibSecondaryCacheTests, OversizedEntryIsSkipped) {
    Open();
    ASSERT_TRUE(Put("big", std::string(2 * 1024 * 1024, 'b')).ok());
    m_cache->Drain();
    EXPECT_FALSE(Has("big"));
}

TEST_F(CacheLibSecondaryCacheTests, OverlongKeyIsSkipped) {
    Open();
    const std::string key(300, 'k');
    ASSERT_TRUE(Put(key, "value").ok());
    m_cache->Drain();
    EXPECT_FALSE(Has(key));
}

TEST_F(CacheLibSecondaryCacheTests, EntryStaysAfterHit) {
    Open();
    ASSERT_TRUE(Put("hit", "value").ok());
    m_cache->Drain();

    bool kept = false;
    auto handle = m_cache->Lookup("hit", &m_helper, nullptr, true, /*advise_erase*/ true, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<Payload*>(handle->Value());
    EXPECT_TRUE(kept);
    EXPECT_FALSE(m_cache->SupportForceErase());
    EXPECT_TRUE(Has("hit"));
}

TEST_F(CacheLibSecondaryCacheTests, CreateFailureIsAMissAndDropsEntry) {
    Open();
    ASSERT_TRUE(Put("bad", "value").ok());
    m_cache->Drain();

    t_failCreate = true;
    bool kept = true;
    EXPECT_EQ(m_cache->Lookup("bad", &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    EXPECT_FALSE(kept);
    t_failCreate = false;
    m_cache->Drain();
    EXPECT_FALSE(Has("bad"));
}

TEST_F(CacheLibSecondaryCacheTests, InsertSavedPreservesSourceTier) {
    Open();
    ASSERT_TRUE(
        m_cache->InsertSaved("tier", "value", rocksdb::kNoCompression, rocksdb::CacheTier::kNonVolatileBlockTier)
            .ok());
    m_cache->Drain();

    t_lastSource = rocksdb::CacheTier::kVolatileTier;
    EXPECT_TRUE(Has("tier"));
    EXPECT_EQ(t_lastSource, rocksdb::CacheTier::kNonVolatileBlockTier);
}

TEST_F(CacheLibSecondaryCacheTests, CapacityIsFixed) {
    Open();
    size_t capacity = 0;
    ASSERT_TRUE(m_cache->GetCapacity(capacity).ok());
    EXPECT_GT(capacity, 0U);
    EXPECT_LE(capacity, 16ULL * 1024 * 1024);
    EXPECT_TRUE(m_cache->SetCapacity(1).IsNotSupported());
    EXPECT_TRUE(m_cache->Deflate(1).IsNotSupported());
    EXPECT_TRUE(m_cache->Inflate(1).IsNotSupported());
}

TEST_F(CacheLibSecondaryCacheTests, RecordsNoStatistics) {
    Open();
    ASSERT_TRUE(Put("stats", "value").ok());
    m_cache->Drain();

    auto stats = rocksdb::CreateDBStatistics();
    bool kept = false;
    auto handle = m_cache->Lookup("stats", &m_helper, nullptr, true, false, stats.get(), kept);
    ASSERT_NE(handle, nullptr);
    delete static_cast<Payload*>(handle->Value());
    EXPECT_EQ(stats->getTickerCount(rocksdb::SECONDARY_CACHE_HITS), 0U);
    EXPECT_EQ(stats->getTickerCount(rocksdb::SECONDARY_CACHE_DATA_HITS), 0U);
}

TEST_F(CacheLibSecondaryCacheTests, SingleThreadPoolsWork) {
    auto options = MakeTestOptions(SecondaryCacheBackend::CacheLib, m_dir);
    options.cachelib.readerThreads = 1;
    options.cachelib.writerThreads = 1;
    m_cache = std::make_unique<CacheLibSecondaryCache>(options);
    ASSERT_TRUE(Put("sync", "value").ok());
    m_cache->Drain();
    EXPECT_TRUE(Has("sync"));
}

TEST_F(CacheLibSecondaryCacheTests, LruEvictionPolicyWorks) {
    auto options = MakeTestOptions(SecondaryCacheBackend::CacheLib, m_dir);
    options.cachelib.evictionPolicy = CacheLibSecondaryCacheOptions::EvictionPolicy::Lru;
    m_cache = std::make_unique<CacheLibSecondaryCache>(options);
    ASSERT_TRUE(Put("lru", "value").ok());
    m_cache->Drain();
    EXPECT_TRUE(Has("lru"));
}

TEST_F(CacheLibSecondaryCacheTests, InvalidOptionsAreRejected) {
    auto tooSmall = MakeTestOptions(SecondaryCacheBackend::CacheLib, m_dir, 1024 * 1024);
    EXPECT_THROW(CacheLibSecondaryCache{tooSmall}, std::invalid_argument);

    auto emptyDir = MakeTestOptions(SecondaryCacheBackend::CacheLib, {});
    EXPECT_THROW(CacheLibSecondaryCache{emptyDir}, std::invalid_argument);

    auto badThreads = MakeTestOptions(SecondaryCacheBackend::CacheLib, m_dir);
    badThreads.cachelib.readerThreads = 0;
    EXPECT_THROW(CacheLibSecondaryCache{badThreads}, std::invalid_argument);

    auto badBlock = MakeTestOptions(SecondaryCacheBackend::CacheLib, m_dir);
    badBlock.cachelib.blockSize = 1000;
    EXPECT_THROW(CacheLibSecondaryCache{badBlock}, std::invalid_argument);
}

TEST_F(CacheLibSecondaryCacheTests, OnlyTheCacheFileIsRemovedOnOpen) {
    const auto other = m_dir / "keep.me";
    { std::ofstream(other) << "x"; }
    Open();
    EXPECT_TRUE(std::filesystem::exists(other));
    EXPECT_TRUE(std::filesystem::exists(m_dir / "navy.cache"));
}
