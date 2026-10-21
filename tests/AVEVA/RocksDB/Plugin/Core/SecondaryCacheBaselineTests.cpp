// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "FileBasedCompressedSecondaryCacheTestHelpers.hpp"
#include "SecondaryCache/BlockDevice.hpp"
#include "SecondaryCache/RecordFormat.hpp"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>

#include <barrier>

namespace Secondary = AVEVA::RocksDB::Plugin::Core::SecondaryCache;
using AVEVA::RocksDB::Plugin::Core::FileBasedSecondaryCacheStats;

namespace {
class SecondaryCacheContractTests : public testing::TestWithParam<FileBasedSecondaryCacheEngine> {
  protected:
    std::filesystem::path directory;
    std::unique_ptr<FileBasedCompressedSecondaryCache> cache;
    rocksdb::Cache::CacheItemHelper noSecondary{rocksdb::CacheEntryRole::kIndexBlock, TestDeleteCb};
    rocksdb::Cache::CacheItemHelper helper{
        rocksdb::CacheEntryRole::kIndexBlock, TestDeleteCb, TestSizeCb, TestSaveToCb, TestCreateCb, &noSecondary};
    void SetUp() override {
        directory =
            MakeTempDir(std::string(testing::UnitTest::GetInstance()->current_test_info()->name()).substr(0, 50) +
                        std::to_string(static_cast<int>(GetParam())));
        FileBasedSecondaryCacheOptions options;
        options.engine = GetParam();
        cache = std::make_unique<FileBasedCompressedSecondaryCache>(directory, options, MakeNullLogger());
    }
    void TearDown() override {
        cache.reset();
        std::filesystem::remove_all(directory);
    }
};
} // namespace

TEST_P(SecondaryCacheContractTests, SavedCompressionChargeReadyHandlesStatsAndAdviseErase) {
    const std::string key("binary\0key", 10);
    helper.create_cb = [](const rocksdb::Slice& bytes, auto type, auto tier, auto*, auto*, auto* out, auto* charge) {
        EXPECT_EQ(type, rocksdb::kZSTD);
        EXPECT_EQ(tier, rocksdb::CacheTier::kNonVolatileBlockTier);
        *out = new TestPayload{bytes.ToString()};
        *charge = 777;
        return rocksdb::Status::OK();
    };
    const auto stats = rocksdb::CreateDBStatistics();
    for (const bool wait : {false, true}) {
        ASSERT_TRUE(cache->InsertSaved(key, "value", rocksdb::kZSTD, rocksdb::CacheTier::kNonVolatileBlockTier).ok());
        bool kept = false;
        auto result = cache->Lookup(key, &helper, nullptr, wait, false, stats.get(), kept);
        ASSERT_NE(result, nullptr);
        EXPECT_TRUE(result->IsReady());
        EXPECT_TRUE(kept);
        EXPECT_EQ(result->Size(), 777u);
        EXPECT_EQ(static_cast<TestPayload*>(result->Value())->data, "value");
        cache->WaitAll({result.get()});
        delete static_cast<TestPayload*>(result->Value());
        result = cache->Lookup(key, &helper, nullptr, wait, true, stats.get(), kept);
        ASSERT_NE(result, nullptr);
        EXPECT_FALSE(kept);
        delete static_cast<TestPayload*>(result->Value());
        kept = true;
        EXPECT_EQ(cache->Lookup(key, &helper, nullptr, wait, false, stats.get(), kept), nullptr);
        EXPECT_FALSE(kept);
    }
    EXPECT_EQ(stats->getTickerCount(rocksdb::SECONDARY_CACHE_INDEX_HITS), 4u);
    EXPECT_EQ(stats->getTickerCount(rocksdb::SECONDARY_CACHE_HITS), 4u);
}

TEST_P(SecondaryCacheContractTests, IncompatibleHelperDoesNotAdmitOrCreate) {
    TestPayload payload{"data"};
    EXPECT_TRUE(cache->Insert("key", &payload, &noSecondary, true).ok());
    bool kept = true;
    EXPECT_EQ(cache->Lookup("key", &noSecondary, nullptr, false, false, nullptr, kept), nullptr);
    EXPECT_FALSE(kept);
}

INSTANTIATE_TEST_SUITE_P(BothEngines, SecondaryCacheContractTests,
                         testing::Values(FileBasedSecondaryCacheEngine::kLegacy,
                                         FileBasedSecondaryCacheEngine::kRegion));

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, InvalidConfigurationPreservesUnrelatedFiles) {
    m_cache.reset();
    const auto sentinel = m_cacheDir / "unrelated";
    std::ofstream(sentinel) << "preserve";
    auto options = MakeOptions();
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, options, nullptr), std::invalid_argument);
    options.admissionPolicy = FileBasedSecondaryCacheAdmissionPolicy::kSecondChance;
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, options, MakeNullLogger()), std::invalid_argument);
    options = MakeOptions();
    options.regionSize = 0;
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, options, MakeNullLogger()), std::invalid_argument);
    options = MakeOptions();
    options.flushBlockSize = 0;
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, options, MakeNullLogger()), std::invalid_argument);
    options = MakeOptions();
    options.maxEntrySize = SIZE_MAX;
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, options, MakeNullLogger()), std::invalid_argument);
    options = MakeOptions();
    options.engine = static_cast<FileBasedSecondaryCacheEngine>(255);
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, options, MakeNullLogger()), std::invalid_argument);
    EXPECT_TRUE(std::filesystem::exists(sentinel));
    options = MakeOptions();
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, options, MakeNullLogger());
    EXPECT_TRUE(std::filesystem::exists(sentinel));
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ColdRestartAndUnexpectedOwnedContentsAreSafe) {
    TestPayload payload{"old value"};
    ASSERT_TRUE(m_cache->Insert("key", &payload, &m_helper, false).ok());
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, MakeOptions(), MakeNullLogger()), std::exception);
    m_cache.reset();
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, MakeOptions(), MakeNullLogger());
    bool kept = true;
    EXPECT_EQ(m_cache->Lookup("key", &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    EXPECT_FALSE(kept);
    m_cache.reset();
    const auto unexpected = m_cacheDir / "ssd-secondary-cache-v1" / "do-not-delete";
    std::ofstream(unexpected) << "not owned";
    EXPECT_THROW(FileBasedCompressedSecondaryCache(m_cacheDir, MakeOptions(), MakeNullLogger()), std::invalid_argument);
    EXPECT_TRUE(std::filesystem::exists(unexpected));
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, DirectoryLinkIsRejectedWithoutTouchingTarget) {
    m_cache.reset();
    const auto target = m_cacheDir / "target";
    std::filesystem::create_directory(target);
    std::ofstream(target / "sentinel") << "preserve";
    const auto link = m_cacheDir / "link";
    std::error_code error;
    std::filesystem::create_directory_symlink(target, link, error);
    if (error) {
        GTEST_SKIP() << "Creating symlinks requires platform permission: " << error.message();
    }
    EXPECT_THROW(FileBasedCompressedSecondaryCache(link, MakeOptions(), MakeNullLogger()), std::invalid_argument);
    EXPECT_TRUE(std::filesystem::exists(target / "sentinel"));
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ForceFalseContinuesRecyclingAndSkipsShortFinalRegion) {
    m_cache.reset();
    auto options = MakeOptions(2 * 512 + 64, 512, 128);
    options.maxEntrySize = 256;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, options, MakeNullLogger());
    TestPayload payload{std::string(128, 'v')};
    for (size_t i = 0; i < 100; ++i) {
        ASSERT_TRUE(m_cache->Insert(MakeKey("key-" + std::to_string(i)), &payload, &m_helper, false).ok());
    }
    bool kept = false;
    EXPECT_EQ(m_cache->Lookup("key-0", &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    auto result = m_cache->Lookup("key-99", &m_helper, nullptr, false, false, nullptr, kept);
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->IsReady());
    EXPECT_EQ(static_cast<TestPayload*>(result->Value())->data, payload.data);
    delete static_cast<TestPayload*>(result->Value());
    FileBasedSecondaryCacheStats stats;
    ASSERT_TRUE(m_cache->GetStats(stats).ok());
    EXPECT_EQ(stats.admitted, 100u);
    EXPECT_GT(stats.reclaims, 0u);
    EXPECT_LE(stats.liveBytes, options.capacity);
    EXPECT_EQ(stats.liveBytes, stats.liveEntries * Secondary::RecordSize(6, 128));
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, MemoryPressureAndOversizeAreBoundedObservableDrops) {
    m_cache.reset();
    auto options = MakeOptions();
    options.metadataBudget = 4096;
    options.transientBudget = 512;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, options, MakeNullLogger());
    TestPayload large{std::string(1024, 'x')};
    EXPECT_TRUE(m_cache->Insert("pressure", &large, &m_helper, true).ok());
    TestPayload oversized{std::string(options.maxEntrySize, 'x')};
    auto helper = m_helper;
    helper.saveto_cb = [](auto, auto, auto, auto) {
        ADD_FAILURE() << "Oversized entry must be rejected before serialization";
        return rocksdb::Status::Aborted();
    };
    EXPECT_TRUE(m_cache->Insert("oversized", &oversized, &helper, true).ok());
    TestPayload tiny{"x"};
    for (size_t i = 0; i < 10000; ++i) {
        ASSERT_TRUE(m_cache->Insert(MakeKey(std::to_string(i)), &tiny, &m_helper, false).ok());
    }
    FileBasedSecondaryCacheStats stats;
    ASSERT_TRUE(m_cache->GetStats(stats).ok());
    EXPECT_LE(stats.metadataBytes, options.metadataBudget);
    EXPECT_EQ(stats.transientBytes, 0u);
    EXPECT_GT(stats.droppedPressure, 0u);
    EXPECT_EQ(stats.droppedTooLarge, 1u);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ZeroCapacityUsesNoDataBuffersAndResizeResetsEntries) {
    TestPayload payload{"value"};
    for (const size_t capacity : {size_t{0}, size_t{4096}, size_t{512}, size_t{1024 * 1024}, size_t{0}}) {
        ASSERT_TRUE(m_cache->SetCapacity(capacity).ok());
        FileBasedSecondaryCacheStats stats;
        ASSERT_TRUE(m_cache->GetStats(stats).ok());
        EXPECT_EQ(stats.capacityBytes, capacity);
        EXPECT_EQ(stats.liveBytes, 0u);
        EXPECT_EQ(stats.liveEntries, 0u);
        if (capacity == 0) {
            EXPECT_EQ(stats.activeBufferBytes, 0u);
            EXPECT_EQ(stats.openHandles, 0u);
        }
        ASSERT_TRUE(m_cache->Insert("key", &payload, &m_helper, false).ok());
    }
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests,
       InvalidResizePreservesEntriesAndDeviceFailureDisablesWithIoStatus) {
    TestPayload payload{"value"};
    ASSERT_TRUE(m_cache->Insert("key", &payload, &m_helper, false).ok());
    EXPECT_TRUE(m_cache->SetCapacity(SIZE_MAX).IsInvalidArgument());
    FileBasedSecondaryCacheStats stats;
    ASSERT_TRUE(m_cache->GetStats(stats).ok());
    EXPECT_EQ(stats.liveEntries, 1u);
    const auto unexpected = m_cacheDir / "ssd-secondary-cache-v1" / "not-owned";
    std::ofstream(unexpected) << "preserve";
    EXPECT_TRUE(m_cache->SetCapacity(4096).IsIOError());
    ASSERT_TRUE(m_cache->GetStats(stats).ok());
    EXPECT_EQ(stats.liveEntries, 0u);
    EXPECT_EQ(stats.liveBytes, 0u);
    EXPECT_EQ(stats.capacityBytes, 0u);
    EXPECT_GT(stats.ioErrors, 0u);
    EXPECT_TRUE(std::filesystem::exists(unexpected));
    std::filesystem::remove(unexpected);
    EXPECT_TRUE(m_cache->SetCapacity(4096).ok());
}

namespace {
struct ReentrantContext : rocksdb::Cache::CreateContext {
    FileBasedCompressedSecondaryCache* cache;
    rocksdb::Cache::CacheItemHelper* helper;
    bool resize;
};

rocksdb::Status ReentrantCreate(const rocksdb::Slice& bytes, rocksdb::CompressionType, rocksdb::CacheTier,
                                rocksdb::Cache::CreateContext* context, rocksdb::MemoryAllocator*,
                                rocksdb::Cache::ObjectPtr* out, size_t* charge) {
    auto& state = *static_cast<ReentrantContext*>(context);
    if (state.resize) {
        EXPECT_TRUE(state.cache->SetCapacity(8 * 1024 * 1024).ok());
    }
    TestPayload replacement{"replacement"};
    EXPECT_TRUE(state.cache->Insert("key", &replacement, state.helper, false).ok());
    *out = new TestPayload{bytes.ToString()};
    *charge = 999;
    return rocksdb::Status::OK();
}
} // namespace

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, CallbackReplacementSurvivesAdviseEraseAndResize) {
    for (const bool resize : {false, true}) {
        TestPayload original{"original"};
        ASSERT_TRUE(m_cache->Insert("key", &original, &m_helper, false).ok());
        auto helper = m_helper;
        helper.create_cb = ReentrantCreate;
        ReentrantContext context{{}, m_cache.get(), &m_helper, resize};
        bool kept = true;
        auto first = m_cache->Lookup("key", &helper, &context, false, true, nullptr, kept);
        ASSERT_NE(first, nullptr);
        EXPECT_FALSE(kept);
        EXPECT_EQ(first->Size(), 999u);
        EXPECT_EQ(static_cast<TestPayload*>(first->Value())->data, "original");
        delete static_cast<TestPayload*>(first->Value());
        auto second = m_cache->Lookup("key", &m_helper, nullptr, true, false, nullptr, kept);
        ASSERT_NE(second, nullptr);
        EXPECT_EQ(static_cast<TestPayload*>(second->Value())->data, "replacement");
        delete static_cast<TestPayload*>(second->Value());
    }
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, FailedCreateOwnsItsFailureOutputAndNullDeleterIsAllowed) {
    TestPayload payload{"value"};
    ASSERT_TRUE(m_cache->Insert("key", &payload, &m_helper, false).ok());
    auto helper = m_helper;
    helper.create_cb = [](const auto&, auto, auto, auto*, auto*, auto* out, auto*) {
        *out = reinterpret_cast<void*>(uintptr_t{1}); // Caller must ignore this on failure.
        return rocksdb::Status::Corruption();
    };
    helper.del_cb = [](auto, auto*) { ADD_FAILURE() << "Failure output was incorrectly deleted"; };
    bool kept = true;
    EXPECT_EQ(m_cache->Lookup("key", &helper, nullptr, true, false, nullptr, kept), nullptr);
    EXPECT_FALSE(kept);
    helper.create_cb = [](const auto&, auto, auto, auto*, auto*, auto* out, auto* charge) {
        static int value = 42;
        *out = &value;
        *charge = sizeof(value);
        return rocksdb::Status::OK();
    };
    helper.del_cb = nullptr;
    auto handle = m_cache->Lookup("key", &helper, nullptr, true, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(*static_cast<int*>(handle->Value()), 42);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, ThrowingCallbacksReturnErrorOrMissRatherThanTerminate) {
    TestPayload payload{"value"};
    auto helper = m_helper;
    helper.size_cb = [](auto) -> size_t { throw std::bad_alloc(); };
    EXPECT_TRUE(m_cache->Insert("key", &payload, &helper, false).IsMemoryLimit());
    helper = m_helper;
    helper.saveto_cb = [](auto, auto, auto, auto) -> rocksdb::Status { throw std::bad_alloc(); };
    EXPECT_TRUE(m_cache->Insert("key", &payload, &helper, false).IsMemoryLimit());
    ASSERT_TRUE(m_cache->Insert("key", &payload, &m_helper, false).ok());
    helper = m_helper;
    helper.create_cb = [](const auto&, auto, auto, auto*, auto*, auto*, auto*) -> rocksdb::Status {
        throw std::bad_alloc();
    };
    bool kept = true;
    EXPECT_EQ(m_cache->Lookup("key", &helper, nullptr, false, false, nullptr, kept), nullptr);
    EXPECT_FALSE(kept);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, EmptySavedValueBinaryLongKeyAndTierArePreserved) {
    const std::string key(1024, '\0');
    auto helper = m_helper;
    helper.create_cb = [](const rocksdb::Slice& bytes, auto compression, auto tier, auto*, auto*, auto* out,
                          auto* charge) {
        EXPECT_EQ(bytes.size(), 0u);
        EXPECT_EQ(compression, rocksdb::kZSTD);
        EXPECT_EQ(tier, rocksdb::CacheTier::kNonVolatileBlockTier);
        *out = new TestPayload{""};
        *charge = 567;
        return rocksdb::Status::OK();
    };
    ASSERT_TRUE(m_cache->InsertSaved(key, "", rocksdb::kZSTD, rocksdb::CacheTier::kNonVolatileBlockTier).ok());
    bool kept = false;
    auto handle = m_cache->Lookup(key, &helper, nullptr, false, false, nullptr, kept);
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(handle->Size(), 567u);
    delete static_cast<TestPayload*>(handle->Value());
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, FileHandlePoolRemainsBoundedAcrossMoreThan512Regions) {
    m_cache.reset();
    auto options = MakeOptions(600 * 256, 256, 64);
    options.maxEntrySize = 256;
    m_cache = std::make_unique<FileBasedCompressedSecondaryCache>(m_cacheDir, options, MakeNullLogger());
    TestPayload payload{std::string(192, 'x')}; // Aligned record is exactly one region.
    for (size_t i = 0; i < 600; ++i) {
        ASSERT_TRUE(m_cache->Insert(MakeKey("key-" + std::to_string(i)), &payload, &m_helper, false).ok());
    }
    for (const auto key : {"key-0", "key-599", "key-250"}) {
        bool kept = false;
        auto handle = m_cache->Lookup(key, &m_helper, nullptr, true, false, nullptr, kept);
        ASSERT_NE(handle, nullptr);
        EXPECT_EQ(static_cast<TestPayload*>(handle->Value())->data, payload.data);
        delete static_cast<TestPayload*>(handle->Value());
    }
    FileBasedSecondaryCacheStats stats;
    ASSERT_TRUE(m_cache->GetStats(stats).ok());
    EXPECT_LE(stats.openHandles, Secondary::FileBlockDevice::kHandleLimit);
    EXPECT_EQ(stats.liveEntries, 600u);
    EXPECT_EQ(stats.activeBufferBytes, options.flushBlockSize);
    EXPECT_EQ(stats.fileBytes, 600u * 256);
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, DefaultLruAdapterKeepsAdmittingAfterMultipleDiskTurnovers) {
    m_cache.reset();
    auto options = MakeOptions(4096, 1024, 128);
    options.maxEntrySize = 512;
    auto secondary = std::make_shared<FileBasedCompressedSecondaryCache>(m_cacheDir, options, MakeNullLogger());
    rocksdb::LRUCacheOptions lru;
    lru.capacity = 512;
    lru.num_shard_bits = 0;
    lru.secondary_cache = secondary;
    auto primary = rocksdb::NewLRUCache(lru);
    for (size_t i = 0; i < 300; ++i) {
        auto* payload = new TestPayload{std::string(256, 'v')};
        ASSERT_TRUE(primary->Insert(MakeKey("key-" + std::to_string(i)), payload, &m_helper, 256).ok());
    }
    FileBasedSecondaryCacheStats stats;
    ASSERT_TRUE(secondary->GetStats(stats).ok());
    EXPECT_GT(stats.admitted, 100u);
    EXPECT_GT(stats.reclaims, 0u);
    bool kept = false;
    EXPECT_EQ(secondary->Lookup("key-0", &m_helper, nullptr, true, false, nullptr, kept), nullptr);
    size_t recentHits = 0;
    for (size_t i = 285; i < 299; ++i) {
        if (auto* handle = primary->Lookup(MakeKey("key-" + std::to_string(i)), &m_helper)) {
            EXPECT_EQ(static_cast<TestPayload*>(primary->Value(handle))->data, std::string(256, 'v'));
            ++recentHits;
            primary->Release(handle);
        }
    }
    EXPECT_GT(recentHits, 0u);
    primary.reset(); // Helper and secondary outlive eviction/destruction callbacks.
}

TEST_F(RegionFileBasedCompressedSecondaryCacheTests, LocalRocksDbReturnsCorrectDataWithSecondaryHits) {
    m_cache.reset();
    auto secondary = std::make_shared<FileBasedCompressedSecondaryCache>(m_cacheDir, MakeOptions(), MakeNullLogger());
    rocksdb::LRUCacheOptions lru;
    lru.capacity = 16 * 1024;
    lru.num_shard_bits = 0;
    lru.secondary_cache = secondary;
    rocksdb::BlockBasedTableOptions table;
    table.block_cache = rocksdb::NewLRUCache(lru);
    table.block_size = 1024;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::kNoCompression;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
    options.statistics = rocksdb::CreateDBStatistics();
    const auto database = m_cacheDir / "database";
    std::unique_ptr<rocksdb::DB> db;
    ASSERT_TRUE(rocksdb::DB::Open(options, database.string(), &db).ok());
    for (size_t i = 0; i < 200; ++i) {
        ASSERT_TRUE(
            db->Put(rocksdb::WriteOptions(), "key-" + std::to_string(i), std::string(1024, char('a' + i % 20))).ok());
    }
    ASSERT_TRUE(db->Flush(rocksdb::FlushOptions()).ok());
    for (size_t pass = 0; pass < 3; ++pass) {
        for (size_t i = 0; i < 200; ++i) {
            std::string value;
            ASSERT_TRUE(db->Get(rocksdb::ReadOptions(), "key-" + std::to_string(i), &value).ok());
            EXPECT_EQ(value, std::string(1024, char('a' + i % 20)));
        }
    }
    FileBasedSecondaryCacheStats stats;
    ASSERT_TRUE(secondary->GetStats(stats).ok());
    EXPECT_GT(stats.admitted, 0u);
    EXPECT_GT(stats.hits, 0u);
    EXPECT_GT(options.statistics->getTickerCount(rocksdb::SECONDARY_CACHE_HITS), 0u);
}
