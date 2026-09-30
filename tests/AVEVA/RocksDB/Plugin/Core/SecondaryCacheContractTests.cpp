// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Behavior every secondary cache backend must provide, run against each
// backend available in this build.

#include "SecondaryCacheTestSupport.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace AVEVA::RocksDB::Plugin::Core;
using namespace AVEVA::RocksDB::Plugin::Core::Testing;

namespace {
struct Payload {
    std::string data;
};

size_t SizeCb(rocksdb::Cache::ObjectPtr obj) { return static_cast<Payload*>(obj)->data.size(); }

rocksdb::Status SaveToCb(rocksdb::Cache::ObjectPtr obj, size_t offset, size_t length, char* out) {
    const auto& data = static_cast<Payload*>(obj)->data;
    if (offset + length > data.size()) {
        return rocksdb::Status::InvalidArgument("out of range");
    }
    std::memcpy(out, data.data() + offset, length);
    return rocksdb::Status::OK();
}

thread_local rocksdb::CompressionType t_lastType = rocksdb::kNoCompression;

rocksdb::Status CreateCb(const rocksdb::Slice& data, rocksdb::CompressionType type, rocksdb::CacheTier,
                         rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*, rocksdb::Cache::ObjectPtr* out,
                         size_t* charge) {
    t_lastType = type;
    auto* payload = new Payload{data.ToString()};
    *out = payload;
    *charge = payload->data.size();
    return rocksdb::Status::OK();
}

void DeleteCb(rocksdb::Cache::ObjectPtr obj, rocksdb::MemoryAllocator*) { delete static_cast<Payload*>(obj); }

std::string ValueFor(const std::string& key, size_t size = 3000) {
    std::string value;
    value.reserve(size);
    while (value.size() < size) {
        value += key;
        value += '|';
    }
    value.resize(size);
    return value;
}

std::string KeyFor(size_t i) {
    char buf[17];
    std::snprintf(buf, sizeof(buf), "k%015zu", i);
    return std::string(buf, 16);
}

std::string TakeValue(rocksdb::SecondaryCacheResultHandle& handle) {
    auto* payload = static_cast<Payload*>(handle.Value());
    std::string data = payload ? payload->data : std::string{};
    delete payload;
    return data;
}

class SecondaryCacheContractTests : public ::testing::TestWithParam<SecondaryCacheBackend> {
  protected:
    std::filesystem::path m_dir;
    std::shared_ptr<rocksdb::SecondaryCache> m_cache;
    rocksdb::Cache::CacheItemHelper m_helperNoSec{rocksdb::CacheEntryRole::kDataBlock, DeleteCb};
    rocksdb::Cache::CacheItemHelper m_helper{rocksdb::CacheEntryRole::kDataBlock, DeleteCb, SizeCb, SaveToCb,
                                             CreateCb, &m_helperNoSec};

    void SetUp() override {
        m_dir = MakeUniqueTempDir("sec_cache_contract");
        m_cache = CreateSecondaryCache(MakeTestOptions(GetParam(), m_dir));
    }

    void TearDown() override {
        m_cache.reset();
        std::filesystem::remove_all(m_dir);
    }

    void Put(const std::string& key, const std::string& value) {
        Payload payload{value};
        ASSERT_TRUE(m_cache->Insert(key, &payload, &m_helper, false).ok());
    }

    std::unique_ptr<rocksdb::SecondaryCacheResultHandle> Get(const std::string& key, bool wait = true) {
        bool kept = false;
        return m_cache->Lookup(key, &m_helper, nullptr, wait, false, nullptr, kept);
    }
};

TEST_P(SecondaryCacheContractTests, RoundTripSynchronousLookup) {
    Put(KeyFor(1), ValueFor(KeyFor(1)));
    Settle(*m_cache);

    auto handle = Get(KeyFor(1));
    ASSERT_NE(handle, nullptr);
    EXPECT_TRUE(handle->IsReady());
    EXPECT_EQ(handle->Size(), ValueFor(KeyFor(1)).size());
    EXPECT_EQ(TakeValue(*handle), ValueFor(KeyFor(1)));
}

TEST_P(SecondaryCacheContractTests, RoundTripAsynchronousLookupWithWaitAll) {
    constexpr size_t kCount = 16;
    for (size_t i = 0; i < kCount; ++i) {
        Put(KeyFor(i), ValueFor(KeyFor(i)));
    }
    Settle(*m_cache);

    std::vector<std::unique_ptr<rocksdb::SecondaryCacheResultHandle>> handles;
    std::vector<rocksdb::SecondaryCacheResultHandle*> raw;
    for (size_t i = 0; i < kCount; ++i) {
        handles.push_back(Get(KeyFor(i), /*wait*/ false));
        ASSERT_NE(handles.back(), nullptr);
        raw.push_back(handles.back().get());
    }
    m_cache->WaitAll(raw);
    for (size_t i = 0; i < kCount; ++i) {
        ASSERT_TRUE(handles[i]->IsReady());
        EXPECT_EQ(TakeValue(*handles[i]), ValueFor(KeyFor(i))) << i;
    }
}

TEST_P(SecondaryCacheContractTests, RoundTripAsynchronousLookupWithWait) {
    Put(KeyFor(7), ValueFor(KeyFor(7)));
    Settle(*m_cache);

    auto handle = Get(KeyFor(7), /*wait*/ false);
    ASSERT_NE(handle, nullptr);
    handle->Wait();
    ASSERT_TRUE(handle->IsReady());
    EXPECT_EQ(TakeValue(*handle), ValueFor(KeyFor(7)));
}

TEST_P(SecondaryCacheContractTests, InsertSavedPreservesCompressionType) {
    const std::string saved = "pretend-compressed-bytes";
    ASSERT_TRUE(m_cache->InsertSaved(KeyFor(2), saved, rocksdb::kZSTD, rocksdb::CacheTier::kVolatileTier).ok());
    Settle(*m_cache);

    t_lastType = rocksdb::kNoCompression;
    auto handle = Get(KeyFor(2));
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(t_lastType, rocksdb::kZSTD);
    EXPECT_EQ(TakeValue(*handle), saved);
}

TEST_P(SecondaryCacheContractTests, MissReturnsNull) {
    EXPECT_EQ(Get(KeyFor(404)), nullptr);

    auto handle = Get(KeyFor(405), /*wait*/ false);
    if (handle) {
        handle->Wait();
        EXPECT_EQ(handle->Value(), nullptr);
    }
}

TEST_P(SecondaryCacheContractTests, NullOrIncompatibleHelperIsIgnored) {
    Payload payload{"value"};
    EXPECT_TRUE(m_cache->Insert(KeyFor(3), &payload, nullptr, false).ok());
    EXPECT_TRUE(m_cache->Insert(KeyFor(3), &payload, &m_helperNoSec, false).ok());
    Settle(*m_cache);
    EXPECT_EQ(Get(KeyFor(3)), nullptr);

    Put(KeyFor(4), "stored");
    Settle(*m_cache);
    bool kept = true;
    EXPECT_EQ(m_cache->Lookup(KeyFor(4), nullptr, nullptr, true, false, nullptr, kept), nullptr);
    EXPECT_FALSE(kept);
}

TEST_P(SecondaryCacheContractTests, ZeroSizeValueIsNotStored) {
    Put(KeyFor(5), "");
    Settle(*m_cache);
    EXPECT_EQ(Get(KeyFor(5)), nullptr);
}

TEST_P(SecondaryCacheContractTests, EraseRemovesEntry) {
    Put(KeyFor(6), ValueFor(KeyFor(6)));
    Settle(*m_cache);
    m_cache->Erase(KeyFor(6));
    EXPECT_EQ(Get(KeyFor(6)), nullptr);
}

TEST_P(SecondaryCacheContractTests, ConcurrentOperationsReturnIntactValues) {
    constexpr size_t kThreads = 8;
    constexpr size_t kOpsPerThread = 400;
    constexpr size_t kKeySpace = 128;
    std::atomic<size_t> hits{0};
    std::atomic<size_t> corrupt{0};

    std::vector<std::thread> threads;
    for (size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 rng(static_cast<unsigned>(t));
            std::uniform_int_distribution<size_t> keyDist(0, kKeySpace - 1);
            std::uniform_int_distribution<int> opDist(0, 9);
            for (size_t i = 0; i < kOpsPerThread; ++i) {
                const auto key = KeyFor(keyDist(rng));
                const int op = opDist(rng);
                if (op < 4) {
                    Payload payload{ValueFor(key, 1000)};
                    (void)m_cache->Insert(key, &payload, &m_helper, false);
                } else if (op < 9) {
                    bool kept = false;
                    auto handle = m_cache->Lookup(key, &m_helper, nullptr, op % 2 == 0, false, nullptr, kept);
                    if (handle) {
                        handle->Wait();
                        if (handle->Value() != nullptr) {
                            ++hits;
                            if (TakeValue(*handle) != ValueFor(key, 1000)) {
                                ++corrupt;
                            }
                        }
                    }
                } else {
                    m_cache->Erase(key);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(corrupt.load(), 0U);
    EXPECT_GT(hits.load(), 0U);
}

TEST_P(SecondaryCacheContractTests, ReopenStartsEmpty) {
    Put(KeyFor(8), ValueFor(KeyFor(8)));
    Settle(*m_cache);
    m_cache.reset();

    m_cache = CreateSecondaryCache(MakeTestOptions(GetParam(), m_dir));
    EXPECT_EQ(Get(KeyFor(8)), nullptr);
}

INSTANTIATE_TEST_SUITE_P(Backends, SecondaryCacheContractTests, ::testing::ValuesIn(AvailableBackends()),
                         BackendName);
} // namespace
