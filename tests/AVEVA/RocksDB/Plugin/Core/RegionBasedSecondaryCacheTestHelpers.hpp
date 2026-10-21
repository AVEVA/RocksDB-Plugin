// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/RegionBasedSecondaryCache.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>

#include <gtest/gtest.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <cstring>
#include <filesystem>
#include <string>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

using AVEVA::RocksDB::Plugin::Core::RegionBasedSecondaryCache;
using AVEVA::RocksDB::Plugin::Core::RegionBasedSecondaryCacheOptions;

namespace {
inline std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> MakeNullLogger() {
    return std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
}

struct TestPayload {
    std::string data;
};

size_t TestSizeCb(rocksdb::Cache::ObjectPtr obj) { return static_cast<TestPayload*>(obj)->data.size(); }

rocksdb::Status TestSaveToCb(rocksdb::Cache::ObjectPtr obj, size_t fromOffset, size_t length, char* outBuffer) {
    const auto* payload = static_cast<TestPayload*>(obj);
    if (fromOffset + length > payload->data.size()) {
        return rocksdb::Status::InvalidArgument("out of range");
    }
    std::memcpy(outBuffer, payload->data.data() + fromOffset, length);
    return rocksdb::Status::OK();
}

rocksdb::Status TestCreateCb(const rocksdb::Slice& data, rocksdb::CompressionType /*type*/,
                             rocksdb::CacheTier /*source*/, rocksdb::Cache::CreateContext* /*context*/,
                             rocksdb::MemoryAllocator* /*allocator*/, rocksdb::Cache::ObjectPtr* outObject,
                             size_t* outCharge) {
    auto* payload = new TestPayload{std::string(data.data(), data.size())};
    *outObject = payload;
    *outCharge = payload->data.size();
    return rocksdb::Status::OK();
}

void TestDeleteCb(rocksdb::Cache::ObjectPtr obj, rocksdb::MemoryAllocator*) { delete static_cast<TestPayload*>(obj); }

std::filesystem::path MakeTempDir(const std::string& suffix) {
#ifdef _WIN32
    const auto process = _getpid();
#else
    const auto process = getpid();
#endif
    auto base =
        std::filesystem::temp_directory_path() / ("aveva_sec_cache_test_" + suffix + "_" + std::to_string(process));
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    return base;
}
} // namespace

class RegionBasedSecondaryCacheTests : public ::testing::Test {
  protected:
    std::filesystem::path m_cacheDir;
    std::unique_ptr<RegionBasedSecondaryCache> m_cache;
    rocksdb::Cache::CacheItemHelper m_helperNoSec{rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb};
    rocksdb::Cache::CacheItemHelper m_helper{
        rocksdb::CacheEntryRole::kDataBlock, TestDeleteCb, TestSizeCb, TestSaveToCb, TestCreateCb, &m_helperNoSec};

    static RegionBasedSecondaryCacheOptions
    MakeOptions(size_t capacity = 8 * 1024 * 1024, size_t regionSize = 256 * 1024, size_t flushBlockSize = 16 * 1024) {
        RegionBasedSecondaryCacheOptions options;
        options.capacity = capacity;
        options.regionSize = regionSize;
        options.flushBlockSize = flushBlockSize;
        options.maxEntrySize = regionSize / 4;
        return options;
    }

    void SetUp() override {
        m_cacheDir = MakeTempDir(::testing::UnitTest::GetInstance()->current_test_info()->name());
        m_cache = std::make_unique<RegionBasedSecondaryCache>(m_cacheDir, MakeOptions(), MakeNullLogger());
    }

    void TearDown() override {
        m_cache.reset();
        std::filesystem::remove_all(m_cacheDir);
    }

    static rocksdb::Slice MakeKey(const std::string& value) { return rocksdb::Slice(value); }
};
