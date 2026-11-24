// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/SecondaryCacheOptions.hpp"

#include "AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp"

#include <rocksdb/secondary_cache.h>

#include <gtest/gtest.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::Testing {
inline std::shared_ptr<SecondaryCacheOptions::Logger> MakeQuietLogger() {
    return std::make_shared<SecondaryCacheOptions::Logger>();
}

inline std::filesystem::path MakeUniqueTempDir(std::string_view prefix) {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string name = std::string(prefix) + "_" + info->test_suite_name() + "_" + info->name();
    for (auto& c : name) {
        if (c == '/' || c == '\\') {
            c = '_';
        }
    }
    auto dir = std::filesystem::temp_directory_path() / name;
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

/// <summary>
/// Options with small, test friendly CacheLib settings (1 MiB regions) so tests
/// can fill the cache quickly.
/// </summary>
inline SecondaryCacheOptions MakeTestOptions(const std::filesystem::path& dir, size_t capacity = 32ULL * 1024 * 1024) {
    SecondaryCacheOptions options;
    options.cacheDir = dir;
    options.capacity = capacity;
    options.logger = MakeQuietLogger();
    options.cachelib.regionSizeBytes = 1024 * 1024;
    return options;
}

/// <summary>
/// A cache value plus the callbacks RocksDB needs to size, serialize, rebuild and delete it.
/// <c>t_lastType</c>/<c>t_lastSource</c> record what the last <c>CreateCb</c> call received, and
/// <c>t_failCreate</c> makes it fail so tests can exercise the deserialization-error path.
/// </summary>
struct Payload {
    std::string data;
};

inline thread_local rocksdb::CompressionType t_lastType = rocksdb::kNoCompression;
inline thread_local rocksdb::CacheTier t_lastSource = rocksdb::CacheTier::kVolatileTier;
inline thread_local bool t_failCreate = false;

inline size_t SizeCb(rocksdb::Cache::ObjectPtr obj) { return static_cast<Payload*>(obj)->data.size(); }

inline rocksdb::Status SaveToCb(rocksdb::Cache::ObjectPtr obj, size_t offset, size_t length, char* out) {
    const auto& data = static_cast<Payload*>(obj)->data;
    if (offset + length > data.size()) {
        return rocksdb::Status::InvalidArgument("out of range");
    }
    std::memcpy(out, data.data() + offset, length);
    return rocksdb::Status::OK();
}

inline rocksdb::Status CreateCb(const rocksdb::Slice& data, rocksdb::CompressionType type, rocksdb::CacheTier source,
                                rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*,
                                rocksdb::Cache::ObjectPtr* out, size_t* charge) {
    t_lastType = type;
    t_lastSource = source;
    if (t_failCreate) {
        return rocksdb::Status::Corruption("injected");
    }
    auto* payload = new Payload{data.ToString()};
    *out = payload;
    *charge = payload->data.size();
    return rocksdb::Status::OK();
}

inline void DeleteCb(rocksdb::Cache::ObjectPtr obj, rocksdb::MemoryAllocator*) { delete static_cast<Payload*>(obj); }

/// <summary>Waits for queued asynchronous writes to be applied.</summary>
inline void Settle(rocksdb::SecondaryCache& cache) {
    if (auto* cachelib = dynamic_cast<CacheLibSecondaryCache*>(&cache)) {
        cachelib->Drain();
    }
}

/// <summary>Returns a CacheLib counter, or -1 when the cache is not CacheLib or the counter is unknown.</summary>
inline double CacheLibCounter(const rocksdb::SecondaryCache& cache, std::string_view name) {
    double result = -1;
    if (const auto* cachelib = dynamic_cast<const CacheLibSecondaryCache*>(&cache)) {
        cachelib->VisitCounters([&](std::string_view counter, double value) {
            if (counter == name) {
                result = value;
            }
        });
    }
    return result;
}
} // namespace AVEVA::RocksDB::Plugin::Core::Testing
