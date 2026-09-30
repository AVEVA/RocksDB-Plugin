// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/SecondaryCacheFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Core/SecondaryCacheOptions.hpp"

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
#include "AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp"
#endif

#include <rocksdb/secondary_cache.h>

#include <gtest/gtest.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::Testing {
/// <summary>Backends that are available in this build, for value-parameterized tests.</summary>
inline std::vector<SecondaryCacheBackend> AvailableBackends() {
    std::vector<SecondaryCacheBackend> backends{SecondaryCacheBackend::FileBased};
    if (IsCacheLibSecondaryCacheAvailable()) {
        backends.push_back(SecondaryCacheBackend::CacheLib);
    }
    return backends;
}

inline std::string BackendName(const ::testing::TestParamInfo<SecondaryCacheBackend>& info) {
    switch (info.param) {
    case SecondaryCacheBackend::FileBased:
        return "FileBased";
    case SecondaryCacheBackend::CacheLib:
        return "CacheLib";
    case SecondaryCacheBackend::Default:
        return "Default";
    }
    return "Unknown";
}

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
inline SecondaryCacheOptions MakeTestOptions(SecondaryCacheBackend backend, const std::filesystem::path& dir,
                                             size_t capacity = 32ULL * 1024 * 1024) {
    SecondaryCacheOptions options;
    options.backend = backend;
    options.cacheDir = dir;
    options.capacity = capacity;
    options.logger = MakeQuietLogger();
    options.cachelib.regionSizeBytes = 1024 * 1024;
    return options;
}

/// <summary>Waits for queued asynchronous writes where the backend has them.</summary>
inline void Settle(rocksdb::SecondaryCache& cache) {
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    if (auto* cachelib = dynamic_cast<CacheLibSecondaryCache*>(&cache)) {
        cachelib->Drain();
    }
#else
    (void)cache;
#endif
}

/// <summary>Returns a CacheLib counter, or -1 when the cache is not CacheLib or the counter is unknown.</summary>
inline double CacheLibCounter([[maybe_unused]] const rocksdb::SecondaryCache& cache,
                              [[maybe_unused]] std::string_view name) {
    double result = -1;
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    if (const auto* cachelib = dynamic_cast<const CacheLibSecondaryCache*>(&cache)) {
        cachelib->VisitCounters([&](std::string_view counter, double value) {
            if (counter == name) {
                result = value;
            }
        });
    }
#endif
    return result;
}
} // namespace AVEVA::RocksDB::Plugin::Core::Testing
