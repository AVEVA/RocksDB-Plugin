// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"

#include <rocksdb/advanced_options.h>
#include <rocksdb/slice.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

namespace {
using AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache;
using AVEVA::RocksDB::Plugin::Core::FileBasedSecondaryCacheAdmissionPolicy;
using AVEVA::RocksDB::Plugin::Core::FileBasedSecondaryCacheEngine;
using AVEVA::RocksDB::Plugin::Core::FileBasedSecondaryCacheOptions;
using AVEVA::RocksDB::Plugin::Core::LocalFilesystem;

struct Payload {
    std::string data;
};

size_t Size(rocksdb::Cache::ObjectPtr obj) { return static_cast<Payload*>(obj)->data.size(); }
rocksdb::Status Save(rocksdb::Cache::ObjectPtr obj, size_t offset, size_t count, char* out) {
    static_cast<Payload*>(obj)->data.copy(out, count, offset);
    return rocksdb::Status::OK();
}
rocksdb::Status Create(const rocksdb::Slice& bytes, rocksdb::CompressionType, rocksdb::CacheTier,
                       rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*, rocksdb::Cache::ObjectPtr* out,
                       size_t* charge) {
    auto value = std::make_unique<Payload>(std::string(bytes.data(), bytes.size()));
    *charge = value->data.size();
    *out = value.release();
    return rocksdb::Status::OK();
}
void Delete(rocksdb::Cache::ObjectPtr obj, rocksdb::MemoryAllocator*) { delete static_cast<Payload*>(obj); }

struct Trial {
    double insertUs;
    double lookupUs;
};

Trial Run(const std::filesystem::path& directory, const size_t count, const size_t size,
          const FileBasedSecondaryCacheEngine engine) {
    const auto logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    const size_t capacity = (count + 1) * (size + 128);
    std::unique_ptr<FileBasedCompressedSecondaryCache> cache;
    if (engine == FileBasedSecondaryCacheEngine::kLegacy) {
        cache = std::make_unique<FileBasedCompressedSecondaryCache>(directory, std::make_shared<LocalFilesystem>(),
                                                                    capacity, logger);
    } else {
        FileBasedSecondaryCacheOptions options;
        options.capacity = capacity;
        options.engine = FileBasedSecondaryCacheEngine::kRegion;
        options.admissionPolicy = FileBasedSecondaryCacheAdmissionPolicy::kAdmitAll;
        cache = std::make_unique<FileBasedCompressedSecondaryCache>(directory, options, logger);
    }
    rocksdb::Cache::CacheItemHelper noSecondary{rocksdb::CacheEntryRole::kDataBlock, Delete};
    rocksdb::Cache::CacheItemHelper helper{rocksdb::CacheEntryRole::kDataBlock, Delete, Size, Save, Create,
                                           &noSecondary};
    Payload value{std::string(size, 'X')};
    std::vector<std::string> keys;
    keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        keys.emplace_back("benchmark-key-" + std::to_string(i));
    }

    const auto beginInsert = std::chrono::steady_clock::now();
    for (const auto& key : keys) {
        auto status = cache->Insert(rocksdb::Slice(key), &value, &helper, true);
        if (!status.ok()) {
            throw std::runtime_error("Insert: " + status.ToString());
        }
    }
    const auto beginLookup = std::chrono::steady_clock::now();
    for (const auto& key : keys) {
        bool kept = false;
        auto result = cache->Lookup(rocksdb::Slice(key), &helper, nullptr, true, false, nullptr, kept);
        if (!result || !kept) {
            throw std::runtime_error("Lookup miss: " + key);
        }
        Delete(result->Value(), nullptr);
    }
    const auto end = std::chrono::steady_clock::now();
    return {std::chrono::duration<double, std::micro>(beginLookup - beginInsert).count() / count,
            std::chrono::duration<double, std::micro>(end - beginLookup).count() / count};
}
} // namespace

int main(int argc, char** argv) {
    try {
        const size_t count = argc > 1 ? std::stoull(argv[1]) : 2000;
        const size_t size = argc > 2 ? std::stoull(argv[2]) : 8192;
        const size_t trials = argc > 3 ? std::stoull(argv[3]) : 7;
        const std::string engineArg = argc > 4 ? argv[4] : "legacy";
        const auto engine = engineArg == "region" ? FileBasedSecondaryCacheEngine::kRegion
                                                  : FileBasedSecondaryCacheEngine::kLegacy;
        if (!count || !size || trials < 3) {
            throw std::invalid_argument("expected positive count and size, at least three trials");
        }
        const auto directory = std::filesystem::temp_directory_path() /
                               (engine == FileBasedSecondaryCacheEngine::kRegion ? "aveva_secondary_benchmark_region"
                                                                                 : "aveva_secondary_benchmark_legacy");
        for (size_t trial = 0; trial < trials; ++trial) {
            const auto sample = Run(directory, count, size, engine);
            std::cout << "engine=" << (engine == FileBasedSecondaryCacheEngine::kRegion ? "region" : "legacy")
                      << " trial=" << trial << " count=" << count << " bytes=" << size
                      << " insert_us=" << sample.insertUs << " lookup_us=" << sample.lookupUs << '\n';
        }
        std::filesystem::remove_all(directory);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "SecondaryCacheBenchmark: " << e.what() << '\n';
        return 1;
    }
}
