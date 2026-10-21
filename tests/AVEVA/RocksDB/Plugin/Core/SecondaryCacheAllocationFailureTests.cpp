// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"

#include <rocksdb/slice.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <new>
#include <string>

// A separate executable keeps global allocation injection out of the GTest process.
namespace {
std::atomic<bool> failNext{false};
int deleted = 0;
struct Payload {
    std::string data;
};
} // namespace

void* operator new(const std::size_t bytes) {
    if (failNext.exchange(false)) {
        throw std::bad_alloc();
    }
    if (void* pointer = std::malloc(bytes == 0 ? 1 : bytes)) {
        return pointer;
    }
    throw std::bad_alloc();
}

void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }

namespace {
void Delete(rocksdb::Cache::ObjectPtr object, rocksdb::MemoryAllocator*) {
    ++deleted;
    delete static_cast<Payload*>(object);
}

size_t Size(rocksdb::Cache::ObjectPtr object) { return static_cast<Payload*>(object)->data.size(); }

rocksdb::Status Save(rocksdb::Cache::ObjectPtr object, const size_t from, const size_t count, char* out) {
    std::memcpy(out, static_cast<Payload*>(object)->data.data() + from, count);
    return rocksdb::Status::OK();
}

rocksdb::Status Create(const rocksdb::Slice& data, rocksdb::CompressionType, rocksdb::CacheTier,
                       rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*, rocksdb::Cache::ObjectPtr* out,
                       size_t* charge) {
    *out = new Payload{data.ToString()};
    *charge = 123;
    // The next allocation must be result-handle construction, after a successful callback.
    failNext.store(true);
    return rocksdb::Status::OK();
}
} // namespace

int main(const int argc, char** argv) {
    using namespace AVEVA::RocksDB::Plugin::Core;
    if (argc != 2) {
        std::cerr << "Expected a test-owned cache directory\n";
        return 1;
    }
    FileBasedSecondaryCacheOptions options;
    options.engine = FileBasedSecondaryCacheEngine::kRegion;
    options.capacity = 4096;
    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    FileBasedCompressedSecondaryCache cache(argv[1], options, logger);
    rocksdb::Cache::CacheItemHelper noSecondary{rocksdb::CacheEntryRole::kDataBlock, Delete};
    rocksdb::Cache::CacheItemHelper helper{
        rocksdb::CacheEntryRole::kDataBlock, Delete, Size, Save, Create, &noSecondary};
    Payload payload{"value"};
    if (!cache.Insert("key", &payload, &helper, false).ok()) {
        std::cerr << "Failed to prepare the allocation-failure test entry\n";
        return 2;
    }
    bool kept = true;
    const auto result = cache.Lookup("key", &helper, nullptr, false, false, nullptr, kept);
    if (result || kept || deleted != 1 || failNext.load()) {
        std::cerr << "Handle OOM must return a miss, clear kept, and destroy the created value exactly once\n";
        return 3;
    }
    FileBasedSecondaryCacheStats stats;
    if (!cache.GetStats(stats).ok() || stats.errors != 1 || stats.hits != 0 || stats.transientBytes != 0) {
        std::cerr << "Handle OOM must count an error, not a hit, and release transient memory\n";
        return 4;
    }
    std::cout << "Handle-allocation OOM safely returned a miss and cleaned the created value\n";
}
