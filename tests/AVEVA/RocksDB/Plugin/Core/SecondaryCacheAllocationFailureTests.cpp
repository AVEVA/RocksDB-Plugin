// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/RegionBasedSecondaryCache.hpp"

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
std::atomic<int> failAfter{-1};
std::atomic<bool> failureInjected{false};
int deleted = 0;
int created = 0;
struct Payload {
    std::string data;
};
} // namespace

void* operator new(const std::size_t bytes) {
    if (failNext.exchange(false)) {
        throw std::bad_alloc();
    }
    const int remaining = failAfter.load();
    if (remaining >= 0 && failAfter.fetch_sub(1) == 0) {
        failAfter.store(-1);
        failureInjected.store(true);
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
    ++created;
    *charge = 123;
    // The next allocation must be result-handle construction, after a successful callback.
    failNext.store(true);
    return rocksdb::Status::OK();
}

#if !defined(_ITERATOR_DEBUG_LEVEL) || _ITERATOR_DEBUG_LEVEL == 0
rocksdb::Status CreateForSweep(const rocksdb::Slice& data, rocksdb::CompressionType, rocksdb::CacheTier,
                               rocksdb::Cache::CreateContext*, rocksdb::MemoryAllocator*,
                               rocksdb::Cache::ObjectPtr* out, size_t* charge) {
    *out = new Payload{data.ToString()};
    ++created;
    *charge = 123;
    return rocksdb::Status::OK();
}
#endif
} // namespace

int main(const int argc, char** argv) {
    using namespace AVEVA::RocksDB::Plugin::Core;
    if (argc != 2) {
        std::cerr << "Expected a test-owned cache directory\n";
        return 1;
    }
    RegionBasedSecondaryCacheOptions options;
    options.capacity = 4096;
    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    RegionBasedSecondaryCache cache(argv[1], options, logger);
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
    if (result || kept || created != 1 || deleted != 1 || failNext.load()) {
        std::cerr << "Handle OOM must return a miss, clear kept, and destroy the created value exactly once\n";
        return 3;
    }
    RegionBasedSecondaryCacheStats stats;
    if (!cache.GetStats(stats).ok() || stats.errors != 1 || stats.hits != 0 || stats.transientBytes != 0) {
        std::cerr << "Handle OOM must count an error, not a hit, and release transient memory\n";
        return 4;
    }

#if !defined(_ITERATOR_DEBUG_LEVEL) || _ITERATOR_DEBUG_LEVEL == 0
    constexpr int firstAllocation = 0;
    constexpr int allocationLimit = 64;
    constexpr int minimumFailures = 2;
    constexpr bool requireSuccessfulSweep = true;
#else
    constexpr int firstAllocation = 1;
    constexpr int allocationLimit = 2;
    constexpr int minimumFailures = 1;
    constexpr bool requireSuccessfulSweep = false;
#endif
    int injectedFailures = 0;
    bool reachedSuccessfulInsert = false;
    if (!cache.SetCapacity(4096).ok()) {
        std::cerr << "Could not reset cache before insertion OOM sweep\n";
        return 5;
    }
    for (int allocation = firstAllocation; allocation < allocationLimit; ++allocation) {
        failureInjected.store(false);
        failAfter.store(allocation);
        const auto status = cache.Insert("oom-insert", &payload, &helper, false);
        failAfter.store(-1);

        if (!failureInjected.load()) {
            if (!status.ok()) {
                std::cerr << "Insertion failed before the OOM sweep exhausted its allocation sites\n";
                return 6;
            }
            reachedSuccessfulInsert = true;
            break;
        }
        ++injectedFailures;
        if (!status.IsMemoryLimit() || !cache.GetStats(stats).ok() || stats.transientBytes != 0 ||
            stats.liveEntries != 0 || stats.liveBytes != 0) {
            std::cerr << "Insertion OOM must return MemoryLimit and release transient memory\n";
            return 7;
        }
    }
    if (injectedFailures < minimumFailures || (requireSuccessfulSweep && !reachedSuccessfulInsert)) {
        std::cerr << "Insertion OOM sweep did not cover multiple allocation sites\n";
        return 8;
    }

    int lookupFailures = 0;
    bool reachedSuccessfulLookup = false;
    auto lookupHelper = helper;
#if !defined(_ITERATOR_DEBUG_LEVEL) || _ITERATOR_DEBUG_LEVEL == 0
    lookupHelper.create_cb = CreateForSweep;
#endif
    if (!cache.Insert("oom-lookup", &payload, &helper, false).ok()) {
        std::cerr << "Could not prepare cache before lookup OOM sweep\n";
        return 9;
    }
    RegionBasedSecondaryCacheStats beforeLookup;
    if (!cache.GetStats(beforeLookup).ok()) {
        std::cerr << "Could not inspect cache before lookup OOM sweep\n";
        return 10;
    }
    for (int allocation = firstAllocation; allocation < allocationLimit; ++allocation) {
        failureInjected.store(false);
        failAfter.store(allocation);
        bool lookupKept = true;
        auto lookupResult = cache.Lookup("oom-lookup", &lookupHelper, nullptr, false, false, nullptr, lookupKept);
        failAfter.store(-1);

        if (!failureInjected.load()) {
            if (!lookupResult || !lookupKept) {
                std::cerr << "Lookup failed before the OOM sweep exhausted its allocation sites\n";
                return 11;
            }
            delete static_cast<Payload*>(lookupResult->Value());
            ++deleted;
            reachedSuccessfulLookup = true;
            break;
        }
        ++lookupFailures;
        if (lookupResult || lookupKept || !cache.GetStats(stats).ok() || stats.transientBytes != 0 ||
            stats.liveEntries != beforeLookup.liveEntries || stats.liveBytes != beforeLookup.liveBytes ||
            created != deleted) {
            std::cerr << "Lookup OOM must return a miss, clean created values, and release transient memory\n";
            return 12;
        }
    }
    if (lookupFailures < minimumFailures || (requireSuccessfulSweep && !reachedSuccessfulLookup) ||
        created != deleted) {
        std::cerr << "Lookup OOM sweep did not cover multiple allocation sites cleanly\n";
        return 13;
    }
    std::cout << "Handle-allocation OOM cleanup and " << injectedFailures << " insertion and " << lookupFailures
              << " lookup allocation-failure sites passed\n";
}
