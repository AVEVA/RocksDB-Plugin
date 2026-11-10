// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/SecondaryCacheFactory.hpp"

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
#include "AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp"
#endif

#include <stdexcept>

namespace AVEVA::RocksDB::Plugin::Core {
bool IsCacheLibSecondaryCacheAvailable() noexcept {
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    return true;
#else
    return false;
#endif
}

std::shared_ptr<rocksdb::SecondaryCache> CreateSecondaryCache(const SecondaryCacheOptions& options) {
    auto backend = options.backend;
    if (backend == SecondaryCacheBackend::Default) {
        backend = SecondaryCacheBackend::CacheLib;
    }

    switch (backend) {
    case SecondaryCacheBackend::CacheLib:
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
        return std::make_shared<CacheLibSecondaryCache>(options);
#else
        throw std::invalid_argument("CreateSecondaryCache: the CacheLib backend is not available in this build");
#endif
    case SecondaryCacheBackend::Default:
        break;
    }
    throw std::invalid_argument("CreateSecondaryCache: unknown backend");
}
} // namespace AVEVA::RocksDB::Plugin::Core
