// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/SecondaryCacheOptions.hpp"

#include <rocksdb/secondary_cache.h>

#include <memory>

namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>True when this build contains the CacheLib secondary cache backend.</summary>
[[nodiscard]] bool IsCacheLibSecondaryCacheAvailable() noexcept;

/// <summary>
/// Creates the secondary cache selected by <paramref name="options"/>.<c>backend</c>, suitable for
/// <c>rocksdb::LRUCacheOptions::secondary_cache</c> / <c>HyperClockCacheOptions::secondary_cache</c>.
/// </summary>
/// <exception cref="std::invalid_argument">Invalid options, or CacheLib requested but unavailable.</exception>
/// <exception cref="std::runtime_error">The cache could not be created.</exception>
[[nodiscard]] std::shared_ptr<rocksdb::SecondaryCache> CreateSecondaryCache(const SecondaryCacheOptions& options);
} // namespace AVEVA::RocksDB::Plugin::Core
