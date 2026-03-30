// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <rocksdb/secondary_cache.h>
#include <span>
#include <optional>

namespace AVEVA::RocksDB::Plugin::Core
{
    /// <summary>
    /// Validated and parsed fields extracted from a mapped cache entry's on-disk header.
    /// The <c>payload</c> span points into the mapped file and is valid while the
    /// associated mapped view is alive.
    /// </summary>
    struct ParsedHeader
    {
        rocksdb::CompressionType compressionType;
        std::span<const char> payload;
    };
}
