// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <cstdint>

namespace AVEVA::RocksDB::Plugin::Azure {
/// <summary>Default values of the public Plugin::Register parameters.</summary>
struct Defaults {
    /// <summary>Size of the page-aligned write buffer of a data file, in bytes.</summary>
    static constexpr int64_t DataFileBufferSize = 128 * 512 * 2;
    /// <summary>Initial capacity of a newly created data file, in bytes.</summary>
    static constexpr int64_t DataFileInitialSize = 128 * 512 * 2;
    /// <summary>Cap on bytes held by RocksDB prefetch buffers across all files of a filesystem.</summary>
    static constexpr int64_t MaxPrefetchBytes = static_cast<int64_t>(256) * 1024 * 1024;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
