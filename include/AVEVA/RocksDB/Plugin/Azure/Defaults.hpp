// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/SignedByteCount.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

namespace AVEVA::RocksDB::Plugin::Azure {
/// <summary>Default values of the public Plugin::Register options.</summary>
struct Defaults {
    /// <summary>Size of the page-aligned write buffer of a data file.</summary>
    static constexpr Core::ByteCount DataFileBufferSize{128 * 512 * 2};
    /// <summary>Initial capacity of a newly created data file.</summary>
    static constexpr Core::ByteCount DataFileInitialSize{128 * 512 * 2};
    /// <summary>Cap on bytes held by RocksDB prefetch buffers across all files of a filesystem.</summary>
    static constexpr SignedByteCount MaxPrefetchBytes{Core::ByteCount{static_cast<std::uint64_t>(256) * 1024 * 1024}};
};
} // namespace AVEVA::RocksDB::Plugin::Azure
