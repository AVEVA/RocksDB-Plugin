// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <cstdint>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// A byte count as the signed 64-bit integer the blob client and RocksDB's file interfaces use for offsets and sizes.
/// Callers have already rejected counts above INT64_MAX (see Plugin::Register), so the conversion cannot wrap.
/// </summary>
[[nodiscard]] constexpr std::int64_t SignedBytes(const Core::ByteCount size) noexcept {
    return static_cast<std::int64_t>(size.Value());
}

/// <summary>The page size in the signed arithmetic that file offsets use.</summary>
inline constexpr std::int64_t SignedPageSize = SignedBytes(Configuration::PageBlob::PageSize);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
