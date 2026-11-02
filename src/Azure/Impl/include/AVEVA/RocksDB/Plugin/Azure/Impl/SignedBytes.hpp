// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <cstdint>
#include <stdexcept>
#include <string>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// A byte count as the signed 64-bit integer the blob client and RocksDB's file interfaces use for offsets and sizes.
/// Callers have already rejected counts above INT64_MAX (see Plugin::Register), so the conversion cannot wrap.
/// </summary>
[[nodiscard]] constexpr std::int64_t SignedBytes(const Core::ByteCount size) noexcept {
    return static_cast<std::int64_t>(size.Value());
}

/// <summary>
/// The offset a signed file position (as RocksDB's file interfaces pass it) names. A negative position names no byte,
/// so it throws std::invalid_argument instead of wrapping to a huge offset.
/// </summary>
[[nodiscard]] constexpr Core::ByteOffset OffsetFromSigned(const std::int64_t position) {
    if (position < 0) {
        throw std::invalid_argument("Negative file offset " + std::to_string(position));
    }
    return Core::ByteOffset{static_cast<std::uint64_t>(position)};
}

/// <summary>The count a signed length names; a negative length throws std::invalid_argument.</summary>
[[nodiscard]] constexpr Core::ByteCount CountFromSigned(const std::int64_t length) {
    if (length < 0) {
        throw std::invalid_argument("Negative byte count " + std::to_string(length));
    }
    return Core::ByteCount{static_cast<std::uint64_t>(length)};
}

/// <summary>The page size in the signed arithmetic that file offsets use.</summary>
inline constexpr std::int64_t SignedPageSize = SignedBytes(Configuration::PageBlob::PageSize);
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
