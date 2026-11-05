// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

// Validation of byte ranges (offset + length) shared by every file wrapper. Blob offsets are signed 64-bit, so a
// range is only usable when both parts are representable and the end offset cannot overflow.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::RangeCheck {
inline constexpr auto MaxOffset = std::numeric_limits<int64_t>::max();

enum class Bounds : std::uint8_t { OutOfRange, InRange };

// InRange when [offset, offset + length) fits in int64_t.
[[nodiscard]] constexpr Bounds Check(const uint64_t offset, const uint64_t length) noexcept {
    constexpr auto max = static_cast<uint64_t>(MaxOffset);
    return offset <= max && length <= max - offset ? Bounds::InRange : Bounds::OutOfRange;
}

[[nodiscard]] constexpr Bounds Check(const int64_t offset, const int64_t length) noexcept {
    return offset >= 0 && length >= 0 && length <= MaxOffset - offset ? Bounds::InRange : Bounds::OutOfRange;
}

// Throws std::invalid_argument (translated to IOStatus::InvalidArgument) for an unusable range.
inline void ThrowIfInvalid(const int64_t offset, const int64_t length, const char* what) {
    if (Check(offset, length) == Bounds::OutOfRange) {
        throw std::invalid_argument(std::string(what) + " range is out of bounds");
    }
}

inline void ThrowIfInvalid(const Core::ByteOffset offset, const Core::ByteCount length, const char* what) {
    if (Check(offset.Value(), length.Value()) == Bounds::OutOfRange) {
        throw std::invalid_argument(std::string(what) + " range is out of bounds");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::RangeCheck
