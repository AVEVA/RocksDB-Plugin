// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

// Validation of byte ranges (offset + length) shared by every file wrapper. Blob offsets are signed 64-bit, so a
// range is only usable when both parts are representable and the end offset cannot overflow.
namespace AVEVA::RocksDB::Plugin::Azure::Impl::RangeCheck {
inline constexpr auto MaxOffset = std::numeric_limits<int64_t>::max();

// True when [offset, offset + length) fits in int64_t.
[[nodiscard]] constexpr bool IsValid(const uint64_t offset, const uint64_t length) noexcept {
    constexpr auto max = static_cast<uint64_t>(MaxOffset);
    return offset <= max && length <= max - offset;
}

[[nodiscard]] constexpr bool IsValid(const int64_t offset, const int64_t length) noexcept {
    return offset >= 0 && length >= 0 && length <= MaxOffset - offset;
}

// Throws std::invalid_argument (translated to IOStatus::InvalidArgument) for an unusable range.
inline void ThrowIfInvalid(const int64_t offset, const int64_t length, const char* what) {
    if (!IsValid(offset, length)) {
        throw std::invalid_argument(std::string(what) + " range is out of bounds");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::RangeCheck
