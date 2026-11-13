// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <compare>
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace AVEVA::RocksDB::Plugin::Azure {
/// <summary>
/// A byte count that the blob service's signed 64-bit sizes can represent. Constructing one above INT64_MAX throws
/// std::out_of_range, so an unrepresentable size can never reach a filesystem.
/// </summary>
class SignedByteCount {
  public:
    static constexpr Core::ByteCount Max{static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())};

    explicit constexpr SignedByteCount(const Core::ByteCount bytes) : m_bytes(bytes) {
        if (bytes > Max) {
            throw std::out_of_range("byte count exceeds INT64_MAX");
        }
    }

    [[nodiscard]] constexpr Core::ByteCount Bytes() const noexcept { return m_bytes; }

    friend constexpr auto operator<=>(const SignedByteCount&, const SignedByteCount&) noexcept = default;

  private:
    Core::ByteCount m_bytes;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
