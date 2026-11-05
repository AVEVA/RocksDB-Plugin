// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <compare>
#include <concepts>
#include <cstdint>
#include <limits>

namespace AVEVA::RocksDB::Plugin::Core {
struct ByteCountTag;
struct ByteOffsetTag;
struct GenerationTag;
struct AccessEpochTag;

/// <summary>
/// An integer that has a meaning of its own, so a byte count cannot be passed where an offset is expected. Both the
/// construction from and the access to the underlying integer are explicit.
/// </summary>
template <typename Tag, typename Rep> class StrongInteger {
  public:
    using ValueType = Rep;

    constexpr StrongInteger() noexcept = default;
    explicit constexpr StrongInteger(Rep value) noexcept : m_value(value) {}

    [[nodiscard]] constexpr Rep Value() const noexcept { return m_value; }

    friend constexpr auto operator<=>(StrongInteger, StrongInteger) noexcept = default;
    friend constexpr bool operator==(StrongInteger, StrongInteger) noexcept = default;

    // Sizes add and subtract; the other kinds of integer do not.
    constexpr StrongInteger& operator+=(StrongInteger other) noexcept
        requires std::same_as<Tag, ByteCountTag>
    {
        m_value += other.m_value;
        return *this;
    }
    constexpr StrongInteger& operator-=(StrongInteger other) noexcept
        requires std::same_as<Tag, ByteCountTag>
    {
        m_value -= other.m_value;
        return *this;
    }
    [[nodiscard]] friend constexpr StrongInteger operator+(StrongInteger lhs, StrongInteger rhs) noexcept
        requires std::same_as<Tag, ByteCountTag>
    {
        return lhs += rhs;
    }
    [[nodiscard]] friend constexpr StrongInteger operator-(StrongInteger lhs, StrongInteger rhs) noexcept
        requires std::same_as<Tag, ByteCountTag>
    {
        return lhs -= rhs;
    }

    // Sequence numbers only move forwards.
    constexpr StrongInteger& operator++() noexcept
        requires std::same_as<Tag, GenerationTag> || std::same_as<Tag, AccessEpochTag>
    {
        ++m_value;
        return *this;
    }

  private:
    Rep m_value{};
};

/// <summary>A number of bytes (a size, a length, or an amount to read).</summary>
using ByteCount = StrongInteger<ByteCountTag, std::uint64_t>;
/// <summary>A position within a file, in bytes from its start.</summary>
using ByteOffset = StrongInteger<ByteOffsetTag, std::uint64_t>;
/// <summary>Identifies one version of a cached item; every replacement takes a fresh value.</summary>
using Generation = StrongInteger<GenerationTag, std::uint64_t>;
/// <summary>A count of cache accesses, used to decide whether an entry was touched recently.</summary>
using AccessEpoch = StrongInteger<AccessEpochTag, std::uint64_t>;

/// <summary>The position one past the last byte of a range that starts at <paramref name="start"/>.</summary>
[[nodiscard]] constexpr ByteOffset EndOf(ByteOffset start, ByteCount length) noexcept {
    return ByteOffset{start.Value() + length.Value()};
}

/// <summary>Bytes <paramref name="size"/> has beyond <paramref name="offset"/>; zero when offset is at or past the
/// end.</summary>
[[nodiscard]] constexpr ByteCount BytesAfter(ByteCount size, ByteOffset offset) noexcept {
    return offset.Value() >= size.Value() ? ByteCount{} : ByteCount{size.Value() - offset.Value()};
}

/// <summary>Sum that stops at the largest representable count instead of wrapping.</summary>
[[nodiscard]] constexpr ByteCount SaturatingAdd(ByteCount lhs, ByteCount rhs) noexcept {
    const auto room = std::numeric_limits<std::uint64_t>::max() - lhs.Value();
    return ByteCount{rhs.Value() > room ? std::numeric_limits<std::uint64_t>::max() : lhs.Value() + rhs.Value()};
}

/// <summary>Difference that stops at zero instead of wrapping.</summary>
[[nodiscard]] constexpr ByteCount SaturatingSubtract(ByteCount lhs, ByteCount rhs) noexcept {
    return rhs.Value() >= lhs.Value() ? ByteCount{} : ByteCount{lhs.Value() - rhs.Value()};
}
} // namespace AVEVA::RocksDB::Plugin::Core
