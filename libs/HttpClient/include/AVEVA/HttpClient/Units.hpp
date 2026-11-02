// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>

namespace AVEVA {
struct ByteCountTag;
struct ConnectionCountTag;

/// <summary>
/// An integer that has a meaning of its own, so a byte count cannot be passed where a connection count is expected.
/// Both the construction from and the access to the underlying integer are explicit.
/// </summary>
template <typename Tag, typename Rep> class StrongInteger {
  public:
    using ValueType = Rep;

    constexpr StrongInteger() noexcept = default;
    explicit constexpr StrongInteger(Rep value) noexcept : m_value(value) {}

    [[nodiscard]] constexpr Rep Value() const noexcept { return m_value; }

    friend constexpr auto operator<=>(StrongInteger, StrongInteger) noexcept = default;
    friend constexpr bool operator==(StrongInteger, StrongInteger) noexcept = default;

  private:
    Rep m_value{};
};

/// <summary>A number of bytes (a size or a limit).</summary>
using ByteCount = StrongInteger<ByteCountTag, std::uint64_t>;
/// <summary>A number of connections; zero means none are allowed or, where documented, no cap.</summary>
using ConnectionCount = StrongInteger<ConnectionCountTag, std::size_t>;
} // namespace AVEVA
