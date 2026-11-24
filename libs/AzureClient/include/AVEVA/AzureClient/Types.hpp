// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Units.hpp>

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace AVEVA::AzureClient {
struct PageBlobSizeTag;
struct PageOffsetTag;
struct SourcePageOffsetTag;
struct PageLengthTag;
struct ChunkSizeTag;
struct ConcurrencyTag;
struct SecretUrlTag;

/// <summary>The size of a page; every page blob offset and length is a multiple of it.</summary>
inline constexpr std::size_t PageBlobPageSize = 512;

/// <summary>
/// A URL that may carry a SAS token, so it is a secret: it never converts to a string implicitly, which keeps it out
/// of keys and log lines by accident.
/// </summary>
using SecretUrl = StrongString<SecretUrlTag>;

/// <summary>
/// A byte value of a page blob that is a whole number of pages (a multiple of PageBlobPageSize). A misaligned value
/// cannot be built, so no page blob operation has to check it.
/// </summary>
/// <typeparam name="Tag">Distinguishes the meaning of the value (a size, an offset, a length).</typeparam>
template <typename Tag> class PageAligned {
  public:
    /// <param name="bytes">The byte value; a multiple of PageBlobPageSize.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="bytes"/> is not page aligned.</exception>
    explicit constexpr PageAligned(std::uint64_t bytes) : m_bytes(Check(bytes)) {}

    /// <returns>The byte value.</returns>
    [[nodiscard]] constexpr std::uint64_t Value() const noexcept { return m_bytes; }

    friend constexpr auto operator<=>(PageAligned, PageAligned) noexcept = default;
    friend constexpr bool operator==(PageAligned, PageAligned) noexcept = default;

  private:
    static constexpr std::uint64_t Check(std::uint64_t bytes) {
        if (bytes % PageBlobPageSize != 0U) {
            throw std::invalid_argument("A page blob offset or length must be a multiple of 512 bytes.");
        }
        return bytes;
    }

    std::uint64_t m_bytes;
};

/// <summary>The size of a page blob in bytes.</summary>
using PageBlobSize = PageAligned<PageBlobSizeTag>;
/// <summary>The first byte of the page blob a write addresses.</summary>
using PageOffset = PageAligned<PageOffsetTag>;
/// <summary>The first byte of the source blob a Put Page From URL copies.</summary>
using SourcePageOffset = PageAligned<SourcePageOffsetTag>;
/// <summary>The number of bytes a Put Page From URL copies.</summary>
using PageLength = PageAligned<PageLengthTag>;

/// <summary>
/// A count that is at least one. Zero cannot be built, so it never needs a special meaning.
/// </summary>
/// <typeparam name="Tag">Distinguishes the meaning of the count.</typeparam>
template <typename Tag> class PositiveSize {
  public:
    /// <param name="value">The count; at least one.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="value"/> is zero.</exception>
    explicit constexpr PositiveSize(std::size_t value) : m_value(Check(value)) {}

    /// <returns>The count.</returns>
    [[nodiscard]] constexpr std::size_t Value() const noexcept { return m_value; }

    friend constexpr auto operator<=>(PositiveSize, PositiveSize) noexcept = default;
    friend constexpr bool operator==(PositiveSize, PositiveSize) noexcept = default;

  private:
    static constexpr std::size_t Check(std::size_t value) {
        if (value == 0U) {
            throw std::invalid_argument("The value must be at least one.");
        }
        return value;
    }

    std::size_t m_value;
};

/// <summary>The size of each ranged request of a chunked transfer, in bytes.</summary>
using ChunkSize = PositiveSize<ChunkSizeTag>;
/// <summary>The number of requests a transfer keeps in flight at once.</summary>
using Concurrency = PositiveSize<ConcurrencyTag>;

/// <summary>
/// The duration of a fixed-length blob lease, which the service only accepts between 15 and 60 seconds.
/// </summary>
class LeaseDuration {
  public:
    /// <summary>The shortest fixed lease the service grants.</summary>
    static constexpr std::chrono::seconds Min{15};
    /// <summary>The longest fixed lease the service grants.</summary>
    static constexpr std::chrono::seconds Max{60};

    /// <param name="duration">The lease duration; between <see cref="Min"/> and <see cref="Max"/>.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="duration"/> is out of range.</exception>
    explicit constexpr LeaseDuration(std::chrono::seconds duration) : m_value(Check(duration)) {}

    /// <returns>The lease duration.</returns>
    [[nodiscard]] constexpr std::chrono::seconds Value() const noexcept { return m_value; }

    friend constexpr auto operator<=>(LeaseDuration, LeaseDuration) noexcept = default;
    friend constexpr bool operator==(LeaseDuration, LeaseDuration) noexcept = default;

  private:
    static constexpr std::chrono::seconds Check(std::chrono::seconds duration) {
        if (duration < Min || duration > Max) {
            throw std::invalid_argument("A fixed lease duration must be between 15 and 60 seconds.");
        }
        return duration;
    }

    std::chrono::seconds m_value;
};
} // namespace AVEVA::AzureClient
