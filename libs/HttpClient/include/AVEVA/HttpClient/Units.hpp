// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

namespace AVEVA {
struct ByteCountTag;
struct ConnectionCountTag;
struct RetryCountTag;
struct HeaderNameTag;
struct HeaderValueTag;
struct HttpUrlTag;
struct PemTextTag;
struct DiagnosticOriginTag;
struct DiagnosticMessageTag;

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
/// <summary>The number of retries after the first attempt; zero disables retries.</summary>
using RetryCount = StrongInteger<RetryCountTag, std::uint32_t>;

/// <summary>
/// A string that has a meaning of its own, so a header value cannot be passed where a header name is expected.
/// Construction is explicit; the text is read through <see cref="Value"/>, whose result lives as long as the object.
/// </summary>
template <typename Tag> class StrongString {
  public:
    StrongString() = default;
    explicit StrongString(std::string value) noexcept : m_value(std::move(value)) {}

    [[nodiscard]] const std::string& Value() const& noexcept AVEVA_LIFETIMEBOUND { return m_value; }
    [[nodiscard]] std::string Value() && noexcept { return std::move(m_value); }

    friend bool operator==(const StrongString&, const StrongString&) = default;

  private:
    std::string m_value;
};

/// <summary>An HTTP header field name.</summary>
using HeaderName = StrongString<HeaderNameTag>;
/// <summary>An HTTP header field value.</summary>
using HeaderValue = StrongString<HeaderValueTag>;
/// <summary>An absolute http(s) URL, exactly as supplied; it is parsed and validated when the request is
/// sent.</summary>
using HttpUrl = StrongString<HttpUrlTag>;
/// <summary>Concatenated PEM-encoded certificates held in memory.</summary>
using PemText = StrongString<PemTextTag>;
/// <summary>The "host:port" a diagnostic event concerns.</summary>
using DiagnosticOrigin = StrongString<DiagnosticOriginTag>;
/// <summary>A short human-readable description of a diagnostic event.</summary>
using DiagnosticMessage = StrongString<DiagnosticMessageTag>;

/// <summary>
/// A strictly positive length of time. A zero or negative timeout is rejected when it is built, so it can never reach
/// a request or a pool and fail there. Values beyond <see cref="Max"/> (such as <c>milliseconds::max()</c>) stand for
/// "no timeout" and are clamped, so no clock arithmetic can overflow; sub-millisecond values round up.
/// </summary>
class Timeout {
  public:
    /// <summary>The longest timeout kept; anything larger is clamped to it.</summary>
    static constexpr std::chrono::hours Max{24 * 365};

    /// <exception cref="std::invalid_argument">The duration is zero or negative.</exception>
    template <typename Rep, typename Period>
    explicit constexpr Timeout(std::chrono::duration<Rep, Period> duration) : m_value(Clamp(duration)) {}

    [[nodiscard]] constexpr std::chrono::milliseconds Value() const noexcept { return m_value; }

    friend constexpr auto operator<=>(Timeout, Timeout) noexcept = default;
    friend constexpr bool operator==(Timeout, Timeout) noexcept = default;

  private:
    template <typename Rep, typename Period>
    static constexpr std::chrono::milliseconds Clamp(std::chrono::duration<Rep, Period> duration) {
        if (duration <= std::chrono::duration<Rep, Period>::zero()) {
            throw std::invalid_argument("A timeout must be positive.");
        }
        if (duration >= Max) {
            return Max;
        }
        return std::chrono::ceil<std::chrono::milliseconds>(duration);
    }

    std::chrono::milliseconds m_value;
};
} // namespace AVEVA
