// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/Detail/Lifetime.hpp>
#include <AVEVA/HttpClient/Units.hpp>

#include <gsl/pointers>

#include <cstddef>
#include <memory>
#include <span>
#include <string>

namespace AVEVA {
/// <summary>
/// The bytes of a request body together with whatever keeps them alive. Copying a body shares the bytes instead of
/// duplicating them, so every copy of a request (and the operation sending it) can read them for as long as it needs
/// to; nothing about the body's lifetime is left to the caller.
/// </summary>
class HttpBody {
  public:
    /// <summary>
    /// An empty body.
    /// </summary>
    HttpBody() noexcept;

    /// <summary>
    /// Takes ownership of text, moving it into shared storage.
    /// </summary>
    /// <param name="content">The bytes to send.</param>
    explicit HttpBody(std::string content);

    /// <summary>
    /// Sends caller memory without copying it. The owner is held by the body (and every copy of it), so the memory
    /// stays valid for as long as anything can still read it.
    /// </summary>
    /// <param name="content">The bytes to send, owned by <paramref name="owner"/>.</param>
    /// <param name="owner">Keeps <paramref name="content"/> alive.</param>
    HttpBody(std::span<const std::byte> content, gsl::not_null<std::shared_ptr<const void>> owner) noexcept;

    /// <summary>
    /// The body length.
    /// </summary>
    /// <returns>The number of bytes.</returns>
    [[nodiscard]] ByteCount Size() const noexcept;

    /// <summary>
    /// The bytes of the body.
    /// </summary>
    /// <returns>A view that stays valid for as long as this body (or a copy of it) is alive.</returns>
    [[nodiscard]] std::span<const std::byte> Bytes() const& noexcept AVEVA_LIFETIMEBOUND;
    std::span<const std::byte> Bytes() && = delete;

  private:
    std::span<const std::byte> m_bytes;
    std::shared_ptr<const void> m_owner;
};
} // namespace AVEVA
