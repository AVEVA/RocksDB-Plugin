// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <chrono>

namespace AVEVA::Private
{
    // Stands in for "no timeout". Callers may pass values such as milliseconds::max(), which would overflow the
    // clock's duration or time_point arithmetic, so every timer and idle limit is clamped to this instead.
    inline constexpr std::chrono::hours EffectivelyInfinite{24 * 365};

    // How long a TLS close_notify exchange may take before the socket is dropped; it only courteously informs the
    // peer, so it must never hold up a completed request for long.
    inline constexpr std::chrono::seconds TlsShutdownGrace{1};
} // namespace AVEVA::Private
