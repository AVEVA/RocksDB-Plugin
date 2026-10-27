// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <atomic>
#include <memory>
#include <utility>

namespace AVEVA::Private {
class IdleSweeper;

/// <summary>
/// Tells an <see cref="IdleSweeper"/> that a pool just gained an idle connection. Holds the sweeper weakly
/// because a pool can outlive the client (and so the sweeper) while in-flight requests still reference it.
/// Declared apart from the sweeper so the pool can name it without depending on the pool types the sweeper
/// sweeps; the call itself is defined in IdleSweeper.cpp.
/// </summary>
class SweeperNotifier {
  public:
    SweeperNotifier() = default;

    /// <summary>
    /// Constructs a notifier for one sweeper.
    /// </summary>
    /// <param name="sweeper">The sweeper to wake.</param>
    /// <param name="scheduled">The sweeper's pending-sweep flag; while set, a wake-up is redundant and the
    /// weak_ptr lock (an atomic refcount round trip) is skipped.</param>
    SweeperNotifier(std::weak_ptr<IdleSweeper> sweeper, std::shared_ptr<std::atomic<bool>> scheduled) noexcept
        : m_sweeper(std::move(sweeper)), m_scheduled(std::move(scheduled)) {}

    /// <summary>
    /// Wakes the sweeper if it still exists.
    /// </summary>
    void operator()() const noexcept;

  private:
    std::weak_ptr<IdleSweeper> m_sweeper;
    std::shared_ptr<std::atomic<bool>> m_scheduled;
};
} // namespace AVEVA::Private
