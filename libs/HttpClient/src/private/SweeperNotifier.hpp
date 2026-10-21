// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

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
    explicit SweeperNotifier(std::weak_ptr<IdleSweeper> sweeper) noexcept : m_sweeper(std::move(sweeper)) {}

    /// <summary>
    /// Wakes the sweeper if it still exists.
    /// </summary>
    void operator()() const noexcept;

  private:
    std::weak_ptr<IdleSweeper> m_sweeper;
};
} // namespace AVEVA::Private
