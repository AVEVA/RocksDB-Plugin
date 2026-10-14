// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <utility>

namespace AVEVA::Private {
/// <summary>
/// The one thing a caller's cancellation slot is wired to. The slot handler is installed once, on the
/// submitting thread, and only ever calls <see cref="Cancel"/>; later stages of the request (limiter queue,
/// running operation) swap the <see cref="SetTarget"/> instead of touching the slot, which Asio does not
/// allow from other threads. Thread-safe.
/// </summary>
class CancelState {
  public:
    using Target = std::function<void()>;

    /// <summary>
    /// Records the cancellation and invokes the current target, outside the lock, if there is one.
    /// </summary>
    void Cancel() {
        Target target;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_cancelled.store(true);
            target = m_target;
        }
        if (target) {
            target();
        }
    }

    /// <summary>
    /// Replaces the target. If cancellation already happened, the new target is invoked right away so a
    /// cancel that fell between two owners is not lost.
    /// </summary>
    /// <param name="target">What to call on cancellation; it must be safe to call from any thread.</param>
    void SetTarget(Target target) {
        bool fireNow = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_target = target;
            fireNow = m_cancelled.load();
        }
        if (fireNow) {
            target();
        }
    }

    /// <summary>
    /// Whether cancellation has been signalled.
    /// </summary>
    [[nodiscard]] bool IsCancelled() const noexcept { return m_cancelled.load(); }

  private:
    std::mutex m_mutex;
    Target m_target;
    std::atomic<bool> m_cancelled{false};
};
} // namespace AVEVA::Private
