// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "StreamTypes.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace AVEVA::Private {
template <typename Stream> class RequestOperation;

/// <summary>
/// The one thing a caller's cancellation slot is wired to. The slot handler is installed once, on the
/// submitting thread, and only ever calls <see cref="Cancel"/>; later stages of the request (limiter queue,
/// running operation) swap the <see cref="SetTarget"/> instead of touching the slot, which Asio does not
/// allow from other threads. Thread-safe.
///
/// The targets are a closed set (a queue entry in the limiter, or a running operation), held in a variant
/// rather than a type-erased callable, so setting or copying one never allocates.
/// </summary>
/// <typeparam name="Core">The client core. Only named, never required to be complete here, because its
/// limiter type refers back to the queued request that holds this state.</typeparam>
template <typename Core> class CancelState {
  public:
    /// <summary>
    /// A request waiting in the per-origin limiter. The origin is shared so that the copy <see cref="Cancel"/>
    /// takes under the lock stays cheap.
    /// </summary>
    struct LimiterTarget {
        std::weak_ptr<typename Core::Limiter> Limiter;
        std::shared_ptr<const std::string> Origin;
        std::uint64_t Ticket;
    };

    using Target = std::variant<std::monostate, std::weak_ptr<RequestOperation<PlainStream>>,
                                std::weak_ptr<RequestOperation<TlsStream>>, LimiterTarget>;

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
        Deliver(target);
    }

    /// <summary>
    /// Replaces the target. If cancellation already happened, the new target is invoked right away so a
    /// cancel that fell between two owners is not lost.
    /// </summary>
    /// <param name="target">What to call on cancellation.</param>
    void SetTarget(Target target) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_cancelled.load()) {
                m_target = std::move(target);
                return;
            }
            // Rare: keep a copy as the current target so a repeated Cancel still reaches it.
            m_target = target;
        }
        Deliver(target);
    }

    /// <summary>
    /// Whether cancellation has been signalled.
    /// </summary>
    [[nodiscard]] bool IsCancelled() const noexcept { return m_cancelled.load(); }

  private:
    // The target runs on whichever thread emits the signal, so each case only hands off to the owner that
    // is safe to call from any thread (the operation posts to its strand).
    static void Deliver(const Target& target) {
        std::visit(
            [](const auto& held) {
                using Held = std::decay_t<decltype(held)>;
                if constexpr (std::is_same_v<Held, LimiterTarget>) {
                    if (auto limiter = held.Limiter.lock()) {
                        limiter->Cancel(*held.Origin, held.Ticket);
                    }
                } else if constexpr (!std::is_same_v<Held, std::monostate>) {
                    if (auto operation = held.lock()) {
                        operation->RequestCancellation();
                    }
                }
            },
            target);
    }

    std::mutex m_mutex;
    Target m_target;
    std::atomic<bool> m_cancelled{false};
};
} // namespace AVEVA::Private
