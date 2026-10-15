// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "ConnectionPool.hpp"
#include "StreamTypes.hpp"
#include "Timeouts.hpp"
#include <atomic>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <utility>

namespace AVEVA::Private {
using PlainConnectionPool = ConnectionPool<PlainStream>;
using TlsConnectionPool = ConnectionPool<TlsStream>;

/// <summary>
/// Closes expired idle pooled sockets even when no request arrives. It sleeps until a pool gains an idle
/// connection, and goes back to sleep once both pools are empty. A dedicated thread is used instead of an
/// io_context timer so a pending timer cannot keep the caller's io_context::run() from returning.
/// </summary>
class IdleSweeper : public std::enable_shared_from_this<IdleSweeper> {
  public:
    /// <summary>
    /// Creates a sweeper together with the plain and TLS pools it sweeps. The pools are built here, with a
    /// notifier that refers back to the sweeper, so neither side needs a second setup step.
    /// </summary>
    /// <param name="maxIdlePerKey">How many idle connections each pool keeps per origin.</param>
    /// <param name="idleTimeout">How long a connection may sit idle; also how often to sweep while either pool
    /// is non-empty (clamped to at least one second).</param>
    /// <returns>The sweeper; its pools are available through <see cref="PlainPool"/> and
    /// <see cref="TlsPool"/>.</returns>
    static std::shared_ptr<IdleSweeper> Create(std::size_t maxIdlePerKey, std::chrono::seconds idleTimeout) {
        std::shared_ptr<IdleSweeper> sweeper(new IdleSweeper(idleTimeout));
        sweeper->m_plainPool = std::make_shared<PlainConnectionPool>(
            maxIdlePerKey, idleTimeout, PlainConnectionPool::DefaultMaxTotalIdle, SweeperNotifier(sweeper));
        sweeper->m_tlsPool = std::make_shared<TlsConnectionPool>(
            maxIdlePerKey, idleTimeout, TlsConnectionPool::DefaultMaxTotalIdle, SweeperNotifier(sweeper));
        return sweeper;
    }

    IdleSweeper(const IdleSweeper&) = delete;
    IdleSweeper& operator=(const IdleSweeper&) = delete;

    /// <summary>
    /// The pool of plain HTTP connections this sweeper sweeps.
    /// </summary>
    const std::shared_ptr<PlainConnectionPool>& PlainPool() const noexcept { return m_plainPool; }

    /// <summary>
    /// The pool of HTTPS connections this sweeper sweeps.
    /// </summary>
    const std::shared_ptr<TlsConnectionPool>& TlsPool() const noexcept { return m_tlsPool; }

    /// <summary>
    /// Called when a pool gains an idle connection; schedules a sweep unless one is already pending.
    /// </summary>
    void Notify() {
        if (!m_scheduled.exchange(true)) {
            m_scheduler->Schedule(weak_from_this(), m_interval);
        }
    }

    /// <summary>
    /// Sweeps both pools and schedules the next sweep while either still holds idle connections. Runs on
    /// the scheduler thread. The pending flag is cleared before sweeping so a release that lands during the
    /// sweep schedules itself instead of being lost.
    /// </summary>
    void RunSweep() {
        m_scheduled = false;
        if (m_plainPool->Sweep() + m_tlsPool->Sweep() > 0 && !m_scheduled.exchange(true)) {
            m_scheduler->Schedule(weak_from_this(), m_interval);
        }
    }

    /// <summary>
    /// Whether the process-wide sweep scheduler (and so its thread) is currently alive; it exists exactly while
    /// at least one sweeper does.
    /// </summary>
    static bool SchedulerIsAlive() { return SweepScheduler::IsAlive(); }

  private:
    class SweepScheduler;

    explicit IdleSweeper(std::chrono::seconds idleTimeout)
        : m_interval(std::clamp(idleTimeout, std::chrono::seconds{1}, std::chrono::seconds{EffectivelyInfinite})) {}

    /// <summary>
    /// One thread shared by every live sweeper, running their sweeps in deadline order. A per-sweeper thread
    /// would cost a stack for each of many short-lived clients. The scheduler is reference counted by the
    /// sweepers: the thread starts when the first sweep is scheduled and is stopped and joined when the last
    /// sweeper is destroyed, so nothing is left running (or joined at static destruction) afterwards.
    /// </summary>
    class SweepScheduler {
      public:
        /// <summary>
        /// The scheduler shared by all live sweepers, created if there is none.
        /// </summary>
        /// <returns>A reference that keeps the scheduler alive.</returns>
        static std::shared_ptr<SweepScheduler> Acquire() {
            std::lock_guard<std::mutex> lock(RegistryMutex());
            auto& registered = Registered();
            if (auto existing = registered.lock()) {
                return existing;
            }
            std::shared_ptr<SweepScheduler> created(new SweepScheduler);
            registered = created;
            return created;
        }

        /// <summary>
        /// Whether a scheduler (and so possibly its thread) currently exists.
        /// </summary>
        /// <returns>True while at least one sweeper is alive.</returns>
        static bool IsAlive() {
            std::lock_guard<std::mutex> lock(RegistryMutex());
            return !Registered().expired();
        }

        SweepScheduler(const SweepScheduler&) = delete;
        SweepScheduler& operator=(const SweepScheduler&) = delete;

        /// <summary>
        /// Queues a sweeper to run after a delay, starting the thread if it is not yet running. The weak
        /// reference means a sweeper destroyed in the meantime is skipped.
        /// </summary>
        /// <param name="sweeper">The sweeper to run.</param>
        /// <param name="delay">How long to wait before running it.</param>
        void Schedule(const std::weak_ptr<IdleSweeper>& sweeper, std::chrono::seconds delay) {
            {
                std::lock_guard<std::mutex> lock(m_state->mutex);
                m_state->queue.emplace(Clock::now() + delay, sweeper);
                if (!m_thread.joinable()) {
                    m_thread = std::jthread([state = m_state](std::stop_token stop) { Run(*state, stop); });
                }
            }
            m_state->wake.notify_all();
        }

        /// <summary>
        /// Stops the thread. If the last sweeper was released by the thread itself, it cannot join itself,
        /// so it is detached; it only touches the shared state, which it keeps alive, and exits at once.
        /// </summary>
        ~SweepScheduler() {
            m_thread.request_stop();
            m_state->wake.notify_all();
            if (m_thread.joinable() && m_thread.get_id() == std::this_thread::get_id()) {
                m_thread.detach();
            }
        }

      private:
        using Clock = std::chrono::steady_clock;

        SweepScheduler() : m_state(std::make_shared<State>()) {}

        struct State {
            std::mutex mutex;
            std::condition_variable_any wake;
            std::multimap<Clock::time_point, std::weak_ptr<IdleSweeper>> queue;
        };

        static std::mutex& RegistryMutex() {
            static std::mutex mutex;
            return mutex;
        }

        static std::weak_ptr<SweepScheduler>& Registered() {
            static std::weak_ptr<SweepScheduler> registered;
            return registered;
        }

        /// <summary>
        /// Waits for the earliest due sweep and runs it, until a stop is requested. A sweep that throws is
        /// swallowed so it cannot end the thread that serves everyone else.
        /// </summary>
        /// <param name="state">The queue to serve; kept alive by the thread itself.</param>
        /// <param name="stop">Signals that the thread should return.</param>
        static void Run(State& state, std::stop_token stop) {
            std::unique_lock<std::mutex> lock(state.mutex);
            while (!stop.stop_requested()) {
                if (state.queue.empty()) {
                    state.wake.wait(lock, stop, [&state] { return !state.queue.empty(); });
                    continue;
                }
                const auto due = state.queue.begin()->first;
                if (Clock::now() < due) {
                    state.wake.wait_until(lock, stop, due, [&state, due] {
                        return !state.queue.empty() && state.queue.begin()->first < due;
                    });
                    continue;
                }
                auto sweeper = state.queue.begin()->second.lock();
                state.queue.erase(state.queue.begin());
                if (!sweeper) {
                    continue;
                }
                lock.unlock();
                try {
                    sweeper->RunSweep();
                } catch (...) {
                    // A failing sweep must not end the thread that serves every other client.
                }
                // May destroy the last sweeper, and with it the scheduler, on this very thread.
                sweeper.reset();
                lock.lock();
            }
        }

        std::shared_ptr<State> m_state;
        std::jthread m_thread; // Declared last so the state exists before the thread starts.
    };
    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::chrono::seconds m_interval;
    std::atomic<bool> m_scheduled{false};
    std::shared_ptr<SweepScheduler> m_scheduler = SweepScheduler::Acquire();
};
} // namespace AVEVA::Private
