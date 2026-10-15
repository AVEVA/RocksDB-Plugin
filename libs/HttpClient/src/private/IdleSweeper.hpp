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
            SweepScheduler::Instance().Schedule(weak_from_this(), m_interval);
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
            SweepScheduler::Instance().Schedule(weak_from_this(), m_interval);
        }
    }

  private:
    explicit IdleSweeper(std::chrono::seconds idleTimeout)
        : m_interval(std::clamp(idleTimeout, std::chrono::seconds{1}, std::chrono::seconds{EffectivelyInfinite})) {}

    /// <summary>
    /// One thread for the whole process, started on first use, that runs every sweeper's sweeps in deadline
    /// order. A per-sweeper thread would cost a stack for each of many short-lived clients.
    /// </summary>
    class SweepScheduler {
      public:
        /// <summary>
        /// The process-wide scheduler.
        /// </summary>
        /// <returns>The shared scheduler.</returns>
        static SweepScheduler& Instance() {
            static SweepScheduler scheduler;
            return scheduler;
        }

        /// <summary>
        /// Queues a sweeper to run after a delay, starting the thread if it is not yet running. The weak
        /// reference means a sweeper destroyed in the meantime is skipped.
        /// </summary>
        /// <param name="sweeper">The sweeper to run.</param>
        /// <param name="delay">How long to wait before running it.</param>
        void Schedule(const std::weak_ptr<IdleSweeper>& sweeper, std::chrono::seconds delay) {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_queue.emplace(Clock::now() + delay, sweeper);
                if (!m_thread.joinable()) {
                    m_thread = std::jthread([this](std::stop_token stop) { Run(stop); });
                }
            }
            m_wake.notify_all();
        }

        ~SweepScheduler() {
            m_thread.request_stop();
            m_wake.notify_all();
        }

      private:
        using Clock = std::chrono::steady_clock;

        /// <summary>
        /// Waits for the earliest due sweep and runs it, until a stop is requested. A sweep that throws is
        /// swallowed so it cannot end the thread that serves everyone else.
        /// </summary>
        /// <param name="stop">Signals that the thread should return.</param>
        void Run(std::stop_token stop) {
            std::unique_lock<std::mutex> lock(m_mutex);
            while (!stop.stop_requested()) {
                if (m_queue.empty()) {
                    m_wake.wait(lock, stop, [this] { return !m_queue.empty(); });
                    continue;
                }
                const auto due = m_queue.begin()->first;
                if (Clock::now() < due) {
                    m_wake.wait_until(lock, stop, due,
                                      [this, due] { return !m_queue.empty() && m_queue.begin()->first < due; });
                    continue;
                }
                auto sweeper = m_queue.begin()->second.lock();
                m_queue.erase(m_queue.begin());
                if (!sweeper) {
                    continue;
                }
                lock.unlock();
                try {
                    sweeper->RunSweep();
                } catch (...) {
                    // A failing sweep must not end the thread that serves every other client.
                }
                sweeper.reset();
                lock.lock();
            }
        }

        std::mutex m_mutex;
        std::condition_variable_any m_wake;
        std::multimap<Clock::time_point, std::weak_ptr<IdleSweeper>> m_queue;
        std::jthread m_thread; // Declared last so every other member exists before the thread starts.
    };

    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::chrono::seconds m_interval;
    std::atomic<bool> m_scheduled{false};
};
} // namespace AVEVA::Private
