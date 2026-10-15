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
class IdleSweeper {
  public:
    /// <summary>
    /// Constructs a sweeper over a plain and a TLS pool.
    /// </summary>
    /// <param name="plainPool">The plain pool to sweep.</param>
    /// <param name="tlsPool">The TLS pool to sweep.</param>
    /// <param name="idleTimeout">How often to sweep while either pool is non-empty; clamped to at least one
    /// second.</param>
    IdleSweeper(std::shared_ptr<PlainConnectionPool> plainPool, std::shared_ptr<TlsConnectionPool> tlsPool,
                std::chrono::seconds idleTimeout)
        : m_plainPool(std::move(plainPool)), m_tlsPool(std::move(tlsPool)),
          m_interval(std::clamp(idleTimeout, std::chrono::seconds{1}, std::chrono::seconds{EffectivelyInfinite})) {}

    IdleSweeper(const IdleSweeper&) = delete;
    IdleSweeper& operator=(const IdleSweeper&) = delete;

    /// <summary>
    /// Called when a pool gains an idle connection; schedules a sweep unless one is already pending.
    /// </summary>
    void Notify() {
        if (!m_scheduled.exchange(true)) {
            SweepScheduler::Instance().Schedule(m_self, m_interval);
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
            SweepScheduler::Instance().Schedule(m_self, m_interval);
        }
    }

    /// <summary>
    /// Must be called once the sweeper is owned by a shared_ptr, so it can be scheduled weakly and simply
    /// be skipped once it is gone.
    /// </summary>
    /// <param name="self">The shared_ptr that owns this sweeper.</param>
    void BindSelf(const std::shared_ptr<IdleSweeper>& self) { m_self = self; }

  private:
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
    std::weak_ptr<IdleSweeper> m_self;
};
} // namespace AVEVA::Private
