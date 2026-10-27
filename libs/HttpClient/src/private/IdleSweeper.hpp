// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"
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
#include <new>
#include <optional>
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
    static constexpr std::chrono::milliseconds DefaultMinInterval{1000};

    /// <summary>
    /// Creates a sweeper together with the plain and TLS pools it sweeps. The pools are built here, with a
    /// notifier that refers back to the sweeper, so neither side needs a second setup step.
    /// </summary>
    /// <param name="maxIdlePerKey">How many idle connections each pool keeps per origin.</param>
    /// <param name="idleTimeout">How long a connection may sit idle; also how often to sweep while either pool
    /// is non-empty (clamped to at least <paramref name="minInterval"/>).</param>
    /// <param name="probeThreshold">How long a pooled connection must have been idle before it is probed for
    /// liveness when reused.</param>
    /// <param name="minInterval">Floor for the sweep interval. Production keeps the one-second default so a tiny idle
    /// timeout cannot spin the shared thread; tests lower it so they run in milliseconds.</param>
    /// <returns>The sweeper; its pools are available through <see cref="PlainPool"/> and
    /// <see cref="TlsPool"/>.</returns>
    static std::shared_ptr<IdleSweeper>
    Create(std::size_t maxIdlePerKey, std::chrono::milliseconds idleTimeout,
           std::chrono::milliseconds probeThreshold = PlainConnectionPool::DefaultProbeThreshold,
           std::chrono::milliseconds minInterval = DefaultMinInterval) {
        std::shared_ptr<IdleSweeper> sweeper(new IdleSweeper(idleTimeout, minInterval));
        sweeper->m_plainPool =
            std::make_shared<PlainConnectionPool>(maxIdlePerKey, idleTimeout, PlainConnectionPool::DefaultMaxTotalIdle,
                                                  SweeperNotifier(sweeper, sweeper->m_scheduledFlag), probeThreshold);
        sweeper->m_tlsPool =
            std::make_shared<TlsConnectionPool>(maxIdlePerKey, idleTimeout, TlsConnectionPool::DefaultMaxTotalIdle,
                                                SweeperNotifier(sweeper, sweeper->m_scheduledFlag), probeThreshold);
        return sweeper;
    }

    /// <summary>
    /// Whole-second timeouts (the public option's unit), clamped before conversion so seconds::max() cannot
    /// overflow.
    /// </summary>
    static std::shared_ptr<IdleSweeper>
    Create(std::size_t maxIdlePerKey, std::chrono::seconds idleTimeout,
           std::chrono::milliseconds probeThreshold = PlainConnectionPool::DefaultProbeThreshold) {
        return Create(maxIdlePerKey,
                      std::chrono::duration_cast<std::chrono::milliseconds>(std::clamp(
                          idleTimeout, std::chrono::seconds::zero(), std::chrono::seconds{EffectivelyInfinite})),
                      probeThreshold, DefaultMinInterval);
    }

    /// <summary>
    /// Removes any pending registration so a destroyed sweeper does not linger in the scheduler queue until its
    /// (possibly very distant) deadline.
    /// </summary>
    ~IdleSweeper() { m_scheduler->Cancel(*this); }

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
    /// Called on every release into a pool; schedules a sweep unless one is already pending. The plain load
    /// keeps the common already-pending case free of a read-modify-write on a line shared by all releasers.
    /// </summary>
    void Notify() noexcept {
        if (!m_scheduled.load(std::memory_order_acquire) && !m_scheduled.exchange(true)) {
            ScheduleSweep();
        }
    }

    /// <summary>
    /// Sweeps both pools and schedules the next sweep while either still holds idle connections. Runs on
    /// the scheduler thread. The pending flag is cleared before sweeping so a release that lands during the
    /// sweep schedules itself instead of being lost.
    /// </summary>
    void RunSweep() {
        std::lock_guard<std::mutex> sweepLock(m_sweepMutex);
        if (m_stopped) {
            return;
        }
        m_scheduled = false;
        if (m_plainPool->Sweep() + m_tlsPool->Sweep() > 0 && !m_scheduled.exchange(true)) {
            ScheduleSweep();
        }
    }

    /// <summary>
    /// Waits for any sweep in flight, prevents later ones, and disposes the idle connections on the calling
    /// thread. The owner calls this before its io_context may go away, so the scheduler thread can never be the
    /// one that posts a disposal to an executor that no longer exists.
    /// </summary>
    void Stop() {
        {
            std::lock_guard<std::mutex> sweepLock(m_sweepMutex);
            m_stopped = true;
        }
        m_scheduler->Cancel(*this);
        m_plainPool->Clear();
        m_tlsPool->Clear();
    }

    /// <summary>
    /// Whether the process-wide sweep scheduler (and so its thread) is currently alive; it exists exactly while
    /// at least one sweeper does.
    /// </summary>
    static bool SchedulerIsAlive() { return SweepScheduler::IsAlive(); }

    /// <summary>
    /// Sets where a failed sweep is reported. Call before any connection is pooled; null reports nothing.
    /// </summary>
    void SetDiagnostics(std::shared_ptr<const DiagnosticsSink> diagnostics) { m_diagnostics = std::move(diagnostics); }

    /// <summary>
    /// How many times the scheduler thread has gone to sleep on a deadline, process-wide. Lets a test wait for the
    /// thread to be asleep instead of guessing with a fixed delay.
    /// </summary>
    static std::uint64_t SchedulerWaitCount() noexcept { return SweepScheduler::WaitCount().load(); }

    /// <summary>
    /// Test hook: makes the next <paramref name="count"/> attempts to schedule a sweep, process-wide, throw.
    /// </summary>
    static void FailNextSchedules(int count) noexcept { SweepScheduler::FailureBudget().store(count); }

    /// <summary>
    /// Test hook: how many registrations the process-wide scheduler currently holds; zero when it does not exist.
    /// </summary>
    static std::size_t SchedulerQueueSize() { return SweepScheduler::QueueSize(); }

  private:
    class SweepScheduler;

    /// <summary>
    /// Queues the next sweep; the caller has already set the pending flag. Scheduling can throw (allocation,
    /// thread creation), and the flag must not stay set then or no later release would ever schedule again.
    /// </summary>
    void ScheduleSweep() noexcept {
        try {
            m_scheduler->Schedule(*this, m_interval);
        } catch (...) {
            m_scheduled = false;
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {},
                                    {}, {},
                                    "Scheduling an idle-connection sweep failed; it will be retried on a release");
        }
    }

    void ReportSweepFailure() const noexcept {
        DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {}, {},
                                {}, "An idle-connection sweep threw; it will be retried on the next release");
    }

    IdleSweeper(std::chrono::milliseconds idleTimeout, std::chrono::milliseconds minInterval)
        : m_interval(std::clamp(idleTimeout, minInterval,
                                std::chrono::duration_cast<std::chrono::milliseconds>(EffectivelyInfinite))) {}

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

        static std::size_t QueueSize() {
            std::shared_ptr<SweepScheduler> scheduler;
            {
                std::lock_guard<std::mutex> lock(RegistryMutex());
                scheduler = Registered().lock();
            }
            if (!scheduler) {
                return 0;
            }
            std::lock_guard<std::mutex> lock(scheduler->m_state->mutex);
            return scheduler->m_state->queue.size();
        }

        static std::atomic<std::uint64_t>& WaitCount() noexcept {
            static std::atomic<std::uint64_t> count{0};
            return count;
        }

        static std::atomic<int>& FailureBudget() noexcept {
            static std::atomic<int> budget{0};
            return budget;
        }

        SweepScheduler(const SweepScheduler&) = delete;
        SweepScheduler& operator=(const SweepScheduler&) = delete;

        /// <summary>
        /// Queues a sweeper to run after a delay, starting the thread if it is not yet running. The weak
        /// reference means a sweeper destroyed in the meantime is skipped. Nothing is queued once the sweeper
        /// has been stopped; the stop flag is checked under the queue lock so it cannot race with Cancel.
        /// </summary>
        /// <param name="sweeper">The sweeper to run; it records its queue deadline so it can be cancelled.</param>
        /// <param name="delay">How long to wait before running it.</param>
        void Schedule(IdleSweeper& sweeper, std::chrono::milliseconds delay) {
            if (FailureBudget().fetch_sub(1) > 0) {
                throw std::bad_alloc();
            }
            {
                std::lock_guard<std::mutex> lock(m_state->mutex);
                if (sweeper.m_stopped.load()) {
                    return;
                }
                const auto due = Clock::now() + delay;
                m_state->queue.emplace(due, Entry{&sweeper, sweeper.weak_from_this()});
                sweeper.m_queuedDue = due;
                if (!m_thread.joinable()) {
                    m_thread = std::jthread([state = m_state](std::stop_token stop) { Run(*state, stop); });
                }
            }
            m_state->wake.notify_all();
        }

        /// <summary>
        /// Drops the sweeper's pending registration, if any. It is found by deadline and owner, so this costs
        /// O(log n) rather than a scan of the queue.
        /// </summary>
        void Cancel(IdleSweeper& sweeper) {
            std::lock_guard<std::mutex> lock(m_state->mutex);
            if (!sweeper.m_queuedDue) {
                return;
            }
            auto [first, last] = m_state->queue.equal_range(*sweeper.m_queuedDue);
            for (; first != last; ++first) {
                if (first->second.owner == &sweeper) {
                    m_state->queue.erase(first);
                    break;
                }
            }
            sweeper.m_queuedDue.reset();
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

        struct Entry {
            const IdleSweeper* owner; // Identity for Cancel; never dereferenced.
            std::weak_ptr<IdleSweeper> sweeper;
        };

        struct State {
            std::mutex mutex;
            std::condition_variable_any wake;
            std::multimap<Clock::time_point, Entry> queue;
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
                    WaitCount().fetch_add(1);
                    state.wake.wait_until(lock, stop, due, [&state, due] {
                        return !state.queue.empty() && state.queue.begin()->first < due;
                    });
                    continue;
                }
                auto sweeper = state.queue.begin()->second.sweeper.lock();
                state.queue.erase(state.queue.begin());
                if (!sweeper) {
                    continue;
                }
                sweeper->m_queuedDue.reset();
                lock.unlock();
                try {
                    sweeper->RunSweep();
                } catch (...) {
                    // A failing sweep must not end the thread that serves every other client.
                    sweeper->ReportSweepFailure();
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
    std::chrono::milliseconds m_interval;
    std::shared_ptr<const DiagnosticsSink> m_diagnostics;
    // Shared with the notifiers so a release can see a pending sweep without locking the weak_ptr.
    std::shared_ptr<std::atomic<bool>> m_scheduledFlag = std::make_shared<std::atomic<bool>>(false);
    std::atomic<bool>& m_scheduled = *m_scheduledFlag;
    std::mutex m_sweepMutex;
    std::atomic<bool> m_stopped{false};
    std::optional<std::chrono::steady_clock::time_point> m_queuedDue; // Guarded by the scheduler queue mutex.
    std::shared_ptr<SweepScheduler> m_scheduler = SweepScheduler::Acquire();
};
} // namespace AVEVA::Private
