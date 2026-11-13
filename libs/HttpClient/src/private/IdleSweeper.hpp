// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "ConnectionPool.hpp"
#include "DiagnosticsSink.hpp"
#include "StreamTypes.hpp"
#include "SweeperNotifier.hpp"
#include "Timeouts.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

namespace AVEVA::Private {
using PlainConnectionPool = ConnectionPool<PlainStream>;
using TlsConnectionPool = ConnectionPool<TlsStream>;

/// <summary>
/// Closes expired idle pooled sockets even when no request arrives. Sweeps run on the caller's io_context from a
/// timer that is only armed while a pool holds idle connections. The client is meant for servers whose io_context
/// runs until shutdown, so a pending timer is harmless; <see cref="Stop"/> cancels it so the sweeper never touches
/// the io_context after the owner is done with it.
/// </summary>
class IdleSweeper : public std::enable_shared_from_this<IdleSweeper> {
    /// <summary>
    /// Restricts construction to <see cref="Create"/> while still letting it use <c>std::make_shared</c>.
    /// </summary>
    class Passkey {
        friend IdleSweeper;
        Passkey() = default;
    };

  public:
    static constexpr std::chrono::milliseconds DefaultMinInterval{1000};

    /// <summary>
    /// Creates a sweeper together with the plain and TLS pools it sweeps. The pools are built here, with a
    /// notifier that refers back to the sweeper, so neither side needs a second setup step.
    /// </summary>
    /// <param name="context">The io_context the sweeps run on; must outlive the sweeper's last <see cref="Stop"/>.</param>
    /// <param name="maxIdlePerKey">How many idle connections each pool keeps per origin.</param>
    /// <param name="idleTimeout">How long a connection may sit idle; also how often to sweep while either pool
    /// is non-empty (clamped to at least <paramref name="minInterval"/>).</param>
    /// <param name="probeThreshold">How long a pooled connection must have been idle before it is probed for
    /// liveness when reused.</param>
    /// <param name="diagnostics">Where a failed sweep or disposal is reported; null reports nothing.</param>
    /// <param name="minInterval">Floor for the sweep interval. Production keeps the one-second default so a tiny idle
    /// timeout cannot spin the io_context; tests lower it so they run in milliseconds.</param>
    /// <returns>The sweeper; its pools are available through <see cref="PlainPool"/> and
    /// <see cref="TlsPool"/>.</returns>
    static std::shared_ptr<IdleSweeper> Create(boost::asio::io_context& context, ConnectionCount maxIdlePerKey,
                                               Timeout idleTimeout, std::chrono::milliseconds probeThreshold,
                                               std::shared_ptr<const DiagnosticsSink> diagnostics,
                                               std::chrono::milliseconds minInterval = DefaultMinInterval) {
        auto sweeper = std::make_shared<IdleSweeper>(Passkey{}, context, idleTimeout, minInterval, diagnostics);
        sweeper->m_plainPool = std::make_shared<PlainConnectionPool>(
            maxIdlePerKey, idleTimeout, PlainConnectionPool::DefaultMaxTotalIdle,
            SweeperNotifier(sweeper, sweeper->m_scheduledFlag), probeThreshold, diagnostics);
        sweeper->m_tlsPool = std::make_shared<TlsConnectionPool>(
            maxIdlePerKey, idleTimeout, TlsConnectionPool::DefaultMaxTotalIdle,
            SweeperNotifier(sweeper, sweeper->m_scheduledFlag), probeThreshold, diagnostics);
        return sweeper;
    }

    /// <summary>
    /// Only <see cref="Create"/> can call this; it finishes the setup by building the pools.
    /// </summary>
    IdleSweeper(Passkey /*onlyCreate*/, boost::asio::io_context& context, Timeout idleTimeout,
                std::chrono::milliseconds minInterval, std::shared_ptr<const DiagnosticsSink> diagnostics)
        : m_timer(std::make_unique<boost::asio::steady_timer>(context)),
          m_interval(std::max(idleTimeout.Value(), minInterval)), m_diagnostics(std::move(diagnostics)) {}

    IdleSweeper(const IdleSweeper&) = delete;
    IdleSweeper& operator=(const IdleSweeper&) = delete;

    /// <summary>
    /// The pool of plain HTTP connections this sweeper sweeps.
    /// </summary>
    std::shared_ptr<PlainConnectionPool> PlainPool() const noexcept { return m_plainPool; }

    /// <summary>
    /// The pool of HTTPS connections this sweeper sweeps.
    /// </summary>
    std::shared_ptr<TlsConnectionPool> TlsPool() const noexcept { return m_tlsPool; }

    /// <summary>
    /// Called on every release into a pool; schedules a sweep unless one is already pending. The plain load
    /// keeps the common already-pending case free of a read-modify-write on a line shared by all releasers.
    /// </summary>
    void Notify() noexcept {
        if (m_scheduled.load(std::memory_order_acquire) == SweepState::Idle &&
            m_scheduled.exchange(SweepState::Pending) == SweepState::Idle) {
            ScheduleSweep();
        }
    }

    /// <summary>
    /// Sweeps both pools and schedules the next sweep while either still holds idle connections. Runs on
    /// the io_context. The pending flag is cleared before sweeping so a release that lands during the
    /// sweep schedules itself instead of being lost.
    /// </summary>
    void RunSweep() {
        std::lock_guard<std::mutex> sweepLock(m_sweepMutex);
        if (m_run == SweeperRun::Stopped) {
            return;
        }
        m_scheduled = SweepState::Idle;
        if (m_plainPool->Sweep() + m_tlsPool->Sweep() > 0 &&
            m_scheduled.exchange(SweepState::Pending) == SweepState::Idle) {
            ScheduleSweep();
        }
    }

    /// <summary>
    /// Cancels the pending sweep, waits for one in flight, prevents later ones, and disposes the idle connections on
    /// the calling thread. The owner calls this before its io_context may go away, so a sweep can never post a
    /// disposal to an executor that no longer exists.
    /// </summary>
    void Stop() noexcept {
        {
            std::lock_guard<std::mutex> sweepLock(m_sweepMutex);
            m_run = SweeperRun::Stopped;
        }
        {
            std::lock_guard<std::mutex> timerLock(m_timerMutex);
            // Destroying the timer cancels its wait, and leaves nothing that refers to the io_context.
            m_timer.reset();
        }
        m_plainPool->Clear();
        m_tlsPool->Clear();
    }

    /// <summary>
    /// Test hook: while disabled, no sweep is ever scheduled, process-wide. A server's io_context runs until shutdown,
    /// but tests that run an io_context to completion would otherwise never see it drain behind the sweep timer.
    /// </summary>
    enum class Sweeping : std::uint8_t { Enabled, Disabled };
    static void SetSweeping(Sweeping mode) noexcept { SweepingMode().store(mode); }

  private:
    static std::atomic<Sweeping>& SweepingMode() noexcept {
        static std::atomic<Sweeping> mode{Sweeping::Enabled};
        return mode;
    }

    /// <summary>
    /// Arms the timer for the next sweep; the caller has already set the pending flag. Arming can throw
    /// (allocation), and the flag must not stay set then or no later release would ever schedule again.
    /// </summary>
    void ScheduleSweep() noexcept {
        try {
            if (SweepingMode().load() == Sweeping::Disabled) {
                m_scheduled = SweepState::Idle;
                return;
            }
            std::lock_guard<std::mutex> timerLock(m_timerMutex);
            if (!m_timer) {
                return;
            }
            m_timer->expires_after(m_interval);
            // Weak, so a sweeper destroyed before the timer fires is simply skipped.
            m_timer->async_wait([weak = weak_from_this()](const boost::system::error_code& error) {
                if (error) {
                    return;
                }
                if (const auto sweeper = weak.lock()) {
                    sweeper->RunSweepGuarded();
                }
            });
        } catch (...) {
            m_scheduled = SweepState::Idle;
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {},
                                    {}, {},
                                    "Scheduling an idle-connection sweep failed; it will be retried on a release");
        }
    }

    /// <summary>
    /// A sweep that throws is swallowed so it cannot unwind into the io_context's run loop.
    /// </summary>
    void RunSweepGuarded() noexcept {
        try {
            RunSweep();
        } catch (...) {
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {},
                                    {}, {}, "An idle-connection sweep threw; it will be retried on the next release");
        }
    }

    std::shared_ptr<PlainConnectionPool> m_plainPool;
    std::shared_ptr<TlsConnectionPool> m_tlsPool;
    std::mutex m_timerMutex;
    std::unique_ptr<boost::asio::steady_timer> m_timer; // Guarded by m_timerMutex; null once stopped.
    std::chrono::milliseconds m_interval;
    const std::shared_ptr<const DiagnosticsSink> m_diagnostics;
    // Shared with the notifiers so a release can see a pending sweep without locking the weak_ptr.
    std::shared_ptr<std::atomic<SweepState>> m_scheduledFlag =
        std::make_shared<std::atomic<SweepState>>(SweepState::Idle);
    std::atomic<SweepState>& m_scheduled = *m_scheduledFlag;
    std::mutex m_sweepMutex;
    enum class SweeperRun : std::uint8_t { Running, Stopped };
    std::atomic<SweeperRun> m_run{SweeperRun::Running};
};
} // namespace AVEVA::Private
