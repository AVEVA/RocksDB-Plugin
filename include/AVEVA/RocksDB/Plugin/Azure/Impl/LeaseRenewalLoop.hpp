// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Keeps leases alive from the host io_context, with no thread of its own: a timer wakes every `interval`, all
/// leases returned by the snapshot are renewed side by side through LockFileImpl::RenewAsync, and failed renewals
/// are retried after `retryDelay`.
///
/// A conflict, any non-HTTP failure, or a lease that is still overdue once the retries are spent is fatal: the loop
/// stops and `onFatal` is invoked once so the owner can fence writes. Transient HTTP failures are only logged.
///
/// While armed, the timer keeps io_context::run() from returning, so the owner must call Stop() (or destroy the
/// loop's owner, which should) before expecting run() to drain.
/// </summary>
class LeaseRenewalLoop : public std::enable_shared_from_this<LeaseRenewalLoop> {
  public:
    using Logger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;
    using Snapshot = std::function<std::vector<std::shared_ptr<LockFileImpl>>()>;

    /// <summary>
    /// Creates the loop and arms the first wake-up. `snapshot` and `onFatal` are only invoked while the loop is
    /// running, never after Stop() returns; they run with the loop's internal mutex held and so must not block or
    /// call back into the loop.
    /// </summary>
    static std::shared_ptr<LeaseRenewalLoop>
    Start(boost::asio::any_io_executor executor, std::shared_ptr<Logger> logger, Snapshot snapshot,
          std::function<void()> onFatal, std::chrono::milliseconds interval,
          std::chrono::milliseconds retryDelay = std::chrono::milliseconds(100), size_t maxAttempts = 5);

    ~LeaseRenewalLoop() = default;
    LeaseRenewalLoop(const LeaseRenewalLoop&) = delete;
    LeaseRenewalLoop& operator=(const LeaseRenewalLoop&) = delete;

    /// <summary>
    /// Cancels the timer and waits for a renewal round that is already on the wire; idempotent. Rounds finish on
    /// the io_context, so it must still be running unless no round is in flight. Must not be called from a thread
    /// running the executor.
    /// </summary>
    void Stop();

    [[nodiscard]] bool IsStopped() const;

  private:
    struct Round;

    LeaseRenewalLoop(boost::asio::any_io_executor executor, std::shared_ptr<Logger> logger, Snapshot snapshot,
                     std::function<void()> onFatal, std::chrono::milliseconds interval,
                     std::chrono::milliseconds retryDelay, size_t maxAttempts);

    // All of the following require m_mutex.
    void ArmTimer(std::chrono::milliseconds delay, std::function<void()> action);
    void ScheduleWakeUp(std::chrono::milliseconds delay);
    void BeginRound(std::vector<std::shared_ptr<LockFileImpl>> locks, size_t attempt);
    void OnRoundDone(const std::shared_ptr<Round>& round);
    void Fail(const std::string& reason);

    boost::asio::any_io_executor m_executor;
    std::shared_ptr<Logger> m_logger;
    Snapshot m_snapshot;
    std::function<void()> m_onFatal;
    std::chrono::milliseconds m_interval;
    std::chrono::milliseconds m_retryDelay;
    size_t m_maxAttempts;

    mutable std::mutex m_mutex;
    std::condition_variable m_roundsIdle;
    boost::asio::steady_timer m_timer;
    size_t m_activeRounds = 0;
    bool m_stopped = false;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
