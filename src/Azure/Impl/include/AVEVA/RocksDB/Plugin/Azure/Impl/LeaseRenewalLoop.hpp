// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Keeps leases alive from the host io_context, with no thread of its own: a timer wakes every `interval`, all
/// leases returned by the snapshot are renewed side by side through LockFileImpl::RenewAsync, and failed renewals
/// are retried with exponential backoff and jitter starting at `retryDelay` (never later than the earliest renewal
/// deadline, and not sooner than a Retry-After hint from the service allows).
///
/// A conflict or other lease-lost failure (by status or BlobStorageErrorCode), any other permanent failure (a status
/// that is not 408, 429 or 5xx, e.g. 401/403/404), a non-service exception, or a lease
/// that is still overdue once the retries are spent is fatal: the loop stops and `onFatal` is invoked once so the
/// owner can fence writes. Transient failures (transport errors with status 0, 408, 429, 5xx) are retried and only
/// logged.
///
/// While armed, the timer keeps io_context::run() from returning, so the owner must call Stop() (or destroy the
/// loop's owner, which should) before expecting run() to drain.
///
/// The snapshot and fatal callables are template parameters rather than erased functions: the loop is created once
/// per filesystem, so there is a single instantiation per owner.
/// </summary>
template <std::invocable TSnapshot, std::invocable TOnFatal, typename TLock = LockFileImpl>
    requires std::convertible_to<std::invoke_result_t<TSnapshot&>, std::vector<std::shared_ptr<TLock>>>
class BasicLeaseRenewalLoop : public std::enable_shared_from_this<BasicLeaseRenewalLoop<TSnapshot, TOnFatal, TLock>> {
  public:
    using Logger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;

    /// <summary>
    /// Creates the loop and arms the first wake-up. `snapshot` and `onFatal` are only invoked while the loop is
    /// running, never after Stop() returns; they run with the loop's internal mutex held and so must not block or
    /// call back into the loop. Renewals (LockFileImpl::RenewAsync) are started with the mutex released, and their
    /// completions run on whatever thread finishes them without taking it.
    /// </summary>
    static std::shared_ptr<BasicLeaseRenewalLoop>
    Start(boost::asio::io_context::executor_type executor, std::shared_ptr<Logger> logger, TSnapshot snapshot,
          TOnFatal onFatal, std::chrono::milliseconds interval,
          std::chrono::milliseconds retryDelay = std::chrono::milliseconds(100), size_t maxAttempts = 5) {
        std::shared_ptr<BasicLeaseRenewalLoop> loop(new BasicLeaseRenewalLoop(std::move(executor), std::move(logger),
                                                                              std::move(snapshot), std::move(onFatal),
                                                                              interval, retryDelay, maxAttempts));
        BOOST_LOG_SEV(*loop->m_logger, boost::log::trivial::info) << "Starting blob lease renewal";
        std::scoped_lock lock(loop->m_mutex);
        loop->ScheduleWakeUp(interval);
        return loop;
    }

    ~BasicLeaseRenewalLoop() = default;
    BasicLeaseRenewalLoop(const BasicLeaseRenewalLoop&) = delete;
    BasicLeaseRenewalLoop& operator=(const BasicLeaseRenewalLoop&) = delete;

    /// <summary>
    /// Cancels the timer and waits for a renewal round that is already on the wire; idempotent. Rounds finish on
    /// the io_context, so it must still be running unless no round is in flight. Must not be called from a thread
    /// running the executor. If a round is still in flight after giveUpAfter (the io_context is not running),
    /// an error is logged and Stop returns anyway: the round's completion only touches state it owns by shared_ptr.
    /// </summary>
    void Stop(const std::chrono::milliseconds giveUpAfter = std::chrono::minutes(2)) {
        std::unique_lock lock(m_mutex);
        const bool wasStopped = std::exchange(m_stopped, true);
        m_timer.cancel();
        // Acquiring the mutex above also waited out any wake-up that was already running, so no new round can start.
        const auto giveUpAt = std::chrono::steady_clock::now() + giveUpAfter;
        while (!m_roundsIdle.wait_for(lock, std::min<std::chrono::milliseconds>(StopWarningInterval, giveUpAfter),
                                      [this] { return m_activeRounds == 0; })) {
            if (std::chrono::steady_clock::now() >= giveUpAt) {
                BOOST_LOG_SEV(*m_logger, boost::log::trivial::error)
                    << "Giving up on " << m_activeRounds << " lease renewal(s) that never completed; the io_context "
                    << "was not running them";
                break;
            }
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::warning)
                << "Still waiting for " << m_activeRounds
                << " lease renewal(s); the io_context must keep running until the filesystem is destroyed";
        }
        if (!wasStopped) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::info) << "Exiting blob lease renewal";
        }
    }

    [[nodiscard]] bool IsStopped() const {
        std::scoped_lock lock(m_mutex);
        return m_stopped;
    }

  private:
    static constexpr auto StopWarningInterval = std::chrono::seconds(5);

    // These failures can never succeed on retry: the service no longer recognises our lease. The code is checked in
    // because 400/412 lease failures carry the same meaning as a 409 (which IsTransient already rejects).
    static bool IsFatalLeaseFailure(const RequestFailedException& e) {
        using AzureClient::BlobStorageErrorCode;
        for (const auto code :
             {BlobStorageErrorCode::LeaseLost, BlobStorageErrorCode::LeaseIdMissing,
              BlobStorageErrorCode::LeaseAlreadyPresent, BlobStorageErrorCode::LeaseIdMismatchWithBlobOperation,
              BlobStorageErrorCode::LeaseNotPresentWithBlobOperation}) {
            if (e.Code == AzureClient::make_error_code(code)) {
                return true;
            }
        }
        return false;
    }

    // Only what the client's own retry policy would retry can plausibly succeed. Anything else (400, 401, 403, 404,
    // 501, permanent transport errors, ...) is permanent for a renewal, and retrying it would only delay fencing until
    // the lease is overdue.
    static bool IsTransient(const RequestFailedException& e) { return AzureErrorTranslator::IsTransient(e); }

    struct Round {
        std::mutex Mutex;
        std::vector<std::shared_ptr<TLock>> Locks;
        std::vector<std::exception_ptr> Errors;
        size_t Pending = 0;
        size_t Attempt = 0;
    };

    // What the timer does when it fires: a regular wake-up (no state) or a retry of the leases that failed.
    struct Retry {
        std::vector<std::shared_ptr<TLock>> Locks;
        size_t Attempt = 0;
    };

    BasicLeaseRenewalLoop(boost::asio::io_context::executor_type executor, std::shared_ptr<Logger> logger,
                          TSnapshot snapshot, TOnFatal onFatal, const std::chrono::milliseconds interval,
                          const std::chrono::milliseconds retryDelay, const size_t maxAttempts)
        : m_executor(executor), m_logger(std::move(logger)), m_snapshot(std::move(snapshot)),
          m_onFatal(std::move(onFatal)), m_interval(interval), m_retryDelay(retryDelay), m_maxAttempts(maxAttempts),
          m_timer(std::move(executor)), m_random(std::random_device{}()) {}

    // All of the following require m_mutex.

    // Every wake-up re-checks m_stopped under the mutex, so a cancelled or already-queued timer completion can never
    // start a round once Stop() has run. The snapshot is taken and the round registered with the mutex held; the
    // renewals themselves are started after it is released (see StartRound).
    void ArmTimer(const std::chrono::milliseconds delay, std::optional<Retry> retry) {
        if (m_stopped) {
            return;
        }
        m_timer.expires_after(delay);
        m_timer.async_wait(
            [self = this->shared_from_this(), retry = std::move(retry)](const boost::system::error_code& ec) mutable {
                std::shared_ptr<Round> round;
                {
                    std::scoped_lock lock(self->m_mutex);
                    if (ec || self->m_stopped) {
                        return;
                    }
                    try {
                        if (retry) {
                            round = self->PrepareRound(std::move(retry->Locks), retry->Attempt);
                        } else {
                            auto locks = self->m_snapshot();
                            BOOST_LOG_SEV(*self->m_logger, boost::log::trivial::debug)
                                << "Attempting to renew " << locks.size() << " leases";
                            round = self->PrepareRound(std::move(locks), 0);
                        }
                    } catch (const std::exception& e) {
                        self->Fail(e.what());
                    } catch (...) {
                        self->Fail("unknown error");
                    }
                }
                if (round) {
                    self->StartRound(round);
                }
            });
    }

    // Sleeps for `delay`, then snapshots the current leases and renews them as a fresh round.
    void ScheduleWakeUp(const std::chrono::milliseconds delay) { ArmTimer(delay, std::nullopt); }

    // Registers a round for the given leases and returns it, or re-arms the timer and returns null when there is
    // nothing to renew. Counting the round before the mutex is released keeps Stop() waiting for it.
    std::shared_ptr<Round> PrepareRound(std::vector<std::shared_ptr<TLock>> locks, const size_t attempt) {
        if (locks.empty()) {
            ScheduleWakeUp(m_interval);
            return nullptr;
        }

        auto round = std::make_shared<Round>();
        round->Locks = std::move(locks);
        round->Errors.resize(round->Locks.size());
        round->Pending = round->Locks.size();
        round->Attempt = attempt;
        ++m_activeRounds;
        return round;
    }

    // Must be called WITHOUT m_mutex: RenewAsync may complete inline and does network setup, and a completion that is
    // the last of its round posts back to the executor, where OnRoundDone takes the mutex.
    void StartRound(const std::shared_ptr<Round>& round) {
        try {
            for (size_t i = 0; i < round->Locks.size(); ++i) {
                round->Locks[i]->RenewAsync(
                    [self = this->shared_from_this(), round, i](std::exception_ptr error, bool) {
                        bool last = false;
                        {
                            std::scoped_lock guard(round->Mutex);
                            round->Errors[i] = std::move(error);
                            last = --round->Pending == 0;
                        }
                        if (last) {
                            boost::asio::post(self->m_executor, [self, round] {
                                std::scoped_lock lock(self->m_mutex);
                                self->OnRoundDone(round);
                            });
                        }
                    });
            }
        } catch (const std::exception& e) {
            AbandonRound(e.what());
        } catch (...) {
            AbandonRound("unknown error");
        }
    }

    // A renewal that could not even be started: the round can never complete, so release it and stop.
    void AbandonRound(const std::string& reason) {
        std::scoped_lock lock(m_mutex);
        --m_activeRounds;
        m_roundsIdle.notify_all();
        if (!m_stopped) {
            Fail(reason);
        }
    }

    // Classifies a finished round and schedules what comes next: retry the failed leases, fail, or sleep until the
    // next interval.
    void OnRoundDone(const std::shared_ptr<Round>& round) {
        const auto finish = [this] {
            --m_activeRounds;
            m_roundsIdle.notify_all();
        };
        if (m_stopped) {
            finish();
            return;
        }

        std::vector<std::shared_ptr<TLock>> stillPending;
        std::chrono::milliseconds serverHint{0};
        std::string fatal;
        for (size_t i = 0; i < round->Locks.size() && fatal.empty(); ++i) {
            if (!round->Errors[i]) {
                continue;
            }
            try {
                std::rethrow_exception(round->Errors[i]);
            } catch (const RequestFailedException& e) {
                if (IsFatalLeaseFailure(e) || !IsTransient(e)) {
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::error)
                        << "Failed to renew lease, lease is lost or expired, or the failure is permanent: " << e.what();
                    fatal = e.what();
                } else {
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::error) << "Failed to renew lease: " << e.what();
                    stillPending.push_back(round->Locks[i]);
                    serverHint = std::max(serverHint, e.RetryAfter.value_or(std::chrono::milliseconds{0}));
                }
            } catch (const std::exception& e) {
                fatal = e.what();
            } catch (...) {
                fatal = "unknown error";
            }
        }

        if (!fatal.empty()) {
            finish();
            Fail(fatal);
            return;
        }

        const size_t nextAttempt = round->Attempt + 1;
        if (!stillPending.empty() && nextAttempt < m_maxAttempts) {
            finish();
            const auto delay = NextRetryDelay(stillPending, round->Attempt, serverHint);
            ArmTimer(delay, Retry{std::move(stillPending), nextAttempt});
            return;
        }

        // A lease that could not be renewed in time may already be held by someone else; stop before writing on its
        // behalf rather than waiting for the service to reject us.
        for (const auto& lock : stillPending) {
            if (lock->IsRenewalOverdue()) {
                finish();
                Fail("Lease renewal did not succeed before the lease expired");
                return;
            }
        }

        // Out of retries but not yet overdue: the next regular wake-up must not land after the renewal deadline, or
        // the lease could lapse while writes continue.
        finish();
        ScheduleWakeUp(ClampToEarliestDeadline(m_interval, stillPending));
    }

    // Shortens `delay` so a wake-up never fires after the earliest renewal deadline of `pending`.
    static std::chrono::milliseconds ClampToEarliestDeadline(std::chrono::milliseconds delay,
                                                             const std::vector<std::shared_ptr<TLock>>& pending) {
        for (const auto& lock : pending) {
            if (const auto remaining = lock->RenewalTimeRemaining()) {
                delay = std::min(delay, std::max(std::chrono::milliseconds{0},
                                                 std::chrono::duration_cast<std::chrono::milliseconds>(*remaining)));
            }
        }
        return delay;
    }

    // Exponential backoff with jitter (half to full of the step, so concurrent filesystems do not retry in
    // lockstep), raised to the service's Retry-After hint but never past the earliest renewal deadline: a retry that
    // lands after the lease is overdue is worthless.
    std::chrono::milliseconds NextRetryDelay(const std::vector<std::shared_ptr<TLock>>& pending, const size_t attempt,
                                             const std::chrono::milliseconds serverHint) {
        auto step = m_retryDelay;
        for (size_t i = 0; i < attempt && step < m_interval; ++i) {
            step *= 2;
        }
        step = std::min(step, std::max(m_interval, m_retryDelay));
        const auto low = step.count() / 2;
        auto delay = std::chrono::milliseconds(
            std::uniform_int_distribution<std::chrono::milliseconds::rep>(low, std::max(low, step.count()))(m_random));
        return ClampToEarliestDeadline(std::max(delay, serverHint), pending);
    }

    void Fail(const std::string& reason) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::fatal) << "Stopping lease renewal: " << reason;
        m_stopped = true;
        m_onFatal();
    }

    boost::asio::io_context::executor_type m_executor;
    std::shared_ptr<Logger> m_logger;
    TSnapshot m_snapshot;
    TOnFatal m_onFatal;
    std::chrono::milliseconds m_interval;
    std::chrono::milliseconds m_retryDelay;
    size_t m_maxAttempts;

    mutable std::mutex m_mutex;
    std::condition_variable m_roundsIdle;
    boost::asio::steady_timer m_timer;
    size_t m_activeRounds = 0;
    bool m_stopped = false;
    std::mt19937 m_random;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
