// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ErrorTransience.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TaskGroup.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>

#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>Whether the renewal loop is still scheduling rounds.</summary>
enum class LoopState : std::uint8_t { Running, Stopped };

/// <summary>A duration with a meaning of its own, so the renewal interval and the retry delay cannot be
/// swapped.</summary>
template <typename Tag> class TaggedDuration {
  public:
    explicit constexpr TaggedDuration(const std::chrono::milliseconds value) noexcept : m_value(value) {}
    [[nodiscard]] constexpr std::chrono::milliseconds Value() const noexcept { return m_value; }

  private:
    std::chrono::milliseconds m_value;
};

struct RenewalIntervalTag;
struct RetryBaseDelayTag;
struct RetryAttemptsTag;
/// <summary>How long the loop sleeps between regular renewal rounds.</summary>
using RenewalInterval = TaggedDuration<RenewalIntervalTag>;
/// <summary>The first retry delay after a failed renewal; later retries back off exponentially from it.</summary>
using RetryBaseDelay = TaggedDuration<RetryBaseDelayTag>;
/// <summary>How many rounds (the first included) a failing lease gets before the loop stops retrying it.</summary>
using RetryAttempts = Core::StrongInteger<RetryAttemptsTag, size_t>;

/// <summary>
/// Keeps leases alive from the host io_context, with no thread of its own. One coroutine sleeps for `interval`, then
/// renews every lease returned by the snapshot side by side through `co_await lock.RenewAsync()`. Failed renewals are
/// retried with exponential backoff and jitter starting at `retryDelay` (never later than the earliest renewal
/// deadline of any held lease, and not sooner than a Retry-After hint from the service allows). While failed leases
/// are retried, the healthy ones are still renewed every `interval`: each retry round that comes due merges in a
/// fresh snapshot, so one transient failure cannot starve the other leases into fencing the filesystem.
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
    /// Creates the loop and starts its coroutine on `executor`. `snapshot` and `onFatal` are only invoked while the
    /// loop is running, never after Stop() returns; they run with the loop's internal mutex held and so must not
    /// block or call back into the loop.
    /// </summary>
    static std::shared_ptr<BasicLeaseRenewalLoop>
    Start(boost::asio::io_context::executor_type executor, std::shared_ptr<Logger> logger, TSnapshot snapshot,
          TOnFatal onFatal, const RenewalInterval interval,
          const RetryBaseDelay retryDelay = RetryBaseDelay{std::chrono::milliseconds(100)},
          const RetryAttempts maxAttempts = RetryAttempts{5}) {
        std::shared_ptr<BasicLeaseRenewalLoop> loop(
            new BasicLeaseRenewalLoop(executor, std::move(logger), std::move(snapshot), std::move(onFatal),
                                      interval.Value(), retryDelay.Value(), maxAttempts.Value()));
        BOOST_LOG_SEV(*loop->m_logger, boost::log::trivial::info) << "Starting blob lease renewal";
        boost::asio::co_spawn(executor, Run(loop), boost::asio::detached);
        return loop;
    }

    ~BasicLeaseRenewalLoop() = default;
    BasicLeaseRenewalLoop(const BasicLeaseRenewalLoop&) = delete;
    BasicLeaseRenewalLoop& operator=(const BasicLeaseRenewalLoop&) = delete;

    /// <summary>
    /// Cancels the timer and waits for a renewal round that is already on the wire; idempotent. Rounds finish on
    /// the io_context, so it must still be running unless no round is in flight. Must not be called from a thread
    /// running the executor. If a round is still in flight after giveUpAfter (the io_context is not running),
    /// an error is logged and Stop returns anyway: the round's coroutine only touches state it owns by shared_ptr.
    /// </summary>
    void Stop(const std::chrono::milliseconds giveUpAfter = std::chrono::minutes(2)) {
        std::unique_lock lock(m_mutex);
        const auto previous = std::exchange(m_state, LoopState::Stopped);
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
        if (previous == LoopState::Running) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::info) << "Exiting blob lease renewal";
        }
    }

    [[nodiscard]] LoopState State() const {
        std::scoped_lock lock(m_mutex);
        return m_state;
    }

  private:
    static constexpr auto StopWarningInterval = std::chrono::seconds(5);

    // These failures can never succeed on retry: the service no longer recognises our lease. The code is checked in
    // because 400/412 lease failures carry the same meaning as a 409 (which a transient failure never is).
    static AzureClient::Transience FailureKind(const RequestFailedException& e) {
        return AzureErrorTranslator::LeaseLossOf(e.Code()) == LeaseLoss::Lost
                   ? AzureClient::Transience::Permanent
                   // Only what the client's own retry policy would retry can plausibly succeed. Anything else (400,
                   // 401, 403, 404, 501, permanent transport errors, ...) is permanent for a renewal, and retrying it
                   // would only delay fencing until the lease is overdue.
                   : TransienceOf(e);
    }

    // The leases that failed a round and the attempt that comes next.
    struct Retry {
        std::vector<std::shared_ptr<TLock>> Locks;
        size_t Attempt = 0;
    };

    // What to do after a round: sleep `Delay`, then either retry the failed leases or take a fresh snapshot.
    struct Next {
        std::chrono::milliseconds Delay;
        std::optional<Retry> Retry;
    };

    BasicLeaseRenewalLoop(boost::asio::io_context::executor_type executor, std::shared_ptr<Logger> logger,
                          TSnapshot snapshot, TOnFatal onFatal, const std::chrono::milliseconds interval,
                          const std::chrono::milliseconds retryDelay, const size_t maxAttempts)
        : m_logger(std::move(logger)), m_snapshot(std::move(snapshot)), m_onFatal(std::move(onFatal)),
          m_interval(interval), m_retryDelay(retryDelay), m_maxAttempts(maxAttempts), m_timer(std::move(executor)),
          m_random(std::random_device{}()) {}

    // The whole lifetime of the loop: sleep, renew, decide what comes next, until stopped or fenced. `self` is held by
    // the coroutine frame, so the loop outlives it even after the owner drops its reference.
    static boost::asio::awaitable<void> Run(std::shared_ptr<BasicLeaseRenewalLoop> self) {
        auto delay = self->m_interval;
        std::optional<Retry> retry;
        try {
            while (co_await self->Sleep(delay) == LoopState::Running) {
                auto next = co_await self->RunRound(std::exchange(retry, std::nullopt));
                if (!next) {
                    co_return;
                }
                delay = next->Delay;
                retry = std::move(next->Retry);
            }
        } catch (const std::exception& e) {
            self->FailIfRunning(e.what());
        } catch (...) {
            self->FailIfRunning("unknown error");
        }
    }

    // Waits `delay`; reports Stopped once the loop has been stopped. Every timer operation happens under m_mutex, as in
    // Stop(): a Stop that lands before the wait starts makes the timer fire at once instead of being lost, and the
    // state check afterwards means a cancelled or already-queued wake-up can never start a round.
    boost::asio::awaitable<LoopState> Sleep(const std::chrono::milliseconds delay) {
        try {
            co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable), void(boost::system::error_code)>(
                [this, delay](auto handler) {
                    std::scoped_lock lock(m_mutex);
                    m_timer.expires_after(m_state == LoopState::Stopped ? std::chrono::milliseconds::zero() : delay);
                    m_timer.async_wait(std::move(handler));
                },
                boost::asio::use_awaitable);
        } catch (const boost::system::system_error& e) {
            if (e.code() != boost::asio::error::operation_aborted) {
                throw;
            }
        }
        std::scoped_lock lock(m_mutex);
        co_return m_state;
    }

    // Renews the leases of one round (a retry's, or a fresh snapshot) and decides what follows. Returns empty when the
    // loop has stopped. Counting the round before the mutex is released keeps Stop() waiting for it.
    boost::asio::awaitable<std::optional<Next>> RunRound(std::optional<Retry> retry) {
        std::vector<std::shared_ptr<TLock>> locks;
        size_t attempt = 0;
        {
            std::scoped_lock lock(m_mutex);
            if (m_state == LoopState::Stopped) {
                co_return std::nullopt;
            }
            try {
                const auto now = std::chrono::steady_clock::now();
                if (retry) {
                    locks = std::move(retry->Locks);
                    attempt = retry->Attempt;
                    // The healthy leases are due as well: renew them with this retry rather than let them lapse.
                    if (now - m_lastFullRound >= m_interval) {
                        MergeSnapshotInto(locks);
                        m_lastFullRound = now;
                    }
                } else {
                    locks = m_snapshot();
                    m_lastFullRound = now;
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::debug)
                        << "Attempting to renew " << locks.size() << " leases";
                }
            } catch (const std::exception& e) {
                Fail(e.what());
                co_return std::nullopt;
            } catch (...) {
                Fail("unknown error");
                co_return std::nullopt;
            }
            if (locks.empty()) {
                co_return Next{m_interval, std::nullopt};
            }
            ++m_activeRounds;
        }

        std::vector<std::exception_ptr> errors;
        {
            const boost::scope::scope_exit release{[this]() noexcept { ReleaseRound(); }};
            errors = co_await RenewAll(locks);
        }

        // If classifying or scheduling throws (for example on allocation), renewal could no longer continue, so fence
        // rather than let the exception escape the host's run().
        std::string failure;
        std::scoped_lock lock(m_mutex);
        try {
            co_return Classify(locks, errors, attempt);
        } catch (const std::exception& e) {
            failure = e.what();
        } catch (...) {
            failure = "unknown error";
        }
        if (m_state == LoopState::Running) {
            try {
                Fail(failure);
            } catch (...) {
                // Fail marks the loop stopped before notifying, so a throwing onFatal cannot be reported twice.
            }
        }
        co_return std::nullopt;
    }

    // Renews every lease concurrently and returns each lease's failure (null on success) in the same order. A
    // failure, including a throw while starting the renewal, is recorded rather than thrown, so the siblings are
    // always awaited.
    static boost::asio::awaitable<std::vector<std::exception_ptr>> RenewAll(std::vector<std::shared_ptr<TLock>> locks) {
        auto errors = std::make_shared<std::vector<std::exception_ptr>>(locks.size());
        TaskGroup group(co_await boost::asio::this_coro::executor, ConcurrencyLimit{locks.size()});
        co_await group.Scope([&](TaskGroup& running) -> boost::asio::awaitable<void> {
            for (size_t i = 0; i < locks.size(); ++i) {
                co_await running.Spawn(RecordRenewal(locks[i], errors, i));
            }
        });
        co_return std::move(*errors);
    }

    static boost::asio::awaitable<void> RecordRenewal(std::shared_ptr<TLock> lock,
                                                      std::shared_ptr<std::vector<std::exception_ptr>> errors,
                                                      const size_t slot) {
        (*errors)[slot] = co_await RenewOne(std::move(lock));
    }

    // The renewal's failure, or null when it succeeded.
    static boost::asio::awaitable<std::exception_ptr> RenewOne(std::shared_ptr<TLock> lock) {
        try {
            (void)co_await lock->RenewAsync();
        } catch (...) {
            co_return std::current_exception();
        }
        co_return nullptr;
    }

    // Requires m_mutex. Adds every lease of a fresh snapshot that `locks` does not already hold.
    void MergeSnapshotInto(std::vector<std::shared_ptr<TLock>>& locks) {
        for (auto& held : m_snapshot()) {
            if (std::ranges::find(locks, held) == locks.end()) {
                locks.push_back(std::move(held));
            }
        }
    }

    // Requires m_mutex. Every lease that bounds the next wake-up: the failed ones and all the healthy ones held.
    std::vector<std::shared_ptr<TLock>> DeadlineBound(const std::vector<std::shared_ptr<TLock>>& pending) {
        auto bound = pending;
        MergeSnapshotInto(bound);
        return bound;
    }

    void ReleaseRound() noexcept {
        std::scoped_lock lock(m_mutex);
        --m_activeRounds;
        m_roundsIdle.notify_all();
    }

    // Requires m_mutex. Classifies a finished round and says what comes next: retry the failed leases, fail (returns
    // empty), or sleep until the next interval.
    std::optional<Next> Classify(const std::vector<std::shared_ptr<TLock>>& locks,
                                 const std::vector<std::exception_ptr>& errors, const size_t attempt) {
        if (m_state == LoopState::Stopped) {
            return std::nullopt;
        }

        std::vector<std::shared_ptr<TLock>> stillPending;
        std::chrono::milliseconds serverHint{0};
        std::string fatal;
        for (size_t i = 0; i < locks.size() && fatal.empty(); ++i) {
            if (!errors[i]) {
                continue;
            }
            try {
                std::rethrow_exception(errors[i]);
            } catch (const RequestFailedException& e) {
                if (FailureKind(e) == AzureClient::Transience::Permanent) {
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::error)
                        << "Failed to renew lease, lease is lost or expired, or the failure is permanent: " << e.what();
                    fatal = e.what();
                } else {
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::error) << "Failed to renew lease: " << e.what();
                    stillPending.push_back(locks[i]);
                    serverHint = std::max(serverHint, e.RetryAfter().value_or(std::chrono::milliseconds{0}));
                }
            } catch (const std::exception& e) {
                fatal = e.what();
            } catch (...) {
                fatal = "unknown error";
            }
        }

        if (!fatal.empty()) {
            Fail(fatal);
            return std::nullopt;
        }

        const size_t nextAttempt = attempt + 1;
        if (!stillPending.empty() && nextAttempt < m_maxAttempts) {
            const auto delay = NextRetryDelay(DeadlineBound(stillPending), attempt, serverHint);
            return Next{delay, Retry{std::move(stillPending), nextAttempt}};
        }

        // A lease that could not be renewed in time may already be held by someone else; stop before writing on its
        // behalf rather than waiting for the service to reject us.
        for (const auto& lock : stillPending) {
            if (lock->RenewalDeadlineState() == AzureClient::DeadlineState::Expired) {
                Fail("Lease renewal did not succeed before the lease expired");
                return std::nullopt;
            }
        }

        // Out of retries but not yet overdue: the next regular wake-up must not land after the renewal deadline, or
        // the lease could lapse while writes continue.
        return Next{ClampToEarliestDeadline(m_interval, DeadlineBound(stillPending)), std::nullopt};
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

    // Requires m_mutex.
    void Fail(const std::string& reason) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::fatal) << "Stopping lease renewal: " << reason;
        m_state = LoopState::Stopped;
        m_onFatal();
    }

    void FailIfRunning(const std::string& reason) {
        std::scoped_lock lock(m_mutex);
        if (m_state == LoopState::Running) {
            try {
                Fail(reason);
            } catch (...) {
                // Fail marks the loop stopped before notifying, so a throwing onFatal cannot be reported twice.
            }
        }
    }

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
    // When the last round that renewed every held lease started. Guarded by m_mutex.
    std::chrono::steady_clock::time_point m_lastFullRound = std::chrono::steady_clock::now();
    LoopState m_state = LoopState::Running;
    std::mt19937 m_random;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
