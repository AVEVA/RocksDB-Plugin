// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LeaseRenewalLoop.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <boost/asio/post.hpp>

#include <string>
#include <utility>

using boost::log::trivial::severity_level;

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
constexpr auto StopWarningInterval = std::chrono::seconds(5);
}

struct LeaseRenewalLoop::Round {
    std::mutex Mutex;
    std::vector<std::shared_ptr<LockFileImpl>> Locks;
    std::vector<std::exception_ptr> Errors;
    size_t Pending = 0;
    size_t Attempt = 0;
};

LeaseRenewalLoop::LeaseRenewalLoop(boost::asio::any_io_executor executor, std::shared_ptr<Logger> logger,
                                   Snapshot snapshot, std::function<void()> onFatal,
                                   const std::chrono::milliseconds interval, const std::chrono::milliseconds retryDelay,
                                   const size_t maxAttempts)
    : m_executor(executor), m_logger(std::move(logger)), m_snapshot(std::move(snapshot)),
      m_onFatal(std::move(onFatal)), m_interval(interval), m_retryDelay(retryDelay), m_maxAttempts(maxAttempts),
      m_timer(std::move(executor)) {}

std::shared_ptr<LeaseRenewalLoop> LeaseRenewalLoop::Start(boost::asio::any_io_executor executor,
                                                          std::shared_ptr<Logger> logger, Snapshot snapshot,
                                                          std::function<void()> onFatal,
                                                          const std::chrono::milliseconds interval,
                                                          const std::chrono::milliseconds retryDelay,
                                                          const size_t maxAttempts) {
    std::shared_ptr<LeaseRenewalLoop> loop(new LeaseRenewalLoop(std::move(executor), std::move(logger),
                                                                std::move(snapshot), std::move(onFatal), interval,
                                                                retryDelay, maxAttempts));
    BOOST_LOG_SEV(*loop->m_logger, severity_level::info) << "Starting blob lease renewal";
    std::scoped_lock lock(loop->m_mutex);
    loop->ScheduleWakeUp(interval);
    return loop;

}

bool LeaseRenewalLoop::IsStopped() const {
    std::scoped_lock lock(m_mutex);
    return m_stopped;
}

void LeaseRenewalLoop::Stop() {
    std::unique_lock lock(m_mutex);
    const bool wasStopped = std::exchange(m_stopped, true);
    m_timer.cancel();
    // Acquiring the mutex above also waited out any wake-up that was already running, so no new round can start.
    while (!m_roundsIdle.wait_for(lock, StopWarningInterval, [this] { return m_activeRounds == 0; })) {
        BOOST_LOG_SEV(*m_logger, severity_level::warning)
            << "Still waiting for " << m_activeRounds
            << " lease renewal(s); the io_context must keep running until the filesystem is destroyed";
    }
    if (!wasStopped) {
        BOOST_LOG_SEV(*m_logger, severity_level::info) << "Exiting blob lease renewal";
    }
}

// Every wake-up re-checks m_stopped under the mutex, so a cancelled or already-queued timer completion can never
// start a round once Stop() has run. The action is invoked with the mutex held.
void LeaseRenewalLoop::ArmTimer(const std::chrono::milliseconds delay, std::function<void()> action) {
    if (m_stopped) {
        return;
    }
    m_timer.expires_after(delay);
    m_timer.async_wait([self = shared_from_this(), action = std::move(action)](const boost::system::error_code& ec) {
        std::scoped_lock lock(self->m_mutex);
        if (ec || self->m_stopped) {
            return;
        }
        try {
            action();
        } catch (const std::exception& e) {
            self->Fail(e.what());
        } catch (...) {
            self->Fail("unknown error");
        }
    });
}

void LeaseRenewalLoop::BeginRound(std::vector<std::shared_ptr<LockFileImpl>> locks, const size_t attempt) {
    if (locks.empty()) {
        ScheduleWakeUp(m_interval);
        return;

    }

    auto round = std::make_shared<Round>();
    round->Locks = std::move(locks);
    round->Errors.resize(round->Locks.size());
    round->Pending = round->Locks.size();
    round->Attempt = attempt;
    ++m_activeRounds;

    // RenewAsync may complete inline (e.g. the lease is not held), possibly while this thread holds m_mutex, so the
    // completion only touches the round's own mutex and hands the rest to the executor.
    for (size_t i = 0; i < round->Locks.size(); ++i) {
        round->Locks[i]->RenewAsync([self = shared_from_this(), round, i](std::exception_ptr error, bool) {
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
}

// Classifies a finished round and schedules what comes next: retry the failed leases, fail, or sleep until the next
// interval. Runs with m_mutex held.
void LeaseRenewalLoop::OnRoundDone(const std::shared_ptr<Round>& round) {
    const auto finish = [this] {
        --m_activeRounds;
        m_roundsIdle.notify_all();
    };
    if (m_stopped) {
        finish();
        return;
    }

    std::vector<std::shared_ptr<LockFileImpl>> stillPending;
    std::string fatal;
    for (size_t i = 0; i < round->Locks.size() && fatal.empty(); ++i) {
        if (!round->Errors[i]) {
            continue;
        }
        try {
            std::rethrow_exception(round->Errors[i]);
        } catch (const RequestFailedException& e) {
            if (e.StatusCode == HttpStatus::Conflict) {
                BOOST_LOG_SEV(*m_logger, severity_level::error)
                    << "Failed to renew lease due to conflict, lease might be expired: " << e.what();
                fatal = e.what();
            } else {
                BOOST_LOG_SEV(*m_logger, severity_level::error) << "Failed to renew lease: " << e.what();
                stillPending.push_back(round->Locks[i]);
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
        ArmTimer(m_retryDelay, [this, locks = std::move(stillPending), nextAttempt]() mutable {
            BeginRound(std::move(locks), nextAttempt);
        });
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

    finish();
    ScheduleWakeUp(m_interval);
}

// Sleeps for `delay`, then snapshots the current leases and renews them as a fresh round.
void LeaseRenewalLoop::ScheduleWakeUp(const std::chrono::milliseconds delay) {
    ArmTimer(delay, [this] {
        auto locks = m_snapshot();
        BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Attempting to renew " << locks.size() << " leases";
        BeginRound(std::move(locks), 0);
    });
}


void LeaseRenewalLoop::Fail(const std::string& reason) {
    BOOST_LOG_SEV(*m_logger, severity_level::fatal) << "Stopping lease renewal: " << reason;
    m_stopped = true;
    m_onFatal();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
