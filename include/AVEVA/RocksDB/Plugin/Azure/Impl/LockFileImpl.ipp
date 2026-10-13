// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/scope/scope_exit.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <cassert>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace LockFileDetail {
// Azure requires proposed lease IDs to be GUID-formatted.
inline std::string NewLeaseId() {
    thread_local boost::uuids::random_generator generator;
    return boost::uuids::to_string(generator());
}

// A held lease (409) is ordinary contention that this loop exists to wait out, so it is retried like a transient
// failure; every other client error (for example 403) cannot succeed on retry and fails fast.
inline bool ShouldRetryAcquire(const AzureClient::BlobStorageError& error) {
    return error.StatusCode == HttpStatus::Conflict || AzureErrorTranslator::IsTransient(error.StatusCode, error.Code);
}
} // namespace LockFileDetail

template <typename TBlob>
BasicLockFileImpl<TBlob>::BasicLockFileImpl(
    std::shared_ptr<ClientRuntimeBase> runtime, std::unique_ptr<TBlob> file, std::chrono::seconds leaseLength,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::string fileName, std::optional<std::chrono::milliseconds> acquireTimeout)
    : m_runtime(std::move(runtime)), m_file(std::move(file)), m_lastRenewalTime(std::chrono::steady_clock::now()),
      m_leaseLength(leaseLength), m_acquireTimeout(acquireTimeout.value_or(leaseLength)), m_logger(std::move(logger)),
      m_fileName(std::move(fileName)) {
    if (m_logger == nullptr) {
        throw std::runtime_error("logger cannot be null.");
    }

    if (m_file == nullptr) {
        throw std::runtime_error("file cannot be null.");
    }
}

template <typename TBlob> bool BasicLockFileImpl<TBlob>::Lock() {
    {
        const std::scoped_lock lock(m_stateMutex);
        // Do not attempt to lock again when a lease is held or another thread is already acquiring one.
        if (m_leaseId.has_value() || m_lockInProgress) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::debug)
                << "Lock already acquired or in progress for '" << m_fileName << "', skipping duplicate lock attempt";
            return false;
        }
        m_lockInProgress = true;
    }
    // The retry loop below blocks on the network and sleeps, so no mutex is held; the flag keeps other Lock calls out.
    const boost::scope::scope_exit clearInProgress([this] {
        const std::scoped_lock lock(m_stateMutex);
        m_lockInProgress = false;
    });

    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::debug)
        << "Attempting to acquire blob lease for '" << m_fileName
        << "' (timeout: " << std::chrono::duration_cast<std::chrono::seconds>(m_acquireTimeout).count() << "s)";
    auto start = std::chrono::steady_clock::now();
    auto end = start;
    // The service starts the lease clock when it receives the request, so the renewal time is taken before sending.
    auto attemptStart = start;
    std::optional<AzureClient::BlobStorageError> lastError;
    // The proposed ID is reused on every attempt: if an acquire succeeded but its response was lost, the retry
    // with the same ID is accepted by the service instead of failing with a conflict until the lease expires.
    const auto leaseId = LockFileDetail::NewLeaseId();
    while ((end - start) < m_acquireTimeout) {
        attemptStart = std::chrono::steady_clock::now();
        AzureClient::AcquireLeaseOptions options;
        options.ProposedLeaseId = leaseId;
        options.Duration = m_leaseLength;
        // Each attempt inherits the client's retry policy, which can run for minutes; cancel it once the overall
        // lock budget is spent so Lock() cannot block far beyond the lease length.
        auto result =
            BlockOnWithin(*m_file, m_acquireTimeout - (attemptStart - start), [&](auto token, auto requestOptions) {
                return m_file->AcquireLeaseAsync(std::move(options), std::move(token), std::move(requestOptions));
            });
        if (result.has_value()) {
            {
                const std::scoped_lock lock(m_stateMutex);
                m_leaseId = leaseId;
            }
            lastError.reset();
            break;
        }

        lastError = result.error();
        if (!LockFileDetail::ShouldRetryAcquire(*lastError)) {
            end = std::chrono::steady_clock::now();
            break;
        }

        // Avoid hammering the service while another owner holds the lease.
        static const constexpr auto retryDelay = std::chrono::milliseconds(250);
        std::this_thread::sleep_for(retryDelay);
        end = std::chrono::steady_clock::now();
    }

    if (lastError.has_value()) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
            << "Failed to acquire blob lease for '" << m_fileName << "' after "
            << std::chrono::duration_cast<std::chrono::seconds>(end - start).count()
            << "s: " << (lastError->Message.empty() ? lastError->Code.message() : lastError->Message);

        // Only a lease held by another owner means "locked"; anything else (auth, TLS, invalid URL, a transient
        // failure that outlasted the retries) is surfaced so AzureErrorTranslator can map it to the right status.
        if (lastError->StatusCode == HttpStatus::Conflict) {
            return false;
        }
        ThrowRequestFailed(*lastError);
    }

    m_lastRenewalTime = attemptStart;
    m_held = true;
    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::info)
        << "Successfully acquired blob lease for '" << m_fileName << "'";
    return true;
}

template <typename TBlob> void BasicLockFileImpl<TBlob>::Renew() const {
    const auto leaseId = CurrentLeaseId();
    if (!leaseId.has_value()) {
        throw std::runtime_error("Cannot renew lease that has not been acquired");
    }
    RenewLease();
}

template <typename TBlob> bool BasicLockFileImpl<TBlob>::RenewIfLocked() const {
    const auto leaseId = CurrentLeaseId();
    if (!leaseId.has_value()) {
        return false;
    }
    try {
        RenewLease();
    } catch (...) {
        // An Unlock that raced with this renewal makes the failure expected, not an error.
        if (CurrentLeaseId() != leaseId) {
            return false;
        }
        throw;
    }
    return true;
}

template <typename TBlob> std::optional<std::string> BasicLockFileImpl<TBlob>::CurrentLeaseId() const {
    const std::scoped_lock lock(m_stateMutex);
    return m_leaseId;
}

template <typename TBlob> std::chrono::steady_clock::duration BasicLockFileImpl<TBlob>::RenewalBudget() const {
    if (HasExceededLeaseLength()) {
        const auto timeSinceRenewal = TimeSinceLastRenewal();
        throw std::runtime_error(
            "Cannot renew expired lease. Time since last renewal: " + std::to_string(timeSinceRenewal.count()) +
            " seconds (max: " + std::to_string(m_leaseLength.count()) + " seconds)");
    }

    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::debug)
        << "Renewing blob lease for '" << m_fileName << "' (time since last renewal: " << TimeSinceLastRenewal().count()
        << "s)";

    // Retries inside one renewal could otherwise outlast the lease while the caller keeps writing as if it held it.
    // The whole request, retries included, is cancelled at the renewal deadline.
    const auto budget = RenewalDeadline() - std::chrono::steady_clock::now();
    if (budget <= std::chrono::steady_clock::duration::zero()) {
        throw std::runtime_error(
            "Cannot renew lease for '" + m_fileName +
            "': the renewal deadline has passed (lease length: " + std::to_string(m_leaseLength.count()) + " seconds)");
    }
    return budget;
}

template <typename TBlob> void BasicLockFileImpl<TBlob>::RenewAsync(RenewCallback callback) const {
    const auto leaseId = CurrentLeaseId();
    if (!leaseId.has_value()) {
        callback(nullptr, false);
        return;
    }
    std::chrono::steady_clock::duration budget;
    try {
        budget = RenewalBudget();
    } catch (...) {
        callback(std::current_exception(), true);
        return;
    }

    AzureClient::RenewLeaseOptions options;
    options.LeaseId = *leaseId;

    // The timer, the cancellation signal and every completion are serialised on one strand: asio timers and
    // cancellation signals are not thread safe, and the AzureClient requires slots to be emitted on its executor.
    struct State {
        State(const boost::asio::io_context::executor_type& executor)
            : Strand(boost::asio::make_strand(executor)), Deadline(Strand) {}
        boost::asio::strand<boost::asio::io_context::executor_type> Strand;
        boost::asio::steady_timer Deadline;
        boost::asio::cancellation_signal Cancel;
        RenewCallback Done;
    };
    auto state = std::make_shared<State>(m_file->get_executor());
    state->Done = std::move(callback);
    const auto requestStart = std::chrono::steady_clock::now();

    boost::asio::post(
        state->Strand, [this, state, leaseId = *leaseId, requestStart, budget, options = std::move(options)]() mutable {
            state->Deadline.expires_after(budget);
            state->Deadline.async_wait([state](const boost::system::error_code& error) {
                if (!error) {
                    state->Cancel.emit(boost::asio::cancellation_type::terminal);
                }
            });
            auto requestOptions = m_file->GetDefaultRequestOptions();
            requestOptions.SetCancellationSlot(state->Cancel.slot());
            // The handler is bound to the strand: the library clears the cancellation slot on the handler's
            // executor, so emitting from the same strand cannot race with that.
            m_file->RenewLeaseAsync(std::move(options),
                                    boost::asio::bind_executor(state->Strand,
                                                               [this, state, leaseId, requestStart](auto result) {
                                                                   state->Deadline.cancel();
                                                                   std::exception_ptr error;
                                                                   bool renewed = true;
                                                                   try {
                                                                       Unwrap(std::move(result));
                                                                       m_lastRenewalTime = requestStart;
                                                                   } catch (...) {
                                                                       // An Unlock that raced with this renewal makes
                                                                       // the failure expected, not an error.
                                                                       if (CurrentLeaseId() != leaseId) {
                                                                           renewed = false;
                                                                       } else {
                                                                           error = std::current_exception();
                                                                       }
                                                                   }
                                                                   state->Done(error, renewed);
                                                               }),
                                    std::move(requestOptions));
        });
}

template <typename TBlob> void BasicLockFileImpl<TBlob>::RenewLease() const {
    ThrowIfRunningOn(m_file->get_executor());
    // One cancellation path: the synchronous renewal waits on the asynchronous one.
    auto outcome = std::make_shared<std::promise<bool>>();
    auto future = outcome->get_future();
    RenewAsync([outcome](std::exception_ptr error, bool renewed) {
        if (error) {
            outcome->set_exception(error);
        } else {
            outcome->set_value(renewed);
        }
    });
    if (!future.get()) {
        throw std::runtime_error("Cannot renew lease for '" + m_fileName + "': it was released");
    }
}

template <typename TBlob> void BasicLockFileImpl<TBlob>::Unlock() {
    const auto leaseId = CurrentLeaseId();
    if (!leaseId.has_value()) {
        throw std::runtime_error("Cannot release lease that has not been acquired");
    }

    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::debug)
        << "Releasing blob lease for '" << m_fileName << "'";
    AzureClient::ReleaseLeaseOptions options;
    options.LeaseId = *leaseId;
    // UnlockFile has already unlinked this lock from renewal, so the lease will lapse regardless of the outcome.
    // Clearing the local state on failure keeps IsRenewalOverdue() from reporting a lease nobody renews as held.
    const boost::scope::scope_exit clearState([this, &leaseId] {
        {
            const std::scoped_lock lock(m_stateMutex);
            if (m_leaseId == leaseId) {
                m_leaseId.reset();
            }
        }
        m_held = false;
    });
    Call(*m_file, [&](auto token) { return m_file->ReleaseLeaseAsync(std::move(options), token); });
    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::debug)
        << "Successfully released blob lease for '" << m_fileName << "'";
}

template <typename TBlob> std::chrono::seconds BasicLockFileImpl<TBlob>::TimeSinceLastRenewal() const {
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                            m_lastRenewalTime.load());
}

template <typename TBlob> bool BasicLockFileImpl<TBlob>::HasExceededLeaseLength() const {
    return TimeSinceLastRenewal() >= m_leaseLength;
}

template <typename TBlob> std::chrono::steady_clock::time_point BasicLockFileImpl<TBlob>::RenewalDeadline() const {
    // Short leases (tests) get a proportionally short margin so the deadline stays after the last renewal.
    const auto margin = std::min<std::chrono::milliseconds>(Configuration::LeaseSafetyMargin,
                                                            std::chrono::milliseconds(m_leaseLength) / 10);
    return m_lastRenewalTime.load() + m_leaseLength - margin;
}

template <typename TBlob> bool BasicLockFileImpl<TBlob>::IsRenewalOverdue() const {
    return m_held.load() && std::chrono::steady_clock::now() >= RenewalDeadline();
}

template <typename TBlob> void BasicLockFileImpl<TBlob>::unlink() {
    boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::unlink();
}

template <typename TBlob> bool BasicLockFileImpl<TBlob>::is_linked() {
    return boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::is_linked();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
