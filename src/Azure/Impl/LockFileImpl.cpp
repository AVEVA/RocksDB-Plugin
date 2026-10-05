// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/AzureErrorTranslator.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <boost/asio/use_future.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <cassert>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

using boost::log::trivial::severity_level;

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
// Azure requires proposed lease IDs to be GUID-formatted.
std::string NewLeaseId() {
    thread_local boost::uuids::random_generator generator;
    return boost::uuids::to_string(generator());
}

// A held lease (409) is ordinary contention that this loop exists to wait out, so it is retried like a transient
// failure; every other client error (for example 403) cannot succeed on retry and fails fast.
bool ShouldRetryAcquire(const AzureClient::BlobStorageError& error) {
    return error.StatusCode == HttpStatus::Conflict || AzureErrorTranslator::IsTransient(error.StatusCode, error.Code);
}
} // namespace
LockFileImpl::LockFileImpl(
    std::shared_ptr<ClientRuntime> runtime, std::unique_ptr<AzureClient::PageBlobClient> file,
    std::chrono::seconds leaseLength,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::string fileName)
    : m_runtime(std::move(runtime)), m_file(std::move(file)), m_lastRenewalTime(std::chrono::steady_clock::now()),
      m_leaseLength(leaseLength), m_logger(std::move(logger)), m_fileName(std::move(fileName)) {
    if (m_logger == nullptr) {
        throw std::runtime_error("logger cannot be null.");
    }

    if (m_file == nullptr) {
        throw std::runtime_error("file cannot be null.");
    }
}

bool LockFileImpl::Lock() {
    const std::scoped_lock lock(m_ioMutex);
    // Do not attempt to lock again when you already have a lock aquired.
    if (m_leaseId.has_value()) {
        BOOST_LOG_SEV(*m_logger, severity_level::debug)
            << "Lock already acquired for '" << m_fileName << "', skipping duplicate lock attempt";
        return false;
    }

    BOOST_LOG_SEV(*m_logger, severity_level::debug)
        << "Attempting to acquire blob lease for '" << m_fileName << "' (timeout: " << m_leaseLength.count() << "s)";
    auto start = std::chrono::steady_clock::now();
    auto end = start;
    // The service starts the lease clock when it receives the request, so the renewal time is taken before sending.
    auto attemptStart = start;
    std::optional<std::string> lastError;
    // The proposed ID is reused on every attempt: if an acquire succeeded but its response was lost, the retry
    // with the same ID is accepted by the service instead of failing with a conflict until the lease expires.
    const auto leaseId = NewLeaseId();
    while ((end - start) < m_leaseLength) {
        attemptStart = std::chrono::steady_clock::now();
        AzureClient::AcquireLeaseOptions options;
        options.ProposedLeaseId = leaseId;
        options.Duration = m_leaseLength;
        auto result = BlockOn(m_file->get_executor(), m_file->AcquireLeaseAsync(std::move(options), boost::asio::use_future));
        if (result.has_value()) {
            m_leaseId = leaseId;
            lastError.reset();
            break;
        }

        lastError = result.error().Message.empty() ? result.error().Code.message() : result.error().Message;
        if (!ShouldRetryAcquire(result.error())) {
            end = std::chrono::steady_clock::now();
            break;
        }

        // Avoid hammering the service while another owner holds the lease.
        static const constexpr auto retryDelay = std::chrono::milliseconds(250);
        std::this_thread::sleep_for(retryDelay);
        end = std::chrono::steady_clock::now();
    }

    if (lastError.has_value()) {
        BOOST_LOG_SEV(*m_logger, severity_level::error)
            << "Failed to acquire blob lease for '" << m_fileName << "' after "
            << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << "s: " << *lastError;

        return false;
    }

    m_lastRenewalTime = attemptStart;
    BOOST_LOG_SEV(*m_logger, severity_level::info) << "Successfully acquired blob lease for '" << m_fileName << "'";
    return true;
}

void LockFileImpl::Renew() const {
    const std::scoped_lock lock(m_ioMutex);
    RenewLocked();
}

bool LockFileImpl::RenewIfLocked() const {
    const std::scoped_lock lock(m_ioMutex);
    if (!m_leaseId.has_value()) {
        return false;
    }
    RenewLocked();
    return true;
}

void LockFileImpl::RenewLocked() const {
    if (!m_leaseId.has_value()) {
        throw std::runtime_error("Cannot renew lease that has not been acquired");
    }

    if (HasExceededLeaseLength()) {
        const auto timeSinceRenewal = TimeSinceLastRenewal();
        throw std::runtime_error(
            "Cannot renew expired lease. Time since last renewal: " + std::to_string(timeSinceRenewal.count()) +
            " seconds (max: " + std::to_string(m_leaseLength.count()) + " seconds)");
    }

    BOOST_LOG_SEV(*m_logger, severity_level::debug)
        << "Renewing blob lease for '" << m_fileName << "' (time since last renewal: " << TimeSinceLastRenewal().count()
        << "s)";
    AzureClient::RenewLeaseOptions options;
    options.LeaseId = *m_leaseId;
    const auto requestStart = std::chrono::steady_clock::now();
    Unwrap(BlockOn(m_file->get_executor(), m_file->RenewLeaseAsync(std::move(options), boost::asio::use_future)));
    m_lastRenewalTime = requestStart;
}

void LockFileImpl::Unlock() {
    const std::scoped_lock lock(m_ioMutex);
    if (!m_leaseId.has_value()) {
        throw std::runtime_error("Cannot release lease that has not been acquired");
    }

    BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Releasing blob lease for '" << m_fileName << "'";
    AzureClient::ReleaseLeaseOptions options;
    options.LeaseId = *m_leaseId;
    Unwrap(BlockOn(m_file->get_executor(), m_file->ReleaseLeaseAsync(std::move(options), boost::asio::use_future)));
    m_leaseId.reset();
    BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Successfully released blob lease for '" << m_fileName << "'";
}

std::chrono::seconds LockFileImpl::TimeSinceLastRenewal() const {
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - m_lastRenewalTime);
}

bool LockFileImpl::HasExceededLeaseLength() const { return TimeSinceLastRenewal() >= m_leaseLength; }

void LockFileImpl::unlink() {
    boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::unlink();
}

bool LockFileImpl::is_linked() {
    return boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::is_linked();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
