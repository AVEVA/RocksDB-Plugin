// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"

#include <boost/asio/use_future.hpp>

#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

using boost::log::trivial::severity_level;

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
// Azure requires proposed lease IDs to be GUID-formatted; build a random (version 4) GUID.
std::string NewLeaseId() {
    thread_local std::mt19937_64 engine{std::random_device{}()};
    std::array<std::uint8_t, 16> bytes{};
    for (auto& value : bytes) {
        value = static_cast<std::uint8_t>(engine());
    }
    bytes[6] = static_cast<std::uint8_t>((bytes[6] & 0x0FU) | 0x40U);
    bytes[8] = static_cast<std::uint8_t>((bytes[8] & 0x3FU) | 0x80U);

    std::array<char, 37> text{};
    std::snprintf(text.data(), text.size(), "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                  bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9],
                  bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
    return std::string{text.data()};
}
} // namespace
LockFileImpl::LockFileImpl(
    std::shared_ptr<ClientRuntime> runtime, std::unique_ptr<AzureClient::PageBlobClient> file,
    std::chrono::seconds leaseLength,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::string fileName)
    : m_runtime(std::move(runtime)), m_file(std::move(file)), m_lastRenewalTime(std::chrono::steady_clock::now()),
      m_leaseLength(leaseLength), m_logger(logger), m_fileName(std::move(fileName)) {
    if (m_logger == nullptr) {
        throw std::runtime_error("logger cannot be null.");
    }

    if (m_file == nullptr) {
        throw std::runtime_error("file cannot be null.");
    }
}

bool LockFileImpl::Lock() {
    // Do not attempt to lock again when you already have a lock aquired.
    if (m_leaseId.has_value()) {
        BOOST_LOG_SEV(*m_logger, severity_level::debug)
            << "Lock already acquired for '" << m_fileName << "', skipping duplicate lock attempt";
        return false;
    }

    BOOST_LOG_SEV(*m_logger, severity_level::debug)
        << "Attempting to acquire blob lease for '" << m_fileName << "' (timeout: " << m_leaseLength.count() << "s)";
    auto start = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::high_resolution_clock::now();
    std::optional<std::string> lastError;
    while ((end - start) < m_leaseLength) {
        AzureClient::AcquireLeaseOptions options;
        options.ProposedLeaseId = NewLeaseId();
        options.Duration = m_leaseLength;
        const auto leaseId = options.ProposedLeaseId;
        auto result = m_file->AcquireLeaseAsync(std::move(options), boost::asio::use_future).get();
        if (result.has_value()) {
            assert(result->Value().LeaseId == leaseId);
            m_leaseId = leaseId;
            lastError.reset();
            break;
        }

        lastError = result.error().Message.empty() ? result.error().Code.message() : result.error().Message;

        // Avoid hammering the service while another owner holds the lease.
        static const constexpr auto retryDelay = std::chrono::milliseconds(250);
        std::this_thread::sleep_for(retryDelay);
        end = std::chrono::high_resolution_clock::now();
    }

    if (lastError.has_value()) {
        BOOST_LOG_SEV(*m_logger, severity_level::error)
            << "Failed to acquire blob lease for '" << m_fileName << "' after "
            << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << "s: " << *lastError;

        return false;
    }

    // Set the initial renewal time when lock is acquired
    m_lastRenewalTime = std::chrono::steady_clock::now();
    BOOST_LOG_SEV(*m_logger, severity_level::info) << "Successfully acquired blob lease for '" << m_fileName << "'";
    return true;
}

void LockFileImpl::Renew() const {
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
    Unwrap(m_file->RenewLeaseAsync(std::move(options), boost::asio::use_future).get());
    m_lastRenewalTime = std::chrono::steady_clock::now();
}

void LockFileImpl::Unlock() {
    if (!m_leaseId.has_value()) {
        throw std::runtime_error("Cannot release lease that has not been acquired");
    }

    BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Releasing blob lease for '" << m_fileName << "'";
    AzureClient::ReleaseLeaseOptions options;
    options.LeaseId = *m_leaseId;
    Unwrap(m_file->ReleaseLeaseAsync(std::move(options), boost::asio::use_future).get());
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
