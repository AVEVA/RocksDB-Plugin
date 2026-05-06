// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"

#include <azure/storage/blobs.hpp>

#include <cassert>
#include <optional>
#include <stdexcept>
#include <string>

using boost::log::trivial::severity_level;

namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    LockFileImpl::LockFileImpl(std::unique_ptr<::Azure::Storage::Blobs::PageBlobClient> file, std::chrono::seconds leaseLength,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger, std::string fileName)
        : m_file(std::move(file)),
          m_lastRenewalTime(std::chrono::steady_clock::now()),
          m_leaseLength(leaseLength),
          m_logger(logger),
          m_fileName(std::move(fileName))
    {
        if (m_logger == nullptr)
        {
            throw std::runtime_error("logger cannot be null.");
        }
    }

    bool LockFileImpl::Lock()
    {
        // Do not attempt to lock again when you already have a lock aquired.
        if (m_lease != nullptr)
        {
            BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Lock already acquired for '" << m_fileName << "', skipping duplicate lock attempt";
            return false;
        }

        BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Attempting to acquire blob lease for '" << m_fileName << "' (timeout: " << m_leaseLength.count() << "s)";
        auto start = std::chrono::high_resolution_clock::now();
        auto end = std::chrono::high_resolution_clock::now();
        std::optional<std::exception> ex;
        while ((end - start) < m_leaseLength)
        {
            try
            {
                const auto leaseId = ::Azure::Storage::Blobs::BlobLeaseClient::CreateUniqueLeaseId();
                m_lease = std::make_unique<::Azure::Storage::Blobs::BlobLeaseClient>(*m_file, leaseId);
                const auto response = m_lease->Acquire(m_leaseLength);
                assert(response.Value.LeaseId == leaseId);

                ex.reset();
                break;
            }
            catch (const ::Azure::Storage::StorageException& e)
            {
                ex = e;
            }

            end = std::chrono::high_resolution_clock::now();
        }

        if (ex.has_value())
        {
            BOOST_LOG_SEV(*m_logger, severity_level::error) << "Failed to acquire blob lease for '" << m_fileName << "' after "
                << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << "s: " << ex->what();

            try
            {
                if (m_lease != nullptr)
                {
                    m_lease->Release();
                }
            }
            catch (...) {}

            return false;
        }

        // Set the initial renewal time when lock is acquired
        m_lastRenewalTime = std::chrono::steady_clock::now();
        BOOST_LOG_SEV(*m_logger, severity_level::info) << "Successfully acquired blob lease for '" << m_fileName << "'";
        return true;
    }

    void LockFileImpl::Renew() const
    {
        if (m_lease == nullptr)
        {
            throw std::runtime_error("Cannot renew lease that has not been acquired");
        }

        if (HasExceededLeaseLength())
        {
            const auto timeSinceRenewal = TimeSinceLastRenewal();
            throw std::runtime_error("Cannot renew expired lease. Time since last renewal: " + 
                std::to_string(timeSinceRenewal.count()) + " seconds (max: " + 
                std::to_string(m_leaseLength.count()) + " seconds)");
        }

        BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Renewing blob lease for '" << m_fileName << "' (time since last renewal: "
            << TimeSinceLastRenewal().count() << "s)";
        [[maybe_unused]] const auto result = m_lease->Renew();
        m_lastRenewalTime = std::chrono::steady_clock::now();
    }

    void LockFileImpl::Unlock()
    {
        if (m_lease == nullptr)
        {
            throw std::runtime_error("Cannot release lease that has not been acquired");
        }

        BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Releasing blob lease for '" << m_fileName << "'";
        m_lease->Release();
        m_lease.reset();
        BOOST_LOG_SEV(*m_logger, severity_level::debug) << "Successfully released blob lease for '" << m_fileName << "'";
    }

    std::chrono::seconds LockFileImpl::TimeSinceLastRenewal() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - m_lastRenewalTime);
    }

    bool LockFileImpl::HasExceededLeaseLength() const
    {
        return TimeSinceLastRenewal() >= m_leaseLength;
    }

    void LockFileImpl::unlink()
    {
        boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::unlink();
    }

    bool LockFileImpl::is_linked()
    {
        return boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>::is_linked();
    }
}
