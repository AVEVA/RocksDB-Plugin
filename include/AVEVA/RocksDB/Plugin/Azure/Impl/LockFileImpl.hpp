// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/log/trivial.hpp>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class LockFileImpl
    : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
    std::shared_ptr<ClientRuntime> m_runtime;
    std::unique_ptr<AzureClient::PageBlobClient> m_file;
    std::optional<std::string> m_leaseId;
    // Read by the renewal thread while Lock/Renew hold m_ioMutex across blocking requests, hence atomic.
    mutable std::atomic<std::chrono::steady_clock::time_point> m_lastRenewalTime;
    std::atomic<bool> m_held{false};
    std::chrono::seconds m_leaseLength;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    std::string m_fileName;
    // Serializes Lock/Renew/Unlock so the renewal thread can renew without holding the filesystem's lock list mutex.
    mutable std::mutex m_ioMutex;

    void RenewLocked() const;

  public:
    LockFileImpl(std::shared_ptr<ClientRuntime> runtime, std::unique_ptr<AzureClient::PageBlobClient> file,
                 std::chrono::seconds leaseLength,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 std::string fileName);
    // Returns false when the lease is already held (by this object or, until the lease length elapses, another
    // owner). Any other acquire failure throws RequestFailedException.
    bool Lock();
    void Renew() const;
    // Renews the lease unless it was released concurrently; returns false when there was nothing to renew.
    [[nodiscard]] bool RenewIfLocked() const;
    void Unlock();

    [[nodiscard]] std::chrono::seconds TimeSinceLastRenewal() const;
    [[nodiscard]] bool HasExceededLeaseLength() const;
    // The last moment a renewal may still complete (and writes may still proceed): the end of the lease measured
    // from when the last successful request was sent, less a safety margin.
    [[nodiscard]] std::chrono::steady_clock::time_point RenewalDeadline() const;
    // True while the lease is held but can no longer be assumed valid. Safe to call while a renewal is blocked.
    [[nodiscard]] bool IsRenewalOverdue() const;

    void unlink();
    bool is_linked();
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
