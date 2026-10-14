// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include <atomic>
#include <boost/intrusive/list.hpp>
#include <boost/log/trivial.hpp>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TBlob>
class BasicLockFileImpl
    : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
    std::shared_ptr<ClientRuntimeBase> m_runtime;
    std::unique_ptr<TBlob> m_file;
    std::optional<std::string> m_leaseId;
    // Read by the renewal thread while Lock/Renew block on the network, hence atomic.
    mutable std::atomic<std::chrono::steady_clock::time_point> m_lastRenewalTime;
    std::atomic<bool> m_held{false};
    std::chrono::seconds m_leaseLength;
    // How long Lock() keeps retrying against a lease held by another owner; the lease length unless overridden.
    std::chrono::milliseconds m_acquireTimeout;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    std::string m_fileName;
    // Guards only m_leaseId and m_lockInProgress and is never held across network I/O or sleeps, so a slow Lock
    // cannot stall the renewal thread or state queries.
    mutable std::mutex m_stateMutex;
    bool m_lockInProgress = false;

    [[nodiscard]] std::optional<std::string> CurrentLeaseId() const;
    void RenewLease() const;
    // Throws when the lease has expired or its renewal deadline has passed; otherwise returns the time left to renew.
    [[nodiscard]] std::chrono::steady_clock::duration RenewalBudget() const;
    // Defaults to steady_clock; replaced only by tests so lease expiry can be simulated without waiting.
    using Clock = std::function<std::chrono::steady_clock::time_point()>;
    Clock m_clock;
    [[nodiscard]] std::chrono::steady_clock::time_point Now() const {
        return m_clock ? m_clock() : std::chrono::steady_clock::now();
    }

  public:
    // Not thread-safe: call before the lock is shared. Also restarts the lease age at the new clock's current time.
    void SetClockForTesting(Clock clock);
    BasicLockFileImpl(
        std::shared_ptr<ClientRuntimeBase> runtime, std::unique_ptr<TBlob> file, std::chrono::seconds leaseLength,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::string fileName, std::optional<std::chrono::milliseconds> acquireTimeout = {});
    // Returns false when the lease is already held (by this object or, until the lease length elapses, another
    // owner). Any other acquire failure throws RequestFailedException.
    bool Lock();
    void Renew() const;
    // Renews the lease unless it was released concurrently; returns false when there was nothing to renew.
    [[nodiscard]] bool RenewIfLocked() const;
    // Called once per RenewAsync with the failure (null on success) and whether a renewal happened (false when the
    // lease was not held or was released concurrently). Runs on the host io_context, or inline in RenewAsync when the
    // renewal cannot be started, so it must not block.
    using RenewCallback = std::function<void(std::exception_ptr, bool)>;
    // Starts a renewal without blocking, so many leases can renew at once. The request is cancelled at the renewal
    // deadline. The caller must keep this object alive until the callback has run.
    void RenewAsync(RenewCallback callback) const;
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
extern template class BasicLockFileImpl<PageBlobClient>;

/// <summary>The production lock file, over the real page blob client. Non-template so it can be forward
/// declared.</summary>
class LockFileImpl : public BasicLockFileImpl<PageBlobClient> {
  public:
    using BasicLockFileImpl<PageBlobClient>::BasicLockFileImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.ipp"
