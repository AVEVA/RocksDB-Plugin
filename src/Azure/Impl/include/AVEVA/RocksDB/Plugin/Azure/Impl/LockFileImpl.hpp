// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RequestHelpers.hpp"
#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <atomic>
#include <boost/intrusive/list.hpp>
#include <boost/log/trivial.hpp>
#include <chrono>
#include <exception>
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
    // Set before ReleaseLease is issued so a renewal already in flight that the service rejects because the release
    // won is classified as benign instead of fencing the filesystem. Guarded by m_stateMutex.
    bool m_releasing = false;

    [[nodiscard]] std::optional<std::string> CurrentLeaseId() const;
    void RenewLease() const;
    struct RenewalAttempt {
        std::string LeaseId;
        std::chrono::steady_clock::time_point Start;
        AzureClient::RenewLeaseOptions Options;
        AzureClient::AzureRequestOptions RequestOptions;
    };
    // Empty when no lease is held; throws when the renewal deadline has passed.
    [[nodiscard]] std::optional<RenewalAttempt> BeginRenewal() const;
    // Records a completed renewal request. `failure` is the request's failure (null on success); returns the failure
    // to report and whether a renewal happened.
    [[nodiscard]] std::pair<std::exception_ptr, bool> FinishRenewal(const std::string& leaseId,
                                                                    std::chrono::steady_clock::time_point requestStart,
                                                                    std::exception_ptr failure) const;
    // Requires m_stateMutex.
    void PublishDeadlineLocked() const;
    // Throws when the lease has expired or its renewal deadline has passed; otherwise returns the time left to renew.
    [[nodiscard]] std::chrono::steady_clock::duration RenewalBudget() const;
    // Added to steady_clock; non-zero only in tests so lease expiry can be simulated without waiting.
    std::atomic<std::chrono::steady_clock::duration> m_clockOffset{std::chrono::steady_clock::duration::zero()};
    [[nodiscard]] std::chrono::steady_clock::time_point Now() const {
        return std::chrono::steady_clock::now() + m_clockOffset.load();
    }

  public:
    // Shifts the lease clock by `offset` from real time. Safe to call at any time.
    void SetClockOffsetForTesting(std::chrono::steady_clock::duration offset) { m_clockOffset = offset; }
    BasicLockFileImpl(
        std::shared_ptr<ClientRuntimeBase> runtime, std::unique_ptr<TBlob> file, std::chrono::seconds leaseLength,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::string fileName, std::optional<std::chrono::milliseconds> acquireTimeout = {});
    ~BasicLockFileImpl();
    // Returns false when the lease is already held (by this object or, until the lease length elapses, another
    // owner). Any other acquire failure throws RequestFailedException.
    bool Lock();
    void Renew() const;
    // Renews the lease unless it was released concurrently; returns false when there was nothing to renew.
    [[nodiscard]] bool RenewIfLocked() const;
    // Starts a renewal without blocking, so many leases can renew at once. The request is cancelled at the renewal
    // deadline. The caller must keep this object alive until the callback has run.
    // `callback(std::exception_ptr, bool)` is called once with the failure (null on success) and whether a renewal
    // happened (false when the lease was not held or was released concurrently). It runs on the host io_context, or
    // inline in RenewAsync when the renewal cannot be started, so it must not block. The renewed flag is false when
    // the lease was released while the request was in flight, which is expected rather than a failure.
    template <typename THandler> void RenewAsync(THandler callback) const {
        std::optional<RenewalAttempt> attempt;
        try {
            attempt = BeginRenewal();
        } catch (...) {
            callback(std::current_exception(), true);
            return;
        }
        if (!attempt.has_value()) {
            callback(nullptr, false);
            return;
        }

        m_file->RenewLeaseAsync(
            std::move(attempt->Options),
            [this, leaseId = std::move(attempt->LeaseId), requestStart = attempt->Start,
             callback = std::move(callback)](auto result) mutable {
                std::exception_ptr failure;
                try {
                    Unwrap(std::move(result));
                } catch (...) {
                    failure = std::current_exception();
                }
                const auto [error, renewed] = FinishRenewal(leaseId, requestStart, std::move(failure));
                callback(error, renewed);
            },
            std::move(attempt->RequestOptions));
    }
    void Unlock();

    [[nodiscard]] std::chrono::seconds TimeSinceLastRenewal() const;
    [[nodiscard]] bool HasExceededLeaseLength() const;
    // The last moment a renewal may still complete (and writes may still proceed): the end of the lease measured
    // from when the last successful request was sent, less a safety margin.
    [[nodiscard]] std::chrono::steady_clock::time_point RenewalDeadline() const;
    // Time left until RenewalDeadline(); empty when the lock is not held.
    [[nodiscard]] std::optional<std::chrono::steady_clock::duration> RenewalTimeRemaining() const;
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
