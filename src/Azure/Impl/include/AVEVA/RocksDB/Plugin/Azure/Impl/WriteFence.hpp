// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// The write fence shared by every file, lock and directory of a filesystem: an irreversible switch plus the earliest
/// lease renewal deadline, both readable lock-free by every mutation.
/// </summary>
class WriteFence {
    std::atomic<bool> m_writesFenced{false};
    // The earliest lease renewal deadline in steady_clock ticks; kNoDeadline when no lease is held. Written under
    // m_leaseMutex (renewals are rare), read lock-free by every mutation.
    static constexpr std::chrono::steady_clock::rep kNoDeadline =
        (std::numeric_limits<std::chrono::steady_clock::rep>::max)();
    std::atomic<std::chrono::steady_clock::rep> m_writeDeadline{kNoDeadline};
    std::mutex m_leaseMutex;
    std::vector<std::pair<const void*, std::chrono::steady_clock::rep>> m_leaseDeadlines;

  public:
    /// <summary>
    /// Makes every later mutation fail. Called once the filesystem can no longer prove it still holds its locks.
    /// Irreversible.
    /// </summary>
    void Fence() noexcept;
    [[nodiscard]] bool Fenced() const noexcept;
    /// <summary>
    /// Throws WritesFencedException when writes are fenced or the earliest lease renewal deadline has passed. The
    /// deadline check closes the window before the renewal loop gives up, in which another writer may already own
    /// the lease. Returns the time left (empty when no deadline applies) so the caller can bound its request without
    /// querying the deadline source a second time.
    /// </summary>
    std::optional<std::chrono::steady_clock::duration> ThrowIfFenced() const;
    /// <summary>
    /// Publishes the renewal deadline of one lease (`owner` identifies it; empty removes it). A lease must republish
    /// whenever it is acquired, renewed or released, and remove itself before it is destroyed.
    /// </summary>
    void SetLeaseDeadline(const void* owner, std::optional<std::chrono::steady_clock::time_point> deadline);
    /// <summary>Time left before mutations must stop; empty when no lease deadline applies.</summary>
    [[nodiscard]] std::optional<std::chrono::steady_clock::duration> TimeRemaining() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
