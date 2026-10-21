// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteFence.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RequestHelpers.hpp"

#include <algorithm>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
void WriteFence::Fence() noexcept { m_writesFenced.store(true, std::memory_order_release); }

bool WriteFence::Fenced() const noexcept { return m_writesFenced.load(std::memory_order_acquire); }

std::optional<std::chrono::steady_clock::duration> WriteFence::ThrowIfFenced() const {
    if (Fenced()) {
        throw WritesFencedException("Writes are disabled because the filesystem can no longer guarantee it holds its "
                                    "locks");
    }
    const auto remaining = TimeRemaining();
    if (remaining && *remaining <= std::chrono::steady_clock::duration::zero()) {
        throw WritesFencedException("Writes are disabled because the lease renewal deadline has passed");
    }
    return remaining;
}

void WriteFence::SetLeaseDeadline(const void* owner,
                                  const std::optional<std::chrono::steady_clock::time_point> deadline) {
    const std::scoped_lock lock(m_leaseMutex);
    const auto it =
        std::ranges::find(m_leaseDeadlines, owner, &std::pair<const void*, std::chrono::steady_clock::rep>::first);
    if (!deadline) {
        if (it != m_leaseDeadlines.end()) {
            m_leaseDeadlines.erase(it);
        }
    } else if (it != m_leaseDeadlines.end()) {
        it->second = deadline->time_since_epoch().count();
    } else {
        m_leaseDeadlines.emplace_back(owner, deadline->time_since_epoch().count());
    }
    auto earliest = kNoDeadline;
    for (const auto& entry : m_leaseDeadlines) {
        earliest = (std::min)(earliest, entry.second);
    }
    m_writeDeadline.store(earliest, std::memory_order_release);
}

std::optional<std::chrono::steady_clock::duration> WriteFence::TimeRemaining() const {
    const auto deadline = m_writeDeadline.load(std::memory_order_acquire);
    if (deadline == kNoDeadline) {
        return std::nullopt;
    }
    return std::chrono::steady_clock::time_point(std::chrono::steady_clock::duration(deadline)) -
           std::chrono::steady_clock::now();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
