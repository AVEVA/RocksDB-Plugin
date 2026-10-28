// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/log/trivial.hpp>

#include <string>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Close bookkeeping shared by the writeable and read/write files, so their durability contract cannot drift apart:
/// Close() is the only durability boundary, and it is a no-op once it has succeeded. A failed Sync leaves the file
/// open so the caller can retry. A moved-from file counts as closed, so it never reports unpersisted data.
/// </summary>
class CloseState {
    bool m_closed = false;

  public:
    CloseState() = default;
    CloseState(CloseState&& other) noexcept : m_closed(std::exchange(other.m_closed, true)) {}
    CloseState(const CloseState&) = delete;
    CloseState& operator=(const CloseState&) = delete;
    CloseState& operator=(CloseState&&) = delete;

    [[nodiscard]] bool IsClosed() const noexcept { return m_closed; }

    // For coroutine closers, which run their own (awaited) sync and then record success here.
    void MarkClosed() noexcept { m_closed = true; }

    template <typename SyncFn> void Close(SyncFn&& sync) {
        if (!m_closed) {
            sync();
            m_closed = true;
        }
    }

    // For destructors, which must not retry or block: reports data that never reached durable storage.
    template <typename Logger>
    void LogIfUnclosed(Logger& logger, const std::string& name, const bool hasUnsyncedData) const {
        if (!m_closed && hasUnsyncedData) {
            BOOST_LOG_SEV(logger, boost::log::trivial::error)
                << "File '" << name << "' was destroyed without a successful Close(); buffered data was not persisted";
        }
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
