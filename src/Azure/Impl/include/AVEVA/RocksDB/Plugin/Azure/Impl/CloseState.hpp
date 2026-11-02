// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <string>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>Whether Close() has succeeded (or the file was moved from).</summary>
enum class CloseStatus : std::uint8_t { Open, Closed };

/// <summary>Whether everything written to a file has reached durable storage.</summary>
enum class Durability : std::uint8_t { Persisted, Unpersisted };

/// <summary>
/// Close bookkeeping shared by the writeable and read/write files, so their durability contract cannot drift apart:
/// Close() is the only durability boundary, and it is a no-op once it has succeeded. A failed Sync leaves the file
/// open so the caller can retry. A moved-from file counts as closed, so it never reports unpersisted data.
/// </summary>
class CloseState {
    CloseStatus m_status = CloseStatus::Open;

  public:
    CloseState() = default;
    CloseState(CloseState&& other) noexcept : m_status(std::exchange(other.m_status, CloseStatus::Closed)) {}
    CloseState(const CloseState&) = delete;
    CloseState& operator=(const CloseState&) = delete;
    CloseState& operator=(CloseState&&) = delete;

    [[nodiscard]] CloseStatus Status() const noexcept { return m_status; }

    // For coroutine closers, which run their own (awaited) sync and then record success here.
    void MarkClosed() noexcept { m_status = CloseStatus::Closed; }

    template <typename SyncFn> void Close(SyncFn&& sync) {
        if (m_status == CloseStatus::Open) {
            sync();
            m_status = CloseStatus::Closed;
        }
    }

    // For destructors, which must not retry or block: reports data that never reached durable storage.
    template <typename Logger>
    void LogIfUnclosed(Logger& logger, const std::string& name, const Durability durability) const {
        if (m_status == CloseStatus::Open && durability == Durability::Unpersisted) {
            BOOST_LOG_SEV(logger, boost::log::trivial::error)
                << "File '" << name << "' was destroyed without a successful Close(); buffered data was not persisted";
        }
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
