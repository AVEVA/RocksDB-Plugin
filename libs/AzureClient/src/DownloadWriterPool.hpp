// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/thread_pool.hpp>

#include <atomic>
#include <cstddef>

namespace AVEVA::AzureClient::Private
{
    // Runs blocking download-sink writes so they never stall the I/O thread. Two threads let downloads to
    // different sinks overlap; each download still has at most one write in flight.
    [[nodiscard]] inline boost::asio::thread_pool& DownloadWriterPool()
    {
        static boost::asio::thread_pool pool{2U};
        return pool;
    }

    // Writes that were handed to the pool and have not yet re-posted their completion to the I/O executor.
    // Tests use it to settle a download deterministically.
    [[nodiscard]] inline std::atomic<std::size_t>& PendingDownloadWrites() noexcept
    {
        static std::atomic<std::size_t> pending{0U};
        return pending;
    }

    [[nodiscard]] inline std::atomic<std::size_t>& StartedDownloadWrites() noexcept
    {
        static std::atomic<std::size_t> started{0U};
        return started;
    }
} // namespace AVEVA::AzureClient::Private
