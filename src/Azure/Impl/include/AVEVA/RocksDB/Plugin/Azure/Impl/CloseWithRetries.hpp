// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <chrono>
#include <exception>
#include <string>
#include <thread>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Runs `close` from a file destructor, which must not throw. Failures of any type are logged and the close is
/// retried, since a transient network error should not lose buffered data. Shared by the writeable and read/write
/// files so their destruction policy cannot drift apart.
/// </summary>
/// <param name="isPermanentFailure">Checked after each failed attempt; true stops retrying.</param>
/// <param name="baseBackoff">Pause after the first failure, doubled after each further one, so a brief outage can
/// pass instead of being hammered five times within a millisecond.</param>
template <typename Logger, typename CloseFn, typename PermanentFn>
void CloseWithRetries(Logger& logger, const std::string& name, CloseFn&& close, PermanentFn&& isPermanentFailure,
                      const std::chrono::milliseconds baseBackoff = std::chrono::milliseconds(50)) noexcept {
    constexpr int maxAttempts = 5;
    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
        try {
            if (attempt > 0) {
                BOOST_LOG_SEV(logger, boost::log::trivial::debug)
                    << "Retrying to close file '" << name << "'. Attempt " << attempt << " of " << maxAttempts;
            }
            close();
            return;
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(logger, boost::log::trivial::warning)
                << "Failed to close file '" << name << "' on attempt " << attempt << ": " << e.what();
        } catch (...) {
            BOOST_LOG_SEV(logger, boost::log::trivial::warning)
                << "Failed to close file '" << name << "' on attempt " << attempt;
        }
        if (isPermanentFailure()) {
            BOOST_LOG_SEV(logger, boost::log::trivial::error)
                << "Giving up closing file '" << name << "': the failure cannot be retried; buffered data may be lost";
            return;
        }
        if (attempt + 1 < maxAttempts) {
            std::this_thread::sleep_for(baseBackoff * (1 << attempt));
        }
    }
    BOOST_LOG_SEV(logger, boost::log::trivial::error)
        << "Giving up closing file '" << name << "' after " << maxAttempts << " attempts; buffered data may be lost";
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
