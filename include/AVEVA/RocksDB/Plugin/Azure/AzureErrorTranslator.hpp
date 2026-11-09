// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <rocksdb/io_status.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <exception>
#include <string_view>
#include <system_error>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure {
/// <summary>Whether an error says the service no longer recognises our lease.</summary>
enum class LeaseLoss : std::uint8_t { NotLost, Lost };

struct AzureErrorTranslator {
    /// <summary>
    /// Translates a failed request, using the HTTP status, the Azure error code and the transport error
    /// (status code 0) to pick the status and its retryable flag. The error code is always in the message.
    /// </summary>
    static rocksdb::IOStatus IOStatusFromError(const RequestFailedException& error);

    /// <summary>
    /// Lost when the service no longer recognises our lease (or another owner holds it), so the blob may be written
    /// by someone else and the request must never be retried.
    /// </summary>
    static LeaseLoss LeaseLossOf(const std::error_code& code);

    /// <summary>
    /// Logs a captured exception (including the Azure request id for RequestFailedException) and converts it to a
    /// status, so no exception escapes into RocksDB. `operation` names what was being attempted and is used in the
    /// log and in the fallback message. Never throws: if logging or building the message fails, a fixed IOError is
    /// returned instead. `severity` lets callers log expected failures (such as a cancelled prefetch) below error.
    /// </summary>
    static rocksdb::IOStatus
    FromException(boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger,
                  const std::exception_ptr& error, std::string_view operation,
                  boost::log::trivial::severity_level severity = boost::log::trivial::error) noexcept;

    /// <summary>
    /// Runs `f` (which returns an IOStatus) and converts any exception it throws to a logged status, so file wrappers
    /// share one try/catch instead of repeating it.
    /// </summary>
    template <class F>
    static rocksdb::IOStatus
    Invoke(boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger,
           std::string_view operation, F&& f) noexcept {
        try {
            return std::forward<F>(f)();
        } catch (...) {
            return FromException(logger, std::current_exception(), operation);
        }
    }

  private:
    static rocksdb::IOStatus IOStatusFromError(const std::string& context, HttpStatusCode statusCode);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
