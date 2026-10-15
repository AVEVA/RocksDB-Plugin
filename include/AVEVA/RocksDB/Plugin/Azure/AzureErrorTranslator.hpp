// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <rocksdb/io_status.h>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <exception>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Azure {
struct AzureErrorTranslator {
    /// <summary>
    /// Translates a failed request, using the HTTP status, the Azure error code and the transport error
    /// (status code 0) to pick the status and its retryable flag. The error code is always in the message.
    /// </summary>
    static rocksdb::IOStatus IOStatusFromError(const RequestFailedException& error);

    /// <summary>
    /// True when retrying the same request may succeed: 408, 429, 5xx, and transport failures.
    /// </summary>
    static bool IsTransient(const RequestFailedException& error);

    /// <summary>
    /// Same classification as above, for callers that only have the status and transport error.
    /// </summary>
    static bool IsTransient(unsigned int statusCode, const std::error_code& code = {});

    /// <summary>
    /// Converts a captured exception to a status without logging (for callers that have no logger).
    /// </summary>
    static rocksdb::IOStatus FromException(const std::exception_ptr& error);

    /// <summary>
    /// For use inside a catch (...) block: logs the active exception (including the Azure request id for
    /// RequestFailedException) and converts it to a status, so no exception escapes into RocksDB.
    /// `operation` names what was being attempted and is used in the log and in the fallback message.
    /// </summary>
    static rocksdb::IOStatus
    FromCurrentException(boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger,
                         std::string_view operation);

    /// <summary>
    /// As FromCurrentException, for a captured exception (e.g. one delivered to an async completion handler).
    /// </summary>
    static rocksdb::IOStatus
    FromException(boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>& logger,
                  const std::exception_ptr& error, std::string_view operation);

  private:
    static rocksdb::IOStatus IOStatusFromError(const std::string& context, unsigned int statusCode);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
