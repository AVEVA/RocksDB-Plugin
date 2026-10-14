// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <exception>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure {
Logger::Logger(Impl::LoggerImpl&& logger) : m_logger(std::make_unique<Impl::LoggerImpl>(std::move(logger))) {}

Logger::~Logger() = default;

namespace {
// The RocksDB logger interface must not throw, so failures are reported on the boost logger instead.
void ReportLoggerFailure(const char* operation, const char* what) noexcept {
    try {
        boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level> logger;
        BOOST_LOG_SEV(logger, boost::log::trivial::error) << "Azure info log " << operation << " failed: " << what;
    } catch (...) {
    }
}
} // namespace

void Logger::Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) {
    try {
        m_logger->Logv(static_cast<int>(log_level), format, ap);
    } catch (const std::exception& ex) {
        ReportLoggerFailure("Logv", ex.what());
    } catch (...) {
        ReportLoggerFailure("Logv", "unknown error");
    }
}

void Logger::Flush() {
    try {
        m_logger->Flush();
    } catch (const std::exception& ex) {
        ReportLoggerFailure("Flush", ex.what());
    } catch (...) {
        ReportLoggerFailure("Flush", "unknown error");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
