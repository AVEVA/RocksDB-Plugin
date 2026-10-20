// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <exception>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure {
Logger::Logger(Impl::LoggerImpl&& logger, std::shared_ptr<DiagnosticLogger> diagnostics)
    : m_logger(std::make_unique<Impl::LoggerImpl>(std::move(logger))),
      m_diagnostics(diagnostics ? std::move(diagnostics) : std::make_shared<DiagnosticLogger>()) {}

Logger::~Logger() = default;

namespace {
// The RocksDB logger interface must not throw, so failures are reported on the boost logger instead.
void ReportLoggerFailure(auto& logger, const char* operation, const char* what) noexcept {
    try {
        BOOST_LOG_SEV(logger, boost::log::trivial::error) << "Azure info log " << operation << " failed: " << what;
    } catch (...) {
    }
}
} // namespace

void Logger::Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) {
    try {
        m_logger->Logv(static_cast<int>(log_level), format, ap);
    } catch (const std::exception& ex) {
        ReportLoggerFailure(*m_diagnostics, "Logv", ex.what());
    } catch (...) {
        ReportLoggerFailure(*m_diagnostics, "Logv", "unknown error");
    }
}

void Logger::Flush() {
    try {
        m_logger->Flush();
    } catch (const std::exception& ex) {
        ReportLoggerFailure(*m_diagnostics, "Flush", ex.what());
    } catch (...) {
        ReportLoggerFailure(*m_diagnostics, "Flush", "unknown error");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure
