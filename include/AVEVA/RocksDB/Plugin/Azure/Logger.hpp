// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class LoggerImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure {
class Logger final : public rocksdb::Logger {
    using DiagnosticLogger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;

    std::unique_ptr<Impl::LoggerImpl> m_logger;
    std::shared_ptr<DiagnosticLogger> m_diagnostics;

  public:
    /// <param name="diagnostics">Where failures of this logger are reported, since the RocksDB logger interface must
    /// not throw. Defaults to a fresh boost logger.</param>
    explicit Logger(Impl::LoggerImpl&& logger, std::shared_ptr<DiagnosticLogger> diagnostics = nullptr);
    ~Logger() override;
    virtual void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) override;
    virtual void Flush() override;
};
} // namespace AVEVA::RocksDB::Plugin::Azure
