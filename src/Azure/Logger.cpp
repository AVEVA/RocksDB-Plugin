// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"

#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure {
Logger::Logger(Impl::LoggerImpl&& logger) : m_logger(std::make_unique<Impl::LoggerImpl>(std::move(logger))) {}

Logger::~Logger() = default;

void Logger::Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) {
    m_logger->Logv(static_cast<int>(log_level), format, ap);
}

void Logger::Flush() { m_logger->Flush(); }
} // namespace AVEVA::RocksDB::Plugin::Azure
