// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"
namespace AVEVA::RocksDB::Plugin::Azure
{
    Logger::Logger(Impl::LoggerImpl logger)
        : m_logger(std::move(logger))
    {
    }

    void Logger::Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap)
    {
        m_logger.Logv(static_cast<int>(log_level), format, ap);
    }

    void Logger::Flush()
    {
        m_logger.Flush();
    }
}
