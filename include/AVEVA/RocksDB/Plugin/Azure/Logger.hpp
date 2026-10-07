// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include <rocksdb/env.h>

#include <memory>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class LoggerImpl;
}
namespace AVEVA::RocksDB::Plugin::Azure
{
    class Logger final : public rocksdb::Logger
    {
        std::unique_ptr<Impl::LoggerImpl> m_logger;
    public:
        explicit Logger(Impl::LoggerImpl&& logger);
        ~Logger() override;
        virtual void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) override;
        virtual void Flush() override;
    };
}
