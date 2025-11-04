#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"
#include <rocksdb/env.h>
namespace AVEVA::RocksDB::Plugin::Azure
{
    class Logger final : public rocksdb::Logger
    {
        Impl::LoggerImpl m_logger;
    public:
        explicit Logger(Impl::LoggerImpl logger);
        virtual void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) override;
        virtual void Flush() override;
    };
}
