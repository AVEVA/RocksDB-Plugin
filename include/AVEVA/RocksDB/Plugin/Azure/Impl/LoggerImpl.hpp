#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include <cstdarg>
#include <memory>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class LoggerImpl
    {
        std::unique_ptr<WriteableFileImpl> m_file;
        int m_logLevel;
        std::vector<char> m_buffer;
    public:
        LoggerImpl(std::unique_ptr<WriteableFileImpl> file, int logLevel);

        void Logv(int logLevel, const char* format, ...);
        void Logv(int logLevel, const char* format, va_list ap);
        void Flush();
    };
}
