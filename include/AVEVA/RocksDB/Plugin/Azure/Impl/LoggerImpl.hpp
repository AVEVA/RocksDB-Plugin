// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include <cstdarg>
#include <chrono>
#include <memory>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class LoggerImpl
    {
        std::unique_ptr<WriteableFileImpl> m_file;
        int m_logLevel;
        std::vector<char> m_buffer;
        std::unique_ptr<LogRateLimiter> m_rateLimiter;
    public:
        // Constructs a logger with no rate limiting.
        LoggerImpl(std::unique_ptr<WriteableFileImpl> file, int logLevel);

        // Constructs a logger with an injected rate limiter.
        LoggerImpl(std::unique_ptr<WriteableFileImpl> file, int logLevel, std::unique_ptr<LogRateLimiter> rateLimiter);

        void Logv(int logLevel, const char* format, ...);
        void Logv(int logLevel, const char* format, va_list ap);
        void Flush();
    };
}
