// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include <chrono>
#include <cstdarg>
#include <memory>
#include <span>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// Where a logger writes. Type-erased so that the logger does not depend on the HTTP transport behind the file.
class LogSink {
  public:
    virtual ~LogSink() = default;
    virtual void Append(std::span<const char> data) = 0;
    virtual void Sync() = 0;
};

// Adapts any writeable file type (Append + Sync) to a LogSink.
template <typename TFile> class FileLogSink final : public LogSink {
    std::unique_ptr<TFile> m_file;

  public:
    explicit FileLogSink(std::unique_ptr<TFile> file) : m_file(std::move(file)) {}
    void Append(std::span<const char> data) override { m_file->Append(data); }
    void Sync() override { m_file->Sync(); }
};

class LoggerImpl {
    std::unique_ptr<LogSink> m_file;
    int m_logLevel;
    std::vector<char> m_buffer;
    std::unique_ptr<LogRateLimiter> m_rateLimiter;

  public:
    // Constructs a logger with no rate limiting.
    LoggerImpl(std::unique_ptr<LogSink> file, int logLevel);
    LoggerImpl(std::unique_ptr<WriteableFileImpl> file, int logLevel);

    // Constructs a logger with an injected rate limiter.
    LoggerImpl(std::unique_ptr<LogSink> file, int logLevel, std::unique_ptr<LogRateLimiter> rateLimiter);
    LoggerImpl(std::unique_ptr<WriteableFileImpl> file, int logLevel, std::unique_ptr<LogRateLimiter> rateLimiter);

    void Logv(int logLevel, const char* format, ...);
    void Logv(int logLevel, const char* format, va_list ap);
    void Flush();
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
