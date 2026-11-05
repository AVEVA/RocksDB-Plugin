// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LogRateLimiter.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"

#include <rocksdb/env.h>

#include <chrono>
#include <cstdarg>
#include <memory>
#include <mutex>
#include <span>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// Where a logger writes. Type-erased so that the logger does not depend on the HTTP transport behind the file.
class LogSink {
  public:
    virtual ~LogSink() = default;
    virtual void Append(std::span<const char> data) = 0;
    virtual void Sync() = 0;
    // Persists everything appended so far and releases the underlying file.
    virtual void Close() = 0;
};

// Adapts any writeable file type (Append + Sync) to a LogSink.
template <typename TFile> class FileLogSink final : public LogSink {
    std::unique_ptr<TFile> m_file;

  public:
    explicit FileLogSink(std::unique_ptr<TFile> file) : m_file(std::move(file)) {}
    void Append(std::span<const char> data) override { m_file->Append(data); }
    void Sync() override { m_file->Sync(); }
    void Close() override { m_file->Close(); }
};

class LoggerImpl {
    std::unique_ptr<LogSink> m_file;
    std::vector<char> m_buffer;
    std::unique_ptr<LogRateLimiter> m_rateLimiter;
    // Heap-allocated so that the logger stays movable. RocksDB calls Logv from many threads.
    std::unique_ptr<std::mutex> m_mutex = std::make_unique<std::mutex>();

  public:
    // Writes to `sink`; a null `rateLimiter` disables rate limiting. Messages are not filtered by level: RocksDB has
    // already filtered them by the info log level it was configured with.
    explicit LoggerImpl(std::unique_ptr<LogSink> sink, std::unique_ptr<LogRateLimiter> rateLimiter = nullptr);

    void Logv(rocksdb::InfoLogLevel logLevel, const char* format, ...);
    void Logv(rocksdb::InfoLogLevel logLevel, const char* format, va_list ap);
    void Flush();
    void Close();
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
