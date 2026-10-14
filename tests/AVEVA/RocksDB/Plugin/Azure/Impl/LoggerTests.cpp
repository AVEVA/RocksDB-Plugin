// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Logger.hpp"

#include <gtest/gtest.h>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using AVEVA::RocksDB::Plugin::Azure::Impl::LoggerImpl;
using AVEVA::RocksDB::Plugin::Azure::Impl::LogSink;

namespace {
// Records every Append as a separate entry; the sleep widens the race window if the logger is not serialised.
class RecordingSink final : public LogSink {
  public:
    std::vector<std::string> lines;
    bool inAppend = false;
    bool overlapped = false;
    bool throwOnAppend = false;
    bool throwOnSync = false;

    void Append(std::span<const char> data) override {
        if (throwOnAppend) {
            throw std::runtime_error("append failed");
        }
        if (inAppend) {
            overlapped = true;
        }
        inAppend = true;
        std::this_thread::yield();
        lines.emplace_back(data.data(), data.size());
        inAppend = false;
    }
    void Sync() override {
        if (throwOnSync) {
            throw std::runtime_error("sync failed");
        }
    }
};
} // namespace

TEST(LoggerTests, ConcurrentLogv_EveryLineIsIntact) {
    // Arrange
    constexpr int kThreads = 8;
    constexpr int kLinesPerThread = 1000;
    auto sink = std::make_unique<RecordingSink>();
    auto* rawSink = sink.get();
    LoggerImpl logger(std::move(sink), 0);

    // Act
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&logger, t] {
            for (int i = 0; i < kLinesPerThread; ++i) {
                logger.Logv(1, " thread=%d line=%d payload=%s\n", t, i, "abcdefghijklmnopqrstuvwxyz");
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Assert
    EXPECT_FALSE(rawSink->overlapped);
    ASSERT_EQ(static_cast<size_t>(kThreads * kLinesPerThread), rawSink->lines.size());
    std::vector<std::vector<bool>> seen(kThreads, std::vector<bool>(kLinesPerThread, false));
    for (const auto& line : rawSink->lines) {
        // "YYYY-MM-DDTHH:MM:SSZ" prefix is 20 characters.
        ASSERT_GT(line.size(), 20u);
        int t = -1;
        int i = -1;
        char payload[64] = {};
        ASSERT_EQ(3, std::sscanf(line.c_str() + 20, " thread=%d line=%d payload=%63s", &t, &i, payload)) << line;
        ASSERT_EQ(std::string("abcdefghijklmnopqrstuvwxyz"), payload) << line;
        ASSERT_EQ('\n', line.back()) << line;
        ASSERT_TRUE(t >= 0 && t < kThreads && i >= 0 && i < kLinesPerThread) << line;
        seen[t][i] = true;
    }
    for (const auto& perThread : seen) {
        for (const bool b : perThread) {
            EXPECT_TRUE(b);
        }
    }
}

TEST(LoggerTests, Logger_SinkThrows_DoesNotEscape) {
    // Arrange
    auto sink = std::make_unique<RecordingSink>();
    sink->throwOnAppend = true;
    sink->throwOnSync = true;
    AVEVA::RocksDB::Plugin::Azure::Logger logger(LoggerImpl(std::move(sink), 0));

    // Act / Assert
    const auto logv = [&logger](const char* format, ...) {
        va_list ap;
        va_start(ap, format);
        EXPECT_NO_THROW(logger.Logv(rocksdb::InfoLogLevel::ERROR_LEVEL, format, ap));
        va_end(ap);
    };
    logv("message %d", 1);
    EXPECT_NO_THROW(logger.Flush());
}
