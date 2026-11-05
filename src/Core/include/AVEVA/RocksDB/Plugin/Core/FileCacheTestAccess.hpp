// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

// Test-only access to FileCache. This header is visible to in-tree targets only and is not installed.
#pragma once
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>How a bounded wait for the background downloader ended.</summary>
enum class IdleWaitResult : std::uint8_t {
    Idle,
    TimedOut,
};

class FileCacheTestAccess {
  public:
    // Blocks until the background downloader has no queued or in-flight work, so a test can assert that something did
    // NOT happen without guessing how long to wait. With a `timeout` it gives up (a guard against hangs) and returns
    // TimedOut if the downloader did not go idle; without one it waits indefinitely.
    static IdleWaitResult WaitUntilIdle(FileCache& cache,
                                        std::optional<std::chrono::milliseconds> timeout = std::nullopt);
};
} // namespace AVEVA::RocksDB::Plugin::Core
