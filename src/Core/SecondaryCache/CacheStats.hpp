// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <atomic>
#include <cstdint>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
struct CacheStats {
    std::atomic<uint64_t> inserts{0};
    std::atomic<uint64_t> admitted{0};
    std::atomic<uint64_t> droppedTooLarge{0};
    std::atomic<uint64_t> droppedPressure{0};
    std::atomic<uint64_t> droppedDisabled{0};
    std::atomic<uint64_t> lookups{0};
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> corruptions{0};
    std::atomic<uint64_t> keyMismatches{0};
    std::atomic<uint64_t> ioErrors{0};
    std::atomic<uint64_t> errors{0};
    std::atomic<uint64_t> reclaims{0};
    std::atomic<uint64_t> payloadBytes{0};
    std::atomic<uint64_t> encodedBytes{0};
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
