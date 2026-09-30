// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
struct PaddedAtomicU64 {
    std::atomic<uint64_t> value{0};
    std::array<std::byte, 64 - sizeof(std::atomic<uint64_t>)> padding{};
};

struct CacheStats {
    PaddedAtomicU64 inserts;
    PaddedAtomicU64 insertsAdmitted;
    PaddedAtomicU64 insertsRejectedByPolicy;
    PaddedAtomicU64 insertsRejectedTooLarge;
    PaddedAtomicU64 insertsDroppedNoBuffer;
    PaddedAtomicU64 lookups;
    PaddedAtomicU64 hits;
    PaddedAtomicU64 missesIndex;
    PaddedAtomicU64 missesKeyMismatch;
    PaddedAtomicU64 missesCrc;
    PaddedAtomicU64 missesRegionReclaimed;
    PaddedAtomicU64 bytesInserted;
    PaddedAtomicU64 bytesWrittenToDevice;
    PaddedAtomicU64 bytesRead;
    PaddedAtomicU64 regionsReclaimed;
    PaddedAtomicU64 entriesEvictedByReclaim;
    PaddedAtomicU64 footerOverflowScans;
};
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
