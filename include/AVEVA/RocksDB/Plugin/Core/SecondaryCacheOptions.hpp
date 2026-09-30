// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/RocksDB/Plugin/Core/Filesystem.hpp"

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

namespace AVEVA::RocksDB::Plugin::Core {
/// <summary>
/// Selects the secondary cache implementation created by <c>CreateSecondaryCache</c>.
/// </summary>
enum class SecondaryCacheBackend {
    /// <summary>CacheLib when the build includes it (Linux), otherwise the file based cache.</summary>
    Default,
    /// <summary><c>FileBasedCompressedSecondaryCache</c>: one file per entry, LRU eviction.</summary>
    FileBased,
    /// <summary><c>CacheLibSecondaryCache</c>: CacheLib Navy BlockCache on a single cache file.</summary>
    CacheLib,
};

/// <summary>
/// Tuning knobs for the CacheLib (Navy) secondary cache. The defaults minimize DRAM
/// because RocksDB's block cache already provides the in-memory tier.
/// Fixed DRAM overhead is roughly
/// <c>2 * cleanRegions * regionSizeBytes + maxParcelMemoryMB + thread stacks</c>
/// plus about 12-16 bytes per cached block for the index.
/// I/O is synchronous (pread/pwrite) on Navy's reader and writer thread pools.
/// </summary>
struct CacheLibSecondaryCacheOptions {
    enum class EvictionPolicy {
        /// <summary>Region FIFO; no per-access DRAM bookkeeping.</summary>
        Fifo,
        /// <summary>Region LRU; tracks region access recency.</summary>
        Lru,
    };

    /// <summary>Size of a Navy region; also the upper bound for a single entry.</summary>
    uint32_t regionSizeBytes = 16U * 1024 * 1024;

    /// <summary>Number of clean regions kept ready for writes.</summary>
    uint32_t cleanRegions = 1;

    /// <summary>Device I/O alignment/block size.</summary>
    uint32_t blockSize = 4096;

    /// <summary>Threads serving lookups; bounds concurrent SSD reads.</summary>
    uint32_t readerThreads = 4;

    /// <summary>Threads applying inserts and removes.</summary>
    uint32_t writerThreads = 4;

    /// <summary>Upper bound for bytes of pending (not yet written) inserts; excess inserts are dropped.</summary>
    uint64_t maxParcelMemoryMB = 32;

    EvictionPolicy evictionPolicy = EvictionPolicy::Fifo;

    /// <summary>Store and verify a checksum for each entry.</summary>
    bool dataChecksum = true;

    /// <summary>
    /// When set, enables Navy's dynamic random admission policy with this target
    /// write rate (bytes/second) to bound SSD wear. When unset every insert is admitted.
    /// </summary>
    std::optional<uint64_t> admissionWriteRateBytesPerSec;

    /// <summary>Minimum folly log level for CacheLib internals (e.g. "WARN", "INFO", "ERR").</summary>
    std::string cachelibLogLevel = "WARN";

    /// <summary>Name of the cache file created inside the cache directory.</summary>
    std::string fileName = "navy.cache";
};

/// <summary>
/// Options for <c>CreateSecondaryCache</c>. Common fields apply to every backend; the
/// nested <c>cachelib</c> block applies only to the CacheLib backend.
/// </summary>
struct SecondaryCacheOptions {
    using Logger = boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>;

    static constexpr size_t kDefaultCapacity = 512ULL * 1024 * 1024; // 512 MiB

    SecondaryCacheBackend backend = SecondaryCacheBackend::Default;

    /// <summary>Directory owned by the cache. Previous cache contents are discarded on open.</summary>
    std::filesystem::path cacheDir;

    /// <summary>Maximum number of bytes stored on disk.</summary>
    size_t capacity = kDefaultCapacity;

    /// <summary>Required logger.</summary>
    std::shared_ptr<Logger> logger;

    /// <summary>File based backend only. When null a <c>LocalFilesystem</c> is used.</summary>
    std::shared_ptr<Filesystem> filesystem;

    CacheLibSecondaryCacheOptions cachelib;
};
} // namespace AVEVA::RocksDB::Plugin::Core
