// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Defaults.hpp"

#include <chrono>
#include <cstdint>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
struct Configuration {
    struct PageBlob {
        static const constexpr int64_t PageSize = 512;
        static const constexpr int64_t PageBits = 9;
        static const constexpr int64_t DefaultSize = Defaults::DataFileInitialSize;
        static const constexpr int64_t DefaultBufferSize = Defaults::DataFileBufferSize;
    };

    // Request sizing and fan-out used by the filesystem layer.
    struct Transfer {
        // The service accepts at most 5000 results per List Blobs page.
        static const constexpr int32_t MaxListPageSize = 5000;
        // Batch delete requests kept in flight at once by DeleteDir (each carries up to 256 blobs).
        static const constexpr std::size_t MaxConcurrentDeleteBatches = 4;
        // Server-side page copy requests kept in flight at once when copying a blob in RenameFile.
        static const constexpr std::size_t MaxConcurrentPageCopies = 4;
        // Largest range copied per request when copying a blob in RenameFile (the service caps Put Page From URL at
        // 4 MiB, and a range must be a multiple of the page size).
        static const constexpr int64_t MaxCopyChunkSize = static_cast<int64_t>(4) * 1024 * 1024;
        // Blobs up to this size are renamed by downloading them once and uploading the bytes to the new name, which
        // takes fewer round trips than the server-side copy (and no temporary blob).
        static const constexpr int64_t MaxInlineRenameSize = static_cast<int64_t>(1) * 1024 * 1024;
        // Chunking used when downloading a blob range into a local file: big enough to amortise request
        // latency, with a few chunks overlapping to keep the link busy.
        static const constexpr std::size_t DownloadChunkSize = static_cast<std::size_t>(4) * 1024 * 1024;
        static const constexpr std::size_t DownloadConcurrency = 4;
    };

    static const constexpr std::chrono::seconds LeaseLength = std::chrono::seconds(20);
    static const constexpr std::chrono::seconds RenewalDelay = std::chrono::seconds(5);
    // Writes stop this long before the lease can expire, so one already in flight cannot land after it lapsed.
    static const constexpr std::chrono::milliseconds LeaseSafetyMargin = std::chrono::milliseconds(2000);
    // How long the destructor waits for in-flight async reads before warning, and the total it waits before giving up
    // (logging an error and carrying on) so a stopped io_context cannot hang shutdown forever.
    static const constexpr std::chrono::seconds AsyncReadDrainWarning = std::chrono::seconds(30);
    static const constexpr std::chrono::seconds ShutdownWaitTimeout = std::chrono::seconds(120);
    // How long a synchronous read waits for an in-flight prefetch of its range before downloading it itself.
    static const constexpr std::chrono::seconds PrefetchWaitTimeout = std::chrono::seconds(30);
    // How long a reader's GetSize reuses the blob metadata it already fetched before asking the service again.
    static const constexpr std::chrono::milliseconds MetadataCacheTtl = std::chrono::milliseconds(100);
    static const constexpr std::chrono::seconds LogRateLimiterCooldown = std::chrono::seconds(30);
    // Default cap on bytes held by prefetch buffers across all files of one filesystem.
    static const constexpr int64_t DefaultPrefetchBudgetBytes = Defaults::MaxPrefetchBytes;
    static const constexpr size_t MaxCacheSize = static_cast<size_t>(1024) * 1024 * 1024; // 1GB
    static const constexpr int MaxClientRetries = 8;
    // Caps the backoff between retries. With 8 retries from the 800 ms default initial delay the waits total at most
    // about 52 s (0.8, 1.6, 3.2, 6.4 s, then 10 s each), so a single call's worst case is that plus 9 attempt
    // timeouts, instead of several minutes with the 1 minute library default.
    static const constexpr std::chrono::milliseconds MaxRetryDelay = std::chrono::seconds(10);
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
