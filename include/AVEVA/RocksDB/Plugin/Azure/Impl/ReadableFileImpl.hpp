// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <boost/log/trivial.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class ReadableFileImpl {
    std::string m_name;
    std::shared_ptr<Core::BlobClient> m_blobClient;
    std::shared_ptr<Core::FileCache> m_fileCache;
    // Random (and async) reads may run concurrently on one file, so the cached blob metadata is guarded.
    // Held by pointer to keep the type movable.
    std::unique_ptr<std::mutex> m_metadataMutex;
    mutable int64_t m_size;
    mutable std::string m_etag;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    // Optional; when set, every ReadAsync is registered with it until its callback has been released.
    std::shared_ptr<AsyncReadTracker> m_asyncReads;

    // One range requested through FSRandomAccessFile::Prefetch. Shared with the download's completion, which runs on
    // the host io_context and may outlive a move of this file.
    struct PrefetchSlot {
        uint64_t Id = 0;
        int64_t Offset = 0;
        int64_t Length = 0; // Also the number of bytes reserved from the shared budget.
        bool Pending = false;
        std::string Data;
    };
    struct PrefetchState {
        std::mutex Mutex;
        std::condition_variable Done;
        uint64_t NextId = 0;
        std::vector<PrefetchSlot> Slots; // Oldest first.
        // Reads waiting for a pending slot (by id) to settle. Kept outside the slots so a slot that is cleared while
        // downloading still releases its waiters when its download finishes.
        std::vector<std::pair<uint64_t, std::move_only_function<void()>>> Waiters;
        std::shared_ptr<std::atomic<int64_t>> Budget;

        ~PrefetchState();
        // Removes a slot and returns its bytes to the budget. Caller holds Mutex.
        void Drop(std::vector<PrefetchSlot>::iterator slot);
        void DropAll();
    };
    std::shared_ptr<PrefetchState> m_prefetch = std::make_shared<PrefetchState>();

    // Serves from a completed prefetch only. With `wait`, first waits (bounded) for a covering pending prefetch.
    // With `allowPartial`, a range that only starts inside a slot is served up to the slot's end and the caller
    // fetches the remainder; the result is then the number of bytes copied, which may be less than requested.
    [[nodiscard]] std::optional<size_t> TryReadFromPrefetch(int64_t offset, int64_t bytesToRead, char* buffer,
                                                            bool wait, bool allowPartial = false) const;
    void ClearPrefetch() const;
    int64_t DownloadWithRetry(const int64_t offset, const int64_t bytesToRead, char* buffer) const;
    [[nodiscard]] std::pair<int64_t, std::string> GetMetadata() const;
    void SetMetadata(int64_t size, std::string etag) const;
    static void ReadAsyncAttempt(std::shared_ptr<const ReadableFileImpl> self, int64_t offset, int64_t bytesToRead,
                                 Core::BlobClient::DownloadCallback callback, int attemptsLeft,
                                 std::chrono::milliseconds timeout);
    static void RefreshMetadataAndReadAsync(std::shared_ptr<const ReadableFileImpl> self, int64_t offset,
                                            int64_t bytesToRead, Core::BlobClient::DownloadCallback callback,
                                            int attemptsLeft, std::chrono::milliseconds timeout);

  public:
    // A blob that keeps changing underneath the reader fails with an IOError after this many metadata refreshes.
    static constexpr int kMaxStaleReadRetries = 5;
    ReadableFileImpl(
        std::string_view name, std::shared_ptr<Core::BlobClient> blobClient, std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::shared_ptr<AsyncReadTracker> asyncReads = nullptr,
        std::shared_ptr<std::atomic<int64_t>> prefetchBudget = nullptr);

    // Reads at an explicit offset; carries no read position of its own.
    [[nodiscard]] int64_t RandomRead(int64_t offset, int64_t bytesToRead, char* buffer) const;

    // Serves a random read from the local file cache only; returns nullopt when it must go to the blob.
    [[nodiscard]] std::optional<size_t> TryReadFromCache(int64_t offset, int64_t bytesToRead, char* buffer) const;
    // If a still-downloading prefetch fully covers the range, queues `resume` to run when that download settles
    // (successfully or not) and returns true; the caller must then not start its own read. Never blocks.
    [[nodiscard]] bool ChainOntoPendingPrefetch(int64_t offset, int64_t bytesToRead,
                                                std::move_only_function<void()>& resume) const;

    using ReadCallback = Core::BlobClient::DownloadCallback;

    // Non-blocking random read from the blob (bypassing the file cache), with the same ETag/size refresh and retry
    // semantics as RandomRead. `self` keeps the file alive until `callback` has run; the callback may run on an
    // io_context thread or inline, so it must not block on blob I/O. The file is released before `callback` runs,
    // and the read stays registered with the AsyncReadTracker (if any) until `callback` itself is released.
    // A non-zero `timeout` caps each blob download of the read (see Core::BlobClient::DownloadAsync).
    static void ReadAsync(std::shared_ptr<const ReadableFileImpl> self, int64_t offset, int64_t bytesToRead,
                          ReadCallback callback, int attemptsLeft = kMaxStaleReadRetries,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

    // Upper bound of a single prefetched range, to keep per-file memory bounded.
    static constexpr int64_t kMaxPrefetchBytes = 8 * 1024 * 1024;

    // Starts a non-blocking download of [offset, offset + n) (clamped to kMaxPrefetchBytes) into a per-file buffer.
    // Blocking reads fully inside that range are served from it, waiting for the download if it is still running;
    // async reads use it only once it has completed. A slot is released once a read consumes its end, and the file
    // keeps at most kMaxPrefetchSlots ranges. Returns false (and starts nothing) when every slot is still downloading
    // or the shared byte budget would be exceeded, so the caller can let RocksDB fall back to its own readahead.
    [[nodiscard]] static bool Prefetch(std::shared_ptr<const ReadableFileImpl> self, int64_t offset, int64_t n);

    static constexpr size_t kMaxPrefetchSlots = 2;
    // Default process-wide cap on prefetched bytes across all files sharing a budget.
    static constexpr int64_t kDefaultPrefetchBudgetBytes = 256 * 1024 * 1024;

    [[nodiscard]] std::string GetETag() const;
    int64_t GetSize() const;
    void RefreshBlobMetadata() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
