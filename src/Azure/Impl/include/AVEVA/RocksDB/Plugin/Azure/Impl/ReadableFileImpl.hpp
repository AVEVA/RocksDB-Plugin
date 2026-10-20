// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <boost/log/trivial.hpp>

#include <atomic>
#include <chrono>
#include <concepts>
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
/// <summary>
/// An async read completion is any callable taking an exception (null on success) and the bytes read. `data` is valid
/// while `owner` (when non-null) or, failing that, until the callback returns; keep `owner` to retain the bytes
/// without copying.
/// </summary>
template <typename TCallback>
concept ReadCompletion = std::invocable<TCallback&, std::exception_ptr, std::string_view, std::shared_ptr<const void>>;

// Neither copyable nor movable: it is built once, in place, by make_shared, and in-flight reads hold it by shared_ptr.
template <typename TBlob> class BasicReadableFileImpl {
    BlobFileHandles<TBlob> m_h;
    // Random (and async) reads may run concurrently on one file, so the cached blob metadata is guarded.
    mutable std::mutex m_metadataMutex;
    mutable int64_t m_size;
    mutable std::string m_etag;
    // Cap on the shared prefetch budget; a Prefetch that would push the budget past it is declined.
    int64_t m_maxPrefetchBytes;
    // When m_size/m_etag were last read from the service; GetSize serves them for MetadataCacheTtl.
    mutable std::chrono::steady_clock::time_point m_metadataFetchedAt;
    // Optional; when set, every ReadAsync is registered with it until its callback has been released.
    std::shared_ptr<AsyncReadTracker> m_asyncReads;

    // One range requested through FSRandomAccessFile::Prefetch. The download's completion runs on the host io_context
    // and holds the file, so this state lives as long as that read.
    struct PrefetchSlot {
        uint64_t Id = 0;
        int64_t Offset = 0;
        int64_t Length = 0; // Also the number of bytes reserved from the shared budget.
        bool Pending = false;
        std::shared_ptr<const void> Owner; // keeps the pooled download buffer behind Data alive
        std::string_view Data;
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
    mutable PrefetchState m_prefetch;

    // Concurrent async reads that need fresh blob metadata (at the cached end, or after a precondition failure) share
    // one in-flight properties call instead of each issuing their own.
    using MetadataCallback = std::move_only_function<void(std::exception_ptr, int64_t, std::string)>;
    struct MetadataRefreshState {
        std::mutex Mutex;
        bool InFlight = false;
        std::vector<MetadataCallback> Waiters;
    };
    mutable MetadataRefreshState m_metadataRefresh;
    static void CoalescedMetadataAsync(const std::shared_ptr<const BasicReadableFileImpl>& file,
                                       MetadataCallback callback);

    // Serves from a completed prefetch only. With `wait`, first waits (bounded) for a covering pending prefetch.
    // With `allowPartial`, a range that only starts inside a slot is served up to the slot's end and the caller
    // fetches the remainder; the result is then the number of bytes copied, which may be less than requested.
    [[nodiscard]] std::optional<size_t> TryReadFromPrefetch(int64_t offset, int64_t bytesToRead, char* buffer,
                                                            bool wait, bool allowPartial = false) const;
    void ClearPrefetch() const;
    int64_t DownloadWithRetry(const int64_t offset, const int64_t bytesToRead, char* buffer) const;
    [[nodiscard]] std::pair<int64_t, std::string> GetMetadata() const;
    void SetMetadata(int64_t size, std::string etag) const;
    template <ReadCompletion TCallback>
    static void ReadAsyncAttempt(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset, int64_t bytesToRead,
                                 TCallback callback, int attemptsLeft, std::chrono::milliseconds timeout);
    template <ReadCompletion TCallback>
    static void RefreshMetadataAndReadAsync(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset,
                                            int64_t bytesToRead, TCallback callback, int attemptsLeft,
                                            std::chrono::milliseconds timeout);

  public:
    // A blob that keeps changing underneath the reader fails with an IOError after this many metadata refreshes.
    static constexpr int kMaxStaleReadRetries = 5;
    BasicReadableFileImpl(const BasicReadableFileImpl&) = delete;
    BasicReadableFileImpl& operator=(const BasicReadableFileImpl&) = delete;
    BasicReadableFileImpl(
        std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
        std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::shared_ptr<AsyncReadTracker> asyncReads = nullptr,
        std::shared_ptr<std::atomic<int64_t>> prefetchBudget = nullptr,
        int64_t maxPrefetchBytes = Configuration::DefaultPrefetchBudgetBytes);

    // Reads at an explicit offset; carries no read position of its own.
    [[nodiscard]] int64_t RandomRead(int64_t offset, int64_t bytesToRead, char* buffer) const;

    // Serves a random read from the local file cache only; returns nullopt when it must go to the blob.
    [[nodiscard]] std::optional<size_t> TryReadFromCache(int64_t offset, int64_t bytesToRead, char* buffer) const;
    // Serves a read from a completed in-memory prefetch only; never waits and never touches the file cache, so it is
    // safe on a host io_context thread (FileCache reads are blocking disk I/O).
    [[nodiscard]] std::optional<size_t> TryReadFromCompletedPrefetch(int64_t offset, int64_t bytesToRead,
                                                                     char* buffer) const;
    // True when a still-downloading prefetch fully covers the range. Advisory: the answer may change before the caller
    // acts, so ChainOntoPendingPrefetch remains the authority.
    [[nodiscard]] bool HasPendingPrefetch(int64_t offset, int64_t bytesToRead) const;
    // If a still-downloading prefetch fully covers the range, queues `resume` to run when that download settles
    // (successfully or not) and returns true; the caller must then not start its own read. Never blocks.
    [[nodiscard]] bool ChainOntoPendingPrefetch(int64_t offset, int64_t bytesToRead,
                                                std::move_only_function<void()>& resume) const;

    // Non-blocking random read from the blob (bypassing the file cache), with the same ETag/size refresh and retry
    // semantics as RandomRead. `self` keeps the file alive until `callback` has run; the callback may run on an
    // io_context thread or inline, so it must not block on blob I/O. The file is released before `callback` runs,
    // and the read stays registered with the AsyncReadTracker (if any) until `callback` itself is released.
    // A non-zero `timeout` caps each blob download of the read (see BlobOperations::DownloadAsync).
    template <ReadCompletion TCallback>
    static void ReadAsync(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset, int64_t bytesToRead,
                          TCallback callback, int attemptsLeft = kMaxStaleReadRetries,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

    // Upper bound of a single prefetched range, to keep per-file memory bounded.
    static constexpr int64_t kMaxPrefetchBytes = 8 * 1024 * 1024;

    // Starts a non-blocking download of [offset, offset + n) (clamped to kMaxPrefetchBytes) into a per-file buffer.
    // Blocking reads fully inside that range are served from it, waiting for the download if it is still running;
    // async reads use it only once it has completed. A slot is released once a read consumes its end, and the file
    // keeps at most kMaxPrefetchSlots ranges. Returns false (and starts nothing) when every slot is still downloading
    // or the shared byte budget would be exceeded, so the caller can let RocksDB fall back to its own readahead.
    [[nodiscard]] static bool Prefetch(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset, int64_t n);

    static constexpr size_t kMaxPrefetchSlots = 2;
    // Default process-wide cap on prefetched bytes across all files sharing a budget.
    static constexpr int64_t kDefaultPrefetchBudgetBytes = Configuration::DefaultPrefetchBudgetBytes;

    // The executor the blob client's completions run on; blocking waits for those completions must not run on it.
    [[nodiscard]] typename TBlob::executor_type ClientExecutor() const { return m_h.Blob->get_executor(); }

    [[nodiscard]] std::string GetETag() const;
    // Compares against the cached ETag under the metadata lock without copying it.
    [[nodiscard]] bool HasETag(std::string_view etag) const;
    int64_t GetSize() const;
    void RefreshBlobMetadata() const;
};
extern template class BasicReadableFileImpl<PageBlobClient>;

/// <summary>The production file, over the real page blob client. Non-template so it can be forward declared.</summary>
class ReadableFileImpl : public BasicReadableFileImpl<PageBlobClient> {
  public:
    using BasicReadableFileImpl<PageBlobClient>::BasicReadableFileImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
