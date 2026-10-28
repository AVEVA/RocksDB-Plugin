// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <boost/container/static_vector.hpp>
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
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
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

/// <summary>
/// The absolute time by which an async read must finish; empty means no deadline. It is fixed once when the read
/// starts, so every step (prefetch wait, metadata refresh, download, stale-read restart) draws on one budget.
/// </summary>
using ReadDeadline = std::optional<std::chrono::steady_clock::time_point>;

namespace ReadableFileDetail {
// Translates to a retryable TimedOut status, like a transport timeout.
inline std::exception_ptr ReadTimedOutError() {
    return std::make_exception_ptr(RequestFailedException(0, "ReadTimedOut", "The read deadline was exceeded", "",
                                                          std::make_error_code(std::errc::timed_out)));
}

// The read's owner went away; translates to an aborted status.
inline std::exception_ptr ReadAbandonedError() {
    return std::make_exception_ptr(RequestFailedException(0, "ReadAbandoned", "Read abandoned before it completed", "",
                                                          std::make_error_code(std::errc::operation_canceled)));
}

inline ReadDeadline DeadlineAfter(const std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0) {
        return std::nullopt;
    }
    // Saturate in milliseconds first: the clock's finer unit would overflow when converting an extreme timeout.
    return std::chrono::steady_clock::now() +
           std::min(timeout, AVEVA::AzureClient::AzureRequestOptions::MaxTotalDeadline);
}

inline bool Expired(const ReadDeadline& deadline) { return deadline && std::chrono::steady_clock::now() >= *deadline; }

// The time left as a request timeout: zero means "no deadline", so a live deadline never rounds down to it.
inline std::chrono::milliseconds Remaining(const ReadDeadline& deadline) {
    if (!deadline) {
        return std::chrono::milliseconds::zero();
    }
    return std::max(std::chrono::milliseconds(1),
                    std::chrono::ceil<std::chrono::milliseconds>(*deadline - std::chrono::steady_clock::now()));
}
} // namespace ReadableFileDetail

// Neither copyable nor movable: it is built once, in place, by make_shared, and in-flight reads hold it by shared_ptr.
template <typename TBlob> class BasicReadableFileImpl {
    BlobFileHandles<TBlob> m_h;
    // Random (and async) reads may run concurrently on one file, so the cached blob metadata is guarded.
    mutable std::mutex m_metadataMutex;
    mutable int64_t m_size;
    mutable std::string m_etag;
    // Bumped (under m_metadataMutex) whenever m_etag changes, so readers can detect a new blob version without
    // copying the ETag.
    mutable std::atomic<uint64_t> m_metadataVersion{0};
    // Cap on the shared prefetch budget; a Prefetch that would push the budget past it is declined.
    int64_t m_maxPrefetchBytes;
    // When m_size/m_etag were last read from the service; GetSize serves them for MetadataCacheTtl.
    mutable std::chrono::steady_clock::time_point m_metadataFetchedAt;
    // Optional; when set, every ReadAsync is registered with it until its callback has been released.
    std::shared_ptr<AsyncReadTracker> m_asyncReads;

    // One range requested through FSRandomAccessFile::Prefetch. The download's completion runs on the host io_context
    // and holds the file, so this state lives as long as that read.
    // Reservation of the backing buffer's real capacity in the shared budget. It is shared by the slot, the pending
    // download's completion and any reader copying from the buffer, so the bytes are refunded only once none of them
    // can still touch the allocation, even if the slot was dropped or shortened earlier.
    struct PrefetchCharge {
        std::shared_ptr<std::atomic<int64_t>> Budget;
        int64_t Bytes = 0;
        PrefetchCharge() = default;
        PrefetchCharge(const PrefetchCharge&) = delete;
        PrefetchCharge& operator=(const PrefetchCharge&) = delete;
        ~PrefetchCharge() {
            if (Budget) {
                Budget->fetch_sub(Bytes);
            }
        }
    };
    // Fixed so the visible slots live inline in the file object; waiter groups are not bounded by it.
    static constexpr size_t kMaxPrefetchSlots = 2;
    struct PrefetchSlot {
        uint64_t Id = 0;
        int64_t Offset = 0;
        int64_t Length = 0; // The visible range; the budget is charged for the buffer capacity via Charge.
        bool Pending = false;
        std::shared_ptr<PrefetchCharge> Charge; // Null when no shared budget is configured.
        std::shared_ptr<const void> Owner;      // keeps the pooled download buffer behind Data alive
        std::string_view Data;
    };
    struct PrefetchState {
        std::mutex Mutex;
        std::condition_variable Done;
        uint64_t NextId = 0;
        using SlotList = boost::container::static_vector<PrefetchSlot, kMaxPrefetchSlots>;
        SlotList Slots; // Oldest first.
        // Reads waiting for a pending slot (by id) to settle. Kept outside the slots so a slot that is cleared while
        // downloading still releases its waiters when its download finishes.
        // Grouped per slot id so completion detaches one group with a non-throwing move instead of allocating.
        struct WaiterGroup {
            uint64_t Id = 0;
            std::vector<std::move_only_function<void()>> Callbacks;
        };
        std::vector<WaiterGroup> Waiters;
        std::shared_ptr<std::atomic<int64_t>> Budget;

        ~PrefetchState();
        // Queues a waiter for `id`. On allocation failure `waiter` is left untouched. Caller holds Mutex.
        void AddWaiter(uint64_t id, std::move_only_function<void()>& waiter);
        // Detaches every waiter of `id` without allocating. Caller holds Mutex.
        [[nodiscard]] std::vector<std::move_only_function<void()>> TakeWaiters(uint64_t id) noexcept;
        // Removes a slot; its budget charge is returned once no download or reader still holds the buffer. Caller
        // holds Mutex.
        void Drop(SlotList::iterator slot);
        void DropAll();
    };
    mutable PrefetchState m_prefetch;

    // Concurrent async reads that need fresh blob metadata (at the cached end, or after a precondition failure) share
    // one in-flight properties call instead of each issuing their own.
    using MetadataCallback = std::move_only_function<void(std::exception_ptr, int64_t, std::string, uint64_t)>;
    struct MetadataRefreshState {
        std::mutex Mutex;
        bool InFlight = false;
        std::vector<MetadataCallback> Waiters;
    };
    mutable MetadataRefreshState m_metadataRefresh;
    static void CoalescedMetadataAsync(const std::shared_ptr<const BasicReadableFileImpl>& file,
                                       MetadataCallback callback);
    // Like CoalescedMetadataAsync, but `continuation` also runs (with a TimedOut error) once `deadline` passes. The
    // shared properties call is never cancelled on behalf of one waiter that gave up.
    template <typename TContinuation>
    static void WaitForMetadataAsync(const std::shared_ptr<const BasicReadableFileImpl>& file,
                                     const ReadDeadline& deadline, TContinuation continuation);

    // Serves from a completed prefetch only. With `wait`, first waits (bounded) for a covering pending prefetch.
    // With `allowPartial`, a range that only starts inside a slot is served up to the slot's end and the caller
    // fetches the remainder; the result is then the number of bytes copied, which may be less than requested.
    [[nodiscard]] std::optional<size_t> TryReadFromPrefetch(int64_t offset, int64_t bytesToRead, char* buffer,
                                                            bool wait, bool allowPartial = false,
                                                            uint64_t* deferredRetire = nullptr) const;
    void ClearPrefetch() const;
    int64_t DownloadWithRetry(const int64_t offset, const int64_t bytesToRead, char* buffer, int& refreshesLeft) const;
    [[nodiscard]] std::pair<int64_t, std::string> GetMetadata() const;
    void SetMetadata(int64_t size, std::string etag) const;
    // Publishes a refreshed (size, ETag) to the runtime cache for later opens, then to this file. `epoch` is the
    // runtime BlobSizeEpoch captured before the fetch started.
    void ApplyRefreshedMetadata(int64_t size, std::string etag, uint64_t epoch) const;
    template <ReadCompletion TCallback>
    static void ReadAsyncAttempt(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset, int64_t bytesToRead,
                                 TCallback callback, int attemptsLeft, ReadDeadline deadline,
                                 std::stop_token stopToken);
    template <ReadCompletion TCallback>
    static void RefreshMetadataAndReadAsync(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset,
                                            int64_t bytesToRead, TCallback callback, int attemptsLeft,
                                            ReadDeadline deadline, std::stop_token stopToken);

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
    // With deferredRetire, a hit that reaches the slot's end does not drop it: the slot's id is stored there instead
    // (left untouched otherwise) and the caller drops it with RetirePrefetch once its whole batch has been served.
    [[nodiscard]] std::optional<size_t> TryReadFromCompletedPrefetch(int64_t offset, int64_t bytesToRead, char* buffer,
                                                                     uint64_t* deferredRetire = nullptr) const;
    // Drops the slot with this id if it is still present; a replaced or already dropped slot is left alone.
    void RetirePrefetch(uint64_t id) const;
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
    // A non-zero `timeout` bounds the whole read, measured from this call: metadata refreshes, downloads (including
    // their transport retries) and stale-read restarts all draw on it, and the read completes with a TimedOut error
    // once it is spent.
    // A stop request on `stopToken` cancels this read's own download (including its transport retries), starts no
    // restart or further download, and completes the read with a cancelled error. It never touches a shared
    // metadata refresh or prefetch that the read may be waiting on.
    template <ReadCompletion TCallback>
    static void ReadAsync(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset, int64_t bytesToRead,
                          TCallback callback, int attemptsLeft = kMaxStaleReadRetries,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds::zero(),
                          std::stop_token stopToken = {});

    // Upper bound of a single prefetched range, to keep per-file memory bounded.
    static constexpr int64_t kMaxPrefetchBytes = 8 * 1024 * 1024;

    // Starts a non-blocking download of [offset, offset + n) (clamped to kMaxPrefetchBytes) into a per-file buffer.
    // Blocking reads fully inside that range are served from it, waiting for the download if it is still running;
    // async reads use it only once it has completed. A slot is released once a read consumes its end, and the file
    // keeps at most kMaxPrefetchSlots ranges. Returns false (and starts nothing) when every slot is still downloading
    // or the shared byte budget would be exceeded, so the caller can let RocksDB fall back to its own readahead.
    [[nodiscard]] static bool Prefetch(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset, int64_t n);

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
