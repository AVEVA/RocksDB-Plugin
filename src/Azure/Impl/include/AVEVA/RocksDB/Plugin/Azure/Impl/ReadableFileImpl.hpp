// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFileHandles.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SignedBytes.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/log/trivial.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// The absolute time by which an async read must finish; empty means no deadline. It is fixed once when the read
/// starts, so every step (prefetch wait, metadata refresh, download, stale-read restart) draws on one budget.
/// </summary>
using ReadDeadline = std::optional<std::chrono::steady_clock::time_point>;

namespace ReadableFileDetail {
enum class DeadlineStatus : std::uint8_t { Live, Expired };
enum class StaleVerdict : std::uint8_t { Fresh, Stale };
enum class RefreshBudget : uint8_t { Exhausted, Spent };
enum class PrefetchProgress : std::uint8_t { Settled, Downloading };
enum class PartialRead : std::uint8_t { Reject, Allow };
enum class MetadataRefreshProgress : std::uint8_t { Idle, InFlight };
enum class PrefetchStart : std::uint8_t { Declined, Started };
enum class PrefetchCoverage : std::uint8_t { Uncovered, Pending };
enum class PrefetchChain : std::uint8_t { Declined, Chained };
enum class ETagMatch : std::uint8_t { Differs, Matches };
enum class ETagChange : std::uint8_t { Unchanged, Changed };
enum class SlotEnd : std::uint8_t { NotReached, Reached };
// Whether a prefetch hit that reaches its slot's end drops the slot now, or leaves that to the caller.
enum class SlotRetirement : std::uint8_t { Immediate, Deferred };
// A read served from a completed prefetch. With SlotRetirement::Deferred, a hit that reached its slot's end names
// the slot in ExhaustedSlot; the caller drops it with RetirePrefetch once its whole batch has been served.
struct PrefetchHit {
    std::size_t Bytes = 0;
    std::optional<std::uint64_t> ExhaustedSlot;
};
enum class WaiterRegistration : std::uint8_t { None, Registered };
// Translates to a retryable TimedOut status, like a transport timeout.
inline std::exception_ptr ReadTimedOutError() {
    return std::make_exception_ptr(RequestFailedException(
        HttpStatus::NoResponse, ServiceErrorCode{"ReadTimedOut"}, ErrorMessage{"The read deadline was exceeded"},
        ServiceRequestId{""}, std::make_error_code(std::errc::timed_out)));
}

// The read's owner went away; translates to an aborted status.
inline std::exception_ptr ReadAbandonedError() {
    return std::make_exception_ptr(RequestFailedException(
        HttpStatus::NoResponse, ServiceErrorCode{"ReadAbandoned"}, ErrorMessage{"Read abandoned before it completed"},
        ServiceRequestId{""}, std::make_error_code(std::errc::operation_canceled)));
}

inline ReadDeadline DeadlineAfter(const std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0) {
        return std::nullopt;
    }
    // Saturate in milliseconds first: the clock's finer unit would overflow when converting an extreme timeout.
    return std::chrono::steady_clock::now() +
           std::min(timeout, AVEVA::AzureClient::AzureRequestOptions::MaxTotalDeadline);
}

inline DeadlineStatus StatusOf(const ReadDeadline& deadline) {
    return deadline && std::chrono::steady_clock::now() >= *deadline ? DeadlineStatus::Expired : DeadlineStatus::Live;
}
} // namespace ReadableFileDetail

// Neither copyable nor movable: it is built once, in place, by make_shared, and in-flight reads hold it by shared_ptr.
template <typename TBlob>
class BasicReadableFileImpl : public std::enable_shared_from_this<BasicReadableFileImpl<TBlob>> {
    BlobFileHandles<TBlob> m_h;
    // Random (and async) reads may run concurrently on one file, so the cached blob metadata is guarded.
    mutable std::mutex m_metadataMutex;
    mutable int64_t m_size;
    mutable std::string m_etag;
    // Bumped (under m_metadataMutex) whenever m_etag changes, so readers can detect a new blob version without
    // copying the ETag.
    mutable std::atomic<uint64_t> m_metadataVersion{0};
    // Cap on the shared prefetch budget; a Prefetch that would push the budget past it is declined.
    Core::ByteCount m_maxPrefetchBytes;
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
        ReadableFileDetail::PrefetchProgress Progress = ReadableFileDetail::PrefetchProgress::Downloading;
        std::shared_ptr<PrefetchCharge> Charge; // Null when no shared budget is configured.
        std::shared_ptr<const void> Owner;      // keeps the pooled download buffer behind Data alive
        std::string_view Data;
    };
    struct PrefetchState {
        std::mutex Mutex;
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
    struct FetchedMetadata {
        int64_t Size = 0;
        std::string ETag;
        uint64_t Epoch = 0;
    };
    // Each waiter owns a one-slot channel the shared refresh completes, so a waiter that timed out never blocks it.
    using MetadataChannel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code,
                                                                               std::exception_ptr, FetchedMetadata)>;
    struct MetadataRefreshState {
        std::mutex Mutex;
        ReadableFileDetail::MetadataRefreshProgress Progress = ReadableFileDetail::MetadataRefreshProgress::Idle;
        std::vector<std::shared_ptr<MetadataChannel>> Waiters;
    };
    mutable MetadataRefreshState m_metadataRefresh;
    static void JoinMetadataRefresh(const std::shared_ptr<const BasicReadableFileImpl>& file,
                                    std::shared_ptr<MetadataChannel> waiter);
    static void CompleteMetadataRefresh(std::shared_ptr<const BasicReadableFileImpl> file, std::exception_ptr error,
                                        const FetchedMetadata& fetched) noexcept;
    // Waits for fresh metadata, sharing one in-flight properties call; a stop request ends this reader's wait.
    static BlobOperations::Task<FetchedMetadata> FetchMetadataAsync(std::shared_ptr<const BasicReadableFileImpl> file);
    static BlobOperations::Task<void> PrefetchAsync(std::shared_ptr<const BasicReadableFileImpl> self, uint64_t id,
                                                    std::shared_ptr<PrefetchCharge> charge, int64_t offset, int64_t n);

    // Serves from a completed prefetch only; a pending one is skipped, so this never blocks.
    // With `allowPartial`, a range that only starts inside a slot is served up to the slot's end and the caller
    // fetches the remainder; the result is then the number of bytes copied, which may be less than requested.
    [[nodiscard]] std::optional<ReadableFileDetail::PrefetchHit>
    TryReadFromPrefetch(int64_t offset, int64_t bytesToRead, char* buffer, ReadableFileDetail::PartialRead partial,
                        ReadableFileDetail::SlotRetirement retirement) const;
    // One-shot signal a read waits on until the prefetch it needs settles.
    using PrefetchSettledChannel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code)>;
    // Waits (bounded by PrefetchWaitTimeout) for the pending prefetch covering `offset`, if there is one.
    [[nodiscard]] BlobOperations::Task<void> WaitForPrefetchAsync(int64_t offset) const;
    void ClearPrefetch() const;
    // The length of the read trimmed to the addressable range; 0 when nothing is left to read.
    [[nodiscard]] static int64_t ClampedLength(int64_t offset, int64_t bytesToRead);
    // What one DownloadWithRetryAsync call achieved, and the stale-read refreshes it left for the rest of the read.
    struct RetriedDownload {
        int64_t BytesRead = 0;
        int RefreshesLeft = 0;
    };
    [[nodiscard]] BlobOperations::Task<RetriedDownload> DownloadWithRetryAsync(int64_t offset, int64_t bytesToRead,
                                                                               char* buffer, int refreshesLeft) const;
    // The body of RandomRead/RandomReadAsync. Holds `file`, so it outlives the coroutine however it is run.
    [[nodiscard]] static BlobOperations::Task<int64_t> RandomReadFrom(std::shared_ptr<const BasicReadableFileImpl> file,
                                                                      int64_t offset, int64_t bytesToRead,
                                                                      char* buffer);
    [[nodiscard]] BlobOperations::Task<void> RefreshBlobMetadataAsync() const;
    [[nodiscard]] std::pair<int64_t, std::string> GetMetadata() const;
    void SetMetadata(int64_t size, std::string etag) const;
    // Publishes a refreshed (size, ETag) to the runtime cache for later opens, then to this file. `epoch` is the
    // runtime BlobSizeEpoch captured before the fetch started.
    void ApplyRefreshedMetadata(int64_t size, std::string etag, uint64_t epoch) const;

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
        Core::ByteCount maxPrefetchBytes = Defaults::MaxPrefetchBytes.Bytes());

    // Reads at an explicit offset; carries no read position of its own. Blocks the calling thread (never run it on the
    // client's executor); RandomReadAsync is the awaitable form. The file must be owned by a shared_ptr (as every
    // file handle holds it), which the read shares for its duration; otherwise it throws std::bad_weak_ptr.
    [[nodiscard]] int64_t RandomRead(int64_t offset, int64_t bytesToRead, char* buffer) const;

    // Awaitable random read into `buffer`: serves from a prefetch (waiting, bounded, for one in flight), then downloads
    // the rest with the usual ETag/size refresh and retry. It does not consult the file cache, whose reads are blocking
    // disk I/O; RandomRead does that first. `self` and `buffer` must stay alive until it completes.
    [[nodiscard]] static BlobOperations::Task<int64_t>
    RandomReadAsync(std::shared_ptr<const BasicReadableFileImpl> self, int64_t offset, int64_t bytesToRead,
                    char* buffer);

    // Serves a random read from the local file cache only; returns nullopt when it must go to the blob.
    [[nodiscard]] std::optional<size_t> TryReadFromCache(int64_t offset, int64_t bytesToRead, char* buffer) const;
    // Serves a read from a completed in-memory prefetch only; never waits and never touches the file cache, so it is
    // safe on a host io_context thread (FileCache reads are blocking disk I/O).
    // See PrefetchHit for what SlotRetirement::Deferred hands back.
    [[nodiscard]] std::optional<ReadableFileDetail::PrefetchHit>
    TryReadFromCompletedPrefetch(int64_t offset, int64_t bytesToRead, char* buffer,
                                 ReadableFileDetail::SlotRetirement retirement) const;
    // Drops the slot with this id if it is still present; a replaced or already dropped slot is left alone.
    void RetirePrefetch(uint64_t id) const;
    // Covered when a still-downloading prefetch fully covers the range. Advisory: the answer may change before the
    // caller acts, so ChainOntoPendingPrefetch remains the authority.
    [[nodiscard]] ReadableFileDetail::PrefetchCoverage HasPendingPrefetch(int64_t offset, int64_t bytesToRead) const;
    // If a still-downloading prefetch fully covers the range, queues `resume` to run when that download settles
    // (successfully or not) and returns Chained; the caller must then not start its own read. Never blocks.
    [[nodiscard]] ReadableFileDetail::PrefetchChain
    ChainOntoPendingPrefetch(int64_t offset, int64_t bytesToRead, std::move_only_function<void()>& resume) const;

    /// The bytes of an async read. `Data` is valid while `Owner` is held; keep it to retain the bytes without copying.
    struct ReadResult {
        std::string_view Data;
        std::shared_ptr<const void> Owner;
    };

    // Non-blocking random read from the blob (bypassing the file cache), with the same ETag/size refresh and retry
    // semantics as RandomRead. Failures are thrown from the awaitable. `self` is held by the coroutine, so the file
    // outlives the read. Run it with StartTracked (or StartOperation) on the client's executor.
    // A stop request on `stopToken` cancels this read's own wait or download (including its transport retries), starts
    // no restart or further download, and fails the read with a cancelled error. It never touches a shared metadata
    // refresh or prefetch that the read may be waiting on. Callers enforce a deadline by requesting a stop when it
    // passes.
    [[nodiscard]] static BlobOperations::Task<ReadResult> ReadAsync(std::shared_ptr<const BasicReadableFileImpl> self,
                                                                    int64_t offset, int64_t bytesToRead,
                                                                    int attemptsLeft = kMaxStaleReadRetries,
                                                                    std::stop_token stopToken = {});

    // Starts `operation` like StartOperation, additionally counting it as in flight for the AsyncReadTracker (if any)
    // until `done` has run and been released.
    template <typename TAwaitable, typename TDone>
    static void StartTracked(const std::shared_ptr<const BasicReadableFileImpl>& file, TAwaitable operation,
                             std::stop_token stopToken, TDone done);

    // Upper bound of a single prefetched range, to keep per-file memory bounded.
    static constexpr int64_t kMaxPrefetchBytes = 8 * 1024 * 1024;

    // Starts a non-blocking download of [offset, offset + n) (clamped to kMaxPrefetchBytes) into a per-file buffer.
    // Blocking reads fully inside that range are served from it, waiting for the download if it is still running;
    // async reads use it only once it has completed. A slot is released once a read consumes its end, and the file
    // keeps at most kMaxPrefetchSlots ranges. Returns Declined (and starts nothing) when every slot is still
    // downloading or the shared byte budget would be exceeded, so the caller can let RocksDB fall back to its own
    // readahead.
    [[nodiscard]] static ReadableFileDetail::PrefetchStart Prefetch(std::shared_ptr<const BasicReadableFileImpl> self,
                                                                    int64_t offset, int64_t n);

    // Default process-wide cap on prefetched bytes across all files sharing a budget.
    static constexpr Core::ByteCount kDefaultPrefetchBudgetBytes = Defaults::MaxPrefetchBytes.Bytes();

    // The executor the blob client's completions run on; blocking waits for those completions must not run on it.
    [[nodiscard]] typename TBlob::executor_type ClientExecutor() const { return m_h.Blob->get_executor(); }

    [[nodiscard]] std::string GetETag() const;
    // Compares against the cached ETag under the metadata lock without copying it.
    [[nodiscard]] ReadableFileDetail::ETagMatch HasETag(std::string_view etag) const;
    int64_t GetSize() const;
    void RefreshBlobMetadata() const;
};
extern template class BasicReadableFileImpl<PageBlobClient>;

/// <summary>The production file, over the real page blob client. Non-template so it can be forward declared.</summary>
class ReadableFileImpl : public BasicReadableFileImpl<PageBlobClient> {
  public:
    using BasicReadableFileImpl<PageBlobClient>::BasicReadableFileImpl;
};

// In a named namespace (not an anonymous one) so every TU that includes this header shares one definition of these.
namespace ReadableFileDetail {
inline RequestFailedException StaleReadException() {
    return RequestFailedException(HttpStatus::PreconditionFailed, ServiceErrorCode{"StaleRead"},
                                  ErrorMessage{"The blob kept changing while it was being read; retries exhausted"},
                                  ServiceRequestId{""}, {});
}

inline std::exception_ptr StaleReadError() { return std::make_exception_ptr(StaleReadException()); }

// The one policy for reads against a blob that may change underneath us, shared by the synchronous
// (DownloadWithRetryAsync) and asynchronous (ReadAsync) paths so they cannot drift apart: a 412 from the
// ETag-conditioned download means "stale", and each refresh-and-retry spends one unit of a fixed budget.
namespace StaleReadPolicy {
inline StaleVerdict StaleVerdictOf(const RequestFailedException& e) {
    return e.StatusCode() == HttpStatus::PreconditionFailed ? StaleVerdict::Stale : StaleVerdict::Fresh;
}

// Spends one refresh from `left`; Exhausted means the budget is spent and the read must fail with StaleRead.
inline RefreshBudget TrySpendRefresh(int& left) { return left-- > 0 ? RefreshBudget::Spent : RefreshBudget::Exhausted; }
} // namespace StaleReadPolicy

// Runs completion waiters, each isolated: one that throws must neither skip the rest (their reads would never
// complete) nor escape into the host io_context's run().
template <typename TLogger>
void RunWaiters(std::vector<std::move_only_function<void()>>& waiters, TLogger& logger) noexcept {
    for (auto& waiter : waiters) {
        try {
            waiter();
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(logger, boost::log::trivial::error) << "Prefetch waiter failed: " << e.what();
        } catch (...) {
            BOOST_LOG_SEV(logger, boost::log::trivial::error) << "Prefetch waiter failed";
        }
    }
}
} // namespace ReadableFileDetail

template <typename TBlob>
BasicReadableFileImpl<TBlob>::BasicReadableFileImpl(
    std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::shared_ptr<AsyncReadTracker> asyncReads, std::shared_ptr<std::atomic<int64_t>> prefetchBudget,
    Core::ByteCount maxPrefetchBytes)
    : m_h(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger)), m_size(0),
      m_maxPrefetchBytes(maxPrefetchBytes), m_asyncReads(std::move(asyncReads)) {
    m_prefetch.Budget = std::move(prefetchBudget);
    if (m_h.Runtime) {
        auto metadata = BlobOperations::GetCachedMetadata(m_h.Runtime, *m_h.Blob);
        m_size = metadata.Size;
        m_etag = std::move(metadata.ETag);
        m_metadataFetchedAt = metadata.FetchedAt;
    } else {
        auto metadata = BlobOperations::GetMetadata(*m_h.Blob);
        m_size = metadata.Size;
        m_etag = std::move(metadata.ETag);
        m_metadataFetchedAt = std::chrono::steady_clock::now();
    }
}

// Readahead past a saturated sequential position would overflow the end offset; nothing exists out there, so the
// length is clamped. 0 means there is nothing to read.
template <typename TBlob>
int64_t BasicReadableFileImpl<TBlob>::ClampedLength(const int64_t offset, const int64_t bytesToRead) {
    if (offset < 0 || bytesToRead <= 0) {
        return 0;
    }
    if (RangeCheck::Check(offset, bytesToRead) == RangeCheck::Bounds::OutOfRange) {
        return RangeCheck::MaxOffset - offset;
    }
    return bytesToRead;
}

template <typename TBlob>
int64_t BasicReadableFileImpl<TBlob>::RandomRead(const int64_t offset, int64_t bytesToRead, char* buffer) const {
    bytesToRead = ClampedLength(offset, bytesToRead);
    if (bytesToRead == 0) {
        return 0;
    }

    // The cache is blocking disk I/O, so it is consulted here on the caller's thread rather than inside the coroutine.
    if (m_h.FileCache) {
        const auto bytesRead = m_h.FileCache->ReadFile(m_h.Name, Core::ByteOffset{static_cast<uint64_t>(offset)},
                                                       std::span<char>{buffer, static_cast<size_t>(bytesToRead)});
        if (bytesRead) {
            return static_cast<int64_t>(bytesRead->Value());
        }
    }

    return Block(*m_h.Blob, RandomReadFrom(this->shared_from_this(), offset, bytesToRead, buffer));
}

template <typename TBlob>
BlobOperations::Task<int64_t>
BasicReadableFileImpl<TBlob>::RandomReadAsync(std::shared_ptr<const BasicReadableFileImpl> self, const int64_t offset,
                                              const int64_t bytesToRead, char* const buffer) {
    co_return co_await RandomReadFrom(std::move(self), offset, bytesToRead, buffer);
}

// Prefetch (waiting for one that is in flight), then download whatever it did not cover. Never touches the file cache.
// `file` lives in this frame, so the file outlives every step of the read.
template <typename TBlob>
BlobOperations::Task<int64_t>
BasicReadableFileImpl<TBlob>::RandomReadFrom(const std::shared_ptr<const BasicReadableFileImpl> file,
                                             const int64_t offset, int64_t bytesToRead, char* const buffer) {
    bytesToRead = ClampedLength(offset, bytesToRead);
    if (bytesToRead == 0) {
        co_return 0;
    }

    int64_t served = 0;
    // Prefetched bytes belong to the version cached at this point; if the download below ends up on another
    // version the copied prefix is stale and the whole range must be read again.
    const auto prefetchVersion = file->m_metadataVersion.load(std::memory_order_acquire);
    co_await file->WaitForPrefetchAsync(offset);
    if (const auto prefetched =
            file->TryReadFromPrefetch(offset, bytesToRead, buffer, ReadableFileDetail::PartialRead::Allow,
                                      ReadableFileDetail::SlotRetirement::Immediate)) {
        served = static_cast<int64_t>(prefetched->Bytes);
        if (served >= bytesToRead) {
            co_return served;
        }
    }

    // Only the part the prefetch did not cover is downloaded. The suffix and the whole-range re-read share one
    // stale-read budget so a single read cannot spend it twice under churn.
    auto download = co_await file->DownloadWithRetryAsync(offset + served, bytesToRead - served, buffer + served,
                                                          kMaxStaleReadRetries);
    if (served > 0 && file->m_metadataVersion.load(std::memory_order_acquire) != prefetchVersion) {
        served = 0;
        download = co_await file->DownloadWithRetryAsync(offset, bytesToRead, buffer, download.RefreshesLeft);
    }
    co_return served + std::max<int64_t>(download.BytesRead, 0);
}

// Waits until the pending prefetch covering `offset` (if any) settles, bounded so a stuck download degrades to a normal
// read instead of a hang. Returning does not promise the data is there: the caller re-checks and falls back.
template <typename TBlob>
BlobOperations::Task<void> BasicReadableFileImpl<TBlob>::WaitForPrefetchAsync(const int64_t offset) const {
    const auto executor = co_await boost::asio::this_coro::executor;
    auto settled = std::make_shared<PrefetchSettledChannel>(executor, 1);
    auto registration = ReadableFileDetail::WaiterRegistration::None;
    try {
        std::scoped_lock lock(m_prefetch.Mutex);
        const auto slot = std::ranges::find_if(m_prefetch.Slots, [&](const PrefetchSlot& candidate) {
            return candidate.Progress == ReadableFileDetail::PrefetchProgress::Downloading &&
                   offset >= candidate.Offset && offset < candidate.Offset + candidate.Length;
        });
        if (slot != m_prefetch.Slots.end()) {
            std::move_only_function<void()> wake = [settled] { settled->try_send(boost::system::error_code{}); };
            m_prefetch.AddWaiter(slot->Id, wake);
            registration = ReadableFileDetail::WaiterRegistration::Registered;
        }
    } catch (...) {
        // Out of memory for the waiter: read without waiting rather than failing the read.
    }
    if (registration == ReadableFileDetail::WaiterRegistration::None) {
        co_return;
    }

    using namespace boost::asio::experimental::awaitable_operators;
    boost::asio::steady_timer timer(executor, Configuration::PrefetchWaitTimeout);
    const auto outcome =
        co_await (settled->async_receive(boost::asio::use_awaitable) || timer.async_wait(boost::asio::use_awaitable));
    if (outcome.index() != 0) {
        BOOST_LOG_SEV(*m_h.Logger, boost::log::trivial::debug)
            << "Timed out waiting for a prefetch of '" << m_h.Name << "'; falling back to a download";
    }
}

template <typename TBlob> std::string BasicReadableFileImpl<TBlob>::GetETag() const { return GetMetadata().second; }

template <typename TBlob>
ReadableFileDetail::ETagMatch BasicReadableFileImpl<TBlob>::HasETag(const std::string_view etag) const {
    std::scoped_lock lock(m_metadataMutex);
    return m_etag == etag ? ReadableFileDetail::ETagMatch::Matches : ReadableFileDetail::ETagMatch::Differs;
}

template <typename TBlob> int64_t BasicReadableFileImpl<TBlob>::GetSize() const {
    {
        std::scoped_lock lock(m_metadataMutex);
        const auto now = m_h.Runtime ? m_h.Runtime->MetadataCache().Now() : std::chrono::steady_clock::now();
        if (now - m_metadataFetchedAt < Configuration::MetadataCacheTtl) {
            return m_size;
        }
    }
    RefreshBlobMetadata();

    return GetMetadata().first;
}

template <typename TBlob> std::pair<int64_t, std::string> BasicReadableFileImpl<TBlob>::GetMetadata() const {
    std::scoped_lock lock(m_metadataMutex);
    return {m_size, m_etag};
}

// Updates the cached size/ETag together. Prefetched bytes belong to one blob version, so they are only discarded
// when the ETag shows the blob changed; a refresh that finds the same version keeps them.
template <typename TBlob> void BasicReadableFileImpl<TBlob>::SetMetadata(const int64_t size, std::string etag) const {
    BOOST_LOG_SEV(*m_h.Logger, boost::log::trivial::debug)
        << "Blob metadata refreshed for file '" << m_h.Name << "' :size = " << size << " bytes, etag = " << etag;
    auto change = ReadableFileDetail::ETagChange::Unchanged;
    {
        std::scoped_lock lock(m_metadataMutex);
        change = m_etag != etag ? ReadableFileDetail::ETagChange::Changed : ReadableFileDetail::ETagChange::Unchanged;
        m_size = size;
        if (change == ReadableFileDetail::ETagChange::Changed) {
            m_metadataVersion.fetch_add(1, std::memory_order_release);
        }
        m_etag = std::move(etag);
        m_metadataFetchedAt = m_h.Runtime ? m_h.Runtime->MetadataCache().Now() : std::chrono::steady_clock::now();
    }
    if (change == ReadableFileDetail::ETagChange::Changed) {
        ClearPrefetch();
    }
}

template <typename TBlob> BasicReadableFileImpl<TBlob>::PrefetchState::~PrefetchState() { DropAll(); }

template <typename TBlob>
void BasicReadableFileImpl<TBlob>::PrefetchState::AddWaiter(const uint64_t id,
                                                            std::move_only_function<void()>& waiter) {
    // Grows geometrically, and only when full: reserving size() + 1 each time would reallocate on every insertion.
    const auto reserveOne = [](auto& vector) {
        if (vector.size() == vector.capacity()) {
            vector.reserve(std::max<size_t>(vector.capacity() * 2, 4));
        }
    };
    auto group = std::ranges::find(Waiters, id, &WaiterGroup::Id);
    if (group == Waiters.end()) {
        reserveOne(Waiters);
        group = Waiters.emplace(Waiters.end());
        group->Id = id;
    }
    // Reserved first so the move below cannot fail and strand the caller's callback.
    try {
        reserveOne(group->Callbacks);
    } catch (...) {
        if (group->Callbacks.empty()) {
            Waiters.erase(group);
        }
        throw;
    }
    group->Callbacks.push_back(std::move(waiter));
}

template <typename TBlob>
std::vector<std::move_only_function<void()>>
BasicReadableFileImpl<TBlob>::PrefetchState::TakeWaiters(const uint64_t id) noexcept {
    std::vector<std::move_only_function<void()>> taken;
    if (const auto group = std::ranges::find(Waiters, id, &WaiterGroup::Id); group != Waiters.end()) {
        taken = std::move(group->Callbacks);
        Waiters.erase(group);
    }
    return taken;
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::PrefetchState::Drop(const SlotList::iterator slot) {
    Slots.erase(slot);
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::PrefetchState::DropAll() { Slots.clear(); }

template <typename TBlob> void BasicReadableFileImpl<TBlob>::ClearPrefetch() const {
    std::scoped_lock lock(m_prefetch.Mutex);
    m_prefetch.DropAll();
}

// Downloads a prefetch range and publishes the outcome to its slot and waiters. Never throws: a failed download drops
// the slot, and the waiters then fall back to their own downloads.
template <typename TBlob>
BlobOperations::Task<void>
BasicReadableFileImpl<TBlob>::PrefetchAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self, const uint64_t id,
                                            std::shared_ptr<PrefetchCharge> charge, const int64_t offset,
                                            const int64_t n) {
    ReadResult result;
    std::exception_ptr error;
    try {
        result = co_await ReadAsync(self, offset, n);
    } catch (...) {
        error = std::current_exception();
    }

    auto* const state = &self->m_prefetch;
    const auto& logger = self->m_h.Logger;
    std::vector<std::move_only_function<void()>> waiters;
    {
        std::scoped_lock lock(state->Mutex);
        // Detached first so nothing after this point can fail before the waiters are notified.
        waiters = state->TakeWaiters(id);
        // A superseded or cleared prefetch (its slot is gone) must not publish its bytes.
        const auto slot = std::ranges::find(state->Slots, id, &PrefetchSlot::Id);
        if (slot != state->Slots.end()) {
            if (error) {
                state->Drop(slot);
            } else {
                slot->Progress = ReadableFileDetail::PrefetchProgress::Settled;
                // A short read keeps its charge: the pooled buffer is still fully allocated.
                if (const auto got = static_cast<int64_t>(result.Data.size()); got < slot->Length) {
                    slot->Length = got;
                }
                // The slot keeps the pooled download buffer instead of copying up to 8 MiB out of it.
                slot->Owner = std::move(result.Owner);
                slot->Data = result.Data;
            }
        }
    }
    // Waiters retry their read, which now finds the data or falls back to a download.
    ReadableFileDetail::RunWaiters(waiters, *logger);
    if (error) {
        try {
            std::rethrow_exception(error);
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*logger, boost::log::trivial::warning)
                << "Prefetch of '" << self->m_h.Name << "' failed: " << e.what();
        } catch (...) {
            BOOST_LOG_SEV(*logger, boost::log::trivial::warning) << "Prefetch of '" << self->m_h.Name << "' failed";
        }
    }
}

template <typename TBlob>
ReadableFileDetail::PrefetchStart
BasicReadableFileImpl<TBlob>::Prefetch(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self, const int64_t offset,
                                       int64_t n) {
    using ReadableFileDetail::PrefetchStart;
    if (offset < 0 || n <= 0) {
        return PrefetchStart::Started;
    }

    // Clamp so that offset + n below cannot overflow int64_t.
    n = std::min({n, kMaxPrefetchBytes, RangeCheck::MaxOffset - offset});
    if (n <= 0) {
        return PrefetchStart::Started;
    }
    auto* const state = &self->m_prefetch;
    uint64_t id = 0;
    std::shared_ptr<PrefetchCharge> charge;
    {
        std::scoped_lock lock(state->Mutex);
        for (const auto& slot : state->Slots) {
            if (offset >= slot.Offset && offset + n <= slot.Offset + slot.Length) {
                return PrefetchStart::Started; // Already (being) fetched.
            }
        }

        // Everything fallible is prepared before budget or slot state changes, so a failure leaves both untouched and
        // the admission below cannot throw. The slots are inline, so the push below needs no allocation and a full list
        // is only ever appended to after its victim is dropped.
        if (state->Budget) {
            charge = std::make_shared<PrefetchCharge>();
        }

        // A declined prefetch must not cost data already held, so nothing is evicted until it is known to fit.
        auto victim = state->Slots.end();
        if (state->Slots.size() >= kMaxPrefetchSlots) {
            // The oldest finished range goes; if all are still downloading, decline so RocksDB reads ahead itself.
            victim = std::ranges::find_if(state->Slots, [](const auto& slot) {
                return slot.Progress == ReadableFileDetail::PrefetchProgress::Settled;
            });
            if (victim == state->Slots.end()) {
                return PrefetchStart::Declined;
            }
        }

        if (charge) {
            // The download buffer is a whole size class, not just the requested length.
            const auto bytes = static_cast<int64_t>(DownloadBufferPool::CapacityFor(static_cast<size_t>(n)));
            // The victim only frees its charge if nothing else (a reader) still holds its buffer.
            const int64_t freed = victim != state->Slots.end() && victim->Charge && victim->Charge.use_count() == 1
                                      ? victim->Charge->Bytes
                                      : 0;
            if (state->Budget->fetch_add(bytes) + bytes - freed > SignedBytes(self->m_maxPrefetchBytes)) {
                state->Budget->fetch_sub(bytes);
                return PrefetchStart::Declined;
            }
            charge->Budget = state->Budget;
            charge->Bytes = bytes;
        }
        if (victim != state->Slots.end()) {
            state->Drop(victim);
        }

        id = ++state->NextId;
        state->Slots.push_back(PrefetchSlot{.Id = id,
                                            .Offset = offset,
                                            .Length = n,
                                            .Progress = ReadableFileDetail::PrefetchProgress::Downloading,
                                            .Charge = charge,
                                            .Owner = {},
                                            .Data = {}});
    }

    // A superseded or cleared prefetch (its slot is gone) must not publish its bytes.
    auto logger = self->m_h.Logger;
    // The completion needs the prefetch state, which is part of the file, so the coroutine keeps the file alive.
    try {
        StartTracked(self, PrefetchAsync(self, id, std::move(charge), offset, n), std::stop_token{},
                     [](std::exception_ptr) {});
    } catch (...) {
        std::vector<std::move_only_function<void()>> waiters;
        {
            std::scoped_lock lock(state->Mutex);
            waiters = state->TakeWaiters(id);
            if (const auto slot = std::ranges::find(state->Slots, id, &PrefetchSlot::Id); slot != state->Slots.end()) {
                state->Drop(slot);
            }
        }
        // The slot is gone, so each waiter falls back to its own download rather than waiting forever.
        ReadableFileDetail::RunWaiters(waiters, *logger);
        throw;
    }
    return PrefetchStart::Started;
}

// Only a completed prefetch is served; a pending one is skipped, so this never blocks. RandomReadAsync waits for a
// pending prefetch first (WaitForPrefetchAsync).
template <typename TBlob>
std::optional<ReadableFileDetail::PrefetchHit>
BasicReadableFileImpl<TBlob>::TryReadFromPrefetch(const int64_t offset, const int64_t bytesToRead, char* buffer,
                                                  const ReadableFileDetail::PartialRead partial,
                                                  const ReadableFileDetail::SlotRetirement retirement) const {
    if (offset < 0 || bytesToRead <= 0) {
        return std::nullopt;
    }

    auto& state = m_prefetch;
    std::unique_lock lock(state.Mutex);
    const auto slot = std::ranges::find_if(state.Slots, [&](const PrefetchSlot& candidate) {
        const auto end = candidate.Offset + candidate.Length;
        return offset >= candidate.Offset && offset < end &&
               (partial == ReadableFileDetail::PartialRead::Allow || offset + bytesToRead <= end);
    });
    if (slot == state.Slots.end() || slot->Progress == ReadableFileDetail::PrefetchProgress::Downloading) {
        return std::nullopt;
    }

    const auto start = static_cast<size_t>(offset - slot->Offset);
    if (start >= slot->Data.size()) {
        return std::nullopt;
    }
    const auto available = slot->Data.size() - start;
    auto length = static_cast<size_t>(bytesToRead);
    if (length > available) {
        if (partial == ReadableFileDetail::PartialRead::Reject) {
            return std::nullopt;
        }
        length = available;
    }

    // Published data is immutable, so copy it outside the lock; `owner` keeps the buffer alive meanwhile.
    const auto id = slot->Id;
    const auto owner = slot->Owner;
    const auto charge = slot->Charge; // keeps the budget charged while the buffer is being read
    const auto source = slot->Data.data() + start;
    const auto slotEnd = offset + bytesToRead >= slot->Offset + slot->Length ? ReadableFileDetail::SlotEnd::Reached
                                                                             : ReadableFileDetail::SlotEnd::NotReached;
    lock.unlock();

    std::copy_n(source, length, buffer);
    // Reading up to or past the end of the range means the scan has moved on; free the memory. A batch reader defers
    // that, because its other members may still need this range.
    if (slotEnd == ReadableFileDetail::SlotEnd::NotReached) {
        return ReadableFileDetail::PrefetchHit{.Bytes = length, .ExhaustedSlot = std::nullopt};
    }
    if (retirement == ReadableFileDetail::SlotRetirement::Deferred) {
        return ReadableFileDetail::PrefetchHit{.Bytes = length, .ExhaustedSlot = id};
    }
    std::scoped_lock relock(state.Mutex);
    if (const auto current = std::ranges::find(state.Slots, id, &PrefetchSlot::Id); current != state.Slots.end()) {
        state.Drop(current);
    }
    return ReadableFileDetail::PrefetchHit{.Bytes = length, .ExhaustedSlot = std::nullopt};
}

template <typename TBlob>
ReadableFileDetail::PrefetchCoverage BasicReadableFileImpl<TBlob>::HasPendingPrefetch(const int64_t offset,
                                                                                      const int64_t bytesToRead) const {
    using ReadableFileDetail::PrefetchCoverage;
    if (offset < 0 || bytesToRead <= 0) {
        return PrefetchCoverage::Uncovered;
    }

    std::scoped_lock lock(m_prefetch.Mutex);
    const bool covered = std::ranges::any_of(m_prefetch.Slots, [&](const PrefetchSlot& candidate) {
        return candidate.Progress == ReadableFileDetail::PrefetchProgress::Downloading && offset >= candidate.Offset &&
               offset + bytesToRead <= candidate.Offset + candidate.Length;
    });
    return covered ? PrefetchCoverage::Pending : PrefetchCoverage::Uncovered;
}

template <typename TBlob>
ReadableFileDetail::PrefetchChain
BasicReadableFileImpl<TBlob>::ChainOntoPendingPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                       std::move_only_function<void()>& resume) const {
    using ReadableFileDetail::PrefetchChain;
    if (offset < 0 || bytesToRead <= 0) {
        return PrefetchChain::Declined;
    }

    std::scoped_lock lock(m_prefetch.Mutex);
    const auto slot = std::ranges::find_if(m_prefetch.Slots, [&](const PrefetchSlot& candidate) {
        return candidate.Progress == ReadableFileDetail::PrefetchProgress::Downloading && offset >= candidate.Offset &&
               offset + bytesToRead <= candidate.Offset + candidate.Length;
    });
    if (slot == m_prefetch.Slots.end()) {
        return PrefetchChain::Declined;
    }
    m_prefetch.AddWaiter(slot->Id, resume);
    return PrefetchChain::Chained;
}

// The file cache first, then a completed prefetch; nullopt sends the read to the blob.
template <typename TBlob>
std::optional<size_t> BasicReadableFileImpl<TBlob>::TryReadFromCache(const int64_t offset, const int64_t bytesToRead,
                                                                     char* buffer) const {
    if (m_h.FileCache && offset >= 0 && bytesToRead > 0) {
        if (const auto cached = m_h.FileCache->ReadFile(m_h.Name, Core::ByteOffset{static_cast<uint64_t>(offset)},
                                                        std::span<char>{buffer, static_cast<size_t>(bytesToRead)})) {
            return static_cast<size_t>(cached->Value());
        }
    }
    if (const auto hit = TryReadFromPrefetch(offset, bytesToRead, buffer, ReadableFileDetail::PartialRead::Reject,
                                             ReadableFileDetail::SlotRetirement::Immediate)) {
        return hit->Bytes;
    }
    return std::nullopt;
}

template <typename TBlob>
std::optional<ReadableFileDetail::PrefetchHit>
BasicReadableFileImpl<TBlob>::TryReadFromCompletedPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                           char* buffer,
                                                           const ReadableFileDetail::SlotRetirement retirement) const {
    return TryReadFromPrefetch(offset, bytesToRead, buffer, ReadableFileDetail::PartialRead::Reject, retirement);
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::RetirePrefetch(const uint64_t id) const {
    std::scoped_lock lock(m_prefetch.Mutex);
    if (const auto slot = std::ranges::find(m_prefetch.Slots, id, &PrefetchSlot::Id); slot != m_prefetch.Slots.end()) {
        m_prefetch.Drop(slot);
    }
}

// Runs `operation` on the client's executor. While it runs the file's AsyncReadTracker (if any) counts it as in
// flight; the token is released after `done`, so the filesystem cannot finish draining while `done` still holds
// references into it.
template <typename TBlob>
template <typename TAwaitable, typename TDone>
void BasicReadableFileImpl<TBlob>::StartTracked(const std::shared_ptr<const BasicReadableFileImpl>& file,
                                                TAwaitable operation, std::stop_token stopToken, TDone done) {
    if (!file->m_asyncReads) {
        StartOperation(file->ClientExecutor(), std::move(operation), std::move(stopToken), std::move(done));
        return;
    }
    // The token is declared first so it is destroyed after `done`.
    StartOperation(file->ClientExecutor(), std::move(operation), std::move(stopToken),
                   [token = file->m_asyncReads->Begin(), done = std::move(done)](std::exception_ptr error,
                                                                                 auto&&... result) mutable {
                       done(std::move(error), std::forward<decltype(result)>(result)...);
                   });
}

// Download-only counterpart of DownloadWithRetryAsync (zero-copy into a pooled buffer), written as straight-line code:
// nothing blocks a thread, every wait is a co_await. A caller's deadline arrives as a stop request, which cancels the
// wait or download in progress; the checks below keep a restart from starting once the read was abandoned.
// Failures are thrown rather than reported through a status.
template <typename TBlob>
BlobOperations::Task<typename BasicReadableFileImpl<TBlob>::ReadResult>
BasicReadableFileImpl<TBlob>::ReadAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self, const int64_t offset,
                                        const int64_t bytesToRead, int attemptsLeft, const std::stop_token stopToken) {
    for (;;) {
        if (offset < 0 || bytesToRead <= 0) {
            co_return ReadResult{};
        }
        // An abandoned read must not start another metadata call, download or restart.
        if (stopToken.stop_requested()) {
            std::rethrow_exception(ReadableFileDetail::ReadAbandonedError());
        }

        auto [size, etag] = self->GetMetadata();
        const auto remaining = std::max<int64_t>(0, size - offset);
        if (remaining == 0) {
            // At the cached end of the blob: only re-read if another writer moved the blob on.
            auto latest = co_await FetchMetadataAsync(self);
            if (latest.ETag == etag) {
                self->ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), latest.Epoch);
                co_return ReadResult{};
            }
            if (ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(attemptsLeft) ==
                ReadableFileDetail::RefreshBudget::Exhausted) {
                throw ReadableFileDetail::StaleReadException();
            }
            self->ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), latest.Epoch);
            continue;
        }

        const auto toRead = std::min(bytesToRead, remaining);
        // co_await may not appear in a handler, so a stale read only leaves the try block and is refreshed below.
        try {
            auto bytes = co_await BlobOperations::DownloadForReadAsync(*self->m_h.Blob, OffsetFromSigned(offset),
                                                                       CountFromSigned(toRead), std::move(etag));
            if (static_cast<int64_t>(bytes.Data.size()) > remaining) {
                bytes.Data = bytes.Data.substr(0, static_cast<size_t>(remaining));
            }
            co_return ReadResult{bytes.Data, std::move(bytes.Owner)};
        } catch (const RequestFailedException& error) {
            if (ReadableFileDetail::StaleReadPolicy::StaleVerdictOf(error) == ReadableFileDetail::StaleVerdict::Fresh) {
                throw;
            }
        }

        if (stopToken.stop_requested()) {
            std::rethrow_exception(ReadableFileDetail::ReadAbandonedError());
        }
        if (ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(attemptsLeft) ==
            ReadableFileDetail::RefreshBudget::Exhausted) {
            throw ReadableFileDetail::StaleReadException();
        }
        auto fresh = co_await FetchMetadataAsync(self);
        self->ApplyRefreshedMetadata(fresh.Size, std::move(fresh.ETag), fresh.Epoch);
    }
}

// Waits for fresh blob metadata. The first reader to ask starts the properties call; readers arriving while it is in
// flight queue behind it (see JoinMetadataRefresh). A stop request ends this reader's wait, but the shared call is
// never cancelled on behalf of one reader that gave up.
template <typename TBlob>
BlobOperations::Task<typename BasicReadableFileImpl<TBlob>::FetchedMetadata>
BasicReadableFileImpl<TBlob>::FetchMetadataAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> file) {
    const auto executor = co_await boost::asio::this_coro::executor;
    auto waiter = std::make_shared<MetadataChannel>(executor, 1);
    JoinMetadataRefresh(file, waiter);

    auto [error, fetched] = co_await waiter->async_receive(boost::asio::use_awaitable);
    if (error) {
        std::rethrow_exception(std::move(error));
    }
    co_return fetched;
}

// A joiner may receive metadata fetched slightly before it asked; if that is already stale its read fails the ETag
// condition again and is retried against its own refresh budget.
template <typename TBlob>
void BasicReadableFileImpl<TBlob>::JoinMetadataRefresh(const std::shared_ptr<const BasicReadableFileImpl>& file,
                                                       std::shared_ptr<MetadataChannel> waiter) {
    auto& state = file->m_metadataRefresh;
    {
        std::scoped_lock lock(state.Mutex);
        state.Waiters.push_back(std::move(waiter));
        if (state.Progress == ReadableFileDetail::MetadataRefreshProgress::InFlight) {
            return;
        }
        state.Progress = ReadableFileDetail::MetadataRefreshProgress::InFlight;
    }
    // Initiation can throw (allocation, client construction, fenced runtime). Without this, the in-flight marker would
    // stay set
    // forever and every later caller would queue behind a request that never completes.
    try {
        // The completion needs the state, which is part of the file, so it holds the file until the waiters are taken;
        // it lets go before running them so the file is not kept alive past the reads that wait on it.
        const auto epoch = file->m_h.Runtime ? file->m_h.Runtime->MetadataCache().SizeEpoch() : 0;
        StartOperation(file->ClientExecutor(), BlobOperations::GetMetadataAsync(*file->m_h.Blob), std::stop_token{},
                       [file = file, epoch](std::exception_ptr error, BlobOperations::BlobMetadata metadata) mutable {
                           CompleteMetadataRefresh(std::move(file), std::move(error),
                                                   FetchedMetadata{metadata.Size, std::move(metadata.ETag), epoch});
                       });
    } catch (...) {
        CompleteMetadataRefresh(file, std::current_exception(), {});
    }
}

// Hands the outcome to every waiter. Each is isolated: one that fails must neither skip the rest (their reads would
// never complete) nor escape into the host io_context's run().
template <typename TBlob>
void BasicReadableFileImpl<TBlob>::CompleteMetadataRefresh(std::shared_ptr<const BasicReadableFileImpl> file,
                                                           const std::exception_ptr error,
                                                           const FetchedMetadata& fetched) noexcept {
    std::vector<std::shared_ptr<MetadataChannel>> waiters;
    {
        auto& refresh = file->m_metadataRefresh;
        std::scoped_lock lock(refresh.Mutex);
        waiters.swap(refresh.Waiters);
        refresh.Progress = ReadableFileDetail::MetadataRefreshProgress::Idle;
    }
    const auto logger = file->m_h.Logger;
    file.reset();
    for (const auto& waiter : waiters) {
        if (!waiter->try_send(boost::system::error_code{}, error, fetched) && logger) {
            BOOST_LOG_SEV(*logger, boost::log::trivial::error) << "Metadata refresh waiter failed";
        }
    }
}

// Downloads into `buffer` from the blob conditioned on the cached ETag. When the blob changed underneath us
// (precondition failure, or reading at the cached end of a blob whose ETag moved on) the metadata is refreshed and the
// read retried, so readers observe data appended by other writers. `refreshesLeft` is the read's remaining budget;
// what is left of it is returned so a follow-up download of the same read cannot spend it twice. The caller keeps the
// file and `buffer` alive until the coroutine finishes.
template <typename TBlob>
BlobOperations::Task<typename BasicReadableFileImpl<TBlob>::RetriedDownload>
BasicReadableFileImpl<TBlob>::DownloadWithRetryAsync(const int64_t offset, const int64_t bytesToRead,
                                                     char* const buffer, int refreshesLeft) const {
    for (;;) {
        auto [size, etag] = GetMetadata();
        const auto remaining = std::max<int64_t>(0, size - offset);
        if (remaining == 0) {
            // One properties call serves both the ETag check and the refresh, so a stale end-of-blob read costs a
            // single round trip.
            const auto epoch = m_h.Runtime ? m_h.Runtime->MetadataCache().SizeEpoch() : 0;
            auto latest = co_await BlobOperations::GetMetadataAsync(*m_h.Blob);
            const auto change = latest.ETag != etag ? ReadableFileDetail::ETagChange::Changed
                                                    : ReadableFileDetail::ETagChange::Unchanged;
            if (change == ReadableFileDetail::ETagChange::Changed &&
                ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft) ==
                    ReadableFileDetail::RefreshBudget::Exhausted) {
                throw ReadableFileDetail::StaleReadException();
            }
            ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), epoch);
            if (change == ReadableFileDetail::ETagChange::Unchanged) {
                co_return RetriedDownload{0, refreshesLeft};
            }
            continue;
        }

        const auto toRead = std::min(bytesToRead, remaining);
        const std::string usedEtag = etag;
        // co_await may not appear in a handler, so a stale read only leaves the try block and is refreshed below.
        try {
            const auto bytesRead = co_await BlobOperations::DownloadAsync(
                *m_h.Blob, std::span<char>(buffer, static_cast<size_t>(toRead)), OffsetFromSigned(offset),
                CountFromSigned(toRead), std::move(etag));
            co_return RetriedDownload{std::min(SignedBytes(bytesRead), remaining), refreshesLeft};
        } catch (const RequestFailedException& ex) {
            if (ReadableFileDetail::StaleReadPolicy::StaleVerdictOf(ex) == ReadableFileDetail::StaleVerdict::Fresh) {
                throw;
            }
        }

        if (ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft) ==
            ReadableFileDetail::RefreshBudget::Exhausted) {
            throw ReadableFileDetail::StaleReadException();
        }
        // Another reader may already have refreshed past the version this request used; retry with it instead of
        // sending a redundant properties request.
        if (HasETag(usedEtag) == ReadableFileDetail::ETagMatch::Matches) {
            co_await RefreshBlobMetadataAsync();
        }
    }
}

template <typename TBlob> BlobOperations::Task<void> BasicReadableFileImpl<TBlob>::RefreshBlobMetadataAsync() const {
    // Query outside the lock so concurrent readers are not serialized behind network round trips. A refresh means the
    // cached entry was stale, so it is replaced for the next open too.
    const auto epoch = m_h.Runtime ? m_h.Runtime->MetadataCache().SizeEpoch() : 0;
    auto metadata = co_await BlobOperations::GetMetadataAsync(*m_h.Blob);
    ApplyRefreshedMetadata(metadata.Size, std::move(metadata.ETag), epoch);
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::RefreshBlobMetadata() const {
    Block(*m_h.Blob, RefreshBlobMetadataAsync());
}

template <typename TBlob>
void BasicReadableFileImpl<TBlob>::ApplyRefreshedMetadata(const int64_t size, std::string etag,
                                                          const uint64_t epoch) const {
    if (m_h.Runtime) {
        m_h.Runtime->MetadataCache().StoreMetadata(m_h.Blob->ResourceUrl(), size, etag, epoch);
    }
    SetMetadata(size, std::move(etag));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
