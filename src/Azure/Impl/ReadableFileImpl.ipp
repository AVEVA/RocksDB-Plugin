// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Definitions of BasicReadableFileImpl; included at the end of ReadableFileImpl.hpp.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/RangeCheck.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <algorithm>
#include <boost/asio/steady_timer.hpp>
#include <boost/log/trivial.hpp>
#include <cassert>
#include <chrono>
#include <functional>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// In a named namespace (not an anonymous one) so every TU that includes this header shares one definition of these.
namespace ReadableFileDetail {
inline RequestFailedException StaleReadException() {
    return RequestFailedException(static_cast<unsigned int>(HttpStatus::PreconditionFailed), "StaleRead",
                                  "The blob kept changing while it was being read; retries exhausted", "", {});
}

inline std::exception_ptr StaleReadError() { return std::make_exception_ptr(StaleReadException()); }

// The one policy for reads against a blob that may change underneath us, shared by the synchronous
// (DownloadWithRetryAsync) and asynchronous (ReadAsync) paths so they cannot drift apart: a 412 from the
// ETag-conditioned download means "stale", and each refresh-and-retry spends one unit of a fixed budget.
namespace StaleReadPolicy {
inline bool IsStale(const RequestFailedException& e) { return e.StatusCode == HttpStatus::PreconditionFailed; }

// Spends one refresh from `left`; false means the budget is exhausted and the read must fail with StaleRead.
inline bool TrySpendRefresh(int& left) { return left-- > 0; }
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
    int64_t maxPrefetchBytes)
    : m_h(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger)), m_size(0),
      m_maxPrefetchBytes(maxPrefetchBytes), m_asyncReads(std::move(asyncReads)) {
    m_prefetch.Budget = std::move(prefetchBudget);
    if (m_h.Runtime) {
        auto metadata = BlobOperations::GetCachedMetadata(*m_h.Runtime, *m_h.Blob);
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
// length is clamped. False means there is nothing to read.
template <typename TBlob> bool BasicReadableFileImpl<TBlob>::ClampRead(const int64_t offset, int64_t& bytesToRead) {
    if (offset >= 0 && !RangeCheck::IsValid(offset, bytesToRead) && bytesToRead > 0) {
        bytesToRead = RangeCheck::MaxOffset - offset;
    }
    return offset >= 0 && bytesToRead > 0;
}

template <typename TBlob>
int64_t BasicReadableFileImpl<TBlob>::RandomRead(const int64_t offset, int64_t bytesToRead, char* buffer) const {
    if (!ClampRead(offset, bytesToRead)) {
        return 0;
    }

    // The cache is blocking disk I/O, so it is consulted here on the caller's thread rather than inside the coroutine.
    if (m_h.FileCache) {
        const auto bytesRead = m_h.FileCache->ReadFile(m_h.Name, offset, bytesToRead, buffer);
        if (bytesRead) {
            return static_cast<int64_t>(*bytesRead);
        }
    }

    // Non-owning: Block returns only after the coroutine has finished, so `this` outlives every use of the pointer.
    const std::shared_ptr<const BasicReadableFileImpl> borrowed(std::shared_ptr<void>{}, this);
    return Block(*m_h.Blob, RandomReadAsync(borrowed, offset, bytesToRead, buffer));
}

// Prefetch (waiting for one that is in flight), then download whatever it did not cover. Never touches the file cache.
template <typename TBlob>
BlobOperations::Task<int64_t>
BasicReadableFileImpl<TBlob>::RandomReadAsync(std::shared_ptr<const BasicReadableFileImpl> self, const int64_t offset,
                                              int64_t bytesToRead, char* const buffer) {
    if (!ClampRead(offset, bytesToRead)) {
        co_return 0;
    }

    int64_t served = 0;
    // Prefetched bytes belong to the version cached at this point; if the download below ends up on another
    // version the copied prefix is stale and the whole range must be read again.
    const auto prefetchVersion = self->m_metadataVersion.load(std::memory_order_acquire);
    co_await self->WaitForPrefetchAsync(offset);
    if (const auto prefetched = self->TryReadFromPrefetch(offset, bytesToRead, buffer, true)) {
        served = static_cast<int64_t>(*prefetched);
        if (served >= bytesToRead) {
            co_return served;
        }
    }

    // Only the part the prefetch did not cover is downloaded. The suffix and the whole-range re-read share one
    // stale-read budget so a single read cannot spend it twice under churn.
    int refreshesLeft = kMaxStaleReadRetries;
    auto bytesRead =
        co_await self->DownloadWithRetryAsync(offset + served, bytesToRead - served, buffer + served, refreshesLeft);
    if (served > 0 && self->m_metadataVersion.load(std::memory_order_acquire) != prefetchVersion) {
        served = 0;
        bytesRead = co_await self->DownloadWithRetryAsync(offset, bytesToRead, buffer, refreshesLeft);
    }
    co_return served + std::max<int64_t>(bytesRead, 0);
}

// Waits until the pending prefetch covering `offset` (if any) settles, bounded so a stuck download degrades to a normal
// read instead of a hang. Returning does not promise the data is there: the caller re-checks and falls back.
template <typename TBlob>
BlobOperations::Task<void> BasicReadableFileImpl<TBlob>::WaitForPrefetchAsync(const int64_t offset) const {
    const auto executor = co_await boost::asio::this_coro::executor;
    auto settled = std::make_shared<PrefetchSettledChannel>(executor, 1);
    bool registered = false;
    try {
        std::scoped_lock lock(m_prefetch.Mutex);
        const auto slot = std::ranges::find_if(m_prefetch.Slots, [&](const PrefetchSlot& candidate) {
            return candidate.Pending && offset >= candidate.Offset && offset < candidate.Offset + candidate.Length;
        });
        if (slot != m_prefetch.Slots.end()) {
            std::move_only_function<void()> wake = [settled] { settled->try_send(boost::system::error_code{}); };
            m_prefetch.AddWaiter(slot->Id, wake);
            registered = true;
        }
    } catch (...) {
        // Out of memory for the waiter: read without waiting rather than failing the read.
    }
    if (!registered) {
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

template <typename TBlob> bool BasicReadableFileImpl<TBlob>::HasETag(const std::string_view etag) const {
    std::scoped_lock lock(m_metadataMutex);
    return m_etag == etag;
}

template <typename TBlob> int64_t BasicReadableFileImpl<TBlob>::GetSize() const {
    {
        std::scoped_lock lock(m_metadataMutex);
        const auto now = m_h.Runtime ? m_h.Runtime->MetadataNow() : std::chrono::steady_clock::now();
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
    bool changed = false;
    {
        std::scoped_lock lock(m_metadataMutex);
        changed = m_etag != etag;
        m_size = size;
        if (changed) {
            m_metadataVersion.fetch_add(1, std::memory_order_release);
        }
        m_etag = std::move(etag);
        m_metadataFetchedAt = m_h.Runtime ? m_h.Runtime->MetadataNow() : std::chrono::steady_clock::now();
    }
    if (changed) {
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
                slot->Pending = false;
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
bool BasicReadableFileImpl<TBlob>::Prefetch(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                            const int64_t offset, int64_t n) {
    if (offset < 0 || n <= 0) {
        return true;
    }

    // Clamp so that offset + n below cannot overflow int64_t.
    n = std::min({n, kMaxPrefetchBytes, RangeCheck::MaxOffset - offset});
    if (n <= 0) {
        return true;
    }
    auto* const state = &self->m_prefetch;
    uint64_t id = 0;
    std::shared_ptr<PrefetchCharge> charge;
    {
        std::scoped_lock lock(state->Mutex);
        for (const auto& slot : state->Slots) {
            if (offset >= slot.Offset && offset + n <= slot.Offset + slot.Length) {
                return true; // Already (being) fetched.
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
            victim = std::ranges::find_if(state->Slots, [](const auto& slot) { return !slot.Pending; });
            if (victim == state->Slots.end()) {
                return false;
            }
        }

        if (charge) {
            // The download buffer is a whole size class, not just the requested length.
            const auto bytes = static_cast<int64_t>(DownloadBufferPool::CapacityFor(static_cast<size_t>(n)));
            // The victim only frees its charge if nothing else (a reader) still holds its buffer.
            const int64_t freed = victim != state->Slots.end() && victim->Charge && victim->Charge.use_count() == 1
                                      ? victim->Charge->Bytes
                                      : 0;
            if (state->Budget->fetch_add(bytes) + bytes - freed > self->m_maxPrefetchBytes) {
                state->Budget->fetch_sub(bytes);
                return false;
            }
            charge->Budget = state->Budget;
            charge->Bytes = bytes;
        }
        if (victim != state->Slots.end()) {
            state->Drop(victim);
        }

        id = ++state->NextId;
        state->Slots.push_back(PrefetchSlot{
            .Id = id, .Offset = offset, .Length = n, .Pending = true, .Charge = charge, .Owner = {}, .Data = {}});
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
    return true;
}

// Only a completed prefetch is served; a pending one is skipped, so this never blocks. RandomReadAsync waits for a
// pending prefetch first (WaitForPrefetchAsync).
template <typename TBlob>
std::optional<size_t> BasicReadableFileImpl<TBlob>::TryReadFromPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                                        char* buffer, const bool allowPartial,
                                                                        uint64_t* const deferredRetire) const {
    if (offset < 0 || bytesToRead <= 0) {
        return std::nullopt;
    }

    auto& state = m_prefetch;
    std::unique_lock lock(state.Mutex);
    const auto slot = std::ranges::find_if(state.Slots, [&](const PrefetchSlot& candidate) {
        const auto end = candidate.Offset + candidate.Length;
        return offset >= candidate.Offset && offset < end && (allowPartial || offset + bytesToRead <= end);
    });
    if (slot == state.Slots.end() || slot->Pending) {
        return std::nullopt;
    }

    const auto start = static_cast<size_t>(offset - slot->Offset);
    if (start >= slot->Data.size()) {
        return std::nullopt;
    }
    const auto available = slot->Data.size() - start;
    auto length = static_cast<size_t>(bytesToRead);
    if (length > available) {
        if (!allowPartial) {
            return std::nullopt;
        }
        length = available;
    }

    // Published data is immutable, so copy it outside the lock; `owner` keeps the buffer alive meanwhile.
    const auto id = slot->Id;
    const auto owner = slot->Owner;
    const auto charge = slot->Charge; // keeps the budget charged while the buffer is being read
    const auto source = slot->Data.data() + start;
    const bool reachedEnd = offset + bytesToRead >= slot->Offset + slot->Length;
    lock.unlock();

    std::copy_n(source, length, buffer);
    // Reading up to or past the end of the range means the scan has moved on; free the memory. A batch reader defers
    // that, because its other members may still need this range.
    if (reachedEnd && deferredRetire) {
        *deferredRetire = id;
    } else if (reachedEnd) {
        std::scoped_lock relock(state.Mutex);
        if (const auto current = std::ranges::find(state.Slots, id, &PrefetchSlot::Id); current != state.Slots.end()) {
            state.Drop(current);
        }
    }
    return length;
}

template <typename TBlob>
bool BasicReadableFileImpl<TBlob>::HasPendingPrefetch(const int64_t offset, const int64_t bytesToRead) const {
    if (offset < 0 || bytesToRead <= 0) {
        return false;
    }

    std::scoped_lock lock(m_prefetch.Mutex);
    return std::ranges::any_of(m_prefetch.Slots, [&](const PrefetchSlot& candidate) {
        return candidate.Pending && offset >= candidate.Offset &&
               offset + bytesToRead <= candidate.Offset + candidate.Length;
    });
}

template <typename TBlob>
bool BasicReadableFileImpl<TBlob>::ChainOntoPendingPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                            std::move_only_function<void()>& resume) const {
    if (offset < 0 || bytesToRead <= 0) {
        return false;
    }

    std::scoped_lock lock(m_prefetch.Mutex);
    const auto slot = std::ranges::find_if(m_prefetch.Slots, [&](const PrefetchSlot& candidate) {
        return candidate.Pending && offset >= candidate.Offset &&
               offset + bytesToRead <= candidate.Offset + candidate.Length;
    });
    if (slot == m_prefetch.Slots.end()) {
        return false;
    }
    m_prefetch.AddWaiter(slot->Id, resume);
    return true;
}

template <typename TBlob>
std::optional<size_t> BasicReadableFileImpl<TBlob>::TryReadFromCache(const int64_t offset, const int64_t bytesToRead,
                                                                     char* buffer) const {
    if (!m_h.FileCache || offset < 0 || bytesToRead <= 0) {
        return TryReadFromPrefetch(offset, bytesToRead, buffer);
    }

    if (const auto cached = m_h.FileCache->ReadFile(m_h.Name, offset, bytesToRead, buffer)) {
        return cached;
    }
    return TryReadFromPrefetch(offset, bytesToRead, buffer);
}

template <typename TBlob>
std::optional<size_t>
BasicReadableFileImpl<TBlob>::TryReadFromCompletedPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                           char* buffer, uint64_t* const deferredRetire) const {
    return TryReadFromPrefetch(offset, bytesToRead, buffer, false, deferredRetire);
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

// Async counterpart of DownloadWithRetry, written as straight-line code: nothing blocks a thread, every wait is a
// co_await. The deadline is checked before each step and bounds each wait, so a short caller timeout cannot be
// stretched by restarts. Failures are thrown rather than reported through a status.
template <typename TBlob>
BlobOperations::Task<typename BasicReadableFileImpl<TBlob>::ReadResult>
BasicReadableFileImpl<TBlob>::ReadAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self, const int64_t offset,
                                        const int64_t bytesToRead, int attemptsLeft,
                                        const std::chrono::milliseconds timeout, const std::stop_token stopToken) {
    const auto deadline = ReadableFileDetail::DeadlineAfter(timeout);
    for (;;) {
        if (offset < 0 || bytesToRead <= 0) {
            co_return ReadResult{};
        }
        // An abandoned read must not start another metadata call, download or restart.
        if (stopToken.stop_requested()) {
            std::rethrow_exception(ReadableFileDetail::ReadAbandonedError());
        }
        if (ReadableFileDetail::Expired(deadline)) {
            std::rethrow_exception(ReadableFileDetail::ReadTimedOutError());
        }

        auto [size, etag] = self->GetMetadata();
        const auto remaining = std::max<int64_t>(0, size - offset);
        if (remaining == 0) {
            // At the cached end of the blob: only re-read if another writer moved the blob on.
            auto latest = co_await FetchMetadataAsync(self, deadline);
            if (latest.ETag == etag) {
                self->ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), latest.Epoch);
                co_return ReadResult{};
            }
            if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(attemptsLeft)) {
                throw ReadableFileDetail::StaleReadException();
            }
            self->ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), latest.Epoch);
            continue;
        }

        const auto toRead = std::min(bytesToRead, remaining);
        // co_await may not appear in a handler, so a stale read only leaves the try block and is refreshed below.
        try {
            auto bytes = co_await BlobOperations::DownloadForReadAsync(*self->m_h.Blob, offset, toRead, std::move(etag),
                                                                       ReadableFileDetail::Remaining(deadline));
            if (static_cast<int64_t>(bytes.Data.size()) > remaining) {
                bytes.Data = bytes.Data.substr(0, static_cast<size_t>(remaining));
            }
            co_return ReadResult{bytes.Data, std::move(bytes.Owner)};
        } catch (const RequestFailedException& error) {
            if (!ReadableFileDetail::StaleReadPolicy::IsStale(error)) {
                throw;
            }
        }

        if (stopToken.stop_requested()) {
            std::rethrow_exception(ReadableFileDetail::ReadAbandonedError());
        }
        if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(attemptsLeft)) {
            throw ReadableFileDetail::StaleReadException();
        }
        // Skip the properties call when the budget is already spent; the restart would only time out afterwards.
        if (ReadableFileDetail::Expired(deadline)) {
            std::rethrow_exception(ReadableFileDetail::ReadTimedOutError());
        }
        auto fresh = co_await FetchMetadataAsync(self, deadline);
        self->ApplyRefreshedMetadata(fresh.Size, std::move(fresh.ETag), fresh.Epoch);
    }
}

// Waits for fresh blob metadata. The first reader to ask starts the properties call; readers arriving while it is in
// flight queue behind it (see JoinMetadataRefresh). The wait ends with a TimedOut error once `deadline` passes, but
// the shared call is never cancelled on behalf of one reader that gave up.
template <typename TBlob>
BlobOperations::Task<typename BasicReadableFileImpl<TBlob>::FetchedMetadata>
BasicReadableFileImpl<TBlob>::FetchMetadataAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> file,
                                                 const ReadDeadline deadline) {
    const auto executor = co_await boost::asio::this_coro::executor;
    auto waiter = std::make_shared<MetadataChannel>(executor, 1);
    JoinMetadataRefresh(file, waiter);

    std::exception_ptr error;
    FetchedMetadata fetched;
    if (!deadline) {
        std::tie(error, fetched) = co_await waiter->async_receive(boost::asio::use_awaitable);
    } else {
        using namespace boost::asio::experimental::awaitable_operators;
        boost::asio::steady_timer timer(executor, *deadline);
        auto outcome = co_await (waiter->async_receive(boost::asio::use_awaitable) ||
                                 timer.async_wait(boost::asio::use_awaitable));
        if (outcome.index() != 0) {
            std::rethrow_exception(ReadableFileDetail::ReadTimedOutError());
        }
        std::tie(error, fetched) = std::move(std::get<0>(outcome));
    }
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
        if (state.InFlight) {
            return;
        }
        state.InFlight = true;
    }
    // Initiation can throw (allocation, client construction, fenced runtime). Without this, InFlight would stay set
    // forever and every later caller would queue behind a request that never completes.
    try {
        // The completion needs the state, which is part of the file, so it holds the file until the waiters are taken;
        // it lets go before running them so the file is not kept alive past the reads that wait on it.
        const auto epoch = file->m_h.Runtime ? file->m_h.Runtime->BlobSizeEpoch() : 0;
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
        refresh.InFlight = false;
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
// read retried, so readers observe data appended by other writers. `refreshesLeft` is the caller's per-read budget.
// The caller keeps the file, `buffer` and `refreshesLeft` alive until the coroutine finishes.
template <typename TBlob>
BlobOperations::Task<int64_t>
BasicReadableFileImpl<TBlob>::DownloadWithRetryAsync(const int64_t offset, const int64_t bytesToRead,
                                                     char* const buffer, int& refreshesLeft) const {
    for (;;) {
        auto [size, etag] = GetMetadata();
        const auto remaining = std::max<int64_t>(0, size - offset);
        if (remaining == 0) {
            // One properties call serves both the ETag check and the refresh, so a stale end-of-blob read costs a
            // single round trip.
            const auto epoch = m_h.Runtime ? m_h.Runtime->BlobSizeEpoch() : 0;
            auto latest = co_await BlobOperations::GetMetadataAsync(*m_h.Blob);
            const bool changed = latest.ETag != etag;
            if (changed && !ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft)) {
                throw ReadableFileDetail::StaleReadException();
            }
            ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), epoch);
            if (!changed) {
                co_return 0;
            }
            continue;
        }

        const auto toRead = std::min(bytesToRead, remaining);
        const std::string usedEtag = etag;
        // co_await may not appear in a handler, so a stale read only leaves the try block and is refreshed below.
        try {
            const auto bytesRead = co_await BlobOperations::DownloadAsync(
                *m_h.Blob, std::span<char>(buffer, static_cast<size_t>(toRead)), offset, toRead, std::move(etag));
            co_return std::min(bytesRead, remaining);
        } catch (const RequestFailedException& ex) {
            if (!ReadableFileDetail::StaleReadPolicy::IsStale(ex)) {
                throw;
            }
        }

        if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft)) {
            throw ReadableFileDetail::StaleReadException();
        }
        // Another reader may already have refreshed past the version this request used; retry with it instead of
        // sending a redundant properties request.
        if (HasETag(usedEtag)) {
            co_await RefreshBlobMetadataAsync();
        }
    }
}

template <typename TBlob> BlobOperations::Task<void> BasicReadableFileImpl<TBlob>::RefreshBlobMetadataAsync() const {
    // Query outside the lock so concurrent readers are not serialized behind network round trips. A refresh means the
    // cached entry was stale, so it is replaced for the next open too.
    const auto epoch = m_h.Runtime ? m_h.Runtime->BlobSizeEpoch() : 0;
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
        m_h.Runtime->CacheBlobMetadata(m_h.Blob->ResourceUrl(), size, etag, epoch);
    }
    SetMetadata(size, std::move(etag));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
