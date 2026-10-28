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

// A metadata waiter and its deadline timer race to run the continuation; whichever claims it first wins.
template <typename TExecutor, typename TContinuation> struct MetadataGate {
    MetadataGate(const TExecutor& executor, TContinuation continuation)
        : Timer(executor), Continuation(std::move(continuation)) {}

    std::optional<TContinuation> Claim() {
        std::scoped_lock lock(Mutex);
        std::optional<TContinuation> claimed(std::move(Continuation));
        Continuation.reset();
        return claimed;
    }

    boost::asio::steady_timer Timer;
    std::mutex Mutex;
    std::optional<TContinuation> Continuation;
};

// The one policy for reads against a blob that may change underneath us, shared by the synchronous
// (DownloadWithRetry) and asynchronous (ReadAsyncAttempt) paths so they cannot drift apart: a 412 from the
// ETag-conditioned download means "stale", and each refresh-and-retry spends one unit of a fixed budget.
namespace StaleReadPolicy {
inline bool IsStale(const RequestFailedException& e) { return e.StatusCode == HttpStatus::PreconditionFailed; }

// Spends one refresh from `left`; false means the budget is exhausted and the read must fail with StaleRead.
inline bool TrySpendRefresh(int& left) { return left-- > 0; }
} // namespace StaleReadPolicy

// Ends an async read. The file is released before the callback runs and the callback (which may hold the read's
// AsyncReadTracker token) is released last, so the filesystem cannot finish draining while this completion still
// holds references into it.
template <typename TBlob, typename TCallback>
void FinishRead(std::shared_ptr<const BasicReadableFileImpl<TBlob>>& self, TCallback& callback,
                std::exception_ptr error, std::string_view data, std::shared_ptr<const void> owner = {}) {
    auto done = std::move(callback);
    self.reset();
    done(std::move(error), data, std::move(owner));
}

// Guarantees a read's completion runs exactly once. A read is restarted from inside completion handlers, where an
// initiation that throws destroys the callback unseen and the read would never finish (stranding its
// AsyncReadRequest and AsyncReadTracker token). Destroying an unused guard completes the read with an error instead;
// that is safe because the callback only completes the AsyncReadRequest, and RocksDB callbacks still run from Poll.
template <typename TCallback> class ReadCompletionGuard {
  public:
    explicit ReadCompletionGuard(TCallback callback) : m_callback(std::move(callback)) {}
    ReadCompletionGuard(const ReadCompletionGuard&) = delete;
    ReadCompletionGuard& operator=(const ReadCompletionGuard&) = delete;
    ReadCompletionGuard& operator=(ReadCompletionGuard&&) = delete;
    ReadCompletionGuard(ReadCompletionGuard&& other) noexcept(std::is_nothrow_move_constructible_v<TCallback>)
        : m_callback(std::move(other.m_callback)), m_pending(std::exchange(other.m_pending, false)) {}

    ~ReadCompletionGuard() noexcept {
        if (!m_pending) {
            return;
        }
        try {
            m_callback(std::make_exception_ptr(
                           RequestFailedException(0, "ReadAbandoned", "Read abandoned before it completed", "", {})),
                       std::string_view{}, std::shared_ptr<const void>{});
        } catch (...) {
            // Nothing left to report to: the callback only completes the request and must not throw.
        }
    }

    void operator()(std::exception_ptr error, std::string_view data, std::shared_ptr<const void> owner) {
        m_pending = false;
        m_callback(std::move(error), data, std::move(owner));
    }

  private:
    TCallback m_callback;
    bool m_pending = true;
};

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

// Same isolation for metadata-refresh waiters. A waiter that throws has been destroyed with its read's completion
// guard, which completes that read with an error, so only the log is left to do.
template <typename TWaiters, typename TLogger>
void RunMetadataWaiters(TWaiters& waiters, const std::shared_ptr<TLogger>& logger, const std::exception_ptr& error,
                        const int64_t size, const std::string& etag, const uint64_t epoch) noexcept {
    for (auto& waiter : waiters) {
        try {
            waiter(error, size, etag, epoch);
        } catch (...) {
            if (logger) {
                BOOST_LOG_SEV(*logger, boost::log::trivial::error) << "Metadata refresh waiter failed";
            }
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

template <typename TBlob>
int64_t BasicReadableFileImpl<TBlob>::RandomRead(const int64_t offset, int64_t bytesToRead, char* buffer) const {
    // Readahead past a saturated sequential position would overflow the end offset; nothing exists out there.
    if (offset >= 0 && !RangeCheck::IsValid(offset, bytesToRead) && bytesToRead > 0) {
        bytesToRead = RangeCheck::MaxOffset - offset;
    }
    if (offset < 0 || bytesToRead <= 0) {
        return 0;
    }

    if (m_h.FileCache) {
        const auto bytesRead = m_h.FileCache->ReadFile(m_h.Name, offset, bytesToRead, buffer);
        if (bytesRead) {
            return static_cast<int64_t>(*bytesRead);
        }
    }

    int64_t served = 0;
    // Prefetched bytes belong to the version cached at this point; if the download below ends up on another
    // version the copied prefix is stale and the whole range must be read again.
    const auto prefetchVersion = m_metadataVersion.load(std::memory_order_acquire);
    if (const auto prefetched = TryReadFromPrefetch(offset, bytesToRead, buffer, true, true)) {
        served = static_cast<int64_t>(*prefetched);
        if (served >= bytesToRead) {
            return served;
        }
    }

    // Only the part the prefetch did not cover is downloaded. The suffix and the whole-range re-read share one
    // stale-read budget so a single read cannot spend it twice under churn.
    int refreshesLeft = kMaxStaleReadRetries;
    auto bytesRead = DownloadWithRetry(offset + served, bytesToRead - served, buffer + served, refreshesLeft);
    if (served > 0 && m_metadataVersion.load(std::memory_order_acquire) != prefetchVersion) {
        served = 0;
        bytesRead = DownloadWithRetry(offset, bytesToRead, buffer, refreshesLeft);
    }
    bytesRead = std::max<int64_t>(bytesRead, 0);

    return served + bytesRead;
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
    {
        std::scoped_lock lock(m_prefetch.Mutex);
        m_prefetch.DropAll();
    }
    m_prefetch.Done.notify_all();
}

template <typename TBlob>
bool BasicReadableFileImpl<TBlob>::Prefetch(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                            const int64_t offset, int64_t n) {
    if (offset < 0 || n <= 0) {
        return true;
    }

    n = std::min(n, kMaxPrefetchBytes);
    auto* const state = &self->m_prefetch;
    uint64_t id = 0;
    std::string name;
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
        name = self->m_h.Name;
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
    // The completion needs the prefetch state, which is part of the file, so it keeps the file alive. The pointer is
    // taken before `self` moves into the read.
    auto keepAlive = self;
    try {
        ReadAsync(std::move(self), offset, n,
                  [state, id, logger, name = std::move(name), keepAlive, charge = std::move(charge)](
                      std::exception_ptr error, std::string_view data, std::shared_ptr<const void> owner) {
                      std::vector<std::move_only_function<void()>> waiters;
                      {
                          std::scoped_lock lock(state->Mutex);
                          // Detached first so nothing after this point can fail before the waiters are notified.
                          waiters = state->TakeWaiters(id);
                          const auto slot = std::ranges::find(state->Slots, id, &PrefetchSlot::Id);
                          if (slot != state->Slots.end()) {
                              if (error) {
                                  state->Drop(slot);
                              } else {
                                  slot->Pending = false;
                                  // A short read keeps its charge: the pooled buffer is still fully allocated.
                                  if (const auto got = static_cast<int64_t>(data.size()); got < slot->Length) {
                                      slot->Length = got;
                                  }
                                  // The slot keeps the pooled download buffer instead of copying up to 8 MiB out of it.
                                  slot->Owner = std::move(owner);
                                  slot->Data = data;
                              }
                          }
                      }
                      state->Done.notify_all();
                      // Waiters retry their read, which now finds the data or falls back to a download.
                      ReadableFileDetail::RunWaiters(waiters, *logger);
                      if (error) {
                          try {
                              std::rethrow_exception(error);
                          } catch (const std::exception& e) {
                              BOOST_LOG_SEV(*logger, boost::log::trivial::warning)
                                  << "Prefetch of '" << name << "' failed: " << e.what();
                          } catch (...) {
                              BOOST_LOG_SEV(*logger, boost::log::trivial::warning)
                                  << "Prefetch of '" << name << "' failed";
                          }
                      }
                  });
    } catch (...) {
        std::vector<std::move_only_function<void()>> waiters;
        {
            std::scoped_lock lock(state->Mutex);
            waiters = state->TakeWaiters(id);
            if (const auto slot = std::ranges::find(state->Slots, id, &PrefetchSlot::Id); slot != state->Slots.end()) {
                state->Drop(slot);
            }
        }
        state->Done.notify_all();
        // The slot is gone, so each waiter falls back to its own download rather than waiting forever.
        ReadableFileDetail::RunWaiters(waiters, *logger);
        throw;
    }
    return true;
}

// With `wait` this runs on RocksDB threads only (never the io_context), so waiting for an in-flight prefetch is safe.
// The wait is bounded so a stuck download degrades to a normal read instead of a hang. Without `wait` a pending
// prefetch is skipped so async callers never block.
template <typename TBlob>
std::optional<size_t> BasicReadableFileImpl<TBlob>::TryReadFromPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                                        char* buffer, const bool wait,
                                                                        const bool allowPartial,
                                                                        uint64_t* const deferredRetire) const {
    if (offset < 0 || bytesToRead <= 0) {
        return std::nullopt;
    }

    auto& state = m_prefetch;
    std::unique_lock lock(state.Mutex);
    const auto find = [&] {
        return std::ranges::find_if(state.Slots, [&](const PrefetchSlot& slot) {
            const auto end = slot.Offset + slot.Length;
            return offset >= slot.Offset && offset < end && (allowPartial || offset + bytesToRead <= end);
        });
    };
    auto slot = find();
    if (slot == state.Slots.end()) {
        return std::nullopt;
    }

    if (slot->Pending) {
        if (!wait) {
            return std::nullopt;
        }
        const auto id = slot->Id;
        const auto settled = state.Done.wait_for(lock, Configuration::PrefetchWaitTimeout, [&] {
            const auto current = std::ranges::find(state.Slots, id, &PrefetchSlot::Id);
            return current == state.Slots.end() || !current->Pending;
        });
        if (!settled) {
            BOOST_LOG_SEV(*m_h.Logger, boost::log::trivial::debug)
                << "Timed out waiting for a prefetch of '" << m_h.Name << "'; falling back to a download";
            return std::nullopt;
        }
        slot = find();
        if (slot == state.Slots.end()) {
            return std::nullopt;
        }
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
        return TryReadFromPrefetch(offset, bytesToRead, buffer, false);
    }

    if (const auto cached = m_h.FileCache->ReadFile(m_h.Name, offset, bytesToRead, buffer)) {
        return cached;
    }
    return TryReadFromPrefetch(offset, bytesToRead, buffer, false);
}

template <typename TBlob>
std::optional<size_t>
BasicReadableFileImpl<TBlob>::TryReadFromCompletedPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                           char* buffer, uint64_t* const deferredRetire) const {
    return TryReadFromPrefetch(offset, bytesToRead, buffer, false, false, deferredRetire);
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::RetirePrefetch(const uint64_t id) const {
    std::scoped_lock lock(m_prefetch.Mutex);
    if (const auto slot = std::ranges::find(m_prefetch.Slots, id, &PrefetchSlot::Id); slot != m_prefetch.Slots.end()) {
        m_prefetch.Drop(slot);
    }
}

template <typename TBlob>
template <ReadCompletion TCallback>
void BasicReadableFileImpl<TBlob>::ReadAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                             const int64_t offset, const int64_t bytesToRead, TCallback callback,
                                             const int attemptsLeft, const std::chrono::milliseconds timeout,
                                             std::stop_token stopToken) {
    ReadableFileDetail::ReadCompletionGuard<TCallback> guarded(std::move(callback));
    const auto deadline = ReadableFileDetail::DeadlineAfter(timeout);
    if (self->m_asyncReads) {
        // The token is declared first so it is released after the callback (and anything the callback holds).
        auto tracked = [token = self->m_asyncReads->Begin(), callback = std::move(guarded)](
                           std::exception_ptr error, std::string_view data, std::shared_ptr<const void> owner) mutable {
            callback(std::move(error), data, std::move(owner));
        };
        ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(tracked), attemptsLeft, deadline,
                         std::move(stopToken));
        return;
    }
    ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(guarded), attemptsLeft, deadline,
                     std::move(stopToken));
}

// Async counterpart of DownloadWithRetry: every step (ETag check, conditional download, metadata refresh) is chained
// through completion callbacks so no thread ever blocks on the io_context. The deadline is checked before each step
// and bounds each wait, so a short caller timeout cannot be stretched by restarts.
template <typename TBlob>
template <ReadCompletion TCallback>
void BasicReadableFileImpl<TBlob>::ReadAsyncAttempt(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                                    const int64_t offset, const int64_t bytesToRead, TCallback callback,
                                                    const int attemptsLeft, const ReadDeadline deadline,
                                                    std::stop_token stopToken) {
    if (offset < 0 || bytesToRead <= 0) {
        ReadableFileDetail::FinishRead(self, callback, nullptr, {});
        return;
    }
    // An abandoned read must not start another metadata call, download or restart.
    if (stopToken.stop_requested()) {
        ReadableFileDetail::FinishRead(self, callback, ReadableFileDetail::ReadAbandonedError(), {});
        return;
    }
    if (ReadableFileDetail::Expired(deadline)) {
        ReadableFileDetail::FinishRead(self, callback, ReadableFileDetail::ReadTimedOutError(), {});
        return;
    }

    auto [size, etag] = self->GetMetadata();
    const auto remaining = std::max<int64_t>(0, size - offset);
    if (remaining == 0) {
        // At the cached end of the blob: only re-read if another writer moved the blob on.
        const auto file = self;
        WaitForMetadataAsync(
            file, deadline,
            [self = std::move(self), offset, bytesToRead, callback = std::move(callback), etag = std::move(etag),
             attemptsLeft, deadline,
             stopToken](std::exception_ptr error, int64_t latestSize, std::string latestEtag, uint64_t epoch) mutable {
                if (error) {
                    ReadableFileDetail::FinishRead(self, callback, error, {});
                    return;
                }
                if (latestEtag == etag) {
                    self->ApplyRefreshedMetadata(latestSize, std::move(latestEtag), epoch);
                    ReadableFileDetail::FinishRead(self, callback, nullptr, {});
                    return;
                }

                auto left = attemptsLeft;
                if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(left)) {
                    ReadableFileDetail::FinishRead(self, callback, ReadableFileDetail::StaleReadError(), {});
                    return;
                }
                self->ApplyRefreshedMetadata(latestSize, std::move(latestEtag), epoch);
                ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), left, deadline,
                                 std::move(stopToken));
            });
        return;
    }

    const auto toRead = std::min(bytesToRead, remaining);
    auto& blob = *self->m_h.Blob;
    BlobOperations::DownloadAsync(
        blob, offset, toRead, std::move(etag), ReadableFileDetail::Remaining(deadline),
        [self = std::move(self), offset, bytesToRead, remaining, callback = std::move(callback), attemptsLeft, deadline,
         stopToken](std::exception_ptr error, unsigned int status, std::string_view data,
                    std::shared_ptr<const void> owner) mutable {
            if (error) {
                if (status == HttpStatus::PreconditionFailed && !stopToken.stop_requested()) {
                    auto left = attemptsLeft;
                    if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(left)) {
                        ReadableFileDetail::FinishRead(self, callback, ReadableFileDetail::StaleReadError(), {});
                        return;
                    }
                    // This runs on the host io_context, so nothing may escape; if the restart throws, its callback is
                    // destroyed unused and the completion guard fails the read.
                    const auto logger = self->m_h.Logger;
                    try {
                        RefreshMetadataAndReadAsync(std::move(self), offset, bytesToRead, std::move(callback), left,
                                                    deadline, std::move(stopToken));
                    } catch (...) {
                        if (logger) {
                            BOOST_LOG_SEV(*logger, boost::log::trivial::error) << "Restarting a stale read failed";
                        }
                    }
                    return;
                }
                ReadableFileDetail::FinishRead(self, callback, error, {});
                return;
            }

            if (static_cast<int64_t>(data.size()) > remaining) {
                data = data.substr(0, static_cast<size_t>(remaining));
            }
            ReadableFileDetail::FinishRead(self, callback, nullptr, data, std::move(owner));
        },
        stopToken);
}

template <typename TBlob>
template <ReadCompletion TCallback>
void BasicReadableFileImpl<TBlob>::RefreshMetadataAndReadAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                                               const int64_t offset, const int64_t bytesToRead,
                                                               TCallback callback, const int attemptsLeft,
                                                               const ReadDeadline deadline, std::stop_token stopToken) {
    // Skip the properties call when the budget is already spent; the restart would only time out afterwards.
    if (ReadableFileDetail::Expired(deadline)) {
        ReadableFileDetail::FinishRead(self, callback, ReadableFileDetail::ReadTimedOutError(), {});
        return;
    }
    const auto file = self;
    WaitForMetadataAsync(file, deadline,
                         [self = std::move(self), offset, bytesToRead, callback = std::move(callback), attemptsLeft,
                          deadline, stopToken = std::move(stopToken)](std::exception_ptr error, int64_t size,
                                                                      std::string etag, uint64_t epoch) mutable {
                             if (error) {
                                 ReadableFileDetail::FinishRead(self, callback, error, {});
                                 return;
                             }

                             self->ApplyRefreshedMetadata(size, std::move(etag), epoch);
                             ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), attemptsLeft,
                                              deadline, std::move(stopToken));
                         });
}

template <typename TBlob>
template <typename TContinuation>
void BasicReadableFileImpl<TBlob>::WaitForMetadataAsync(const std::shared_ptr<const BasicReadableFileImpl>& file,
                                                        const ReadDeadline& deadline, TContinuation continuation) {
    if (!deadline) {
        CoalescedMetadataAsync(file, std::move(continuation));
        return;
    }

    using Gate = ReadableFileDetail::MetadataGate<typename TBlob::executor_type, TContinuation>;
    const auto gate = std::make_shared<Gate>(file->ClientExecutor(), std::move(continuation));
    gate->Timer.expires_at(*deadline);
    gate->Timer.async_wait([gate, logger = file->m_h.Logger](const boost::system::error_code& cancelled) {
        if (cancelled) {
            return;
        }
        try {
            if (auto expired = gate->Claim()) {
                (*expired)(ReadableFileDetail::ReadTimedOutError(), 0, std::string(), 0);
            }
        } catch (...) {
            // Runs on the host io_context: the continuation's completion guard fails the read if it throws.
            if (logger) {
                BOOST_LOG_SEV(*logger, boost::log::trivial::error) << "Timing out a metadata wait failed";
            }
        }
    });
    CoalescedMetadataAsync(file, [gate](std::exception_ptr error, int64_t size, std::string etag, uint64_t epoch) {
        if (auto claimed = gate->Claim()) {
            gate->Timer.cancel();
            (*claimed)(std::move(error), size, std::move(etag), epoch);
        }
    });
}

// The first caller starts the properties call; callers arriving while it is in flight just queue behind it. A joiner
// may thus receive metadata fetched slightly before it asked; if that is already stale its read fails the ETag
// condition again and is retried against its own refresh budget.
template <typename TBlob>
void BasicReadableFileImpl<TBlob>::CoalescedMetadataAsync(
    const std::shared_ptr<const BasicReadableFileImpl>& file,
    typename BasicReadableFileImpl<TBlob>::MetadataCallback callback) {
    auto& state = file->m_metadataRefresh;
    {
        std::scoped_lock lock(state.Mutex);
        state.Waiters.push_back(std::move(callback));
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
        BlobOperations::GetMetadataAsync(
            *file->m_h.Blob, [file = file, epoch](std::exception_ptr error, int64_t size, std::string etag) mutable {
                std::vector<typename BasicReadableFileImpl<TBlob>::MetadataCallback> waiters;
                {
                    auto& refresh = file->m_metadataRefresh;
                    std::scoped_lock lock(refresh.Mutex);
                    waiters.swap(refresh.Waiters);
                    refresh.InFlight = false;
                }
                const auto logger = file->m_h.Logger;
                file.reset();
                ReadableFileDetail::RunMetadataWaiters(waiters, logger, error, size, etag, epoch);
            });
    } catch (...) {
        const auto error = std::current_exception();
        std::vector<typename BasicReadableFileImpl<TBlob>::MetadataCallback> waiters;
        {
            std::scoped_lock lock(state.Mutex);
            waiters.swap(state.Waiters);
            state.InFlight = false;
        }
        ReadableFileDetail::RunMetadataWaiters(waiters, file->m_h.Logger, error, 0, std::string(), 0);
    }
}

// Downloads from the blob conditioned on the cached ETag. When the blob changed underneath us (precondition
// failure, or reading at the cached end of a blob whose ETag moved on) the metadata is refreshed and the read
// retried, so readers observe data appended by other writers. `refreshesLeft` is the caller's per-read budget.
template <typename TBlob>
int64_t BasicReadableFileImpl<TBlob>::DownloadWithRetry(const int64_t offset, const int64_t bytesToRead, char* buffer,
                                                        int& refreshesLeft) const {
    int64_t bytesRead = 0;

    bool success = false;
    do {
        auto [size, etag] = GetMetadata();
        auto remaining = std::max<int64_t>(0, size - offset);
        if (remaining == 0) {
            // One properties call serves both the ETag check and the refresh, so a stale end-of-blob read costs a
            // single round trip.
            const auto epoch = m_h.Runtime ? m_h.Runtime->BlobSizeEpoch() : 0;
            auto latest = BlobOperations::GetMetadata(*m_h.Blob);
            if (latest.ETag != etag) {
                if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft)) {
                    throw ReadableFileDetail::StaleReadException();
                }
                ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), epoch);
                continue;
            }
            ApplyRefreshedMetadata(latest.Size, std::move(latest.ETag), epoch);

            return 0;
        }

        auto toRead = std::min(bytesToRead, remaining);
        const std::string usedEtag = etag;
        try {
            bytesRead = BlobOperations::Download(*m_h.Blob, std::span<char>(buffer, static_cast<size_t>(toRead)),
                                                 offset, toRead, std::move(etag));
            bytesRead = std::min(bytesRead, remaining);
            success = true;
        } catch (const RequestFailedException& ex) {
            if (ReadableFileDetail::StaleReadPolicy::IsStale(ex)) {
                if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft)) {
                    throw ReadableFileDetail::StaleReadException();
                }
                // Another thread may already have refreshed past the version this request used; retry with it
                // instead of sending a redundant properties request.
                if (HasETag(usedEtag)) {
                    RefreshBlobMetadata();
                }
            } else {
                throw;
            }
        }
    } while (!success);

    return bytesRead;
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::RefreshBlobMetadata() const {
    // Query outside the lock so concurrent readers are not serialized behind network round trips. A refresh means the
    // cached entry was stale, so it is replaced for the next open too.
    const auto epoch = m_h.Runtime ? m_h.Runtime->BlobSizeEpoch() : 0;
    auto metadata = BlobOperations::GetMetadata(*m_h.Blob);
    ApplyRefreshedMetadata(metadata.Size, std::move(metadata.ETag), epoch);
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
