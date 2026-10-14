// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Definitions of BasicReadableFileImpl; included at the end of ReadableFileImpl.hpp.
#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <algorithm>
#include <boost/log/trivial.hpp>
#include <cassert>
#include <chrono>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// In a named namespace (not an anonymous one) so every TU that includes this header shares one definition of these.
namespace ReadableFileDetail {
inline RequestFailedException StaleReadException() {
    return RequestFailedException(static_cast<unsigned int>(HttpStatus::PreconditionFailed), "StaleRead",
                                  "The blob kept changing while it was being read; retries exhausted", "", {});
}

inline std::exception_ptr StaleReadError() { return std::make_exception_ptr(StaleReadException()); }

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
template <typename TBlob>
void FinishRead(std::shared_ptr<const BasicReadableFileImpl<TBlob>>& self, ReadCallback& callback,
                std::exception_ptr error, std::string_view data) {
    auto done = std::exchange(callback, nullptr);
    self.reset();
    done(std::move(error), data);
}
} // namespace ReadableFileDetail

template <typename TBlob>
BasicReadableFileImpl<TBlob>::BasicReadableFileImpl(
    std::string_view name, std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TBlob> blob,
    std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::shared_ptr<AsyncReadTracker> asyncReads, std::shared_ptr<std::atomic<int64_t>> prefetchBudget)
    : m_h(name, std::move(runtime), std::move(blob), std::move(fileCache), std::move(logger)),
      m_metadataMutex(std::make_unique<std::mutex>()), m_size(0), m_asyncReads(std::move(asyncReads)) {
    m_prefetch->Budget = std::move(prefetchBudget);
    auto metadata = BlobOperations::GetMetadata(*m_h.Blob);
    m_size = metadata.Size;
    m_etag = std::move(metadata.ETag);
    m_metadataFetchedAt = std::chrono::steady_clock::now();
}

template <typename TBlob>
int64_t BasicReadableFileImpl<TBlob>::RandomRead(const int64_t offset, const int64_t bytesToRead, char* buffer) const {
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
    if (const auto prefetched = TryReadFromPrefetch(offset, bytesToRead, buffer, true, true)) {
        served = static_cast<int64_t>(*prefetched);
        if (served >= bytesToRead) {
            return served;
        }
    }

    // Only the part the prefetch did not cover is downloaded.
    auto bytesRead = DownloadWithRetry(offset + served, bytesToRead - served, buffer + served);
    bytesRead = std::max<int64_t>(bytesRead, 0);

    return served + bytesRead;
}

template <typename TBlob> std::string BasicReadableFileImpl<TBlob>::GetETag() const { return GetMetadata().second; }

template <typename TBlob> bool BasicReadableFileImpl<TBlob>::HasETag(const std::string_view etag) const {
    std::scoped_lock lock(*m_metadataMutex);
    return m_etag == etag;
}

template <typename TBlob> int64_t BasicReadableFileImpl<TBlob>::GetSize() const {
    {
        std::scoped_lock lock(*m_metadataMutex);
        if (std::chrono::steady_clock::now() - m_metadataFetchedAt < Configuration::MetadataCacheTtl) {
            return m_size;
        }
    }
    RefreshBlobMetadata();

    return GetMetadata().first;
}

template <typename TBlob> std::pair<int64_t, std::string> BasicReadableFileImpl<TBlob>::GetMetadata() const {
    std::scoped_lock lock(*m_metadataMutex);
    return {m_size, m_etag};
}

// Updates the cached size/ETag together. Prefetched bytes belong to one blob version, so they are only discarded
// when the ETag shows the blob changed; a refresh that finds the same version keeps them.
template <typename TBlob> void BasicReadableFileImpl<TBlob>::SetMetadata(const int64_t size, std::string etag) const {
    BOOST_LOG_SEV(*m_h.Logger, boost::log::trivial::debug)
        << "Blob metadata refreshed for file '" << m_h.Name << "' :size = " << size << " bytes, etag = " << etag;
    bool changed = false;
    {
        std::scoped_lock lock(*m_metadataMutex);
        changed = m_etag != etag;
        m_size = size;
        m_etag = std::move(etag);
        m_metadataFetchedAt = std::chrono::steady_clock::now();
    }
    if (changed) {
        ClearPrefetch();
    }
}

template <typename TBlob> BasicReadableFileImpl<TBlob>::PrefetchState::~PrefetchState() { DropAll(); }

template <typename TBlob>
void BasicReadableFileImpl<TBlob>::PrefetchState::Drop(const std::vector<PrefetchSlot>::iterator slot) {
    if (Budget) {
        Budget->fetch_sub(slot->Length);
    }
    Slots.erase(slot);
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::PrefetchState::DropAll() {
    while (!Slots.empty()) {
        Drop(Slots.begin());
    }
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::ClearPrefetch() const {
    {
        std::scoped_lock lock(m_prefetch->Mutex);
        m_prefetch->DropAll();
    }
    m_prefetch->Done.notify_all();
}

template <typename TBlob>
bool BasicReadableFileImpl<TBlob>::Prefetch(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                            const int64_t offset, int64_t n) {
    if (offset < 0 || n <= 0) {
        return true;
    }

    n = std::min(n, kMaxPrefetchBytes);
    auto state = self->m_prefetch;
    uint64_t id = 0;
    {
        std::scoped_lock lock(state->Mutex);
        for (const auto& slot : state->Slots) {
            if (offset >= slot.Offset && offset + n <= slot.Offset + slot.Length) {
                return true; // Already (being) fetched.
            }
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

        if (state->Budget) {
            const int64_t freed = victim != state->Slots.end() ? victim->Length : 0;
            if (state->Budget->fetch_add(n) + n - freed > kDefaultPrefetchBudgetBytes) {
                state->Budget->fetch_sub(n);
                return false;
            }
        }
        if (victim != state->Slots.end()) {
            state->Drop(victim);
        }

        id = ++state->NextId;
        state->Slots.push_back(PrefetchSlot{.Id = id, .Offset = offset, .Length = n, .Pending = true, .Data = {}});
    }

    // A superseded or cleared prefetch (its slot is gone) must not publish its bytes.
    auto logger = self->m_h.Logger;
    auto name = self->m_h.Name;
    try {
        ReadAsync(std::move(self), offset, n,
                  [state, id, logger = std::move(logger), name = std::move(name)](std::exception_ptr error,
                                                                                  std::string_view data) {
                      std::vector<std::move_only_function<void()>> waiters;
                      {
                          std::scoped_lock lock(state->Mutex);
                          const auto slot = std::ranges::find(state->Slots, id, &PrefetchSlot::Id);
                          if (slot != state->Slots.end()) {
                              if (error) {
                                  state->Drop(slot);
                              } else {
                                  slot->Pending = false;
                                  slot->Data.assign(data);
                              }
                          }
                          for (auto it = state->Waiters.begin(); it != state->Waiters.end();) {
                              if (it->first == id) {
                                  waiters.push_back(std::move(it->second));
                                  it = state->Waiters.erase(it);
                              } else {
                                  ++it;
                              }
                          }
                      }
                      state->Done.notify_all();
                      // Waiters retry their read, which now finds the data or falls back to a download.
                      for (auto& waiter : waiters) {
                          waiter();
                      }
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
        {
            std::scoped_lock lock(state->Mutex);
            if (const auto slot = std::ranges::find(state->Slots, id, &PrefetchSlot::Id); slot != state->Slots.end()) {
                state->Drop(slot);
            }
        }
        state->Done.notify_all();
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
                                                                        const bool allowPartial) const {
    if (offset < 0 || bytesToRead <= 0) {
        return std::nullopt;
    }

    auto& state = *m_prefetch;
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

    std::copy_n(slot->Data.data() + start, length, buffer);
    // Reading up to or past the end of the range means the scan has moved on; free the memory.
    if (offset + bytesToRead >= slot->Offset + slot->Length) {
        state.Drop(slot);
    }
    return length;
}

template <typename TBlob>
bool BasicReadableFileImpl<TBlob>::ChainOntoPendingPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                            std::move_only_function<void()>& resume) const {
    if (offset < 0 || bytesToRead <= 0) {
        return false;
    }

    std::scoped_lock lock(m_prefetch->Mutex);
    const auto slot = std::ranges::find_if(m_prefetch->Slots, [&](const PrefetchSlot& candidate) {
        return candidate.Pending && offset >= candidate.Offset &&
               offset + bytesToRead <= candidate.Offset + candidate.Length;
    });
    if (slot == m_prefetch->Slots.end()) {
        return false;
    }
    m_prefetch->Waiters.emplace_back(slot->Id, std::move(resume));
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
void BasicReadableFileImpl<TBlob>::ReadAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                             const int64_t offset, const int64_t bytesToRead, ReadCallback callback,
                                             const int attemptsLeft, const std::chrono::milliseconds timeout) {
    if (self->m_asyncReads) {
        callback = [callback = std::move(callback), token = self->m_asyncReads->Begin()](
                       std::exception_ptr error, std::string_view data) { callback(std::move(error), data); };
    }
    ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), attemptsLeft, timeout);
}

// Async counterpart of DownloadWithRetry: every step (ETag check, conditional download, metadata refresh) is chained
// through completion callbacks so no thread ever blocks on the io_context.
template <typename TBlob>
void BasicReadableFileImpl<TBlob>::ReadAsyncAttempt(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                                    const int64_t offset, const int64_t bytesToRead,
                                                    ReadCallback callback, const int attemptsLeft,
                                                    const std::chrono::milliseconds timeout) {
    if (offset < 0 || bytesToRead <= 0) {
        ReadableFileDetail::FinishRead(self, callback, nullptr, {});
        return;
    }

    const auto [size, etag] = self->GetMetadata();
    const auto remaining = std::max<int64_t>(0, size - offset);
    if (remaining == 0) {
        // At the cached end of the blob: only re-read if another writer moved the blob on.
        const auto& file = *self;
        CoalescedMetadataAsync(
            file, [self = std::move(self), offset, bytesToRead, callback = std::move(callback), etag, attemptsLeft,
                   timeout](std::exception_ptr error, int64_t latestSize, std::string latestEtag) mutable {
                if (error) {
                    ReadableFileDetail::FinishRead(self, callback, error, {});
                    return;
                }
                if (latestEtag == etag) {
                    ReadableFileDetail::FinishRead(self, callback, nullptr, {});
                    return;
                }

                auto left = attemptsLeft;
                if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(left)) {
                    ReadableFileDetail::FinishRead(self, callback, ReadableFileDetail::StaleReadError(), {});
                    return;
                }
                self->SetMetadata(latestSize, std::move(latestEtag));
                ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), left, timeout);
            });
        return;
    }

    const auto toRead = std::min(bytesToRead, remaining);
    auto& blob = *self->m_h.Blob;
    BlobOperations::DownloadAsync(
        blob, offset, toRead, etag, timeout,
        [self = std::move(self), offset, bytesToRead, remaining, callback = std::move(callback), attemptsLeft,
         timeout](std::exception_ptr error, unsigned int status, std::string_view data) mutable {
            if (error) {
                if (status == HttpStatus::PreconditionFailed) {
                    auto left = attemptsLeft;
                    if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(left)) {
                        ReadableFileDetail::FinishRead(self, callback, ReadableFileDetail::StaleReadError(), {});
                        return;
                    }
                    RefreshMetadataAndReadAsync(std::move(self), offset, bytesToRead, std::move(callback), left,
                                                timeout);
                    return;
                }
                ReadableFileDetail::FinishRead(self, callback, error, {});
                return;
            }

            if (static_cast<int64_t>(data.size()) > remaining) {
                data = data.substr(0, static_cast<size_t>(remaining));
            }
            ReadableFileDetail::FinishRead(self, callback, nullptr, data);
        });
}

template <typename TBlob>
void BasicReadableFileImpl<TBlob>::RefreshMetadataAndReadAsync(std::shared_ptr<const BasicReadableFileImpl<TBlob>> self,
                                                               const int64_t offset, const int64_t bytesToRead,
                                                               ReadCallback callback, const int attemptsLeft,
                                                               const std::chrono::milliseconds timeout) {
    const auto& file = *self;
    CoalescedMetadataAsync(file, [self = std::move(self), offset, bytesToRead, callback = std::move(callback),
                                  attemptsLeft,
                                  timeout](std::exception_ptr error, int64_t size, std::string etag) mutable {
        if (error) {
            ReadableFileDetail::FinishRead(self, callback, error, {});
            return;
        }

        self->SetMetadata(size, std::move(etag));
        ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), attemptsLeft, timeout);
    });
}

// The first caller starts the properties call; callers arriving while it is in flight just queue behind it. A joiner
// may thus receive metadata fetched slightly before it asked; if that is already stale its read fails the ETag
// condition again and is retried against its own refresh budget.
template <typename TBlob>
void BasicReadableFileImpl<TBlob>::CoalescedMetadataAsync(
    const BasicReadableFileImpl& file, typename BasicReadableFileImpl<TBlob>::MetadataCallback callback) {
    auto state = file.m_metadataRefresh;
    {
        std::scoped_lock lock(state->Mutex);
        state->Waiters.push_back(std::move(callback));
        if (state->InFlight) {
            return;
        }
        state->InFlight = true;
    }
    // Initiation can throw (allocation, client construction, fenced runtime). Without this, InFlight would stay set
    // forever and every later caller would queue behind a request that never completes.
    try {
        BlobOperations::GetMetadataAsync(
            *file.m_h.Blob, [state](std::exception_ptr error, int64_t size, std::string etag) {
                std::vector<typename BasicReadableFileImpl<TBlob>::MetadataCallback> waiters;
                {
                    std::scoped_lock lock(state->Mutex);
                    waiters.swap(state->Waiters);
                    state->InFlight = false;
                }
                for (auto& waiter : waiters) {
                    waiter(error, size, etag);
                }
            });
    } catch (...) {
        const auto error = std::current_exception();
        std::vector<typename BasicReadableFileImpl<TBlob>::MetadataCallback> waiters;
        {
            std::scoped_lock lock(state->Mutex);
            waiters.swap(state->Waiters);
            state->InFlight = false;
        }
        for (auto& waiter : waiters) {
            waiter(error, 0, std::string());
        }
    }
}

// Downloads from the blob conditioned on the cached ETag. When the blob changed underneath us (precondition
// failure, or reading at the cached end of a blob whose ETag moved on) the metadata is refreshed and the read
// retried, so readers observe data appended by other writers.
template <typename TBlob>
int64_t BasicReadableFileImpl<TBlob>::DownloadWithRetry(const int64_t offset, const int64_t bytesToRead,
                                                        char* buffer) const {
    int64_t bytesRead = 0;

    bool success = false;
    int refreshesLeft = kMaxStaleReadRetries;
    do {
        const auto [size, etag] = GetMetadata();
        auto remaining = std::max<int64_t>(0, size - offset);
        if (remaining == 0) {
            // One properties call serves both the ETag check and the refresh, so a stale end-of-blob read costs a
            // single round trip.
            auto latest = BlobOperations::GetMetadata(*m_h.Blob);
            if (latest.ETag != etag) {
                if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft)) {
                    throw ReadableFileDetail::StaleReadException();
                }
                SetMetadata(latest.Size, std::move(latest.ETag));
                continue;
            }

            return 0;
        }

        auto toRead = std::min(bytesToRead, remaining);
        try {
            bytesRead = BlobOperations::Download(*m_h.Blob, std::span<char>(buffer, static_cast<size_t>(toRead)),
                                                 offset, toRead, etag);
            bytesRead = std::min(bytesRead, remaining);
            success = true;
        } catch (const RequestFailedException& ex) {
            if (ReadableFileDetail::StaleReadPolicy::IsStale(ex)) {
                if (!ReadableFileDetail::StaleReadPolicy::TrySpendRefresh(refreshesLeft)) {
                    throw ReadableFileDetail::StaleReadException();
                }
                RefreshBlobMetadata();
            } else {
                throw;
            }
        }
    } while (!success);

    return bytesRead;
}

template <typename TBlob> void BasicReadableFileImpl<TBlob>::RefreshBlobMetadata() const {
    // Query outside the lock so concurrent readers are not serialized behind network round trips.
    auto metadata = BlobOperations::GetMetadata(*m_h.Blob);
    SetMetadata(metadata.Size, std::move(metadata.ETag));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
