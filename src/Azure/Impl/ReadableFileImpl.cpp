// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"

#include <boost/log/trivial.hpp>

#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include <cassert>
#include <chrono>
#include <utility>

using namespace boost::log::trivial;
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
RequestFailedException StaleReadException() {
    return RequestFailedException(static_cast<unsigned int>(HttpStatus::PreconditionFailed), "StaleRead",
                                  "The blob kept changing while it was being read; retries exhausted", "", {});
}

std::exception_ptr StaleReadError() { return std::make_exception_ptr(StaleReadException()); }

// Ends an async read. The file is released before the callback runs and the callback (which may hold the read's
// AsyncReadTracker token) is released last, so the filesystem cannot finish draining while this completion still
// holds references into it.
void FinishRead(std::shared_ptr<const ReadableFileImpl>& self, ReadableFileImpl::ReadCallback& callback,
                std::exception_ptr error, std::string data) {
    auto done = std::exchange(callback, nullptr);
    self.reset();
    done(std::move(error), std::move(data));
}
} // namespace
ReadableFileImpl::ReadableFileImpl(
    std::string_view name, std::shared_ptr<Core::BlobClient> blobClient, std::shared_ptr<Core::FileCache> fileCache,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::shared_ptr<AsyncReadTracker> asyncReads)
    : m_name(name), m_blobClient(std::move(blobClient)), m_fileCache(std::move(fileCache)), m_offset(0),
      m_metadataMutex(std::make_unique<std::mutex>()), m_size(0), m_logger(std::move(logger)),
      m_asyncReads(std::move(asyncReads)) {
    auto metadata = m_blobClient->GetMetadata();
    m_size = metadata.Size;
    m_etag = std::move(metadata.ETag);
}

int64_t ReadableFileImpl::SequentialRead(const int64_t bytesToRead, char* buffer) {
    if (bytesToRead <= 0) {
        return 0;
    }

    if (m_fileCache) {
        const auto bytesRead = m_fileCache->ReadFile(m_name, m_offset, bytesToRead, buffer);
        if (bytesRead) {
            m_offset += static_cast<int64_t>(*bytesRead);
            return static_cast<int64_t>(*bytesRead);
        }
    }

    assert(GetMetadata().first >= m_offset && "m_size needs to be bigger than m_offset or else we will overflow");

    auto bytesRead = DownloadWithRetry(m_offset, bytesToRead, buffer);
    bytesRead = std::max<int64_t>(bytesRead, 0);

    m_offset += bytesRead;
    return bytesRead;
}

int64_t ReadableFileImpl::RandomRead(const int64_t offset, const int64_t bytesToRead, char* buffer) const {
    if (offset < 0 || bytesToRead <= 0) {
        return 0;
    }

    if (m_fileCache) {
        const auto bytesRead = m_fileCache->ReadFile(m_name, offset, bytesToRead, buffer);
        if (bytesRead) {
            return static_cast<int64_t>(*bytesRead);
        }
    }

    if (const auto prefetched = TryReadFromPrefetch(offset, bytesToRead, buffer)) {
        return static_cast<int64_t>(*prefetched);
    }

    auto bytesRead = DownloadWithRetry(offset, bytesToRead, buffer);
    bytesRead = std::max<int64_t>(bytesRead, 0);

    return bytesRead;
}

int64_t ReadableFileImpl::GetOffset() const { return m_offset; }

void ReadableFileImpl::Skip(const int64_t n) { m_offset += n; }

int64_t ReadableFileImpl::GetSize() const {
    RefreshBlobMetadata();

    return GetMetadata().first;
}

std::pair<int64_t, std::string> ReadableFileImpl::GetMetadata() const {
    std::scoped_lock lock(*m_metadataMutex);
    return {m_size, m_etag};
}

void ReadableFileImpl::SetMetadata(const int64_t size, std::string etag) const {
    BOOST_LOG_SEV(*m_logger, debug) << "Blob metadata refreshed for file '" << m_name << "' :size = " << size
                                    << " bytes, etag = " << etag;
    {
        std::scoped_lock lock(*m_metadataMutex);
        m_size = size;
        m_etag = std::move(etag);
    }
    // The blob changed, so previously prefetched bytes may be stale.
    ClearPrefetch();
}

void ReadableFileImpl::ClearPrefetch() const {
    {
        std::scoped_lock lock(m_prefetch->Mutex);
        ++m_prefetch->Generation;
        m_prefetch->Length = 0;
        m_prefetch->Pending = false;
        m_prefetch->Data.clear();
    }
    m_prefetch->Done.notify_all();
}

void ReadableFileImpl::Prefetch(std::shared_ptr<const ReadableFileImpl> self, const int64_t offset, int64_t n) {
    if (offset < 0 || n <= 0) {
        return;
    }

    n = std::min(n, kMaxPrefetchBytes);
    auto state = self->m_prefetch;
    uint64_t generation = 0;
    {
        std::scoped_lock lock(state->Mutex);
        generation = ++state->Generation;
        state->Offset = offset;
        state->Length = n;
        state->Pending = true;
        state->Data.clear();
    }
    state->Done.notify_all();

    // A superseded or cleared prefetch (different generation) must not publish its bytes.
    ReadAsync(std::move(self), offset, n, [state, generation](std::exception_ptr error, std::string data) {
        {
            std::scoped_lock lock(state->Mutex);
            if (state->Generation != generation) {
                return;
            }
            state->Pending = false;
            if (error) {
                state->Length = 0;
            } else {
                state->Data = std::move(data);
            }
        }
        state->Done.notify_all();
    });
}

// Runs on RocksDB threads only (never the io_context), so waiting for an in-flight prefetch is safe. The wait is
// bounded so a stuck download degrades to a normal read instead of a hang.
std::optional<size_t> ReadableFileImpl::TryReadFromPrefetch(const int64_t offset, const int64_t bytesToRead,
                                                            char* buffer) const {
    if (offset < 0 || bytesToRead <= 0) {
        return std::nullopt;
    }

    auto& state = *m_prefetch;
    std::unique_lock lock(state.Mutex);
    const auto covers = [&] {
        return state.Length > 0 && offset >= state.Offset && offset + bytesToRead <= state.Offset + state.Length;
    };
    if (!covers()) {
        return std::nullopt;
    }

    if (state.Pending && !state.Done.wait_for(lock, std::chrono::seconds(30), [&] { return !state.Pending; })) {
        return std::nullopt;
    }

    const auto start = static_cast<size_t>(offset - state.Offset);
    const auto length = static_cast<size_t>(bytesToRead);
    if (!covers() || start + length > state.Data.size()) {
        return std::nullopt;
    }

    std::copy_n(state.Data.data() + start, length, buffer);
    return length;
}

std::optional<size_t> ReadableFileImpl::TryReadFromCache(const int64_t offset, const int64_t bytesToRead,
                                                         char* buffer) const {
    if (!m_fileCache || offset < 0 || bytesToRead <= 0) {
        return TryReadFromPrefetch(offset, bytesToRead, buffer);
    }

    if (const auto cached = m_fileCache->ReadFile(m_name, offset, bytesToRead, buffer)) {
        return cached;
    }
    return TryReadFromPrefetch(offset, bytesToRead, buffer);
}

void ReadableFileImpl::ReadAsync(std::shared_ptr<const ReadableFileImpl> self, const int64_t offset,
                                 const int64_t bytesToRead, ReadCallback callback, const int attemptsLeft,
                                 const std::chrono::milliseconds timeout) {
    if (self->m_asyncReads) {
        callback = [callback = std::move(callback), token = self->m_asyncReads->Begin()](
                       std::exception_ptr error, std::string data) { callback(std::move(error), std::move(data)); };
    }
    ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), attemptsLeft, timeout);
}

// Async counterpart of DownloadWithRetry: every step (ETag check, conditional download, metadata refresh) is chained
// through completion callbacks so no thread ever blocks on the io_context.
void ReadableFileImpl::ReadAsyncAttempt(std::shared_ptr<const ReadableFileImpl> self, const int64_t offset,
                                        const int64_t bytesToRead, ReadCallback callback, const int attemptsLeft,
                                        const std::chrono::milliseconds timeout) {
    if (offset < 0 || bytesToRead <= 0) {
        FinishRead(self, callback, nullptr, {});
        return;
    }

    const auto [size, etag] = self->GetMetadata();
    const auto remaining = std::max<int64_t>(0, size - offset);
    if (remaining == 0) {
        // At the cached end of the blob: only re-read if another writer moved the blob on.
        auto* blobClient = self->m_blobClient.get();
        blobClient->GetMetadataAsync(
            [self = std::move(self), offset, bytesToRead, callback = std::move(callback), etag, attemptsLeft,
             timeout](std::exception_ptr error, int64_t latestSize, std::string latestEtag) mutable {
                if (error) {
                    FinishRead(self, callback, error, {});
                    return;
                }
                if (latestEtag == etag) {
                    FinishRead(self, callback, nullptr, {});
                    return;
                }

                if (attemptsLeft <= 0) {
                    FinishRead(self, callback, StaleReadError(), {});
                    return;
                }
                self->SetMetadata(latestSize, std::move(latestEtag));
                ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), attemptsLeft - 1, timeout);
            });
        return;
    }

    const auto toRead = std::min(bytesToRead, remaining);
    auto* blobClient = self->m_blobClient.get();
    blobClient->DownloadAsync(offset, toRead, etag, timeout,
                              [self = std::move(self), offset, bytesToRead, remaining, callback = std::move(callback),
                               attemptsLeft, timeout](std::exception_ptr error, std::string data) mutable {
                                  if (error) {
                                      try {
                                          std::rethrow_exception(error);
                                      } catch (const RequestFailedException& ex) {
                                          if (ex.StatusCode == HttpStatus::PreconditionFailed) {
                                              if (attemptsLeft <= 0) {
                                                  FinishRead(self, callback, StaleReadError(), {});
                                                  return;
                                              }
                                              RefreshMetadataAndReadAsync(std::move(self), offset, bytesToRead,
                                                                          std::move(callback), attemptsLeft - 1,
                                                                          timeout);
                                              return;
                                          }
                                      } catch (...) {
                                      }
                                      FinishRead(self, callback, error, {});
                                      return;
                                  }

                                  if (static_cast<int64_t>(data.size()) > remaining) {
                                      data.resize(static_cast<size_t>(remaining));
                                  }
                                  FinishRead(self, callback, nullptr, std::move(data));
                              });
}

void ReadableFileImpl::RefreshMetadataAndReadAsync(std::shared_ptr<const ReadableFileImpl> self, const int64_t offset,
                                                   const int64_t bytesToRead,
                                                   Core::BlobClient::DownloadCallback callback, const int attemptsLeft,
                                                   const std::chrono::milliseconds timeout) {
    auto* blobClient = self->m_blobClient.get();
    blobClient->GetMetadataAsync([self = std::move(self), offset, bytesToRead, callback = std::move(callback),
                                  attemptsLeft,
                                  timeout](std::exception_ptr error, int64_t size, std::string etag) mutable {
        if (error) {
            FinishRead(self, callback, error, {});
            return;
        }

        self->SetMetadata(size, std::move(etag));
        ReadAsyncAttempt(std::move(self), offset, bytesToRead, std::move(callback), attemptsLeft, timeout);
    });
}

// Downloads from the blob conditioned on the cached ETag. When the blob changed underneath us (precondition
// failure, or reading at the cached end of a blob whose ETag moved on) the metadata is refreshed and the read
// retried, so readers observe data appended by other writers.
int64_t ReadableFileImpl::DownloadWithRetry(const int64_t offset, const int64_t bytesToRead, char* buffer) const {
    int64_t bytesRead = 0;

    bool success = false;
    int refreshesLeft = kMaxStaleReadRetries;
    do {
        const auto [size, etag] = GetMetadata();
        auto remaining = std::max<int64_t>(0, size - offset);
        if (remaining == 0) {
            auto latestEtag = m_blobClient->GetEtag();
            if (latestEtag != etag) {
                if (refreshesLeft-- <= 0) {
                    throw StaleReadException();
                }
                RefreshBlobMetadata();
                continue;
            }

            return 0;
        }

        auto toRead = std::min(bytesToRead, remaining);
        try {
            bytesRead =
                m_blobClient->Download(std::span<char>(buffer, static_cast<size_t>(toRead)), offset, toRead, etag);
            bytesRead = std::min(bytesRead, remaining);
            success = true;
        } catch (const RequestFailedException& ex) {
            if (ex.StatusCode == HttpStatus::PreconditionFailed) {
                if (refreshesLeft-- <= 0) {
                    throw StaleReadException();
                }
                RefreshBlobMetadata();
            } else {
                throw;
            }
        }
    } while (!success);

    return bytesRead;
}

void ReadableFileImpl::RefreshBlobMetadata() const {
    // Query outside the lock so concurrent readers are not serialized behind network round trips.
    auto metadata = m_blobClient->GetMetadata();
    SetMetadata(metadata.Size, std::move(metadata.ETag));
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
