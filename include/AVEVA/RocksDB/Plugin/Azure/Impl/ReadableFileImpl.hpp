// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AsyncReadTracker.hpp"
#include "AVEVA/RocksDB/Plugin/Core/BlobClient.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <boost/log/trivial.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class ReadableFileImpl {
    std::string m_name;
    std::shared_ptr<Core::BlobClient> m_blobClient;
    std::shared_ptr<Core::FileCache> m_fileCache;
    int64_t m_offset;
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
    struct PrefetchState {
        std::mutex Mutex;
        std::condition_variable Done;
        uint64_t Generation = 0;
        int64_t Offset = 0;
        int64_t Length = 0;
        bool Pending = false;
        std::string Data;
    };
    std::shared_ptr<PrefetchState> m_prefetch = std::make_shared<PrefetchState>();

    [[nodiscard]] std::optional<size_t> TryReadFromPrefetch(int64_t offset, int64_t bytesToRead, char* buffer) const;
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
        std::shared_ptr<AsyncReadTracker> asyncReads = nullptr);

    // NOTE: Increments m_offset
    [[nodiscard]] int64_t SequentialRead(int64_t bytesToRead, char* buffer);

    // NOTE: Random so doesn't affect the sequential reads
    [[nodiscard]] int64_t RandomRead(int64_t offset, int64_t bytesToRead, char* buffer) const;

    // Serves a random read from the local file cache only; returns nullopt when it must go to the blob.
    [[nodiscard]] std::optional<size_t> TryReadFromCache(int64_t offset, int64_t bytesToRead, char* buffer) const;

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
    // Later reads fully inside that range are served from it, waiting for the download if it is still running.
    // Only the latest range is kept, and it is dropped when the blob's metadata changes.
    static void Prefetch(std::shared_ptr<const ReadableFileImpl> self, int64_t offset, int64_t n);

    int64_t GetOffset() const;
    void Skip(int64_t n);
    int64_t GetSize() const;
    void RefreshBlobMetadata() const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
