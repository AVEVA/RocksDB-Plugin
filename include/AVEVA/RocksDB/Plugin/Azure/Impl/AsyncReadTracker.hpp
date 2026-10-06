// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/asio/any_io_executor.hpp>

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Counts asynchronous blob reads still running on the host io_context, so that BlobFilesystemImpl, which owns the
/// HTTP client and the file caches, can wait for them before releasing those.
///
/// A read that RocksDB abandons (handle deleted, AbortIO, file closed) keeps running until its download completes.
/// Without this, tearing the filesystem down mid-read would let that completion drop the last references to the
/// HTTP client (destroying it while it is still on the stack) or a FileCache (whose destructor joins a thread that
/// may itself be waiting on the same io_context).
/// </summary>
class AsyncReadTracker : public std::enable_shared_from_this<AsyncReadTracker> {
  public:
    /// <summary>
    /// Held by a read for as long as it runs; copies share one registration.
    /// </summary>
    using Token = std::shared_ptr<const void>;

    explicit AsyncReadTracker(boost::asio::any_io_executor executor);

    /// <summary>
    /// Registers a read. When the last copy of the token is released the read is ended from a fresh task on the
    /// executor, so the count only drops once the completion that released the token has fully unwound.
    /// </summary>
    [[nodiscard]] Token Begin();

    /// <summary>
    /// Blocks until every registered read has ended. Must not be called from a thread running the executor.
    /// </summary>
    void Drain();

    [[nodiscard]] size_t InFlight() const;

  private:
    struct Registration;

    void End() noexcept;

    boost::asio::any_io_executor m_executor;
    mutable std::mutex m_mutex;
    std::condition_variable m_idle;
    size_t m_inFlight = 0;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
