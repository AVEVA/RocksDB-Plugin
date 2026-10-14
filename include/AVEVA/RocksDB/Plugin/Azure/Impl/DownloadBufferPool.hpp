// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Recycles the buffers async reads download into, so a steady stream of small reads does not allocate per read.
///
/// The download target is deliberately not RocksDB's scratch buffer: an abandoned read may have its scratch freed
/// while the transfer is still writing, so the bytes land in a pooled buffer that the completion owns and are copied
/// into scratch only if the read is still live (see AsyncReadRequest::Complete). That one copy remains by design.
/// </summary>
class DownloadBufferPool : public std::enable_shared_from_this<DownloadBufferPool> {
  public:
    // Buffers above this size are not retained, so one large read cannot pin memory for the process lifetime.
    static constexpr std::size_t MaxPooledBufferBytes = 4U * 1024U * 1024U;
    static constexpr std::size_t MaxPooledBuffers = 32;

    /// <summary>A buffer on loan from the pool; it returns itself when the last owner lets go.</summary>
    class Lease {
      public:
        Lease(std::shared_ptr<DownloadBufferPool> pool, std::vector<char> buffer)
            : m_pool(std::move(pool)), m_buffer(std::move(buffer)) {}
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        ~Lease() { m_pool->Return(std::move(m_buffer)); }

        [[nodiscard]] char* Data() noexcept { return m_buffer.data(); }
        [[nodiscard]] std::size_t Size() const noexcept { return m_buffer.size(); }

      private:
        std::shared_ptr<DownloadBufferPool> m_pool;
        std::vector<char> m_buffer;
    };

    /// <summary>The process-wide pool. Leases keep it alive, so they may outlive any caller.</summary>
    static std::shared_ptr<DownloadBufferPool> Shared() {
        static const auto pool = std::make_shared<DownloadBufferPool>();
        return pool;
    }

    /// <summary>A buffer of exactly `size` bytes; its contents are unspecified.</summary>
    [[nodiscard]] std::shared_ptr<Lease> Acquire(const std::size_t size) {
        std::vector<char> buffer;
        {
            std::scoped_lock lock(m_mutex);
            if (!m_free.empty()) {
                buffer = std::move(m_free.back());
                m_free.pop_back();
            }
        }
        // resize() on a recycled buffer only touches bytes beyond its previous size, so steady-state reads of a
        // similar size do no allocation and no zero-filling.
        buffer.resize(size);
        return std::make_shared<Lease>(shared_from_this(), std::move(buffer));
    }

    /// <summary>Buffers currently held for reuse (for tests and diagnostics).</summary>
    [[nodiscard]] std::size_t PooledCount() const {
        std::scoped_lock lock(m_mutex);
        return m_free.size();
    }

  private:
    void Return(std::vector<char> buffer) {
        if (buffer.capacity() > MaxPooledBufferBytes) {
            return;
        }
        std::scoped_lock lock(m_mutex);
        if (m_free.size() < MaxPooledBuffers) {
            m_free.push_back(std::move(buffer));
        }
    }

    mutable std::mutex m_mutex;
    std::vector<std::vector<char>> m_free;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
