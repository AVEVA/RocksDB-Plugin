// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Recycles the buffers async reads download into, so a steady stream of small reads does not allocate per read.
///
/// Buffers are kept in power-of-two size classes: a request is served from the smallest class that fits, so a buffer
/// is only reused for reads of a similar size and a big buffer is never pinned by a small read. Fresh buffers are
/// allocated uninitialised (the download overwrites them).
///
/// The download target is deliberately not RocksDB's scratch buffer: an abandoned read may have its scratch freed
/// while the transfer is still writing, so the bytes land in a pooled buffer that the completion owns and are copied
/// into scratch only if the read is still live (see AsyncReadRequest::Complete). That one copy remains by design.
/// </summary>
class DownloadBufferPool : public std::enable_shared_from_this<DownloadBufferPool> {
  public:
    // Buffers above this size are not retained, so one large read cannot pin memory for the process lifetime.
    static constexpr std::size_t MaxPooledBufferBytes = 4U * 1024U * 1024U;
    // Smallest size class; smaller requests share it.
    static constexpr std::size_t MinBufferBytes = 512;
    // Buffers retained per size class.
    static constexpr std::size_t MaxPooledBuffersPerClass = 8;

    /// <summary>A buffer on loan from the pool; it returns itself when the last owner lets go.</summary>
    class Lease {
      public:
        Lease(std::shared_ptr<DownloadBufferPool> pool, std::unique_ptr<char[]> buffer, std::size_t capacity,
              std::size_t size)
            : m_pool(std::move(pool)), m_buffer(std::move(buffer)), m_capacity(capacity), m_size(size) {}
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        ~Lease() { m_pool->Return(std::move(m_buffer), m_capacity); }

        [[nodiscard]] char* Data() noexcept { return m_buffer.get(); }
        [[nodiscard]] std::size_t Size() const noexcept { return m_size; }

      private:
        std::shared_ptr<DownloadBufferPool> m_pool;
        std::unique_ptr<char[]> m_buffer;
        std::size_t m_capacity;
        std::size_t m_size;
    };

    /// <summary>The process-wide pool. Leases keep it alive, so they may outlive any caller.</summary>
    static std::shared_ptr<DownloadBufferPool> Shared() {
        static const auto pool = std::make_shared<DownloadBufferPool>();
        return pool;
    }

    /// <summary>A buffer of exactly `size` bytes; its contents are unspecified.</summary>
    [[nodiscard]] std::shared_ptr<Lease> Acquire(const std::size_t size) {
        const std::size_t capacity = size > MaxPooledBufferBytes ? size : std::bit_ceil(std::max(size, MinBufferBytes));
        std::unique_ptr<char[]> buffer;
        if (capacity <= MaxPooledBufferBytes) {
            std::scoped_lock lock(m_mutex);
            auto& free = m_free[ClassOf(capacity)];
            if (!free.empty()) {
                buffer = std::move(free.back());
                free.pop_back();
            }
        }
        if (!buffer) {
            buffer = std::make_unique_for_overwrite<char[]>(capacity);
        }
        return std::make_shared<Lease>(shared_from_this(), std::move(buffer), capacity, size);
    }

    /// <summary>Buffers currently held for reuse (for tests and diagnostics).</summary>
    [[nodiscard]] std::size_t PooledCount() const {
        std::scoped_lock lock(m_mutex);
        std::size_t count = 0;
        for (const auto& free : m_free) {
            count += free.size();
        }
        return count;
    }

  private:
    static constexpr std::size_t ClassCount = std::bit_width(MaxPooledBufferBytes / MinBufferBytes);

    // Index of the power-of-two capacity within [MinBufferBytes, MaxPooledBufferBytes].
    static constexpr std::size_t ClassOf(const std::size_t capacity) {
        return static_cast<std::size_t>(std::countr_zero(capacity / MinBufferBytes));
    }

    void Return(std::unique_ptr<char[]> buffer, const std::size_t capacity) {
        if (capacity > MaxPooledBufferBytes) {
            return;
        }
        std::scoped_lock lock(m_mutex);
        if (auto& free = m_free[ClassOf(capacity)]; free.size() < MaxPooledBuffersPerClass) {
            free.push_back(std::move(buffer));
        }
    }

    mutable std::mutex m_mutex;
    std::array<std::vector<std::unique_ptr<char[]>>, ClassCount> m_free;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
