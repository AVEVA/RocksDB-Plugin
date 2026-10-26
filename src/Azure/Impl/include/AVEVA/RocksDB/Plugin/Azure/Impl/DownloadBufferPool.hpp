// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/container/static_vector.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>Production hooks: none. Tests substitute a static type to pause a return at a precise point.</summary>
struct NoDownloadBufferPoolHooks {
    // Runs after a returned buffer is published and its bytes are accounted, with no lock held.
    static void AfterPublish() noexcept {}
};

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
template <typename Hooks = NoDownloadBufferPoolHooks>
class BasicDownloadBufferPool : public std::enable_shared_from_this<BasicDownloadBufferPool<Hooks>> {
  public:
    // Buffers above this size are not retained, so one large read cannot pin memory for the process lifetime.
    static constexpr std::size_t MaxPooledBufferBytes = 4U * 1024U * 1024U;
    // Smallest size class; smaller requests share it.
    static constexpr std::size_t MinBufferBytes = 512;
    // Buffers retained per size class.
    static constexpr std::size_t MaxPooledBuffersPerClass = 8;
    // Total bytes retained across every class; when exceeded the largest free buffers are released first.
    static constexpr std::size_t MaxPooledTotalBytes = 16U * 1024U * 1024U;

    /// <summary>A buffer on loan from the pool; it returns itself when the last owner lets go.</summary>
    class Lease {
      public:
        Lease(std::shared_ptr<BasicDownloadBufferPool> pool, std::unique_ptr<char[]> buffer, std::size_t capacity,
              std::size_t size)
            : m_pool(std::move(pool)), m_buffer(std::move(buffer)), m_capacity(capacity), m_size(size) {}
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        ~Lease() { m_pool->Return(std::move(m_buffer), m_capacity); }

        [[nodiscard]] char* Data() noexcept { return m_buffer.get(); }
        [[nodiscard]] std::size_t Size() const noexcept { return m_size; }

      private:
        std::shared_ptr<BasicDownloadBufferPool> m_pool;
        std::unique_ptr<char[]> m_buffer;
        std::size_t m_capacity;
        std::size_t m_size;
    };

    /// <summary>The process-wide pool. Leases keep it alive, so they may outlive any caller.</summary>
    static std::shared_ptr<BasicDownloadBufferPool> Shared() {
        static const auto pool = std::make_shared<BasicDownloadBufferPool>();
        return pool;
    }

    /// <summary>The bytes actually allocated for a request of `size`; callers budgeting memory should charge
    /// this.</summary>
    [[nodiscard]] static constexpr std::size_t CapacityFor(const std::size_t size) {
        return size > MaxPooledBufferBytes ? size : std::bit_ceil(std::max(size, MinBufferBytes));
    }

    /// <summary>A buffer of exactly `size` bytes; its contents are unspecified.</summary>
    [[nodiscard]] std::shared_ptr<Lease> Acquire(const std::size_t size) {
        const std::size_t capacity = CapacityFor(size);
        std::unique_ptr<char[]> buffer;
        if (capacity <= MaxPooledBufferBytes) {
            auto& sizeClass = m_classes[ClassOf(capacity)];
            std::scoped_lock lock(sizeClass.Mutex);
            if (!sizeClass.Free.empty()) {
                buffer = std::move(sizeClass.Free.back());
                sizeClass.Free.pop_back();
                m_pooledBytes.fetch_sub(capacity, std::memory_order_relaxed);
            }
        }
        if (!buffer) {
            buffer = std::make_unique_for_overwrite<char[]>(capacity);
        }
        return std::make_shared<Lease>(this->shared_from_this(), std::move(buffer), capacity, size);
    }

    /// <summary>Bytes currently held for reuse (for tests and diagnostics).</summary>
    [[nodiscard]] std::size_t PooledBytes() const { return m_pooledBytes.load(std::memory_order_relaxed); }

    /// <summary>Buffers currently held for reuse (for tests and diagnostics).</summary>
    [[nodiscard]] std::size_t PooledCount() const {
        std::size_t count = 0;
        for (const auto& sizeClass : m_classes) {
            std::scoped_lock lock(sizeClass.Mutex);
            count += sizeClass.Free.size();
        }
        return count;
    }

  private:
    static constexpr std::size_t ClassCount = std::bit_width(MaxPooledBufferBytes / MinBufferBytes);

// One lock per size class, on its own cache line, so threads reading different sizes never contend or false-share.
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4324) // padding is the point
#endif
    struct alignas(64) SizeClass {
        mutable std::mutex Mutex;
        boost::container::static_vector<std::unique_ptr<char[]>, MaxPooledBuffersPerClass> Free;
    };
#ifdef _MSC_VER
#pragma warning(pop)
#endif

    // Index of the power-of-two capacity within [MinBufferBytes, MaxPooledBufferBytes].
    static constexpr std::size_t ClassOf(const std::size_t capacity) {
        return static_cast<std::size_t>(std::countr_zero(capacity / MinBufferBytes));
    }

    // Called from ~Lease, so it must not throw: the free list has fixed capacity, so publishing never allocates.
    void Return(std::unique_ptr<char[]> buffer, const std::size_t capacity) noexcept {
        if (capacity > MaxPooledBufferBytes) {
            return;
        }
        std::size_t total = 0;
        {
            auto& sizeClass = m_classes[ClassOf(capacity)];
            std::scoped_lock lock(sizeClass.Mutex);
            if (sizeClass.Free.size() >= MaxPooledBuffersPerClass) {
                return;
            }
            sizeClass.Free.push_back(std::move(buffer));
            // Accounted in the same critical section that makes the buffer visible, so an Acquire that takes it
            // can never subtract bytes that were not yet added.
            total = m_pooledBytes.fetch_add(capacity, std::memory_order_relaxed) + capacity;
        }
        Hooks::AfterPublish();
        if (total > MaxPooledTotalBytes) {
            EvictLargestWhileOverCap();
        }
    }

    // Largest buffers go first: they free the most memory per eviction and are the costliest to keep for the rare big
    // read. Evicted buffers are collected under each class lock and freed after it is released, so a free never
    // stalls other threads. The batch is per class and fixed-capacity, so eviction never allocates.
    void EvictLargestWhileOverCap() noexcept {
        for (std::size_t index = ClassCount;
             index-- > 0 && m_pooledBytes.load(std::memory_order_relaxed) > MaxPooledTotalBytes;) {
            auto& sizeClass = m_classes[index];
            const std::size_t bytes = MinBufferBytes << index;
            // Declared before the lock so it is destroyed (freeing the buffers) after the lock is released.
            boost::container::static_vector<std::unique_ptr<char[]>, MaxPooledBuffersPerClass> evicted;
            std::scoped_lock lock(sizeClass.Mutex);
            while (!sizeClass.Free.empty() && m_pooledBytes.load(std::memory_order_relaxed) > MaxPooledTotalBytes) {
                evicted.push_back(std::move(sizeClass.Free.back()));
                sizeClass.Free.pop_back();
                m_pooledBytes.fetch_sub(bytes, std::memory_order_relaxed);
            }
        }
    }

    std::atomic<std::size_t> m_pooledBytes = 0;
    std::array<SizeClass, ClassCount> m_classes;
};

using DownloadBufferPool = BasicDownloadBufferPool<>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
