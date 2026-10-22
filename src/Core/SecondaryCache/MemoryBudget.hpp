// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache {
/// <summary>
/// Thrown when a MemoryBudget would exceed its limit. Derives from std::bad_alloc so standard containers propagate
/// it like an allocation failure; callers catch it first to turn budget pressure into a counted drop.
/// </summary>
class BudgetExceeded final : public std::bad_alloc {};

/// <summary>
/// Atomic byte counter that enforces a memory limit. It counts bytes requested through BudgetAllocator, not
/// heap overhead, so it is an accounting bound rather than an exact RSS bound.
/// </summary>
struct MemoryBudget {
    /// Maximum bytes that may be acquired. Not atomic: synchronize changes against Acquire and all other readers.
    size_t limit;
    /// Bytes currently acquired.
    std::atomic<size_t> used{0};

    /// <summary>Atomically reserves bytes, or reserves nothing and throws.</summary>
    /// <param name="bytes">Bytes to reserve.</param>
    /// <exception cref="BudgetExceeded">used + bytes would exceed limit.</exception>
    void Acquire(const size_t bytes) {
        size_t current = used.load(std::memory_order_relaxed);
        do {
            if (bytes > limit || current > limit - bytes) {
                throw BudgetExceeded();
            }
        } while (!used.compare_exchange_weak(current, current + bytes, std::memory_order_relaxed));
    }

    /// <summary>Returns bytes previously reserved with Acquire.</summary>
    /// <param name="bytes">Bytes to release; must not exceed the outstanding reservation.</param>
    void Release(const size_t bytes) noexcept { used.fetch_sub(bytes, std::memory_order_relaxed); }
};

/// <summary>
/// Standard allocator that charges every request to a MemoryBudget before delegating to std::allocator. It counts
/// actual allocator requests, including the overlap while a container grows or rehashes. Copies (including rebinds)
/// share the budget and compare equal.
/// </summary>
template <typename T> struct BudgetAllocator {
    using value_type = T;
    /// Non-owning; must outlive every container using this allocator.
    MemoryBudget* budget;
    explicit BudgetAllocator(MemoryBudget& source) noexcept : budget(&source) {}
    template <typename U> BudgetAllocator(const BudgetAllocator<U>& other) noexcept : budget(other.budget) {}

    /// <summary>Charges the budget, then allocates; the charge is rolled back if allocation fails.</summary>
    /// <param name="count">Number of T objects.</param>
    /// <returns>Uninitialized storage for count objects.</returns>
    /// <exception cref="BudgetExceeded">The budget is exhausted or count * sizeof(T) overflows.</exception>
    /// <exception cref="std::bad_alloc">The heap allocation failed.</exception>
    T* allocate(const size_t count) {
        if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
            throw BudgetExceeded();
        }
        const size_t bytes = count * sizeof(T);
        budget->Acquire(bytes);
        try {
            return std::allocator<T>{}.allocate(count);
        } catch (...) {
            budget->Release(bytes);
            throw;
        }
    }

    /// <summary>Frees storage from allocate and releases its charge.</summary>
    /// <param name="pointer">Storage returned by allocate.</param>
    /// <param name="count">The count passed to allocate.</param>
    void deallocate(T* pointer, const size_t count) noexcept {
        std::allocator<T>{}.deallocate(pointer, count);
        budget->Release(count * sizeof(T));
    }
    template <typename U> bool operator==(const BudgetAllocator<U>& other) const noexcept {
        return budget == other.budget;
    }
};

/// Budget-charged byte buffer for record copies.
using RecordBytes = std::vector<std::byte, BudgetAllocator<std::byte>>;
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
