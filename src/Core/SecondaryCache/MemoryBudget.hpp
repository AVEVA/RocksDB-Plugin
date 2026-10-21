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
class BudgetExceeded final : public std::bad_alloc {};

struct MemoryBudget {
    size_t limit;
    std::atomic<size_t> used{0};

    void Acquire(const size_t bytes) {
        size_t current = used.load(std::memory_order_relaxed);
        do {
            if (bytes > limit || current > limit - bytes) {
                throw BudgetExceeded();
            }
        } while (!used.compare_exchange_weak(current, current + bytes, std::memory_order_relaxed));
    }
    void Release(const size_t bytes) noexcept { used.fetch_sub(bytes, std::memory_order_relaxed); }
};

// Counts actual allocator requests, including container growth/rehash overlap.
template <typename T> struct BudgetAllocator {
    using value_type = T;
    MemoryBudget* budget;
    explicit BudgetAllocator(MemoryBudget& source) noexcept : budget(&source) {}
    template <typename U> BudgetAllocator(const BudgetAllocator<U>& other) noexcept : budget(other.budget) {}

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
    void deallocate(T* pointer, const size_t count) noexcept {
        std::allocator<T>{}.deallocate(pointer, count);
        budget->Release(count * sizeof(T));
    }
    template <typename U> bool operator==(const BudgetAllocator<U>& other) const noexcept {
        return budget == other.budget;
    }
};

using RecordBytes = std::vector<std::byte, BudgetAllocator<std::byte>>;
} // namespace AVEVA::RocksDB::Plugin::Core::SecondaryCache
