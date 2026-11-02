// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>

namespace AVEVA::Private {

/// <summary>
/// A bump arena over an inline buffer with heap overflow, for the short-lived Beast header fields of one
/// request or response. Individual deallocations are no-ops; everything is released by Reset(), so the arena
/// must outlive (or be reset only after the destruction of) every container allocating from it.
/// </summary>
class HeaderArena {
  public:
    HeaderArena() = default;
    HeaderArena(const HeaderArena&) = delete;
    HeaderArena& operator=(const HeaderArena&) = delete;
    ~HeaderArena() { Reset(); }

    /// <summary>
    /// Returns storage for <paramref name="bytes"/> bytes aligned to <paramref name="alignment"/>.
    /// </summary>
    /// <exception cref="std::bad_alloc">The heap overflow allocation failed.</exception>
    [[nodiscard]] void* Allocate(std::size_t bytes, std::size_t alignment) {
        if (void* const inlineSlot = TryBump(m_inline, InlineCapacity, m_used, bytes, alignment)) {
            return inlineSlot;
        }
        // Overflow blocks are sized for the request, with room to align within them.
        const auto total = sizeof(Block) + bytes + alignment;
        auto* const block = static_cast<Block*>(::operator new(total));
        block->next = m_overflow;
        m_overflow = block;
        std::size_t used = 0;
        auto* const data = reinterpret_cast<std::byte*>(block) + sizeof(Block);
        return TryBump(data, bytes + alignment, used, bytes, alignment);
    }

    /// <summary>
    /// Releases all storage handed out so far. No container allocating from the arena may be alive.
    /// </summary>
    void Reset() noexcept {
        while (m_overflow != nullptr) {
            Block* const next = m_overflow->next;
            ::operator delete(m_overflow);
            m_overflow = next;
        }
        m_used = 0;
    }

  private:
    struct Block {
        Block* next;
    };

    static constexpr std::size_t InlineCapacity = 4096;

    static void* TryBump(std::byte* base, std::size_t capacity, std::size_t& used, std::size_t bytes,
                         std::size_t alignment) noexcept {
        const auto start = reinterpret_cast<std::uintptr_t>(base) + used;
        const auto aligned = (start + alignment - 1) & ~(static_cast<std::uintptr_t>(alignment) - 1);
        const auto offset = used + (aligned - start);
        if (offset > capacity || bytes > capacity - offset) {
            return nullptr;
        }
        used = offset + bytes;
        return base + offset;
    }

    alignas(std::max_align_t) std::byte m_inline[InlineCapacity];
    std::size_t m_used = 0;
    Block* m_overflow = nullptr;
};

/// <summary>
/// A concrete, non-polymorphic allocator drawing from a <see cref="HeaderArena"/>.
/// </summary>
template <typename T> class ArenaAllocator {
  public:
    using value_type = T;

    explicit ArenaAllocator(HeaderArena& arena) noexcept : m_arena(&arena) {}
    template <typename U> ArenaAllocator(const ArenaAllocator<U>& other) noexcept : m_arena(other.Arena()) {}

    [[nodiscard]] T* allocate(std::size_t count) {
        return static_cast<T*>(m_arena->Allocate(count * sizeof(T), alignof(T)));
    }
    void deallocate(T*, std::size_t) noexcept {}

    [[nodiscard]] HeaderArena* Arena() const noexcept { return m_arena; }

    template <typename U> bool operator==(const ArenaAllocator<U>& other) const noexcept {
        return m_arena == other.Arena();
    }

  private:
    HeaderArena* m_arena;
};

} // namespace AVEVA::Private
