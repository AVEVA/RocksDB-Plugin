// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <array>
#include <cstddef>
#include <memory_resource>

namespace AVEVA::Private {

/// <summary>
/// A bump arena over an inline buffer with heap overflow, for the short-lived Beast header fields of one
/// request or response. Individual deallocations are no-ops; everything is released by Reset(), so the arena
/// must outlive (or be reset only after the destruction of) every container allocating from it.
/// </summary>
class HeaderArena {
  public:
    HeaderArena() : m_resource(m_inline.data(), m_inline.size(), std::pmr::new_delete_resource()) {}
    HeaderArena(const HeaderArena&) = delete;
    HeaderArena& operator=(const HeaderArena&) = delete;
    HeaderArena(HeaderArena&&) = delete;
    HeaderArena& operator=(HeaderArena&&) = delete;
    ~HeaderArena() = default;

    /// <summary>
    /// Returns storage for <paramref name="bytes"/> bytes aligned to <paramref name="alignment"/>.
    /// </summary>
    /// <exception cref="std::bad_alloc">The heap overflow allocation failed.</exception>
    [[nodiscard]] void* Allocate(std::size_t bytes, std::size_t alignment) {
        return m_resource.allocate(bytes, alignment);
    }

    /// <summary>
    /// Releases all storage handed out so far and starts again from the inline buffer. No container allocating from
    /// the arena may be alive.
    /// </summary>
    void Reset() noexcept { m_resource.release(); }

  private:
    static constexpr std::size_t InlineCapacity = 4096;

    // Declared before the resource, which is constructed over it.
    alignas(std::max_align_t) std::array<std::byte, InlineCapacity> m_inline{};
    std::pmr::monotonic_buffer_resource m_resource;
};

/// <summary>
/// A concrete, non-polymorphic allocator drawing from a <see cref="HeaderArena"/>.
/// </summary>
template <typename T> class ArenaAllocator {
  public:
    using value_type = T;

    explicit ArenaAllocator(HeaderArena& arena) noexcept : m_arena(&arena) {}
    // Implicit on purpose: the Allocator requirements make containers rebind and convert allocators implicitly.
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
