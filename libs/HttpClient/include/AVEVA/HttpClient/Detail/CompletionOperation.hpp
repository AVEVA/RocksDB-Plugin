// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <cassert>
#include <cstddef>
#include <memory>
#include <new>
#include <system_error>
#include <type_traits>
#include <utility>

namespace AVEVA::Detail {
/// <summary>
/// The one place a request's completion handler is erased: an intrusive, heap-allocated operation with a
/// pair of function pointers, the same technique Asio uses internally. Each request owns exactly one, so
/// the client can queue and carry requests of any handler type without a std::function per hop and without
/// itself being a template on the handler.
///
/// Not intended for direct use; <c>HttpClient::SendAsync</c> creates these.
/// </summary>
struct CompletionDeleter;
class CompletionOperation {
  public:
    CompletionOperation(const CompletionOperation&) = delete;
    CompletionOperation& operator=(const CompletionOperation&) = delete;

  protected:
    using CompleteFunction = void (*)(CompletionOperation*, std::error_code, HttpResponse);
    using DestroyFunction = void (*)(CompletionOperation*) noexcept;

    CompletionOperation(CompleteFunction complete, DestroyFunction destroy) noexcept
        : m_complete(complete), m_destroy(destroy) {}

    // Deliberately non-virtual and non-public: the owner always destroys through m_destroy.
    ~CompletionOperation() = default;

  private:
    friend struct CompletionDeleter;
    friend void Complete(std::unique_ptr<CompletionOperation, CompletionDeleter>, std::error_code, HttpResponse);

    /// <summary>
    /// Frees the operation and then invokes the handler with the outcome. The operation must not be used
    /// afterwards.
    /// </summary>
    /// <param name="error">The outcome of the request.</param>
    /// <param name="response">The response; empty on failure.</param>
    void Complete(std::error_code error, HttpResponse response) {
        assert(m_complete != nullptr);
        m_complete(this, error, std::move(response));
    }

    /// <summary>
    /// Frees the operation without invoking the handler. The operation must not be used afterwards.
    /// </summary>
    void Destroy() noexcept {
        assert(m_destroy != nullptr);
        m_destroy(this);
    }

    CompleteFunction m_complete;
    DestroyFunction m_destroy;
};

/// <summary>
/// Frees an unfinished operation, so one that is dropped without completing does not leak its handler.
/// </summary>
struct CompletionDeleter {
    void operator()(CompletionOperation* operation) const noexcept { operation->Destroy(); }
};

/// <summary>
/// Sole owner of a not-yet-completed operation.
/// </summary>
using CompletionPointer = std::unique_ptr<CompletionOperation, CompletionDeleter>;

/// <summary>
/// Completes and releases an operation. The handler is invoked after the operation's memory is freed, so
/// it may start the next operation from inside the call.
/// </summary>
/// <param name="operation">The operation to complete; consumed.</param>
/// <param name="error">The outcome of the request.</param>
/// <param name="response">The response; empty on failure.</param>
inline void Complete(CompletionPointer operation, std::error_code error, HttpResponse response) {
    operation.release()->Complete(error, std::move(response));
}

/// <summary>
/// A completion operation holding a callable, allocated through <typeparamref name="Allocator"/>.
/// </summary>
/// <typeparam name="Callable">Invoked as <c>(std::error_code, HttpResponse)</c> exactly once.</typeparam>
/// <typeparam name="Allocator">Rebound to allocate the operation itself.</typeparam>
template <typename Callable, typename Allocator> class CallableCompletionOperation final : public CompletionOperation {
    using OperationAllocator =
        typename std::allocator_traits<Allocator>::template rebind_alloc<CallableCompletionOperation>;
    using Traits = std::allocator_traits<OperationAllocator>;
    static_assert(std::is_pointer_v<typename Traits::pointer>, "Allocators with fancy pointers are not supported");

  public:
    /// <summary>
    /// Allocates an operation that owns the callable.
    /// </summary>
    /// <param name="callable">The callable to invoke on completion.</param>
    /// <param name="allocator">The allocator to take the operation's storage from.</param>
    /// <returns>The new operation.</returns>
    template <typename C> static CompletionPointer Create(C&& callable, const Allocator& allocator) {
        OperationAllocator operationAllocator(allocator);
        auto* storage = Traits::allocate(operationAllocator, 1);
        try {
            return CompletionPointer(::new (static_cast<void*>(storage))
                                         CallableCompletionOperation(std::forward<C>(callable), allocator));
        } catch (...) {
            Traits::deallocate(operationAllocator, storage, 1);
            throw;
        }
    }

  private:
    template <typename C>
    CallableCompletionOperation(C&& callable, const Allocator& allocator)
        : CompletionOperation(&CompleteThenFree, &Free), m_callable(std::forward<C>(callable)), m_allocator(allocator) {
    }

    static void Free(CompletionOperation* base) noexcept {
        auto* self = static_cast<CallableCompletionOperation*>(base);
        OperationAllocator operationAllocator(self->m_allocator);
        self->~CallableCompletionOperation();
        Traits::deallocate(operationAllocator, self, 1);
    }

    static void CompleteThenFree(CompletionOperation* base, std::error_code error, HttpResponse response) {
        auto* self = static_cast<CallableCompletionOperation*>(base);
        Callable callable(std::move(self->m_callable));
        Free(base);
        std::move(callable)(error, std::move(response));
    }

    Callable m_callable;
    Allocator m_allocator;
};

/// <summary>
/// Wraps a callable in a completion operation.
/// </summary>
/// <param name="callable">Invoked as <c>(std::error_code, HttpResponse)</c> exactly once.</param>
/// <param name="allocator">The allocator to take the operation's storage from.</param>
/// <returns>The new operation.</returns>
template <typename Callable, typename Allocator = std::allocator<void>>
CompletionPointer MakeCompletion(Callable&& callable, const Allocator& allocator = Allocator()) {
    return CallableCompletionOperation<std::decay_t<Callable>, Allocator>::Create(std::forward<Callable>(callable),
                                                                                  allocator);
}
} // namespace AVEVA::Detail
