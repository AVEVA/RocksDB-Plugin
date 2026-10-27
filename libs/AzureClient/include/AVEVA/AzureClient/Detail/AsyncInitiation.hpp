// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlockOn.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/WithRequestOptions.hpp>

#include <boost/asio/append.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>

#include <atomic>
#include <concepts>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient::Private {
// Only terminal and total cancellation abort an operation; a partial request has nothing safe to hand back
// half-done, so every cancellation slot handler ignores it.
[[nodiscard]] inline bool IsAbortingCancellation(boost::asio::cancellation_type_t type) noexcept {
    return (type & (boost::asio::cancellation_type::terminal | boost::asio::cancellation_type::total)) !=
           boost::asio::cancellation_type::none;
}

// Grants InitiateClientOperation access to a client's private MakeSnapshot().
struct ClientSnapshotAccess {
    template <class TClient> [[nodiscard]] static TClient Make(const TClient& client) { return client.MakeSnapshot(); }
};

// Preserves the associated executor/allocator and adopts the handler's cancellation slot
// unless requestOptions already carries an explicit one.
//
// The work guard on the handler's executor is taken here, at initiation: if that executor belongs to a
// different io_context, its run() could otherwise return before the completion is dispatched. An adopted
// slot is cleared on the handler's executor, immediately before the handler runs, so the clear is serialized
// with the caller emitting on that executor instead of racing on the transport thread.
// Concrete (non-erased) wrapper produced by BindAssociationsAndAdoptCancellation. `Direct` is the fast path
// where the handler already runs on the default executor and no slot was adopted: it is invoked in place.
// Work always holds the executor busy until the completion has been delivered.
template <class TArg, class THandler, class TExecutor, class TIoExecutor, class TAllocator, class TSlot>
struct BoundHandler {
    THandler Handler;
    TExecutor Executor;
    TAllocator Allocator;
    TSlot Slot;
    bool AdoptedSlot;
    bool Direct;
    std::optional<boost::asio::executor_work_guard<TExecutor>> Work;
    // Keeps the client's own context running when the handler completes on a distinct executor; empty otherwise.
    std::optional<boost::asio::executor_work_guard<TIoExecutor>> IoWork;

    // Executors without running_in_this_thread() (e.g. use_future's) are thread-agnostic sinks, so inline is safe.
    [[nodiscard]] bool OnExecutorThread() const noexcept {
        if constexpr (requires { Executor.running_in_this_thread(); }) {
            return Executor.running_in_this_thread();
        } else {
            return true;
        }
    }

    // The fixed parameter type lets producers pass anything convertible (e.g. std::unexpected).
    void operator()(TArg result) {
        // A completion can arrive on a foreign thread (e.g. a credential on another io_context); only a call that is
        // already on the client executor may skip the dispatch.
        if (Direct && OnExecutorThread()) {
            std::move(Handler)(std::move(result));
            Work.reset();
            IoWork.reset();
            return;
        }
        DispatchOwned(std::move(result));
    }

  private:
    // The handler, result and work guards live in shared state until the dispatched task actually runs. A task moved
    // into a dispatch that throws is destroyed with everything it captured, which would lose the completion and
    // release the guards even though the operation succeeded; sharing the state lets the dispatch be retried.
    struct Delivery {
        Delivery(BoundHandler& source, TArg&& value)
            : Handler(std::move(source.Handler)), Result(std::move(value)), Work(std::move(source.Work)),
              IoWork(std::move(source.IoWork)), Slot(source.Slot), Adopted(source.AdoptedSlot) {}

        // Exactly once, however the dispatch and the fallback interleave. The adopted slot is only cleared on the
        // handler's executor, where it is serialized with the caller's emissions.
        void Run(bool onHandlerExecutor) {
            if (Done.exchange(true, std::memory_order_acq_rel)) {
                return;
            }
            if (Adopted && onHandlerExecutor) {
                Slot.clear();
            }
            std::move(Handler)(std::move(Result));
        }

        THandler Handler;
        TArg Result;
        std::optional<boost::asio::executor_work_guard<TExecutor>> Work;
        std::optional<boost::asio::executor_work_guard<TIoExecutor>> IoWork;
        TSlot Slot;
        bool Adopted;
        std::atomic<bool> Done{false};
    };

    static constexpr int DispatchAttempts = 3;

    void DispatchOwned(TArg&& result) {
        std::shared_ptr<Delivery> delivery;
        for (int attempt = 0; attempt < DispatchAttempts && !delivery; ++attempt) {
            try {
                delivery = std::allocate_shared<Delivery>(Allocator, *this, std::move(result));
            } catch (...) {
            }
        }
        if (!delivery) {
            // Nothing was moved from yet, so the handler can still be run (once) rather than lost.
            std::move(Handler)(std::move(result));
            return;
        }
        for (int attempt = 0; attempt < DispatchAttempts; ++attempt) {
            try {
                boost::asio::dispatch(Executor,
                                      boost::asio::bind_allocator(Allocator, [delivery]() { delivery->Run(true); }));
                return;
            } catch (...) {
            }
        }
        // The executor keeps rejecting work; the result is still the caller's, so it runs here, once.
        delivery->Run(false);
    }
};

template <class TArg, class RawHandler, class TExecutor>
[[nodiscard]] auto BindAssociationsAndAdoptCancellationOn(RawHandler& handler, const TExecutor& defaultExecutor,
                                                          AzureRequestOptions& requestOptions) {
    auto slot = boost::asio::get_associated_cancellation_slot(handler, boost::asio::cancellation_slot());
    const bool adoptedSlot = slot.is_connected() && !requestOptions.GetCancellationSlot().is_connected();
    if (adoptedSlot) {
        requestOptions.SetCancellationSlot(slot);
    }

    auto executor = boost::asio::get_associated_executor(handler, defaultExecutor);
    // Executors of different types (e.g. a strand) are never equal to the default one.
    bool usesDefaultExecutor = false;
    if constexpr (std::equality_comparable_with<decltype(executor), const TExecutor&>) {
        usesDefaultExecutor = executor == defaultExecutor;
    }
    const bool direct = usesDefaultExecutor && !adoptedSlot;

    auto allocator = boost::asio::get_associated_allocator(handler);
    using Executor = decltype(executor);
    // Held even on the direct path: credential or file work off the executor would otherwise let run() return
    // before the completion is delivered. The direct path releases it right after the inline invocation.
    std::optional<boost::asio::executor_work_guard<Executor>> work{boost::asio::make_work_guard(executor)};
    // A distinct completion executor does not keep the client's context alive while foreign-thread credential or
    // file work finishes and the continuation must post back to it.
    std::optional<boost::asio::executor_work_guard<TExecutor>> ioWork;
    if (!usesDefaultExecutor) {
        ioWork.emplace(boost::asio::make_work_guard(defaultExecutor));
    }
    return BoundHandler<TArg, RawHandler, Executor, TExecutor, decltype(allocator), decltype(slot)>{
        std::move(handler), executor, allocator, slot, adoptedSlot, direct, std::move(work), std::move(ioWork)};
}
// Initiates a client operation completing with std::expected<Response<Result>, BlobStorageError>:
// at initiation, binds the handler, then calls (client->*impl)(args..., handler, requestOptions).
// `args` are decay-copied into the initiation so deferred tokens never dangle (pass std::ref to
// deliberately capture a caller-owned object by reference). The effective request options are, in
// order of precedence: a WithRequestOptions(...) token, the explicit `requestOptions` argument, and
// the client's DefaultRequestOptions.
template <class Result, class Client, class Impl, class CompletionToken, class... Args>
auto InitiateClientOperation(Client* client, Impl impl, CompletionToken&& token,
                             std::optional<AzureRequestOptions> requestOptions, Args&&... args) {
    using Signature = void(std::expected<Response<Result>, BlobStorageError>);

    // Chosen once so only the options actually used are copied (the client defaults are the costly fallback).
    AzureRequestOptions effectiveOptions = [&]() -> AzureRequestOptions {
        if constexpr (IsRequestOptionsToken<std::remove_cvref_t<CompletionToken>>) {
            return token.Options;
        } else {
            return requestOptions ? std::move(*requestOptions) : client->GetDefaultRequestOptions();
        }
    }();

    // A WithRequestOptions(...) wrapper is transparent: the token it carries is what gets initiated.
    auto&& innerToken = [&]() -> decltype(auto) {
        if constexpr (IsRequestOptionsToken<std::remove_cvref_t<CompletionToken>>) {
            return (std::forward<CompletionToken>(token).Token);
        } else {
            return std::forward<CompletionToken>(token);
        }
    }();

    // Checked before initiating so misuse never leaves a request in flight, also behind WithRequestOptions(...).
    if constexpr (IsBlockOnToken<decltype(innerToken)>) {
        ThrowIfRunningOn(client->get_executor());
    }

    return boost::asio::async_initiate<Signature>(
        // The initiation owns a snapshot of the client rather than pointing at it: a deferred operation runs
        // after this call returns, by which time the caller's client may have been moved from or destroyed.
        [snapshot = ClientSnapshotAccess::Make(*client),
         impl](auto handler, AzureRequestOptions initiationRequestOptions, auto... initiationArgs) mutable {
            auto boundHandler =
                BindAssociationsAndAdoptCancellationOn<std::expected<Response<Result>, BlobStorageError>>(
                    handler, snapshot.get_executor(), initiationRequestOptions);
            std::invoke(impl, snapshot, std::move(initiationArgs)..., std::move(boundHandler),
                        std::move(initiationRequestOptions));
        },
        std::forward<decltype(innerToken)>(innerToken), std::move(effectiveOptions), std::forward<Args>(args)...);
}

// Asio requires that a completion handler never run before its initiating function returns.
// Several early-exit validation paths (invalid arguments, unopenable files, invalid ranges)
// would otherwise call `completion(...)` synchronously. This posts the completion onto the
// handler's associated executor (or `httpClient.get_executor()` if the handler has none)
// instead, deferring invocation until the caller's io_context/strand runs it, matching the
// documented contract for every other completion (which all arrive from genuine async I/O).
template <HttpClientLike THttp, class THandler, class... Args>
void PostCompletion(THttp& httpClient, THandler&& handler, Args&&... args) {
    auto executor = boost::asio::get_associated_executor(handler, httpClient.get_executor());
    boost::asio::post(executor, boost::asio::append(std::forward<THandler>(handler), std::forward<Args>(args)...));
}
} // namespace AVEVA::AzureClient::Private
