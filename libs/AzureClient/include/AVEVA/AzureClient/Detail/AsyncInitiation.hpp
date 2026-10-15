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

#include <concepts>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient::Private {
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
template <class TArg, class THandler, class TExecutor, class TAllocator, class TSlot> struct BoundHandler {
    THandler Handler;
    TExecutor Executor;
    TAllocator Allocator;
    TSlot Slot;
    bool AdoptedSlot;
    bool Direct;
    std::optional<boost::asio::executor_work_guard<TExecutor>> Work;

    // The fixed parameter type lets producers pass anything convertible (e.g. std::unexpected).
    void operator()(TArg result) {
        if (Direct) {
            std::move(Handler)(std::move(result));
            return;
        }
        boost::asio::dispatch(Executor,
                              boost::asio::bind_allocator(
                                  Allocator, [handler = std::move(Handler), result = std::move(result),
                                              work = std::move(Work), slot = Slot, adopted = AdoptedSlot]() mutable {
                                      if (adopted) {
                                          slot.clear();
                                      }
                                      std::move(handler)(std::move(result));
                                  }));
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
    std::optional<boost::asio::executor_work_guard<Executor>> work;
    if (!direct) {
        work.emplace(boost::asio::make_work_guard(executor));
    }
    return BoundHandler<TArg, RawHandler, Executor, decltype(allocator), decltype(slot)>{
        std::move(handler), executor, allocator, slot, adoptedSlot, direct, std::move(work)};
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

    AzureRequestOptions effectiveOptions =
        requestOptions ? std::move(*requestOptions) : client->GetDefaultRequestOptions();
    if constexpr (IsRequestOptionsToken<std::remove_cvref_t<CompletionToken>>) {
        effectiveOptions = token.Options;
    }

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
