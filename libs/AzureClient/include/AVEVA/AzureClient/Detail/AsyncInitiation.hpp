#pragma once

#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/WithRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

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

#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient::Private
{
    // Preserves the associated executor/allocator and adopts the handler's cancellation slot
    // unless requestOptions already carries an explicit one.
    template <class THandler, class RawHandler>
    [[nodiscard]] THandler BindAssociationsAndAdoptCancellationOn(RawHandler& handler,
        const IHttpClient::executor_type& defaultExecutor,
        HttpRequestOptions& requestOptions)
    {
        if (auto slot = boost::asio::get_associated_cancellation_slot(handler, boost::asio::cancellation_slot());
            slot.is_connected() && !requestOptions.GetCancellationSlot().is_connected())
        {
            requestOptions.SetCancellationSlot(slot);
        }

        auto executor = boost::asio::get_associated_executor(handler, defaultExecutor);
        if (executor == defaultExecutor)
        {
            return THandler{std::move(handler)};
        }

        auto allocator = boost::asio::get_associated_allocator(handler);
        if constexpr (std::is_same_v<decltype(allocator), std::allocator<void>>)
        {
            // No explicit allocator association: dispatch onto associated executor with a
            // work guard to preserve executor lifetime until the handler runs.
            return THandler{[executor, handler = std::move(handler)](auto result) mutable
            {
                auto work = boost::asio::make_work_guard(executor);
                boost::asio::dispatch(executor,
                    [handler = std::move(handler), result = std::move(result), work = std::move(work)]() mutable
                {
                    std::move(handler)(std::move(result));
                });
            }};
        }
        else
        {
            // Allocator explicitly associated: ensure bound allocator is used for the dispatched call.
            return THandler{[executor, allocator, handler = std::move(handler)](auto result) mutable
            {
                auto work = boost::asio::make_work_guard(executor);
                boost::asio::dispatch(executor,
                    boost::asio::bind_allocator(allocator,
                        [handler = std::move(handler), result = std::move(result), work = std::move(work)]() mutable
                {
                    std::move(handler)(std::move(result));
                }));
            }};
        }
    }

    template <class THandler, class RawHandler>
    [[nodiscard]] THandler BindAssociationsAndAdoptCancellation(RawHandler& handler,
        IHttpClient& httpClient,
        HttpRequestOptions& requestOptions)
    {
        return BindAssociationsAndAdoptCancellationOn<THandler>(handler, httpClient.get_executor(), requestOptions);
    }

    // Forwards the completion token as an rvalue so move-only tokens remain move-only.
    template <class Signature, class Initiation, class CompletionToken, class... Args>
    auto InitiateAsync(Initiation&& initiation, CompletionToken&& token, Args&&... args)
    {
        return boost::asio::async_initiate<Signature>(std::forward<Initiation>(initiation),
            std::forward<CompletionToken>(token),
            std::forward<Args>(args)...);
    }

    // Initiates a client operation completing with std::expected<Response<Result>, BlobStorageError>:
    // at initiation, binds the handler, then calls (client->*impl)(args..., handler, requestOptions).
    // `args` are decay-copied into the initiation so deferred tokens never dangle (pass std::ref to
    // deliberately capture a caller-owned object by reference). The effective request options are, in
    // order of precedence: a WithRequestOptions(...) token, the explicit `requestOptions` argument, and
    // the client's DefaultRequestOptions.
    template <class Result, class Client, class Impl, class CompletionToken, class... Args>
    auto InitiateClientOperation(Client* client,
        Impl impl,
        CompletionToken&& token,
        std::optional<HttpRequestOptions> requestOptions,
        Args&&... args)
    {
        if constexpr (IsRequestOptionsToken<std::remove_cvref_t<CompletionToken>>)
        {
            std::remove_cvref_t<CompletionToken> wrapper = std::forward<CompletionToken>(token);
            return InitiateClientOperation<Result>(client,
                impl,
                std::move(wrapper.Token),
                std::optional<HttpRequestOptions>{std::move(wrapper.Options)},
                std::forward<Args>(args)...);
        }
        else
        {
            using Signature = void(std::expected<Response<Result>, BlobStorageError>);
            HttpRequestOptions effectiveOptions =
                requestOptions ? std::move(*requestOptions) : client->GetDefaultRequestOptions();
            return InitiateAsync<Signature>(
                [client, impl](auto handler, HttpRequestOptions initiationRequestOptions, auto... initiationArgs)
            {
                auto boundHandler = BindAssociationsAndAdoptCancellationOn<std::move_only_function<Signature>>(handler,
                    client->get_executor(),
                    initiationRequestOptions);
                std::invoke(impl,
                    *client,
                    std::move(initiationArgs)...,
                    std::move(boundHandler),
                    std::move(initiationRequestOptions));
            },
                std::forward<CompletionToken>(token),
                std::move(effectiveOptions),
                std::forward<Args>(args)...);
        }
    }

    // Asio requires that a completion handler never run before its initiating function returns.
    // Several early-exit validation paths (invalid arguments, unopenable files, invalid ranges)
    // would otherwise call `completion(...)` synchronously. This posts the completion onto the
    // handler's associated executor (or `httpClient.get_executor()` if the handler has none)
    // instead, deferring invocation until the caller's io_context/strand runs it, matching the
    // documented contract for every other completion (which all arrive from genuine async I/O).
    template <class THandler, class... Args>
    void PostCompletion(IHttpClient& httpClient, THandler&& handler, Args&&... args)
    {
        auto executor = boost::asio::get_associated_executor(handler, httpClient.get_executor());
        boost::asio::post(executor, boost::asio::append(std::forward<THandler>(handler), std::forward<Args>(args)...));
    }
} // namespace AVEVA::AzureClient::Private
