// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>

#include <boost/asio/async_result.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient {
// Called when a source fails, with its position in the chain and whether it was the last one to try. Failures of
// earlier sources are routine when probing, so the chain itself stays silent and leaves reporting to this callback.
using SourceFailureObserver = std::function<void(std::size_t index, std::error_code error, bool isLast)>;

// Tries a list of credentials in order and completes with the token of the first one that succeeds. Fails with the
// error of the last source when none of them succeed (BlobStorageErrorCode::AuthenticationFailed when the chain is
// empty). Null sources are dropped.
template <TokenCredentialLike TSource>
class ChainedTokenCredential final : public std::enable_shared_from_this<ChainedTokenCredential<TSource>> {
    std::vector<std::shared_ptr<TSource>> m_sources;
    SourceFailureObserver m_observer;

    // The state of one GetTokenAsync call. It is a template on the handler so that the continuation handed to each
    // source has one fixed type however long the chain is.
    template <class Handler> struct Run : std::enable_shared_from_this<Run<Handler>> {
        std::shared_ptr<ChainedTokenCredential> Owner;
        std::vector<std::string> Scopes;
        Handler Completion;

        Run(std::shared_ptr<ChainedTokenCredential> owner, std::vector<std::string> scopes, Handler completion)
            : Owner(std::move(owner)), Scopes(std::move(scopes)), Completion(std::move(completion)) {}

        void Try(std::size_t index, std::error_code lastError) {
            if (index >= Owner->m_sources.size()) {
                Completion(lastError, AccessToken{});
                return;
            }

            Owner->m_sources[index]->GetTokenAsync(
                Scopes, [self = this->shared_from_this(), index](std::error_code error, AccessToken token) mutable {
                    if (!error) {
                        self->Completion(error, std::move(token));
                        return;
                    }

                    if (self->Owner->m_observer) {
                        self->Owner->m_observer(index, error, index + 1 >= self->Owner->m_sources.size());
                    }
                    self->Try(index + 1, error);
                });
        }
    };

  public:
    explicit ChainedTokenCredential(std::vector<std::shared_ptr<TSource>> sources, SourceFailureObserver observer = {})
        : m_sources(std::move(sources)), m_observer(std::move(observer)) {
        std::erase(m_sources, nullptr);
    }

    template <boost::asio::completion_token_for<TokenSignature> CompletionToken>
    auto GetTokenAsync(std::vector<std::string> scopes, CompletionToken&& token) {
        return boost::asio::async_initiate<TokenSignature>(
            [this](auto handler, std::vector<std::string> requestedScopes) {
                using HandlerType = std::decay_t<decltype(handler)>;
                auto run = std::make_shared<Run<HandlerType>>(this->shared_from_this(), std::move(requestedScopes),
                                                              std::move(handler));
                run->Try(0, make_error_code(BlobStorageErrorCode::AuthenticationFailed));
            },
            token, std::move(scopes));
    }
};
} // namespace AVEVA::AzureClient
