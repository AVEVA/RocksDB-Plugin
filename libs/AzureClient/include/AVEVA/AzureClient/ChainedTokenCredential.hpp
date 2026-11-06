// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Outcomes.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/Units.hpp>

#include <boost/asio/awaitable.hpp>

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
struct SourceIndexTag;
/// <summary>The zero-based position of a source in a credential chain.</summary>
using SourceIndex = StrongInteger<SourceIndexTag, std::size_t>;

/// <summary>
/// Called when a source fails, with its position in the chain and whether it was the last one to try. Failures of
/// earlier sources are routine when probing, so the chain itself stays silent and leaves reporting to this callback.
/// </summary>
using SourceFailureObserver = std::function<void(SourceIndex index, std::error_code error, ChainPosition position)>;

/// <summary>
/// Tries a list of credentials in order and returns the token of the first one that succeeds. Fails with the
/// error of the last source when none of them succeed (<c>BlobStorageErrorCode::AuthenticationFailed</c> when the
/// chain is empty). Null sources are dropped. A pending request shares ownership of the chain, so the chain only
/// exists behind a shared_ptr (see <c>Create</c>).
/// </summary>
/// <typeparam name="TSource">The credential type of every source in the chain.</typeparam>
template <TokenCredentialLike TSource>
class ChainedTokenCredential final : public std::enable_shared_from_this<ChainedTokenCredential<TSource>> {
    /// <summary>Keeps the constructor unusable outside Create while still allowing std::make_shared.</summary>
    struct CreateKey {
        explicit CreateKey() = default;
    };

    std::vector<std::shared_ptr<TSource>> m_sources;
    SourceFailureObserver m_observer;

  public:
    /// <summary>
    /// Builds the chain, dropping null sources.
    /// </summary>
    /// <param name="sources">The credentials to try, in order.</param>
    /// <param name="observer">Told about each failing source; may be empty.</param>
    /// <returns>The new chain.</returns>
    [[nodiscard]] static std::shared_ptr<ChainedTokenCredential> Create(std::vector<std::shared_ptr<TSource>> sources,
                                                                        SourceFailureObserver observer = {}) {
        return std::make_shared<ChainedTokenCredential>(CreateKey{}, std::move(sources), std::move(observer));
    }

    /// <summary>Builds the chain; reachable only through <c>Create</c>.</summary>
    /// <param name="key">Proof that the call came from <c>Create</c>.</param>
    /// <param name="sources">The credentials to try, in order.</param>
    /// <param name="observer">Told about each failing source; may be empty.</param>
    ChainedTokenCredential(CreateKey /*key*/, std::vector<std::shared_ptr<TSource>> sources,
                           SourceFailureObserver observer)
        : m_sources(std::move(sources)), m_observer(std::move(observer)) {
        std::erase(m_sources, nullptr);
    }

    /// <summary>
    /// Asks each source in turn until one yields a token, reporting every failure to the observer.
    /// </summary>
    /// <param name="scopes">The scopes the token must cover.</param>
    /// <returns>The first successful token; otherwise the last source's error.</returns>
    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        // Taken eagerly, before the lazy coroutine starts, so the frame never reaches back through this.
        return GetTokenImpl(this->shared_from_this(), std::move(scopes));
    }

  private:
    /// <summary>
    /// The body of <c>GetTokenAsync</c>; the frame owns the chain, so it stays alive while a source is pending.
    /// </summary>
    /// <param name="self">The chain to ask.</param>
    /// <param name="scopes">The scopes the token must cover.</param>
    /// <returns>The first successful token; otherwise the last source's error.</returns>
    static boost::asio::awaitable<TokenResult> GetTokenImpl(std::shared_ptr<ChainedTokenCredential> self,
                                                            std::vector<std::string> scopes) {
        std::error_code lastError = make_error_code(BlobStorageErrorCode::AuthenticationFailed);
        const std::size_t count = self->m_sources.size();
        for (std::size_t index = 0; index < count; ++index) {
            TokenResult result = co_await self->m_sources[index]->GetTokenAsync(scopes);
            if (result.has_value()) {
                co_return result;
            }
            lastError = result.error();
            if (self->m_observer) {
                self->m_observer(SourceIndex{index}, lastError,
                                 index + 1 >= count ? ChainPosition::Last : ChainPosition::Intermediate);
            }
        }
        co_return std::unexpected(lastError);
    }
};
} // namespace AVEVA::AzureClient
