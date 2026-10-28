// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>

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
// Called when a source fails, with its position in the chain and whether it was the last one to try. Failures of
// earlier sources are routine when probing, so the chain itself stays silent and leaves reporting to this callback.
using SourceFailureObserver = std::function<void(std::size_t index, std::error_code error, bool isLast)>;

// Tries a list of credentials in order and returns the token of the first one that succeeds. Fails with the
// error of the last source when none of them succeed (BlobStorageErrorCode::AuthenticationFailed when the chain is
// empty). Null sources are dropped.
template <TokenCredentialLike TSource>
class ChainedTokenCredential final : public std::enable_shared_from_this<ChainedTokenCredential<TSource>> {
    std::vector<std::shared_ptr<TSource>> m_sources;
    SourceFailureObserver m_observer;

  public:
    explicit ChainedTokenCredential(std::vector<std::shared_ptr<TSource>> sources, SourceFailureObserver observer = {})
        : m_sources(std::move(sources)), m_observer(std::move(observer)) {
        std::erase(m_sources, nullptr);
    }

    [[nodiscard]] boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        // Keeps the chain alive while a source is pending.
        const auto self = this->shared_from_this();
        std::error_code lastError = make_error_code(BlobStorageErrorCode::AuthenticationFailed);
        for (std::size_t index = 0; index < m_sources.size(); ++index) {
            TokenResult result = co_await m_sources[index]->GetTokenAsync(scopes);
            if (result.has_value()) {
                co_return result;
            }
            lastError = result.error();
            if (m_observer) {
                m_observer(index, lastError, index + 1 >= m_sources.size());
            }
        }
        co_return std::unexpected(lastError);
    }
};
} // namespace AVEVA::AzureClient
