// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>

#include <memory>
#include <optional>
#include <utility>

namespace AVEVA::AzureClient::Private {
/// <summary>
/// State shared by the blob and container clients: the co-owned HTTP client and the immutable target. A plain base
/// class template, so nothing is type-erased.
/// </summary>
/// <typeparam name="THttp">The HTTP client type the operations send their requests through.</typeparam>
/// <typeparam name="TTarget">The immutable description of what the client addresses.</typeparam>
template <HttpClientLike THttp, class TTarget> class ClientCore {
  public:
    /// <summary>The executor of the HTTP client, which the operations' coroutines run on.</summary>
    using executor_type = typename THttp::executor_type;

    /// <summary>Gets the executor of the HTTP client.</summary>
    /// <returns>The executor the HTTP client runs its requests on.</returns>
    [[nodiscard]] executor_type get_executor() const { return m_httpClient->get_executor(); }

    /// <summary>
    /// Request options (timeout, response body limit) applied to every operation that is not given its own via the
    /// trailing requestOptions argument.
    /// </summary>
    /// <returns>The client's default options, valid for as long as the client is.</returns>
    [[nodiscard]] const AzureRequestOptions& GetDefaultRequestOptions() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target->Connection->DefaultRequestOptions;
    }

  protected:
    /// <summary>The options an operation runs with: the caller's, else the client's defaults.</summary>
    /// <param name="requestOptions">The options the caller passed, if any.</param>
    /// <returns>A copy of <paramref name="requestOptions"/> when set, otherwise of the client's defaults.</returns>
    [[nodiscard]] AzureRequestOptions Resolve(std::optional<AzureRequestOptions> requestOptions) const {
        return requestOptions ? std::move(*requestOptions) : GetDefaultRequestOptions();
    }
    /// <summary>
    /// Shares ownership of the HTTP client for as long as this core, any client made from it, or any operation they
    /// have in flight exists.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests through.</param>
    /// <param name="target">What the client addresses.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="httpClient"/> is null.</exception>
    ClientCore(std::shared_ptr<THttp> httpClient, std::shared_ptr<const TTarget> target)
        : m_httpClient(RequireHttpClient(std::move(httpClient))), m_target(std::move(target)) {}

    std::shared_ptr<THttp> m_httpClient;
    std::shared_ptr<const TTarget> m_target;
};
} // namespace AVEVA::AzureClient::Private
