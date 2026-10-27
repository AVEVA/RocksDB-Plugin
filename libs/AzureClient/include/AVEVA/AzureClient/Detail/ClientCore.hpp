// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>

#include <gsl/pointers>
#include <memory>
#include <utility>

namespace AVEVA::AzureClient::Private {
// State shared by the blob and container clients: the HTTP client (borrowed, or co-owned when built from a
// shared_ptr) and the immutable target. A plain base class template, so nothing is type-erased.
template <HttpClientLike THttp, class TTarget> class ClientCore {
  public:
    using executor_type = typename THttp::executor_type;

    [[nodiscard]] executor_type get_executor() const { return m_httpClient->get_executor(); }

    // Request options (timeout, response body limit, cancellation slot) applied to every operation
    // that is not given its own via WithRequestOptions(...) or the trailing requestOptions argument.
    [[nodiscard]] const AzureRequestOptions& GetDefaultRequestOptions() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target->Connection->DefaultRequestOptions;
    }

  protected:
    ClientCore(THttp& httpClient, std::shared_ptr<const TTarget> target, std::shared_ptr<THttp> owner = nullptr)
        : m_httpClient(&httpClient), m_httpOwner(std::move(owner)), m_target(std::move(target)) {}
    // Shares ownership of the HTTP client for as long as this core or any client made from it exists.
    ClientCore(std::shared_ptr<THttp> httpClient, std::shared_ptr<const TTarget> target)
        : m_httpClient(&RequireHttpClient(httpClient)), m_httpOwner(std::move(httpClient)),
          m_target(std::move(target)) {}

    gsl::not_null<THttp*> m_httpClient;
    // Set when the client was given shared ownership of the HTTP client; null when it only borrows it.
    std::shared_ptr<THttp> m_httpOwner;
    std::shared_ptr<const TTarget> m_target;
};
} // namespace AVEVA::AzureClient::Private
