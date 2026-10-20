// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>

#include <concepts>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient {
namespace Private {
struct ConnectionState;
} // namespace Private

// Account-level operations, and the factory for container clients.
//
// Thread safety: concurrent ...Async calls on one instance are supported. Caller-supplied buffers are
// not synchronized by the library. Completion handlers are not serialized with each other and may run
// inline, before the initiating call returns.
//
// Lifetime: stores a non-owning reference to the HTTP client, which must outlive this client and every
// in-flight ...Async operation; destroying either before a completion handler runs is undefined behavior.
//
// Cancellation: AzureRequestOptions carries a boost::asio::cancellation_slot that aborts in-flight work
// (completing with an operation_aborted-flavored BlobStorageError); a slot associated with the
// completion token is adopted when AzureRequestOptions has none.
//
// Operations: GetBlobContainerClient is synchronous and cheap; it only builds a client that shares this one's
// connection state. Asynchronous operations live on the container and blob clients.
template <HttpClientLike THttp, TokenCredentialLike TCred> class BasicBlobServiceClient {
  public:
    using ContainerClientType = BasicBlobContainerClient<THttp, TCred>;

    BasicBlobServiceClient(THttp& httpClient, BlobServiceClientOptions options,
                           std::shared_ptr<TCred> credential = nullptr)
        : m_httpClient(&httpClient),
          m_connection(Private::MakeConnectionState(std::move(options), credential != nullptr)),
          m_credential(Private::MakeCachedCredential(std::move(credential))) {}
    // Shares ownership of the HTTP client for as long as this client or any client made from it exists. Operations
    // already in flight borrow the HTTP client without owning it, so keep a client (or the shared_ptr) alive until they
    // complete.
    BasicBlobServiceClient(std::shared_ptr<THttp> httpClient, BlobServiceClientOptions options,
                           std::shared_ptr<TCred> credential = nullptr)
        : BasicBlobServiceClient(Private::RequireHttpClient(httpClient), std::move(options), std::move(credential)) {
        m_httpOwner = std::move(httpClient);
    }
    BasicBlobServiceClient(const BasicBlobServiceClient&) = delete;
    BasicBlobServiceClient& operator=(const BasicBlobServiceClient&) = delete;
    BasicBlobServiceClient(BasicBlobServiceClient&&) noexcept = default;
    BasicBlobServiceClient& operator=(BasicBlobServiceClient&&) noexcept = default;
    ~BasicBlobServiceClient() = default;

    // The executor associated with this client's underlying HTTP client; usable as the default
    // executor argument for a boost::asio::default_completion_token_t, or to schedule work that
    // must run on the same executor as this client's completions.
    using executor_type = typename THttp::executor_type;

    [[nodiscard]] executor_type get_executor() const { return m_httpClient->get_executor(); }

    // Returned by value; wrap it in a shared_ptr when shared ownership is needed.
    [[nodiscard]] ContainerClientType GetBlobContainerClient(std::string containerName) const {
        return ContainerClientType{*m_httpClient, m_connection, m_credential, std::move(containerName), m_httpOwner};
    }

  private:
    THttp* m_httpClient = nullptr;
    // Set when the client was given shared ownership of the HTTP client; null when it only borrows it.
    std::shared_ptr<THttp> m_httpOwner;
    // Shared immutable connection state referenced by child clients.
    std::shared_ptr<const Private::ConnectionState> m_connection;
    std::shared_ptr<CachedTokenCredential<TCred>> m_credential;
};

// The production client type.
using BlobServiceClient = BasicBlobServiceClient<HttpClient>;
extern template class BasicBlobServiceClient<HttpClient>;
} // namespace AVEVA::AzureClient
