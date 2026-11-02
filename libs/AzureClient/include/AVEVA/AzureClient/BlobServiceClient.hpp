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
#include <gsl/pointers>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient {
namespace Private {
/// <summary>
/// Forward declaration of the immutable connection settings that a service client shares with the clients made from
/// it.
/// </summary>
struct ConnectionState;
} // namespace Private

/// <summary>
/// Account-level operations, and the factory for container clients.
/// </summary>
/// <remarks>
/// See BlobClient.hpp for the thread-safety, lifetime and cancellation contract; it is the same here.
///
/// Operations: GetBlobContainerClient is synchronous and cheap; it only builds a client that shares this one's
/// connection state. Asynchronous operations live on the container and blob clients.
/// </remarks>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred> class BasicBlobServiceClient {
  public:
    /// <summary>The type of the container clients this client creates.</summary>
    using ContainerClientType = BasicBlobContainerClient<THttp, TCred>;

    /// <summary>
    /// Creates a client that borrows the HTTP client.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with; it must outlive this client and everything
    /// made from it.</param>
    /// <param name="options">The account endpoint and authentication settings.</param>
    /// <param name="credential">The token credential to authorize with; null when the options carry the
    /// authentication (SAS, Shared Key or a static bearer token).</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="options"/> are invalid.</exception>
    BasicBlobServiceClient(THttp& httpClient, BlobServiceClientOptions options,
                           std::shared_ptr<TCred> credential = nullptr)
        : m_httpClient(&httpClient),
          m_connection(Private::MakeConnectionState(std::move(options), Private::PresenceOf(credential))),
          m_credential(Private::MakeCachedCredential(std::move(credential))) {}

    /// <summary>
    /// Creates a client that shares ownership of the HTTP client for as long as this client or any client made from
    /// it exists. Operations already in flight borrow the HTTP client without owning it, so keep a client (or the
    /// shared_ptr) alive until they complete.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with.</param>
    /// <param name="options">The account endpoint and authentication settings.</param>
    /// <param name="credential">The token credential to authorize with; null when the options carry the
    /// authentication (SAS, Shared Key or a static bearer token).</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="httpClient"/> is null or
    /// <paramref name="options"/> are invalid.</exception>
    BasicBlobServiceClient(std::shared_ptr<THttp> httpClient, BlobServiceClientOptions options,
                           std::shared_ptr<TCred> credential = nullptr)
        : BasicBlobServiceClient(Private::RequireHttpClient(httpClient), std::move(options), std::move(credential)) {
        m_httpOwner = std::move(httpClient);
    }
    /// <summary>Not copyable: the client owns shared connection state that child clients reference.</summary>
    BasicBlobServiceClient(const BasicBlobServiceClient&) = delete;
    /// <summary>Not copy-assignable.</summary>
    BasicBlobServiceClient& operator=(const BasicBlobServiceClient&) = delete;
    /// <summary>Moves the client; the moved-from client must not be used.</summary>
    BasicBlobServiceClient(BasicBlobServiceClient&&) noexcept = default;
    /// <summary>Move-assigns the client; the moved-from client must not be used.</summary>
    BasicBlobServiceClient& operator=(BasicBlobServiceClient&&) noexcept = default;
    /// <summary>Destroys the client; clients made from it keep their own share of the connection state.</summary>
    ~BasicBlobServiceClient() = default;

    /// <summary>
    /// The executor of this client's underlying HTTP client.
    /// </summary>
    using executor_type = typename THttp::executor_type;

    /// <summary>
    /// The executor on which the underlying HTTP client runs its work.
    /// </summary>
    /// <returns>The executor of the underlying HTTP client.</returns>
    [[nodiscard]] executor_type get_executor() const { return m_httpClient->get_executor(); }

    /// <summary>
    /// Creates a client for one container of the account. Returned by value; wrap it in a shared_ptr when shared
    /// ownership is needed.
    /// </summary>
    /// <param name="containerName">The name of the container.</param>
    /// <returns>A container client that shares this client's connection state and HTTP client.</returns>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="containerName"/> is not a valid container
    /// name.</exception>
    [[nodiscard]] ContainerClientType GetBlobContainerClient(std::string containerName) const {
        return ContainerClientType{*m_httpClient, m_connection, m_credential, std::move(containerName), m_httpOwner};
    }

  private:
    gsl::not_null<THttp*> m_httpClient;
    // Set when the client was given shared ownership of the HTTP client; null when it only borrows it.
    std::shared_ptr<THttp> m_httpOwner;
    // Shared immutable connection state referenced by child clients.
    std::shared_ptr<const Private::ConnectionState> m_connection;
    std::shared_ptr<CachedTokenCredential<TCred>> m_credential;
};

/// <summary>The production client type.</summary>
using BlobServiceClient = BasicBlobServiceClient<HttpClient>;
/// <summary>Explicit instantiation declaration of the production client; its members are instantiated
/// elsewhere.</summary>
extern template class BasicBlobServiceClient<HttpClient>;
} // namespace AVEVA::AzureClient
