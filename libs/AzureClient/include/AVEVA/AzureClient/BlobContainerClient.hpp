// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/ClientForwardDeclarations.hpp>
#include <AVEVA/AzureClient/Detail/BlobBatch.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/ClientCore.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/awaitable.hpp>

#include <algorithm>
#include <concepts>
#include <expected>
#include <functional>
#include <gsl/pointers>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
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

/// <summary>
/// Builds the Create Container request.
/// </summary>
/// <param name="target">The container to create.</param>
/// <param name="options">The creation options.</param>
/// <returns>The request, ready to send.</returns>
[[nodiscard]] HttpRequest BuildCreateContainerRequest(const ContainerTarget& target,
                                                      const CreateBlobContainerOptions& options);
} // namespace Private

/// <summary>
/// Container operations, and the factory for clients of the blobs it contains.
/// </summary>
/// <remarks>
/// Every operation is a C++20 coroutine yielding std::expected&lt;Response&lt;T&gt;, BlobStorageError&gt;, e.g.
/// <c>co_await client.ListBlobsAsync();</c>. See BlobClient.hpp for the thread-safety, lifetime and cancellation
/// contract; it is the same here.
/// </remarks>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicBlobContainerClient : Private::ClientCore<THttp, Private::BasicContainerTarget<TCred>> {
    using ClientCore = Private::ClientCore<THttp, Private::BasicContainerTarget<TCred>>;

  public:
    /// <summary>The type of the blob clients this client creates.</summary>
    using PageBlobClientType = BasicPageBlobClient<THttp, TCred>;

    /// <summary>
    /// Creates a client that shares ownership of the HTTP client for as long as this client or any client made from
    /// it exists. Operations already in flight share that ownership too, so the client may be destroyed while they
    /// run.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with.</param>
    /// <param name="options">The container, endpoint and authentication settings.</param>
    /// <param name="credential">The token credential to authorize with; null when the options carry the
    /// authentication (SAS, Shared Key or a static bearer token).</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="httpClient"/> is null or
    /// <paramref name="options"/> are invalid.</exception>
    BasicBlobContainerClient(std::shared_ptr<THttp> httpClient, const BlobContainerClientOptions& options,
                             std::shared_ptr<TCred> credential = nullptr)
        : ClientCore(std::move(httpClient), std::make_shared<const Private::BasicContainerTarget<TCred>>(
                                                Private::MakeContainerTarget(options, std::move(credential)))) {}
    /// <summary>Not copyable: the client owns shared connection state that child clients reference.</summary>
    BasicBlobContainerClient(const BasicBlobContainerClient&) = delete;
    /// <summary>Not copy-assignable.</summary>
    BasicBlobContainerClient& operator=(const BasicBlobContainerClient&) = delete;
    /// <summary>Moves the client; the moved-from client must not be used.</summary>
    BasicBlobContainerClient(BasicBlobContainerClient&&) noexcept = default;
    /// <summary>Move-assigns the client; the moved-from client must not be used.</summary>
    BasicBlobContainerClient& operator=(BasicBlobContainerClient&&) noexcept = default;
    /// <summary>Destroys the client; clients made from it keep their own share of the connection state.</summary>
    ~BasicBlobContainerClient() = default;

    /// <summary>
    /// The executor associated with this client's underlying HTTP client; use it to schedule work that
    /// must run on the same executor as this client's operations.
    /// </summary>
    using executor_type = typename THttp::executor_type;
    using ClientCore::get_executor;
    using ClientCore::GetDefaultRequestOptions;

    /// <summary>
    /// Lists the blobs of the container, one page per call.
    /// </summary>
    /// <param name="options">Prefix, continuation marker, page size and metadata selection.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The page of blobs, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::ListBlobsResult>>
    ListBlobsAsync(ListBlobsOptions options = {}, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ListBlobsImpl(m_httpClient, m_target,         std::move(options),
                                     ClientCore::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Creates the container unless it already exists.
    /// </summary>
    /// <remarks>
    /// This applies to every *IfNotExists and *IfExists method. If the container already exists (or, for a delete,
    /// is already gone), the call still succeeds, but the Value() is empty and Response::Error() holds the service
    /// error that was set aside. Any other failure is returned as <c>unexpected(BlobStorageError)</c>. Check
    /// Response::Error() before treating the call as a real success.
    /// If a transient failure happens after the request was sent, the call retries automatically. The retry can then
    /// find the container this same call created and report that it already exists. So a set Error() means "the
    /// container exists", not "someone else created it".
    /// </remarks>
    /// <param name="options">The metadata of the new container.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time of the container, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::CreateBlobContainerResult>>
    CreateIfNotExistsAsync(CreateBlobContainerOptions options = {},
                           std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return CreateIfNotExistsImpl(m_httpClient, m_target, std::move(options),
                                     ClientCore::Resolve(std::move(requestOptions)));
    }

    /// <summary>
    /// Creates a client for one page blob in the container.
    /// </summary>
    /// <remarks>
    /// Blob clients are cheap to create and can be moved, so there is no need to keep them on the heap.
    /// </remarks>
    /// <param name="blobName">The name of the blob within the container.</param>
    /// <returns>A page blob client that shares this client's connection state and HTTP client.</returns>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="blobName"/> is empty.</exception>
    [[nodiscard]] PageBlobClientType GetPageBlobClient(std::string blobName) const {
        return PageBlobClientType{m_httpClient, MakeBlobTarget(std::move(blobName))};
    }

    /// <summary>
    /// Gets the URL of a blob, without creating a client for it.
    /// </summary>
    /// <remarks>
    /// This is the same value ResourceUrl() returns on the client from GetPageBlobClient(blobName). It is handy when
    /// you only need to identify a blob, for example to check a cache before creating a client.
    /// </remarks>
    /// <param name="blobName">The name of the blob within the container.</param>
    /// <returns>The blob's URL without any query string.</returns>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="blobName"/> is empty.</exception>
    [[nodiscard]] std::string GetBlobResourceUrl(std::string_view blobName) const {
        if (blobName.empty()) {
            throw std::invalid_argument("BlobName must not be empty.");
        }
        const auto& container = m_target->EncodedResource;
        std::string url;
        url.reserve(container.size() + blobName.size() + 1U);
        url += container;
        url += '/';
        Private::UrlEncodeAppend(url, blobName, "/");
        return url;
    }

    /// <summary>
    /// Deletes several blobs at once using batch requests.
    /// </summary>
    /// <remarks>
    /// Each batch request deletes up to 256 blobs. Longer lists are split into several requests, and
    /// DeleteBlobsOptions::MaxConcurrentRequests controls how many run at the same time.
    /// The result has one item per name, in the same order. A blob that no longer exists counts as deleted (like
    /// DeleteIfExistsAsync); any other failure for a single blob is reported in that item's Error.
    /// If a whole request fails, or a name is invalid (empty or over 1024 characters), the call returns
    /// <c>unexpected(BlobStorageError)</c>. Blobs in requests that already completed may have been deleted by then.
    /// </remarks>
    /// <param name="blobNames">The names of the blobs to delete.</param>
    /// <param name="options">How many batch requests may run at once.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>One outcome per name, or the error that failed a whole request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DeleteBlobsResult>>
    DeleteBlobsAsync(std::vector<std::string> blobNames, DeleteBlobsOptions options = {},
                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DeleteBlobsAsync(
            m_httpClient, gsl::not_null<std::shared_ptr<const Private::BasicContainerTarget<TCred>>>(m_target),
            std::move(blobNames),             std::move(options),
                        ClientCore::Resolve(std::move(requestOptions)));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobServiceClient;
    /// <summary>
    /// Shares the service client's validated connection state (no re-normalisation).
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with; shared with the service client.</param>
    /// <param name="connection">The service client's connection state.</param>
    /// <param name="credential">The service client's cached token credential.</param>
    /// <param name="containerName">The name of the container.</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="containerName"/> is not a valid container
    /// name.</exception>
    BasicBlobContainerClient(std::shared_ptr<THttp> httpClient,
                             std::shared_ptr<const Private::ConnectionState> connection,
                             std::shared_ptr<CachedTokenCredential<TCred>> credential, std::string containerName)
        : ClientCore(std::move(httpClient),
               std::make_shared<const Private::BasicContainerTarget<TCred>>(Private::MakeContainerTarget<TCred>(
                   std::move(connection), std::move(credential), std::move(containerName)))) {}

    /// <summary>Builds the target of a blob in this container.</summary>
    /// <param name="blobName">The name of the blob within the container.</param>
    /// <returns>The validated blob target.</returns>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="blobName"/> is empty.</exception>
    [[nodiscard]] std::shared_ptr<const Private::BasicBlobTarget<TCred>> MakeBlobTarget(std::string blobName) const {
        return std::make_shared<const Private::BasicBlobTarget<TCred>>(Private::MakeBlobTarget<TCred>(
            m_target->Connection, m_target->Credential, Private::ContainerNameText{m_target->ContainerName},
            Private::BlobNameText{std::move(blobName)}));
    }

    /// <summary>The body of ListBlobsAsync; a static coroutine so that its frame owns what the operation
    /// uses.</summary>
    /// <param name="httpClient">The HTTP client to send the request with; kept alive by the frame.</param>
    /// <param name="target">The container to list.</param>
    /// <param name="options">The listing options.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>The page of blobs, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::ListBlobsResult>>
    ListBlobsImpl(std::shared_ptr<THttp> httpClient, std::shared_ptr<const Private::BasicContainerTarget<TCred>> target,
                  ListBlobsOptions options, AzureRequestOptions requestOptions) {
        std::vector<std::pair<std::string, std::string>> queryParameters{{"comp", "list"}};
        if (!options.Prefix.empty()) {
            queryParameters.emplace_back("prefix", options.Prefix);
        }
        if (!options.Marker.empty()) {
            queryParameters.emplace_back("marker", options.Marker);
        }
        if (options.MaxResults.has_value()) {
            queryParameters.emplace_back("maxresults", std::to_string(*options.MaxResults));
        }
        if (options.Metadata == MetadataListing::Include) {
            queryParameters.emplace_back("include", "metadata");
        }

        HttpRequest request =
            Private::BuildContainerRequest(*target, HttpMethod::Get, Private::BuildQueryString(queryParameters));
        co_return co_await Private::SendAndParse<Models::ListBlobsResult>(
            *httpClient, *target, std::move(request),
            [](const HttpResponse& value) { return Private::ParseListBlobsResultXml(value.GetBody()); },
            std::move(requestOptions));
    }

    /// <summary>The body of CreateIfNotExistsAsync; a static coroutine so that its frame owns what the operation
    /// uses.</summary>
    /// <param name="httpClient">The HTTP client to send the request with; kept alive by the frame.</param>
    /// <param name="target">The container to create.</param>
    /// <param name="options">The creation options.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <returns>The ETag and last-modified time of the container, or the error that failed the request.</returns>
    static boost::asio::awaitable<BlobResult<Models::CreateBlobContainerResult>>
    CreateIfNotExistsImpl(std::shared_ptr<THttp> httpClient,
                          std::shared_ptr<const Private::BasicContainerTarget<TCred>> target,
                          CreateBlobContainerOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::SendAndParse<Models::CreateBlobContainerResult>(
            *httpClient, *target, Private::BuildCreateContainerRequest(*target, options),
            Private::ParseETagAndLastModified<Models::CreateBlobContainerResult>, std::move(requestOptions),
            ReplaySafety::Unsafe, {BlobStorageErrorCode::ContainerAlreadyExists});
    }

    using ClientCore::m_httpClient;
    using ClientCore::m_target;
};

/// <summary>The production client type.</summary>
using BlobContainerClient = BasicBlobContainerClient<HttpClient>;
/// <summary>Explicit instantiation declaration of the production client; its members are instantiated
/// elsewhere.</summary>
extern template class BasicBlobContainerClient<HttpClient>;
} // namespace AVEVA::AzureClient
