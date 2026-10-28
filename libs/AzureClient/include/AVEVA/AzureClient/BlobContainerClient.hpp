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
struct ConnectionState;

[[nodiscard]] HttpRequest BuildCreateContainerRequest(const ContainerTarget& target,
                                                      const CreateBlobContainerOptions& options);
} // namespace Private

// Container operations, and the factory for clients of the blobs it contains.
//
// Every operation is a C++20 coroutine yielding std::expected<Response<T>, BlobStorageError>, e.g.
// `co_await client.ListBlobsAsync();`. See BlobClient.hpp for the thread-safety, lifetime and cancellation
// contract; it is the same here.
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicBlobContainerClient : Private::ClientCore<THttp, Private::BasicContainerTarget<TCred>> {
    using Core = Private::ClientCore<THttp, Private::BasicContainerTarget<TCred>>;

  public:
    using PageBlobClientType = BasicPageBlobClient<THttp, TCred>;

    BasicBlobContainerClient(THttp& httpClient, const BlobContainerClientOptions& options,
                             std::shared_ptr<TCred> credential = nullptr)
        : Core(httpClient, std::make_shared<const Private::BasicContainerTarget<TCred>>(
                               Private::MakeContainerTarget(options, std::move(credential)))) {}
    // Shares ownership of the HTTP client for as long as this client or any client made from it exists. Operations
    // already in flight borrow the HTTP client without owning it, so keep a client (or the shared_ptr) alive until they
    // complete.
    BasicBlobContainerClient(std::shared_ptr<THttp> httpClient, const BlobContainerClientOptions& options,
                             std::shared_ptr<TCred> credential = nullptr)
        : Core(std::move(httpClient), std::make_shared<const Private::BasicContainerTarget<TCred>>(
                                          Private::MakeContainerTarget(options, std::move(credential)))) {}
    BasicBlobContainerClient(const BasicBlobContainerClient&) = delete;
    BasicBlobContainerClient& operator=(const BasicBlobContainerClient&) = delete;
    BasicBlobContainerClient(BasicBlobContainerClient&&) noexcept = default;
    BasicBlobContainerClient& operator=(BasicBlobContainerClient&&) noexcept = default;
    ~BasicBlobContainerClient() = default;

    // The executor associated with this client's underlying HTTP client; usable as the default
    // executor argument for a boost::asio::default_completion_token_t, or to schedule work that
    // must run on the same executor as this client's completions.
    using executor_type = typename THttp::executor_type;
    using Core::get_executor;
    using Core::GetDefaultRequestOptions;

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::ListBlobsResult>>
    ListBlobsAsync(ListBlobsOptions options = {}, std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ListBlobsImpl(*m_httpClient, m_target, std::move(options), Core::Resolve(std::move(requestOptions)));
    }

    // Suppressed-error contract (applies to every *IfNotExists / *IfExists method): when the target already
    // exists (or, for delete, is absent) the expected is engaged, Value() is default-constructed, and
    // Response::Error() carries the suppressed service error. Any other failure arrives as
    // `unexpected(BlobStorageError)`. Check Response::Error() before treating the call as a real success.
    // Replay ambiguity: when a transient failure after the request was sent causes an automatic retry, the retry can
    // find the container this very call created and report it as already existing, so an engaged Error() means
    // "it exists", not "another caller created it".
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::CreateBlobContainerResult>>
    CreateIfNotExistsAsync(CreateBlobContainerOptions options = {},
                           std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return CreateIfNotExistsImpl(*m_httpClient, m_target, std::move(options),
                                     Core::Resolve(std::move(requestOptions)));
    }

    // Clients are cheap, movable values (one shared target allocation each); no heap-allocated wrapper is needed.
    [[nodiscard]] PageBlobClientType GetPageBlobClient(std::string blobName) const {
        return PageBlobClientType{*m_httpClient, MakeBlobTarget(std::move(blobName)), m_httpOwner};
    }

    // The ResourceUrl() that GetPageBlobClient(blobName) would report, built without constructing a client. For
    // callers that only need the identity key of a blob, e.g. to consult a cache before paying for a client.
    [[nodiscard]] std::string BlobResourceUrl(std::string_view blobName) const {
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

    // Deletes the named blobs with Blob Batch requests (up to 256 deletes per request; longer lists are split and
    // run DeleteBlobsOptions::MaxConcurrentRequests at a time). The result holds one item per name, in order:
    // a blob that no longer exists counts as deleted (like DeleteIfExistsAsync), any other per-blob failure is in
    // that item's Error. Failure of a whole request, or an invalid name (empty or over 1024 characters), completes
    // as `unexpected(BlobStorageError)`; blobs of the requests that did complete may then already be deleted.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::DeleteBlobsResult>>
    DeleteBlobsAsync(std::vector<std::string> blobNames, DeleteBlobsOptions options = {},
                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::DeleteBlobsAsync(*m_httpClient, m_target, std::move(blobNames), std::move(options),
                                         Core::Resolve(std::move(requestOptions)));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobServiceClient;
    BasicBlobContainerClient(THttp& httpClient, std::shared_ptr<const Private::BasicContainerTarget<TCred>> target,
                             std::shared_ptr<THttp> owner)
        : Core(httpClient, std::move(target), std::move(owner)) {}

    // Shares the service client's validated connection state (no re-normalisation).
    BasicBlobContainerClient(THttp& httpClient, std::shared_ptr<const Private::ConnectionState> connection,
                             std::shared_ptr<CachedTokenCredential<TCred>> credential, std::string containerName,
                             std::shared_ptr<THttp> owner = nullptr)
        : Core(httpClient,
               std::make_shared<const Private::BasicContainerTarget<TCred>>(Private::MakeContainerTarget<TCred>(
                   std::move(connection), std::move(credential), std::move(containerName))),
               std::move(owner)) {}

    [[nodiscard]] std::shared_ptr<const Private::BasicBlobTarget<TCred>> MakeBlobTarget(std::string blobName) const {
        return std::make_shared<const Private::BasicBlobTarget<TCred>>(Private::MakeBlobTarget<TCred>(
            m_target->Connection, m_target->Credential, m_target->ContainerName, std::move(blobName)));
    }

    static boost::asio::awaitable<BlobResult<Models::ListBlobsResult>>
    ListBlobsImpl(THttp& httpClient, std::shared_ptr<const Private::BasicContainerTarget<TCred>> target,
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
        if (options.IncludeMetadata) {
            queryParameters.emplace_back("include", "metadata");
        }

        HttpRequest request =
            Private::BuildContainerRequest(*target, HttpMethod::Get, Private::BuildQueryString(queryParameters));
        co_return co_await Private::SendAndParse<Models::ListBlobsResult>(
            httpClient, *target, std::move(request),
            [](const HttpResponse& value) { return Private::ParseListBlobsResultXml(value.GetBody()); },
            std::move(requestOptions));
    }

    static boost::asio::awaitable<BlobResult<Models::CreateBlobContainerResult>>
    CreateIfNotExistsImpl(THttp& httpClient, std::shared_ptr<const Private::BasicContainerTarget<TCred>> target,
                          CreateBlobContainerOptions options, AzureRequestOptions requestOptions) {
        co_return co_await Private::SendAndParse<Models::CreateBlobContainerResult>(
            httpClient, *target, Private::BuildCreateContainerRequest(*target, options),
            Private::ParseETagAndLastModified<Models::CreateBlobContainerResult>, std::move(requestOptions), false,
            {BlobStorageErrorCode::ContainerAlreadyExists, BlobStorageErrorCode::ConditionNotMet});
    }

    using Core::m_httpClient;
    using Core::m_httpOwner;
    using Core::m_target;
};

// The production client type.
using BlobContainerClient = BasicBlobContainerClient<HttpClient>;
extern template class BasicBlobContainerClient<HttpClient>;
} // namespace AVEVA::AzureClient
