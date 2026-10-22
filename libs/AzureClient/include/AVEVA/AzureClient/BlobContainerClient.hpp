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

#include <boost/asio/async_result.hpp>

#include <algorithm>
#include <concepts>
#include <expected>
#include <functional>
#include <gsl/pointers>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient {
namespace Private {
struct ConnectionState;

[[nodiscard]] HttpRequest BuildCreateContainerRequest(const ContainerTarget& target,
                                                      const CreateBlobContainerOptions& options);
} // namespace Private

// Container operations, and the factory for clients of the blobs it contains.
//
// Thread safety: concurrent ...Async calls on one instance are supported. Completion
// handlers are not serialized with each other and may run inline, before the initiating call returns.
//
// Lifetime: stores a non-owning reference to the HTTP client, which must outlive this client and every
// in-flight ...Async operation; destroying either before a completion handler runs is undefined behavior.
// A deferred operation (omitted token) snapshots this client's state when created, so it stays valid if the
// client is moved from or destroyed before the operation is started.
//
// Cancellation: AzureRequestOptions carries a boost::asio::cancellation_slot that aborts in-flight work
// (completing with an operation_aborted-flavored BlobStorageError); a slot associated with the
// completion token is adopted when AzureRequestOptions has none.
//
// Completion tokens: every ...Async operation accepts any Boost.Asio CompletionToken (callback,
// use_future, use_awaitable, ...) and completes with std::expected<Response<T>, BlobStorageError>.
// Omitting the token yields a deferred, directly co_await-able operation, e.g. `co_await client.ListBlobsAsync();`.
// Overloads that also take an options struct exclude it from the defaulted token via `requires`.
template <HttpClientLike THttp, TokenCredentialLike TCred> class BasicBlobContainerClient {
  public:
    using PageBlobClientType = BasicPageBlobClient<THttp, TCred>;

    BasicBlobContainerClient(THttp& httpClient, const BlobContainerClientOptions& options,
                             std::shared_ptr<TCred> credential = nullptr)
        : m_httpClient(&httpClient), m_target(std::make_shared<const Private::BasicContainerTarget<TCred>>(
                                         Private::MakeContainerTarget(options, std::move(credential)))) {}
    // Shares ownership of the HTTP client for as long as this client or any client made from it exists. Operations
    // already in flight borrow the HTTP client without owning it, so keep a client (or the shared_ptr) alive until they
    // complete.
    BasicBlobContainerClient(std::shared_ptr<THttp> httpClient, const BlobContainerClientOptions& options,
                             std::shared_ptr<TCred> credential = nullptr)
        : BasicBlobContainerClient(Private::RequireHttpClient(httpClient), options, std::move(credential)) {
        m_httpOwner = std::move(httpClient);
    }
    BasicBlobContainerClient(const BasicBlobContainerClient&) = delete;
    BasicBlobContainerClient& operator=(const BasicBlobContainerClient&) = delete;
    BasicBlobContainerClient(BasicBlobContainerClient&&) noexcept = default;
    BasicBlobContainerClient& operator=(BasicBlobContainerClient&&) noexcept = default;
    ~BasicBlobContainerClient() = default;

    // The executor associated with this client's underlying HTTP client; usable as the default
    // executor argument for a boost::asio::default_completion_token_t, or to schedule work that
    // must run on the same executor as this client's completions.
    using executor_type = typename THttp::executor_type;

    [[nodiscard]] executor_type get_executor() const { return m_httpClient->get_executor(); }

    // Request options (timeout, response body limit, cancellation slot) applied to every operation
    // that is not given its own via WithRequestOptions(...) or the trailing requestOptions argument.
    [[nodiscard]] const AzureRequestOptions& GetDefaultRequestOptions() const noexcept AVEVA_LIFETIMEBOUND {
        return m_target->Connection->DefaultRequestOptions;
    }

    // The default completion token used when a ...Async call omits an explicit token
    // (matching this client's executor_type, per boost::asio::default_completion_token).
    using DefaultCompletionToken = boost::asio::default_completion_token<executor_type>::type;

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ListBlobsOptions>)
    [[nodiscard]] auto ListBlobsAsync(CompletionToken&& token = CompletionToken{},
                                      std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ListBlobsAsync(ListBlobsOptions{}, std::forward<CompletionToken>(token), std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ListBlobsAsync(ListBlobsOptions options, CompletionToken&& token = CompletionToken{},
                                      std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ListBlobsResult>(
            this,
            [](auto& client, auto&&... args) { client.ListBlobsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // Suppressed-error contract (applies to every *IfNotExists / *IfExists method): when the target already
    // exists (or, for delete, is absent) the expected is engaged, Value() is default-constructed, and
    // Response::Error() carries the suppressed service error. Any other failure arrives as
    // `unexpected(BlobStorageError)`. Check Response::Error() before treating the call as a real success.
    // Replay ambiguity: when a transient failure after the request was sent causes an automatic retry, the retry can
    // find the container this very call created and report it as already existing, so an engaged Error() means
    // "it exists", not "another caller created it".
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateBlobContainerOptions>)
    [[nodiscard]] auto CreateIfNotExistsAsync(CompletionToken&& token = CompletionToken{},
                                              std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return CreateIfNotExistsAsync(CreateBlobContainerOptions{}, std::forward<CompletionToken>(token),
                                      std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateIfNotExistsAsync(CreateBlobContainerOptions options,
                                              CompletionToken&& token = CompletionToken{},
                                              std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this,
            [](auto& client, auto&&... args) {
                client.CreateIfNotExistsAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // Clients are cheap, movable values (one shared target allocation each); no heap-allocated wrapper is needed.
    [[nodiscard]] PageBlobClientType GetPageBlobClient(std::string blobName) const {
        return PageBlobClientType{*m_httpClient, MakeBlobTarget(std::move(blobName)), m_httpOwner};
    }

    // Deletes the named blobs with Blob Batch requests (up to 256 deletes per request; longer lists are split and
    // run DeleteBlobsOptions::MaxConcurrentRequests at a time). The result holds one item per name, in order:
    // a blob that no longer exists counts as deleted (like DeleteIfExistsAsync), any other per-blob failure is in
    // that item's Error. Failure of a whole request, or an invalid name (empty or over 1024 characters), completes
    // as `unexpected(BlobStorageError)`; blobs of the requests that did complete may then already be deleted.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DeleteBlobsOptions>)
    [[nodiscard]] auto DeleteBlobsAsync(std::vector<std::string> blobNames, CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return DeleteBlobsAsync(std::move(blobNames), DeleteBlobsOptions{}, std::forward<CompletionToken>(token),
                                std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto DeleteBlobsAsync(std::vector<std::string> blobNames, DeleteBlobsOptions options,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::DeleteBlobsResult>(
            this,
            [](auto& client, auto&&... args) { client.DeleteBlobsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blobNames), std::move(options));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobServiceClient;
    friend struct Private::ClientSnapshotAccess;
    BasicBlobContainerClient(THttp& httpClient, std::shared_ptr<const Private::BasicContainerTarget<TCred>> target,
                             std::shared_ptr<THttp> owner)
        : m_httpClient(&httpClient), m_httpOwner(std::move(owner)), m_target(std::move(target)) {}

    // A client sharing this one's state, owned by a deferred operation so it survives the original being moved from.
    [[nodiscard]] BasicBlobContainerClient MakeSnapshot() const {
        return BasicBlobContainerClient{*m_httpClient, m_target, m_httpOwner};
    }

    // Shares the service client's validated connection state (no re-normalisation).
    BasicBlobContainerClient(THttp& httpClient, std::shared_ptr<const Private::ConnectionState> connection,
                             std::shared_ptr<CachedTokenCredential<TCred>> credential, std::string containerName,
                             std::shared_ptr<THttp> owner = nullptr)
        : m_httpClient(&httpClient), m_httpOwner(std::move(owner)),
          m_target(std::make_shared<const Private::BasicContainerTarget<TCred>>(Private::MakeContainerTarget<TCred>(
              std::move(connection), std::move(credential), std::move(containerName)))) {}

    [[nodiscard]] std::shared_ptr<const Private::BasicBlobTarget<TCred>> MakeBlobTarget(std::string blobName) const {
        return std::make_shared<const Private::BasicBlobTarget<TCred>>(Private::MakeBlobTarget<TCred>(
            m_target->Connection, m_target->Credential, m_target->ContainerName, std::move(blobName)));
    }

    template <class THandler>
    void ListBlobsAsyncImpl(ListBlobsOptions options, THandler completion, AzureRequestOptions requestOptions = {}) {
        ListBlobsPageAsync(*m_httpClient, m_target, std::move(options), std::move(completion), requestOptions);
    }

    template <class THandler>
    static void ListBlobsPageAsync(THttp& httpClient,
                                   const std::shared_ptr<const Private::BasicContainerTarget<TCred>>& target,
                                   ListBlobsOptions options, THandler completion, AzureRequestOptions requestOptions) {
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
        Private::SendAuthorizedRequestAsync(
            httpClient, *target, std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
                Private::CompleteParsed<Models::ListBlobsResult>(
                    error, std::move(response),
                    [](const HttpResponse& value) { return Private::ParseListBlobsResultXml(value.GetBody()); },
                    std::move(completion));
            },
            requestOptions);
    }

    template <class THandler>
    void CreateIfNotExistsAsyncImpl(const CreateBlobContainerOptions& options, THandler completion,
                                    AzureRequestOptions requestOptions = {}) {
        Private::SendAndParse<Models::CreateBlobContainerResult>(
            *m_httpClient, *m_target, Private::BuildCreateContainerRequest(*m_target, options),
            Private::ParseETagAndLastModified<Models::CreateBlobContainerResult>,
            Private::SuppressErrors<Models::CreateBlobContainerResult>(std::move(completion),
                                                                       BlobStorageErrorCode::ContainerAlreadyExists,
                                                                       BlobStorageErrorCode::ConditionNotMet),
            requestOptions);
    }

    template <class THandler>
    void DeleteBlobsAsyncImpl(std::vector<std::string> blobNames, const DeleteBlobsOptions& options,
                              THandler completion, AzureRequestOptions requestOptions = {}) {
        Private::DeleteBlobsAsync(*m_httpClient, m_target, std::move(blobNames), options, std::move(completion),
                                  requestOptions);
    }

    gsl::not_null<THttp*> m_httpClient;
    // Set when the client was given shared ownership of the HTTP client; null when it only borrows it.
    std::shared_ptr<THttp> m_httpOwner;
    std::shared_ptr<const Private::BasicContainerTarget<TCred>> m_target;
};

// The production client type.
using BlobContainerClient = BasicBlobContainerClient<HttpClient>;
extern template class BasicBlobContainerClient<HttpClient>;
} // namespace AVEVA::AzureClient
