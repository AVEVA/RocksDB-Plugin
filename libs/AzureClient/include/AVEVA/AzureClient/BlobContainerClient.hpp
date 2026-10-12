// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <boost/asio/async_result.hpp>

#include <concepts>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <vector>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient {
namespace Private {
struct ConnectionState;
struct ContainerTarget;
} // namespace Private

template <HttpClientLike THttp> class BasicBlobServiceClient;

struct BlobContainerClientOptions {
    std::string ServiceEndpoint;
    std::string ContainerName;
    // Authorization precedence is SAS > SharedKey > TokenCredential > BearerToken (see BlobClientOptions).
    // BearerToken/TokenCredential require https unless the host is loopback (see BlobClientOptions).
    std::string SasToken;
    std::string BearerToken;
    std::shared_ptr<ITokenCredential> TokenCredential;
    std::vector<std::string> TokenScopes{"https://storage.azure.com/.default"};
    SharedKeyCredentialOptions SharedKey;
    std::string ApiVersion = std::string(DefaultApiVersion);

    RetryOptions Retry;
    HttpRequestOptions DefaultRequestOptions;
};

// Container operations, and the factory for clients of the blobs it contains.
//
// Thread safety: concurrent ...Async calls on one instance are supported. Stream and span arguments are
// not synchronized by the library and must not be touched until the operation completes. Completion
// handlers are not serialized with each other and may run inline, before the initiating call returns.
//
// Lifetime: stores a non-owning reference to the HTTP client, which must outlive this client and every
// in-flight ...Async operation; destroying either before a completion handler runs is undefined behavior.
//
// Cancellation: HttpRequestOptions carries a boost::asio::cancellation_slot that aborts in-flight work
// (completing with an operation_aborted-flavored BlobStorageError); a slot associated with the
// completion token is adopted when HttpRequestOptions has none.
//
// Completion tokens: every ...Async operation accepts any Boost.Asio CompletionToken (callback,
// use_future, use_awaitable, ...) and completes with std::expected<Response<T>, BlobStorageError>.
// Omitting the token yields a deferred, directly co_await-able operation, e.g. `co_await client.ListBlobsAsync();`.
// Overloads that also take an options struct exclude it from the defaulted token via `requires`.
template <HttpClientLike THttp> class BasicBlobContainerClient {
  public:
    using CreateCompletionHandler =
        std::move_only_function<void(std::expected<Response<Models::CreateBlobContainerResult>, BlobStorageError>)>;
    using ListBlobsCompletionHandler =
        std::move_only_function<void(std::expected<Response<Models::ListBlobsResult>, BlobStorageError>)>;

    using BlobClientType = BasicBlobClient<THttp>;
    using BlockBlobClientType = BasicBlockBlobClient<THttp>;
    using PageBlobClientType = BasicPageBlobClient<THttp>;

    BasicBlobContainerClient(THttp& httpClient, const BlobContainerClientOptions& options);
    BasicBlobContainerClient(const BasicBlobContainerClient&) = delete;
    BasicBlobContainerClient& operator=(const BasicBlobContainerClient&) = delete;
    BasicBlobContainerClient(BasicBlobContainerClient&&) noexcept = default;
    BasicBlobContainerClient& operator=(BasicBlobContainerClient&&) noexcept = default;
    virtual ~BasicBlobContainerClient() = default;

    // The executor associated with this client's underlying HTTP client; usable as the default
    // executor argument for a boost::asio::default_completion_token_t, or to schedule work that
    // must run on the same executor as this client's completions.
    using executor_type = typename THttp::executor_type;

    [[nodiscard]] executor_type get_executor() const { return m_httpClient->get_executor(); }

    // Request options (timeout, response body limit, cancellation slot) applied to every operation
    // that is not given its own via WithRequestOptions(...) or the trailing requestOptions argument.
    [[nodiscard]] const HttpRequestOptions& GetDefaultRequestOptions() const noexcept;

    // The default completion token used when a ...Async call omits an explicit token
    // (matching this client's executor_type, per boost::asio::default_completion_token).
    using DefaultCompletionToken = boost::asio::default_completion_token<executor_type>::type;

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateBlobContainerOptions>)
    [[nodiscard]] auto CreateAsync(CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this, &BasicBlobContainerClient::CreateAsyncImpl, std::forward<CompletionToken>(token),
            std::move(requestOptions), CreateBlobContainerOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateAsync(CreateBlobContainerOptions options, CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this, &BasicBlobContainerClient::CreateAsyncImpl, std::forward<CompletionToken>(token),
            std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ListBlobsOptions>)
    [[nodiscard]] auto ListBlobsAsync(CompletionToken&& token = CompletionToken{},
                                      std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ListBlobsResult>(
            this, &BasicBlobContainerClient::ListBlobsAsyncImpl, std::forward<CompletionToken>(token),
            std::move(requestOptions), ListBlobsOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ListBlobsAsync(ListBlobsOptions options, CompletionToken&& token = CompletionToken{},
                                      std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ListBlobsResult>(
            this, &BasicBlobContainerClient::ListBlobsAsyncImpl, std::forward<CompletionToken>(token),
            std::move(requestOptions), std::move(options));
    }

    // Suppressed-error contract (applies to every *IfNotExists / *IfExists method): when the target already
    // exists (or, for delete, is absent) the expected is engaged, Value() is default-constructed, and
    // Response::Error() carries the suppressed service error. Any other failure arrives as
    // `unexpected(BlobStorageError)`. Check Response::Error() before treating the call as a real success.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateBlobContainerOptions>)
    [[nodiscard]] auto CreateIfNotExistsAsync(CompletionToken&& token = CompletionToken{},
                                              std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this, &BasicBlobContainerClient::CreateIfNotExistsAsyncImpl, std::forward<CompletionToken>(token),
            std::move(requestOptions), CreateBlobContainerOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateIfNotExistsAsync(CreateBlobContainerOptions options,
                                              CompletionToken&& token = CompletionToken{},
                                              std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this, &BasicBlobContainerClient::CreateIfNotExistsAsyncImpl, std::forward<CompletionToken>(token),
            std::move(requestOptions), std::move(options));
    }

    [[nodiscard]] virtual std::unique_ptr<BlobClientType> GetBlobClient(std::string blobName) const;
    [[nodiscard]] virtual std::unique_ptr<BlockBlobClientType> GetBlockBlobClient(std::string blobName) const;
    [[nodiscard]] virtual std::unique_ptr<PageBlobClientType> GetPageBlobClient(std::string blobName) const;

  private:
    template <HttpClientLike> friend class BasicBlobServiceClient;
    // Shares the service client's validated connection state (no re-normalisation).
    BasicBlobContainerClient(THttp& httpClient, std::shared_ptr<const Private::ConnectionState> connection,
                             std::string containerName);

    // Type-erased operation entry points; virtual so tests can substitute a mock (see BasicBlobClient).
    virtual void CreateAsyncImpl(const CreateBlobContainerOptions& options, CreateCompletionHandler completion,
                                 HttpRequestOptions requestOptions = {});
    virtual void ListBlobsAsyncImpl(ListBlobsOptions options, ListBlobsCompletionHandler completion,
                                    HttpRequestOptions requestOptions = {});
    static void ListBlobsPageAsync(THttp& httpClient, const std::shared_ptr<const Private::ContainerTarget>& target,
                                   ListBlobsOptions options, ListBlobsCompletionHandler completion,
                                   HttpRequestOptions requestOptions);
    virtual void CreateIfNotExistsAsyncImpl(const CreateBlobContainerOptions& options,
                                            CreateCompletionHandler completion, HttpRequestOptions requestOptions = {});
    THttp* m_httpClient = nullptr;
    std::shared_ptr<const Private::ContainerTarget> m_target;
};

using BlobContainerClient = BasicBlobContainerClient<HttpClient>;
} // namespace AVEVA::AzureClient
