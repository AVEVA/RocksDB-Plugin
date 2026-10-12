// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/ClientForwardDeclarations.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
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
} // namespace Private

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
template <HttpClientLike THttp, TokenCredentialLike TCred> class BasicBlobContainerClient {
  public:
    using BlobClientType = BasicBlobClient<THttp, TCred>;
    using BlockBlobClientType = BasicBlockBlobClient<THttp, TCred>;
    using PageBlobClientType = BasicPageBlobClient<THttp, TCred>;

    BasicBlobContainerClient(THttp& httpClient, const BlobContainerClientOptions& options,
                             std::shared_ptr<TCred> credential = nullptr);
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
    [[nodiscard]] const HttpRequestOptions& GetDefaultRequestOptions() const noexcept;

    // The default completion token used when a ...Async call omits an explicit token
    // (matching this client's executor_type, per boost::asio::default_completion_token).
    using DefaultCompletionToken = boost::asio::default_completion_token<executor_type>::type;

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateBlobContainerOptions>)
    [[nodiscard]] auto CreateAsync(CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this, [](auto& client, auto&&... args) { client.CreateAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), CreateBlobContainerOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateAsync(CreateBlobContainerOptions options, CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this, [](auto& client, auto&&... args) { client.CreateAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ListBlobsOptions>)
    [[nodiscard]] auto ListBlobsAsync(CompletionToken&& token = CompletionToken{},
                                      std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ListBlobsResult>(
            this,
            [](auto& client, auto&&... args) { client.ListBlobsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), ListBlobsOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ListBlobsAsync(ListBlobsOptions options, CompletionToken&& token = CompletionToken{},
                                      std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ListBlobsResult>(
            this,
            [](auto& client, auto&&... args) { client.ListBlobsAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
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
            this,
            [](auto& client, auto&&... args) {
                client.CreateIfNotExistsAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), CreateBlobContainerOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateIfNotExistsAsync(CreateBlobContainerOptions options,
                                              CompletionToken&& token = CompletionToken{},
                                              std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(
            this,
            [](auto& client, auto&&... args) {
                client.CreateIfNotExistsAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    [[nodiscard]] std::unique_ptr<BlobClientType> GetBlobClient(std::string blobName) const;
    [[nodiscard]] std::unique_ptr<BlockBlobClientType> GetBlockBlobClient(std::string blobName) const;
    [[nodiscard]] std::unique_ptr<PageBlobClientType> GetPageBlobClient(std::string blobName) const;

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobServiceClient;
    // Shares the service client's validated connection state (no re-normalisation).
    BasicBlobContainerClient(THttp& httpClient, std::shared_ptr<const Private::ConnectionState> connection,
                             std::shared_ptr<CachedTokenCredential<TCred>> credential, std::string containerName);

    // Operation entry points (see BasicBlobClient).
    template <class THandler>
    void CreateAsyncImpl(const CreateBlobContainerOptions& options, THandler completion,
                         HttpRequestOptions requestOptions = {});
    template <class THandler>
    void ListBlobsAsyncImpl(ListBlobsOptions options, THandler completion, HttpRequestOptions requestOptions = {});
    template <class THandler>
    static void ListBlobsPageAsync(THttp& httpClient,
                                   const std::shared_ptr<const Private::BasicContainerTarget<TCred>>& target,
                                   ListBlobsOptions options, THandler completion, HttpRequestOptions requestOptions);
    template <class THandler>
    void CreateIfNotExistsAsyncImpl(const CreateBlobContainerOptions& options, THandler completion,
                                    HttpRequestOptions requestOptions = {});
    THttp* m_httpClient = nullptr;
    std::shared_ptr<const Private::BasicContainerTarget<TCred>> m_target;
};

using BlobContainerClient = BasicBlobContainerClient<HttpClient>;
} // namespace AVEVA::AzureClient

// Operation entry points are member templates, so their definitions must be visible to every user.
#include <AVEVA/AzureClient/Detail/BlobContainerClientImpl.hpp>
