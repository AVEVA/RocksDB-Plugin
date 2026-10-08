// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/ITokenCredential.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
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
#include <utility>
#include <vector>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient
{
    namespace Private
    {
        struct ConnectionState;
    } // namespace Private

    class BlobContainerClient;

    struct BlobServiceClientOptions
    {
        std::string ServiceEndpoint;
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

    // Account-level operations, and the factory for container clients.
    //
    // Thread safety: concurrent ...Async calls on one instance are supported. Caller-supplied buffers are
    // not synchronized by the library. Completion handlers are not serialized with each other and may run
    // inline, before the initiating call returns.
    //
    // Lifetime: stores a non-owning reference to the IHttpClient, which must outlive this client and every
    // in-flight ...Async operation; destroying either before a completion handler runs is undefined behavior.
    //
    // Cancellation: HttpRequestOptions carries a boost::asio::cancellation_slot that aborts in-flight work
    // (completing with an operation_aborted-flavored BlobStorageError); a slot associated with the
    // completion token is adopted when HttpRequestOptions has none.
    //
    // Completion tokens: every ...Async operation accepts any Boost.Asio CompletionToken (callback,
    // use_future, use_awaitable, ...) and completes with std::expected<Response<T>, BlobStorageError>.
    // Omitting the token yields a deferred, directly co_await-able operation, e.g. `co_await
    // client.GetBlobContainerClient(...);`. Overloads that also take an options struct exclude it from the defaulted
    // token via `requires`.
    class BlobServiceClient final
    {
      public:
        BlobServiceClient(IHttpClient& httpClient, BlobServiceClientOptions options);
        BlobServiceClient(const BlobServiceClient&) = delete;
        BlobServiceClient& operator=(const BlobServiceClient&) = delete;
        BlobServiceClient(BlobServiceClient&&) noexcept = default;
        BlobServiceClient& operator=(BlobServiceClient&&) noexcept = default;
        ~BlobServiceClient() = default;

        // The executor associated with this client's underlying IHttpClient; usable as the default
        // executor argument for a boost::asio::default_completion_token_t, or to schedule work that
        // must run on the same executor as this client's completions.
        using executor_type = IHttpClient::executor_type;

        [[nodiscard]] executor_type get_executor() const
        {
            return m_httpClient->get_executor();
        }

        // Request options (timeout, response body limit, cancellation slot) applied to every operation
        // that is not given its own via WithRequestOptions(...) or the trailing requestOptions argument.
        [[nodiscard]] const HttpRequestOptions& GetDefaultRequestOptions() const noexcept;

        // The default completion token used when a ...Async call omits an explicit token
        // (matching this client's executor_type, per boost::asio::default_completion_token).
        using DefaultCompletionToken = boost::asio::default_completion_token<executor_type>::type;

        [[nodiscard]] BlobContainerClient GetBlobContainerClient(std::string containerName) const;

      private:
        IHttpClient* m_httpClient = nullptr;
        // Shared immutable connection state referenced by child clients.
        std::shared_ptr<const Private::ConnectionState> m_connection;
    };
} // namespace AVEVA::AzureClient
