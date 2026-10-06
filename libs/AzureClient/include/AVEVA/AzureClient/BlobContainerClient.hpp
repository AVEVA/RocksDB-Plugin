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

namespace AVEVA::AzureClient
{
    namespace Private
    {
        struct ConnectionState;
        struct ContainerTarget;
    } // namespace Private

    class BlobServiceClient;

    struct BlobContainerClientOptions
    {
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
    // Lifetime: stores a non-owning reference to the IHttpClient, which must outlive this client and every
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
    class BlobContainerClient final
    {
      public:
        using CreateCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::CreateBlobContainerResult>, BlobStorageError>)>;
        using ListBlobsCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::ListBlobsResult>, BlobStorageError>)>;

        BlobContainerClient(IHttpClient& httpClient, const BlobContainerClientOptions& options);
        BlobContainerClient(const BlobContainerClient&) = delete;
        BlobContainerClient& operator=(const BlobContainerClient&) = delete;
        BlobContainerClient(BlobContainerClient&&) noexcept = default;
        BlobContainerClient& operator=(BlobContainerClient&&) noexcept = default;
        ~BlobContainerClient() = default;

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

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateBlobContainerOptions>)
        [[nodiscard]] auto CreateAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(this,
                &BlobContainerClient::CreateAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                CreateBlobContainerOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CreateAsync(CreateBlobContainerOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(this,
                &BlobContainerClient::CreateAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ListBlobsOptions>)
        [[nodiscard]] auto ListBlobsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ListBlobsResult>(this,
                &BlobContainerClient::ListBlobsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                ListBlobsOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ListBlobsAsync(ListBlobsOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ListBlobsResult>(this,
                &BlobContainerClient::ListBlobsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // Suppressed-error contract (applies to every *IfNotExists / *IfExists method): when the target already
        // exists (or, for delete, is absent) the expected is engaged, Value() is default-constructed, and
        // Response::Error() carries the suppressed service error. Any other failure arrives as
        // `unexpected(BlobStorageError)`. Check Response::Error() before treating the call as a real success.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateBlobContainerOptions>)
        [[nodiscard]] auto CreateIfNotExistsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(this,
                &BlobContainerClient::CreateIfNotExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                CreateBlobContainerOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CreateIfNotExistsAsync(CreateBlobContainerOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateBlobContainerResult>(this,
                &BlobContainerClient::CreateIfNotExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        [[nodiscard]] BlobClient GetBlobClient(std::string blobName) const;
        [[nodiscard]] BlockBlobClient GetBlockBlobClient(std::string blobName) const;
        [[nodiscard]] PageBlobClient GetPageBlobClient(std::string blobName) const;

      private:
        friend class BlobServiceClient;
        // Shares the service client's validated connection state (no re-normalisation).
        BlobContainerClient(IHttpClient& httpClient,
            std::shared_ptr<const Private::ConnectionState> connection,
            std::string containerName);

        void CreateAsyncImpl(const CreateBlobContainerOptions& options,
            CreateCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        void ListBlobsAsyncImpl(ListBlobsOptions options,
            ListBlobsCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        static void ListBlobsPageAsync(IHttpClient& httpClient,
            const std::shared_ptr<const Private::ContainerTarget>& target,
            ListBlobsOptions options,
            ListBlobsCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void CreateIfNotExistsAsyncImpl(const CreateBlobContainerOptions& options,
            CreateCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        IHttpClient* m_httpClient = nullptr;
        std::shared_ptr<const Private::ContainerTarget> m_target;
    };
} // namespace AVEVA::AzureClient
