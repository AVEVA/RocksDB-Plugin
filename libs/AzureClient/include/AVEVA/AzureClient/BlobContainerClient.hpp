#pragma once

#include <AVEVA/AzureClient/AppendBlobClient.hpp>
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
        using DeleteCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::DeleteBlobContainerResult>, BlobStorageError>)>;
        using GetPropertiesCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::BlobContainerProperties>, BlobStorageError>)>;
        using ListBlobsCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::ListBlobsResult>, BlobStorageError>)>;
        using ExistsCompletionHandler = std::move_only_function<void(std::expected<Response<bool>, BlobStorageError>)>;
        using FindBlobsByTagsCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::FindBlobsByTagsResult>, BlobStorageError>)>;
        using AcquireLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::AcquireBlobLeaseResult>, BlobStorageError>)>;
        using RenewLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::RenewBlobLeaseResult>, BlobStorageError>)>;
        using ChangeLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::ChangeBlobLeaseResult>, BlobStorageError>)>;
        using ReleaseLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::ReleaseBlobLeaseResult>, BlobStorageError>)>;
        using BreakLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::BreakBlobLeaseResult>, BlobStorageError>)>;

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
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DeleteBlobContainerOptions>)
        [[nodiscard]] auto DeleteAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DeleteBlobContainerResult>(this,
                &BlobContainerClient::DeleteAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                DeleteBlobContainerOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DeleteAsync(DeleteBlobContainerOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DeleteBlobContainerResult>(this,
                &BlobContainerClient::DeleteAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, GetBlobContainerPropertiesOptions>)
        [[nodiscard]] auto GetPropertiesAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::BlobContainerProperties>(this,
                &BlobContainerClient::GetPropertiesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                GetBlobContainerPropertiesOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto GetPropertiesAsync(GetBlobContainerPropertiesOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::BlobContainerProperties>(this,
                &BlobContainerClient::GetPropertiesAsyncImpl,
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

        // Resolves to `true`/`false`. Only a not-found response yields `false`; authentication, throttling,
        // timeout and transport failures still produce `unexpected(BlobStorageError)`.
        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ExistsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<bool>(this,
                &BlobContainerClient::ExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
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

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DeleteBlobContainerOptions>)
        [[nodiscard]] auto DeleteIfExistsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DeleteBlobContainerResult>(this,
                &BlobContainerClient::DeleteIfExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                DeleteBlobContainerOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DeleteIfExistsAsync(DeleteBlobContainerOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DeleteBlobContainerResult>(this,
                &BlobContainerClient::DeleteIfExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // Lists every page by following NextMarker from options.Marker and completes once with all blobs and prefixes
        // (NextMarker empty; RawResponse() is the last page's). The whole listing is held in memory; use
        // ListBlobsAsync with Marker/NextMarker to stream large listings page by page. Request options apply per page.
        // WARNING: memory use grows with the container size; set ListBlobsOptions::MaxItems to cap it (the
        // operation then fails with std::errc::value_too_large and stops requesting further pages).
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ListBlobsOptions>)
        [[nodiscard]] auto ListBlobsAllAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ListBlobsResult>(this,
                &BlobContainerClient::ListBlobsAllAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                ListBlobsOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ListBlobsAllAsync(ListBlobsOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ListBlobsResult>(this,
                &BlobContainerClient::ListBlobsAllAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // Find Blobs by Tags. `where` is the tag filter expression, e.g. "\"status\" = 'ready'"; follow NextMarker
        // via options.Marker for further pages.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, FindBlobsByTagsOptions>)
        [[nodiscard]] auto FindBlobsByTagsAsync(std::string where,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::FindBlobsByTagsResult>(this,
                &BlobContainerClient::FindBlobsByTagsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(where),
                FindBlobsByTagsOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto FindBlobsByTagsAsync(std::string where,
            FindBlobsByTagsOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::FindBlobsByTagsResult>(this,
                &BlobContainerClient::FindBlobsByTagsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(where),
                std::move(options));
        }

        // Container leases (Lease Container). Only IfModifiedSince/IfUnmodifiedSince conditions apply.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, AcquireLeaseOptions>)
        [[nodiscard]] auto AcquireLeaseAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::AcquireBlobLeaseResult>(this,
                &BlobContainerClient::AcquireLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                AcquireLeaseOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto AcquireLeaseAsync(AcquireLeaseOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::AcquireBlobLeaseResult>(this,
                &BlobContainerClient::AcquireLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto RenewLeaseAsync(RenewLeaseOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::RenewBlobLeaseResult>(this,
                &BlobContainerClient::RenewLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ChangeLeaseAsync(ChangeLeaseOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ChangeBlobLeaseResult>(this,
                &BlobContainerClient::ChangeLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ReleaseLeaseAsync(ReleaseLeaseOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ReleaseBlobLeaseResult>(this,
                &BlobContainerClient::ReleaseLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, BreakLeaseOptions>)
        [[nodiscard]] auto BreakLeaseAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::BreakBlobLeaseResult>(this,
                &BlobContainerClient::BreakLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                BreakLeaseOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto BreakLeaseAsync(BreakLeaseOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::BreakBlobLeaseResult>(this,
                &BlobContainerClient::BreakLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        [[nodiscard]] BlobClient GetBlobClient(std::string blobName) const;
        [[nodiscard]] BlockBlobClient GetBlockBlobClient(std::string blobName) const;
        [[nodiscard]] PageBlobClient GetPageBlobClient(std::string blobName) const;
        [[nodiscard]] AppendBlobClient GetAppendBlobClient(std::string blobName) const;

      private:
        friend class BlobServiceClient;
        // Shares the service client's validated connection state (no re-normalisation).
        BlobContainerClient(IHttpClient& httpClient,
            std::shared_ptr<const Private::ConnectionState> connection,
            std::string containerName);

        void CreateAsyncImpl(const CreateBlobContainerOptions& options,
            CreateCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        void DeleteAsyncImpl(const DeleteBlobContainerOptions& options,
            DeleteCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        void GetPropertiesAsyncImpl(const GetBlobContainerPropertiesOptions& options,
            GetPropertiesCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        void ListBlobsAsyncImpl(ListBlobsOptions options,
            ListBlobsCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        static void ListBlobsPageAsync(IHttpClient& httpClient,
            const std::shared_ptr<const Private::ContainerTarget>& target,
            ListBlobsOptions options,
            ListBlobsCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ListBlobsAllAsyncImpl(ListBlobsOptions options,
            ListBlobsCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void FindBlobsByTagsAsyncImpl(const std::string& where,
            const FindBlobsByTagsOptions& options,
            FindBlobsByTagsCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ExistsAsyncImpl(ExistsCompletionHandler completion, HttpRequestOptions requestOptions = {});
        void CreateIfNotExistsAsyncImpl(const CreateBlobContainerOptions& options,
            CreateCompletionHandler completion,
            HttpRequestOptions requestOptions = {});
        void DeleteIfExistsAsyncImpl(const DeleteBlobContainerOptions& options,
            DeleteCompletionHandler completion,
            HttpRequestOptions requestOptions = {});

        void AcquireLeaseAsyncImpl(AcquireLeaseOptions options,
            AcquireLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void RenewLeaseAsyncImpl(RenewLeaseOptions options,
            RenewLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ChangeLeaseAsyncImpl(ChangeLeaseOptions options,
            ChangeLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options,
            ReleaseLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void BreakLeaseAsyncImpl(BreakLeaseOptions options,
            BreakLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);

        IHttpClient* m_httpClient = nullptr;
        std::shared_ptr<const Private::ContainerTarget> m_target;
    };
} // namespace AVEVA::AzureClient
