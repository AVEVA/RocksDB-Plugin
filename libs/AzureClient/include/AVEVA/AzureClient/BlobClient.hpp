// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <boost/asio/async_result.hpp>

#include <concepts>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <type_traits>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient
{
    namespace Private
    {
        struct BlobTarget;
    } // namespace Private

    class BlobContainerClient;

    // Operations common to every blob type; BlockBlobClient and PageBlobClient derive
    // from it. Usable on its own (e.g. via BlobContainerClient::GetBlobClient) when the blob type is
    // irrelevant.
    //
    // Thread safety: distinct instances are independent; concurrent ...Async calls on one instance are
    // supported (connection state is immutable once constructed). Stream and span arguments are not
    // synchronized by the library and must not be touched by the caller until the operation completes.
    // Completion handlers are not serialized with each other and may run inline, before the initiating
    // call returns, or on any thread that drives the IHttpClient.
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
    // Omitting the token yields a deferred, directly co_await-able operation (DefaultCompletionToken).
    // Overloads that also take an options struct exclude it from the defaulted token via `requires`.
    class BlobClient
    {
      public:
        using DownloadCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::DownloadBlobResult>, BlobStorageError>)>;
        using DownloadToCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>)>;
        using SetMetadataCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::SetBlobMetadataResult>, BlobStorageError>)>;
        using AcquireLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::AcquireBlobLeaseResult>, BlobStorageError>)>;
        using RenewLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::RenewBlobLeaseResult>, BlobStorageError>)>;
        using ReleaseLeaseCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::ReleaseBlobLeaseResult>, BlobStorageError>)>;
        using DeleteCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::DeleteBlobResult>, BlobStorageError>)>;
        using GetPropertiesCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::BlobProperties>, BlobStorageError>)>;
        using ExistsCompletionHandler = std::move_only_function<void(std::expected<Response<bool>, BlobStorageError>)>;

        BlobClient(IHttpClient& httpClient, const BlobClientOptions& options);
        BlobClient(const BlobClient&) = delete;
        BlobClient& operator=(const BlobClient&) = delete;
        BlobClient(BlobClient&&) noexcept = default;
        BlobClient& operator=(BlobClient&&) noexcept = default;
        ~BlobClient() = default;

        // The executor of the underlying IHttpClient (the default executor for completions).
        using executor_type = IHttpClient::executor_type;

        [[nodiscard]] executor_type get_executor() const
        {
            return m_httpClient->get_executor();
        }

        // Request options (timeout, response body limit, cancellation slot) applied to every operation
        // that is not given its own via WithRequestOptions(...) or the trailing requestOptions argument.
        [[nodiscard]] const HttpRequestOptions& GetDefaultRequestOptions() const noexcept;

        using DefaultCompletionToken = boost::asio::default_completion_token<executor_type>::type;

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DownloadBlobOptions>)
        [[nodiscard]] auto DownloadAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobResult>(this,
                &BlobClient::DownloadAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                DownloadBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DownloadAsync(DownloadBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobResult>(this,
                &BlobClient::DownloadAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // DownloadToAsync overload set (the first argument picks the sink, the options type picks the mode):
        //   (stream|path, DownloadToOptions)   - whole blob (or DownloadToOptions::Range), chunked and optionally parallel.
        //   (stream|path, token)               - whole blob with default DownloadToOptions, chunked.
        //   (stream|path, DownloadBlobOptions) - honours DownloadBlobOptions::Range; goes through the same chunked engine.
        // The std::string path overloads are deprecated forwarding wrappers for the std::filesystem::path ones.
        // Stream/file writes run on the client's executor (the threads completing HTTP requests): use a dedicated
        // io_context or a fast stream. To cancel, emit the signal from the handler's executor/strand.

        // Chunked download with an explicit ChunkSize and Concurrency (see DownloadToOptions). `stream` is
        // captured by reference and must outlive the operation.
        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DownloadToAsync(std::ostream& stream,
            DownloadToOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadToStreamAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::ref(stream),
                std::move(options));
        }

        // Chunked download into `path`, via a temporary sibling file that replaces `path` only on success.
        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path,
            DownloadToOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadToFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                path,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DownloadBlobOptions> &&
                     !std::same_as<std::remove_cvref_t<CompletionToken>, DownloadToOptions>)
        [[nodiscard]] auto DownloadToAsync(std::ostream& stream,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadToStreamAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::ref(stream),
                DownloadToOptions{});
        }

        // Downloads the range straight into `destination`, avoiding an intermediate copy of the body. The span must
        // stay valid until the operation completes. A range larger than the span fails; BytesWritten reports the
        // bytes stored when the blob ends before the requested range does.
        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DownloadToAsync(std::span<char> destination,
            DownloadBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadRangeToSpanAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                destination,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DownloadToAsync(std::ostream& stream,
            DownloadBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadRangeToStreamAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::ref(stream),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DownloadBlobOptions> &&
                     !std::same_as<std::remove_cvref_t<CompletionToken>, DownloadToOptions>)
        [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadToFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                path,
                DownloadToOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DownloadToAsync(const std::filesystem::path& path,
            DownloadBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadRangeToFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                path,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DownloadBlobOptions> &&
                     !std::same_as<std::remove_cvref_t<CompletionToken>, DownloadToOptions>)
        [[deprecated("Pass a std::filesystem::path")]] [[nodiscard]] auto DownloadToAsync(const std::string& path,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadToFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::filesystem::path{path},
                DownloadToOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[deprecated("Pass a std::filesystem::path")]] [[nodiscard]] auto DownloadToAsync(const std::string& path,
            DownloadBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DownloadBlobToResult>(this,
                &BlobClient::DownloadRangeToFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::filesystem::path{path},
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, DeleteBlobOptions>)
        [[nodiscard]] auto DeleteAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DeleteBlobResult>(this,
                &BlobClient::DeleteAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                DeleteBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DeleteAsync(DeleteBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DeleteBlobResult>(this,
                &BlobClient::DeleteAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DeleteIfExistsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return DeleteIfExistsAsync(DeleteBlobOptions{},
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
        }

        // Suppressed-error contract: a not-found response is not a failure here. The expected is engaged, the
        // Value() is default-constructed, and Response::Error() carries the suppressed service error; any other
        // failure arrives as `unexpected(BlobStorageError)`. Check Response::Error() before assuming deletion.
        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto DeleteIfExistsAsync(DeleteBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::DeleteBlobResult>(this,
                &BlobClient::DeleteIfExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, GetBlobPropertiesOptions>)
        [[nodiscard]] auto GetPropertiesAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::BlobProperties>(this,
                &BlobClient::GetPropertiesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                GetBlobPropertiesOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto GetPropertiesAsync(GetBlobPropertiesOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::BlobProperties>(this,
                &BlobClient::GetPropertiesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // Resolves to `true`/`false` for existence. Only a not-found response yields `false`; authentication,
        // throttling, timeout and transport failures still produce `unexpected(BlobStorageError)`.
        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ExistsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<bool>(this,
                &BlobClient::ExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto SetMetadataAsync(SetBlobMetadataOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::SetBlobMetadataResult>(this,
                &BlobClient::SetMetadataAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, AcquireLeaseOptions>)
        [[nodiscard]] auto AcquireLeaseAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::AcquireBlobLeaseResult>(this,
                &BlobClient::AcquireLeaseAsyncImpl,
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
                &BlobClient::AcquireLeaseAsyncImpl,
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
                &BlobClient::RenewLeaseAsyncImpl,
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
                &BlobClient::ReleaseLeaseAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

      protected:
        friend class BlobContainerClient;
        // Shares the container client's validated connection state (no re-normalisation).
        BlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target);

        // Connection state owned by this client; derived blob-type clients read it to build requests.
        [[nodiscard]] IHttpClient& HttpClient() const noexcept
        {
            return *m_httpClient;
        }

        [[nodiscard]] const Private::BlobTarget& Target() const noexcept
        {
            return *m_target;
        }

        // For operations that outlive the call and must keep the target alive.
        [[nodiscard]] const std::shared_ptr<const Private::BlobTarget>& SharedTarget() const noexcept
        {
            return m_target;
        }

      private:
        IHttpClient* m_httpClient = nullptr;
        std::shared_ptr<const Private::BlobTarget> m_target;

        void DownloadAsyncImpl(DownloadBlobOptions options,
            DownloadCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void DownloadToStreamAsyncImpl(std::ostream& stream,
            DownloadToOptions options,
            DownloadToCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void DownloadToFileAsyncImpl(const std::filesystem::path& path,
            DownloadToOptions options,
            DownloadToCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void DownloadRangeToStreamAsyncImpl(std::ostream& stream,
            DownloadBlobOptions options,
            DownloadToCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void DownloadRangeToSpanAsyncImpl(std::span<char> destination,
            DownloadBlobOptions options,
            DownloadToCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void DownloadRangeToFileAsyncImpl(const std::filesystem::path& path,
            DownloadBlobOptions options,
            DownloadToCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void DeleteAsyncImpl(const DeleteBlobOptions& options,
            DeleteCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void DeleteIfExistsAsyncImpl(const DeleteBlobOptions& options,
            DeleteCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void GetPropertiesAsyncImpl(const GetBlobPropertiesOptions& options,
            GetPropertiesCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ExistsAsyncImpl(ExistsCompletionHandler completion, HttpRequestOptions requestOptions);
        void SetMetadataAsyncImpl(const SetBlobMetadataOptions& options,
            SetMetadataCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void AcquireLeaseAsyncImpl(AcquireLeaseOptions options,
            AcquireLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void RenewLeaseAsyncImpl(RenewLeaseOptions options,
            RenewLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ReleaseLeaseAsyncImpl(ReleaseLeaseOptions options,
            ReleaseLeaseCompletionHandler completion,
            HttpRequestOptions requestOptions);
    };
} // namespace AVEVA::AzureClient
