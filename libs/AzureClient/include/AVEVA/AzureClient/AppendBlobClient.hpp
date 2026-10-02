#pragma once
#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <concepts>
#include <cstddef>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient
{
    // Append blob operations; the common blob operations, lifetime, cancellation and completion-token
    // contracts come from BlobClient.
    class AppendBlobClient final : public BlobClient
    {
      public:
        using CreateCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::CreateAppendBlobResult>, BlobStorageError>)>;
        using AppendBlockCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::AppendBlockResult>, BlobStorageError>)>;
        using SealCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::SealAppendBlobResult>, BlobStorageError>)>;

        AppendBlobClient(IHttpClient& httpClient, const BlobClientOptions& options);

        // Type-preserving equivalents of BlobClient::WithSnapshot / WithVersionId (see BlobClient).
        [[nodiscard]] AppendBlobClient WithSnapshot(std::string snapshot) const;
        [[nodiscard]] AppendBlobClient WithVersionId(std::string versionId) const;

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateAppendBlobOptions>)
        [[nodiscard]] auto CreateAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateAppendBlobResult>(this,
                &AppendBlobClient::CreateAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                CreateAppendBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CreateAsync(CreateAppendBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateAppendBlobResult>(this,
                &AppendBlobClient::CreateAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // Creates the append blob unless it already exists. An existing blob is reported as success with
        // Response::Error() carrying the suppressed BlobAlreadyExists/ConditionNotMet error.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreateAppendBlobOptions>)
        [[nodiscard]] auto CreateIfNotExistsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateAppendBlobResult>(this,
                &AppendBlobClient::CreateIfNotExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                CreateAppendBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CreateIfNotExistsAsync(CreateAppendBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreateAppendBlobResult>(this,
                &AppendBlobClient::CreateIfNotExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // Seals the append blob, making it read-only (Append Blob Seal).
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, SealAppendBlobOptions>)
        [[nodiscard]] auto SealAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::SealAppendBlobResult>(this,
                &AppendBlobClient::SealAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                SealAppendBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto SealAsync(SealAppendBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::SealAppendBlobResult>(this,
                &AppendBlobClient::SealAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // WARNING: this overload sets no append-position condition, so an automatically retried append (for
        // example after a timeout where the first attempt actually succeeded) may append the same block twice.
        // Prefer the options overload with AppendBlockOptions::IfAppendPositionEqual, or set MaxRetries = 0.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, AppendBlockOptions>)
        [[nodiscard]] auto AppendBlockAsync(std::string content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::AppendBlockResult>(this,
                &AppendBlobClient::AppendBlockStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(content),
                AppendBlockOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto AppendBlockAsync(std::string content,
            AppendBlockOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::AppendBlockResult>(this,
                &AppendBlobClient::AppendBlockStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(content),
                std::move(options));
        }

        // `content` is read when the operation is *initiated*; it must stay valid until then. With the default
        // deferred completion token, initiation happens when the returned operation is `co_await`ed or
        // launched, which may be long after this call returns. See the README lifetime section.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, AppendBlockOptions>)
        [[nodiscard]] auto AppendBlockAsync(std::span<const std::byte> content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::AppendBlockResult>(this,
                &AppendBlobClient::AppendBlockBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                content,
                AppendBlockOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto AppendBlockAsync(std::span<const std::byte> content,
            AppendBlockOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::AppendBlockResult>(this,
                &AppendBlobClient::AppendBlockBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                content,
                std::move(options));
        }

      private:
        friend class BlobContainerClient;
        AppendBlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target);

        void CreateAsyncImpl(const CreateAppendBlobOptions& options,
            CreateCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void CreateIfNotExistsAsyncImpl(CreateAppendBlobOptions options,
            CreateCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void SealAsyncImpl(const SealAppendBlobOptions& options,
            SealCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void AppendBlockBytesAsyncImpl(std::span<const std::byte> content,
            const AppendBlockOptions& options,
            AppendBlockCompletionHandler completion,
            HttpRequestOptions requestOptions);
        // Moves the caller's std::string straight into the request body (no copy via a byte span).
        void AppendBlockStringAsyncImpl(std::string content,
            const AppendBlockOptions& options,
            AppendBlockCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void SendAppendBlockRequest(HttpRequest request,
            const AppendBlockOptions& options,
            AppendBlockCompletionHandler completion,
            HttpRequestOptions requestOptions);
    };
} // namespace AVEVA::AzureClient
