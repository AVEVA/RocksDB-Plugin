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
#include <filesystem>
#include <functional>
#include <istream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient
{
    // Block blob operations; the common blob operations, lifetime, cancellation and completion-token
    // contracts come from BlobClient.
    class BlockBlobClient final : public BlobClient
    {
      public:
        using UploadCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::UploadBlockBlobResult>, BlobStorageError>)>;
        using StageBlockCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::StageBlockResult>, BlobStorageError>)>;
        using CommitBlockListCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::CommitBlockListResult>, BlobStorageError>)>;

        BlockBlobClient(IHttpClient& httpClient, const BlobClientOptions& options);

        // Type-preserving equivalents of BlobClient::WithSnapshot / WithVersionId (see BlobClient).
        [[nodiscard]] BlockBlobClient WithSnapshot(std::string snapshot) const;
        [[nodiscard]] BlockBlobClient WithVersionId(std::string versionId) const;

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions>)
        [[nodiscard]] auto UploadAsync(std::string content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(content),
                UploadBlockBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadAsync(std::string content,
            UploadBlockBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(content),
                std::move(options));
        }

        // `content` is read when the operation is *initiated*; it must stay valid until then. With the default
        // deferred completion token, initiation happens when the returned operation is `co_await`ed or
        // launched, which may be long after this call returns. See the README lifetime section.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions>)
        [[nodiscard]] auto UploadAsync(std::span<const std::byte> content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                content,
                UploadBlockBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadAsync(std::span<const std::byte> content,
            UploadBlockBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                content,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions> &&
                     !std::same_as<std::remove_cvref_t<CompletionToken>, StageBlockOptions>)
        [[nodiscard]] auto StageBlockAsync(std::string blockId,
            std::string content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::StageBlockResult>(this,
                &BlockBlobClient::StageBlockStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockId),
                StageBlockOptions{},
                std::move(content));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto StageBlockAsync(std::string blockId,
            std::string content,
            StageBlockOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::StageBlockResult>(this,
                &BlockBlobClient::StageBlockStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockId),
                std::move(options),
                std::move(content));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions> &&
                     !std::same_as<std::remove_cvref_t<CompletionToken>, StageBlockOptions>)
        [[nodiscard]] auto StageBlockAsync(std::string blockId,
            std::span<const std::byte> content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::StageBlockResult>(this,
                &BlockBlobClient::StageBlockBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockId),
                content,
                StageBlockOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto StageBlockAsync(std::string blockId,
            std::span<const std::byte> content,
            StageBlockOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::StageBlockResult>(this,
                &BlockBlobClient::StageBlockBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockId),
                content,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CommitBlockListOptions>)
        [[nodiscard]] auto CommitBlockListAsync(std::vector<std::string> blockIds,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CommitBlockListResult>(this,
                &BlockBlobClient::CommitBlockListAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockIds),
                CommitBlockListOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CommitBlockListAsync(std::vector<std::string> blockIds,
            CommitBlockListOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CommitBlockListResult>(this,
                &BlockBlobClient::CommitBlockListAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockIds),
                std::move(options));
        }

      private:
        friend class BlobContainerClient;
        BlockBlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target);

        void UploadBytesAsyncImpl(std::span<const std::byte> content,
            const UploadBlockBlobOptions& options,
            UploadCompletionHandler completion,
            HttpRequestOptions requestOptions);
        // Moves the caller's std::string straight into the request body (no copy via a byte span).
        void UploadStringAsyncImpl(std::string content,
            const UploadBlockBlobOptions& options,
            UploadCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void SendUploadRequest(HttpRequest request,
            const UploadBlockBlobOptions& options,
            UploadCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void StageBlockBytesAsyncImpl(const std::string& blockId,
            std::span<const std::byte> content,
            const StageBlockOptions& options,
            StageBlockCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void StageBlockStringAsyncImpl(const std::string& blockId,
            const StageBlockOptions& options,
            std::string content,
            StageBlockCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void SendStageBlockRequest(HttpRequest request,
            StageBlockCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void CommitBlockListAsyncImpl(const std::vector<std::string>& blockIds,
            const CommitBlockListOptions& options,
            CommitBlockListCompletionHandler completion,
            HttpRequestOptions requestOptions);
    };
} // namespace AVEVA::AzureClient
