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
    // contracts come from BlobClient. Multi-request operations (UploadFromAsync) keep the IHttpClient
    // requirement until their single completion; cancelling aborts whichever request is in flight.
    class BlockBlobClient final : public BlobClient
    {
      public:
        using UploadCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::UploadBlockBlobResult>, BlobStorageError>)>;
        using StageBlockCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::StageBlockResult>, BlobStorageError>)>;
        using CommitBlockListCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::CommitBlockListResult>, BlobStorageError>)>;
        using GetBlockListCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::GetBlockListResult>, BlobStorageError>)>;

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

        // Zero-copy upload: the operation shares ownership of `content` until it completes and sends it without
        // copying. `content` must not be modified while the operation is in flight. A null pointer completes with
        // std::errc::invalid_argument.
        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadAsync(std::shared_ptr<const std::vector<std::byte>> content,
            UploadBlockBlobOptions options = {},
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadSharedBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(content),
                std::move(options));
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
        [[deprecated(
            "Use StageBlockOptions: Put Block ignores every other UploadBlockBlobOptions field.")]] [[nodiscard]] auto
        StageBlockAsync(std::string blockId,
            std::string content,
            const UploadBlockBlobOptions& options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return StageBlockAsync(std::move(blockId),
                std::move(content),
                ToStageBlockOptions(options),
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
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
        [[deprecated(
            "Use StageBlockOptions: Put Block ignores every other UploadBlockBlobOptions field.")]] [[nodiscard]] auto
        StageBlockAsync(std::string blockId,
            std::span<const std::byte> content,
            const UploadBlockBlobOptions& options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return StageBlockAsync(std::move(blockId),
                content,
                ToStageBlockOptions(options),
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
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

        // Put Block From URL: stages `blockId` with content read by the service from `sourceUri`.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, StageBlockFromUriOptions>)
        [[nodiscard]] auto StageBlockFromUriAsync(std::string blockId,
            std::string sourceUri,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::StageBlockResult>(this,
                &BlockBlobClient::StageBlockFromUriAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockId),
                std::move(sourceUri),
                StageBlockFromUriOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto StageBlockFromUriAsync(std::string blockId,
            std::string sourceUri,
            StageBlockFromUriOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::StageBlockResult>(this,
                &BlockBlobClient::StageBlockFromUriAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(blockId),
                std::move(sourceUri),
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto GetBlockListAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::GetBlockListResult>(this,
                &BlockBlobClient::GetBlockListAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CreateIfNotExistsAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return CreateIfNotExistsAsync(UploadBlockBlobOptions{},
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CreateIfNotExistsAsync(UploadBlockBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::CreateIfNotExistsAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

        // UploadFromAsync (path or istream): file/stream reads run on the client's executor (the threads completing
        // HTTP requests), so a slow disk or blocking stream stalls other in-flight requests; use a dedicated
        // io_context or a fast stream. To cancel, emit the signal from the handler's executor/strand.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadFromOptions>)
        [[nodiscard]] auto UploadFromAsync(const std::filesystem::path& path,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadFromFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                path,
                UploadFromOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadFromAsync(const std::filesystem::path& path,
            UploadFromOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadFromFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                path,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadFromOptions>)
        [[nodiscard]] auto UploadFromAsync(const std::string& path,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadFromFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::filesystem::path{path},
                UploadFromOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadFromAsync(const std::string& path,
            UploadFromOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadFromFileAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::filesystem::path{path},
                std::move(options));
        }

        // `stream` is captured by reference and must outlive the operation.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadFromOptions>)
        [[nodiscard]] auto UploadFromAsync(std::istream& stream,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadFromStreamAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::ref(stream),
                UploadFromOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadFromAsync(std::istream& stream,
            UploadFromOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(this,
                &BlockBlobClient::UploadFromStreamAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::ref(stream),
                std::move(options));
        }

      private:
        [[nodiscard]] static StageBlockOptions ToStageBlockOptions(const UploadBlockBlobOptions& options)
        {
            return StageBlockOptions{.Conditions = options.Conditions,
                .TransactionalContentMd5 = options.TransactionalContentMd5,
                .TransactionalContentCrc64 = options.TransactionalContentCrc64};
        }

        friend class BlobContainerClient;
        BlockBlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target);

        void UploadBytesAsyncImpl(std::span<const std::byte> content,
            const UploadBlockBlobOptions& options,
            UploadCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void UploadSharedBytesAsyncImpl(std::shared_ptr<const std::vector<std::byte>> content,
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
        void GetBlockListAsyncImpl(GetBlockListCompletionHandler completion, HttpRequestOptions requestOptions);
        void StageBlockFromUriAsyncImpl(const std::string& blockId,
            const std::string& sourceUri,
            StageBlockFromUriOptions options,
            StageBlockCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void CreateIfNotExistsAsyncImpl(UploadBlockBlobOptions options,
            UploadCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void UploadFromFileAsyncImpl(const std::filesystem::path& path,
            UploadFromOptions options,
            UploadCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void UploadFromStreamAsyncImpl(std::istream& stream,
            UploadFromOptions options,
            UploadCompletionHandler completion,
            HttpRequestOptions requestOptions);
    };
} // namespace AVEVA::AzureClient
