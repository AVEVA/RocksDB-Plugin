#pragma once
#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
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
    inline constexpr std::size_t PageBlobPageSize = 512;

    // Page blob operations; the common blob operations, lifetime, cancellation and completion-token
    // contracts come from BlobClient.
    //
    // Alignment: every `offset`, `length`, `contentLength` and `newSize` is a byte value and must be a
    // multiple of PageBlobPageSize (512). Misaligned values are rejected before any request is sent: the
    // completion receives std::errc::invalid_argument.
    class PageBlobClient final : public BlobClient
    {
      public:
        using CreateCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::CreatePageBlobResult>, BlobStorageError>)>;
        using UploadPagesCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::UploadPagesResult>, BlobStorageError>)>;
        using ClearPagesCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::ClearPagesResult>, BlobStorageError>)>;
        using ResizeCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::ResizePageBlobResult>, BlobStorageError>)>;
        using GetPageRangesCompletionHandler =
            std::move_only_function<void(std::expected<Response<Models::GetPageRangesResult>, BlobStorageError>)>;

        PageBlobClient(IHttpClient& httpClient, const BlobClientOptions& options);

        // Type-preserving equivalents of BlobClient::WithSnapshot / WithVersionId (see BlobClient).
        [[nodiscard]] PageBlobClient WithSnapshot(std::string snapshot) const;
        [[nodiscard]] PageBlobClient WithVersionId(std::string versionId) const;

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreatePageBlobOptions>)
        [[nodiscard]] auto CreateAsync(std::uint64_t contentLength,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreatePageBlobResult>(this,
                &PageBlobClient::CreateAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                contentLength,
                CreatePageBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto CreateAsync(std::uint64_t contentLength,
            CreatePageBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::CreatePageBlobResult>(this,
                &PageBlobClient::CreateAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                contentLength,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesOptions>)
        [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset,
            std::string content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadPagesResult>(this,
                &PageBlobClient::UploadPagesStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                offset,
                std::move(content),
                UploadPagesOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset,
            std::string content,
            UploadPagesOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadPagesResult>(this,
                &PageBlobClient::UploadPagesStringAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                offset,
                std::move(content),
                std::move(options));
        }

        // `content` is read when the operation is *initiated*; it must stay valid until then. With the default
        // deferred completion token, initiation happens when the returned operation is `co_await`ed or
        // launched, which may be long after this call returns. See the README lifetime section.
        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesOptions>)
        [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset,
            std::span<const std::byte> content,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadPagesResult>(this,
                &PageBlobClient::UploadPagesBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                offset,
                content,
                UploadPagesOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset,
            std::span<const std::byte> content,
            UploadPagesOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::UploadPagesResult>(this,
                &PageBlobClient::UploadPagesBytesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                offset,
                content,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ClearPagesOptions>)
        [[nodiscard]] auto ClearPagesAsync(std::uint64_t offset,
            std::uint64_t length,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ClearPagesResult>(this,
                &PageBlobClient::ClearPagesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                offset,
                length,
                ClearPagesOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ClearPagesAsync(std::uint64_t offset,
            std::uint64_t length,
            ClearPagesOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ClearPagesResult>(this,
                &PageBlobClient::ClearPagesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                offset,
                length,
                std::move(options));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ResizePageBlobOptions> &&
                     !std::same_as<std::remove_cvref_t<CompletionToken>, CreatePageBlobOptions>)
        [[nodiscard]] auto ResizeAsync(std::uint64_t newSize,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ResizePageBlobResult>(this,
                &PageBlobClient::ResizeAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                newSize,
                ResizePageBlobOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto ResizeAsync(std::uint64_t newSize,
            ResizePageBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::ResizePageBlobResult>(this,
                &PageBlobClient::ResizeAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                newSize,
                std::move(options));
        }

        // Only options.Conditions was ever used; HttpHeaders, Metadata and AccessTier are ignored.
        // Deprecated; scheduled for removal in the next breaking release.
        template <class CompletionToken = DefaultCompletionToken>
        [[deprecated("Use ResizeAsync(newSize, ResizePageBlobOptions{...})")]] [[nodiscard]] auto ResizeAsync(
            std::uint64_t newSize,
            CreatePageBlobOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return ResizeAsync(newSize,
                ResizePageBlobOptions{.Conditions = std::move(options.Conditions)},
                std::forward<CompletionToken>(token),
                std::move(requestOptions));
        }

        template <class CompletionToken = DefaultCompletionToken>
            requires(!std::same_as<std::remove_cvref_t<CompletionToken>, GetPageRangesOptions>)
        [[nodiscard]] auto GetPageRangesAsync(CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::GetPageRangesResult>(this,
                &PageBlobClient::GetPageRangesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                GetPageRangesOptions{});
        }

        template <class CompletionToken = DefaultCompletionToken>
        [[nodiscard]] auto GetPageRangesAsync(GetPageRangesOptions options,
            CompletionToken&& token = CompletionToken{},
            std::optional<HttpRequestOptions> requestOptions = std::nullopt)
        {
            return Private::InitiateClientOperation<Models::GetPageRangesResult>(this,
                &PageBlobClient::GetPageRangesAsyncImpl,
                std::forward<CompletionToken>(token),
                std::move(requestOptions),
                std::move(options));
        }

      private:
        friend class BlobContainerClient;
        PageBlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target);

        void CreateAsyncImpl(std::uint64_t contentLength,
            const CreatePageBlobOptions& options,
            CreateCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void UploadPagesBytesAsyncImpl(std::uint64_t offset,
            std::span<const std::byte> content,
            const UploadPagesOptions& options,
            UploadPagesCompletionHandler completion,
            HttpRequestOptions requestOptions);
        // Moves the caller's std::string straight into the request body (no copy via a byte span).
        void UploadPagesStringAsyncImpl(std::uint64_t offset,
            std::string content,
            const UploadPagesOptions& options,
            UploadPagesCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void SendUploadPagesRequest(HttpRequest request,
            const UploadPagesOptions& options,
            UploadPagesCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ClearPagesAsyncImpl(std::uint64_t offset,
            std::uint64_t length,
            const ClearPagesOptions& options,
            ClearPagesCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void ResizeAsyncImpl(std::uint64_t newSize,
            const ResizePageBlobOptions& options,
            ResizeCompletionHandler completion,
            HttpRequestOptions requestOptions);
        void GetPageRangesAsyncImpl(GetPageRangesOptions options,
            GetPageRangesCompletionHandler completion,
            HttpRequestOptions requestOptions);
    };
} // namespace AVEVA::AzureClient
