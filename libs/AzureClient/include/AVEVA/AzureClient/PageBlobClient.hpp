// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

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

namespace AVEVA::AzureClient {
inline constexpr std::size_t PageBlobPageSize = 512;

// Page blob operations; the common blob operations, lifetime, cancellation and completion-token
// contracts come from BlobClient.
//
// Alignment: every `offset`, `length`, `contentLength` and `newSize` is a byte value and must be a
// multiple of PageBlobPageSize (512). Misaligned values are rejected before any request is sent: the
// completion receives std::errc::invalid_argument.
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicPageBlobClient : public BasicBlobClient<THttp, TCred> {
    using Base = BasicBlobClient<THttp, TCred>;

  public:
    using typename Base::DefaultCompletionToken;

    BasicPageBlobClient(THttp& httpClient, const BlobClientOptions& options,
                        std::shared_ptr<TCred> credential = nullptr);

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreatePageBlobOptions>)
    [[nodiscard]] auto CreateAsync(std::uint64_t contentLength, CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreatePageBlobResult>(
            this, [](auto& client, auto&&... args) { client.CreateAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), contentLength, CreatePageBlobOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateAsync(std::uint64_t contentLength, CreatePageBlobOptions options,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreatePageBlobResult>(
            this, [](auto& client, auto&&... args) { client.CreateAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), contentLength, std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesOptions>)
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::string content,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesStringAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, std::move(content),
            UploadPagesOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::string content, UploadPagesOptions options,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesStringAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, std::move(content),
            std::move(options));
    }

    // Zero-copy upload: the request body views `content`, which the request keeps alive until the HTTP
    // operation (including retries) has finished, so the caller may drop its reference immediately.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesOptions>)
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesSharedAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, std::move(content),
            UploadPagesOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                                        UploadPagesOptions options, CompletionToken&& token = CompletionToken{},
                                        std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesSharedAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, std::move(content),
            std::move(options));
    }

    // `content` is read when the operation is *initiated*; it must stay valid until then. With the default
    // deferred completion token, initiation happens when the returned operation is `co_await`ed or
    // launched, which may be long after this call returns. See the README lifetime section.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesOptions>)
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::span<const std::byte> content,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesBytesAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, content, UploadPagesOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::span<const std::byte> content,
                                        UploadPagesOptions options, CompletionToken&& token = CompletionToken{},
                                        std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesBytesAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, content, std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ClearPagesOptions>)
    [[nodiscard]] auto ClearPagesAsync(std::uint64_t offset, std::uint64_t length,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ClearPagesResult>(
            this,
            [](auto& client, auto&&... args) { client.ClearPagesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, length, ClearPagesOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ClearPagesAsync(std::uint64_t offset, std::uint64_t length, ClearPagesOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ClearPagesResult>(
            this,
            [](auto& client, auto&&... args) { client.ClearPagesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, length, std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ResizePageBlobOptions> &&
                 !std::same_as<std::remove_cvref_t<CompletionToken>, CreatePageBlobOptions>)
    [[nodiscard]] auto ResizeAsync(std::uint64_t newSize, CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ResizePageBlobResult>(
            this, [](auto& client, auto&&... args) { client.ResizeAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), newSize, ResizePageBlobOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ResizeAsync(std::uint64_t newSize, ResizePageBlobOptions options,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ResizePageBlobResult>(
            this, [](auto& client, auto&&... args) { client.ResizeAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), newSize, std::move(options));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    BasicPageBlobClient(THttp& httpClient, std::shared_ptr<const Private::BasicBlobTarget<TCred>> target);

    template <class THandler>
    void CreateAsyncImpl(std::uint64_t contentLength, const CreatePageBlobOptions& options, THandler completion,
                         HttpRequestOptions requestOptions);
    template <class THandler>
    void UploadPagesBytesAsyncImpl(std::uint64_t offset, std::span<const std::byte> content,
                                   const UploadPagesOptions& options, THandler completion,
                                   HttpRequestOptions requestOptions);
    template <class THandler>
    void UploadPagesSharedAsyncImpl(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                                    const UploadPagesOptions& options, THandler completion,
                                    HttpRequestOptions requestOptions);
    // Shared by the byte-span and shared-buffer uploads; a non-null `keepAlive` makes the body a view of
    // `content` instead of a copy.
    template <class THandler>
    void UploadPagesCoreAsync(std::uint64_t offset, std::span<const std::byte> content,
                              std::shared_ptr<const void> keepAlive, const UploadPagesOptions& options,
                              THandler completion, HttpRequestOptions requestOptions);
    // Moves the caller's std::string straight into the request body (no copy via a byte span).
    template <class THandler>
    void UploadPagesStringAsyncImpl(std::uint64_t offset, std::string content, const UploadPagesOptions& options,
                                    THandler completion, HttpRequestOptions requestOptions);
    template <class THandler>
    void SendUploadPagesRequest(HttpRequest request, const UploadPagesOptions& options, THandler completion,
                                HttpRequestOptions requestOptions);
    template <class THandler>
    void ClearPagesAsyncImpl(std::uint64_t offset, std::uint64_t length, const ClearPagesOptions& options,
                             THandler completion, HttpRequestOptions requestOptions);
    template <class THandler>
    void ResizeAsyncImpl(std::uint64_t newSize, const ResizePageBlobOptions& options, THandler completion,
                         HttpRequestOptions requestOptions);
};

using PageBlobClient = BasicPageBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient

// Operation entry points are member templates, so their definitions must be visible to every user.
#include <AVEVA/AzureClient/Detail/PageBlobClientImpl.hpp>
