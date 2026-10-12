// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

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

namespace AVEVA::AzureClient {
// Block blob operations; the common blob operations, lifetime, cancellation and completion-token
// contracts come from BlobClient.
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicBlockBlobClient : public BasicBlobClient<THttp, TCred> {
    using Base = BasicBlobClient<THttp, TCred>;

  public:
    using typename Base::DefaultCompletionToken;

    BasicBlockBlobClient(THttp& httpClient, const BlobClientOptions& options,
                         std::shared_ptr<TCred> credential = nullptr);

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions>)
    [[nodiscard]] auto UploadAsync(std::string content, CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.UploadStringAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(content),
            UploadBlockBlobOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadAsync(std::string content, UploadBlockBlobOptions options,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.UploadStringAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(content), std::move(options));
    }

    // `content` is read when the operation is *initiated*; it must stay valid until then. With the default
    // deferred completion token, initiation happens when the returned operation is `co_await`ed or
    // launched, which may be long after this call returns. See the README lifetime section.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions>)
    [[nodiscard]] auto UploadAsync(std::span<const std::byte> content, CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.UploadBytesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), content, UploadBlockBlobOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadAsync(std::span<const std::byte> content, UploadBlockBlobOptions options,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.UploadBytesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), content, std::move(options));
    }

    // Zero-copy upload: the request body views `content`, which the request keeps alive until the HTTP
    // operation (including retries) has finished, so the caller may drop its reference immediately.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions>)
    [[nodiscard]] auto UploadAsync(std::shared_ptr<const std::vector<char>> content,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.UploadSharedAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(content),
            UploadBlockBlobOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadAsync(std::shared_ptr<const std::vector<char>> content, UploadBlockBlobOptions options,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadBlockBlobResult>(
            this,
            [](auto& client, auto&&... args) { client.UploadSharedAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(content), std::move(options));
    }

    // Zero-copy variant of StageBlockAsync; see UploadAsync(shared_ptr) for the lifetime contract.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions> &&
                 !std::same_as<std::remove_cvref_t<CompletionToken>, StageBlockOptions>)
    [[nodiscard]] auto StageBlockAsync(std::string blockId, std::shared_ptr<const std::vector<char>> content,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::StageBlockResult>(
            this,
            [](auto& client, auto&&... args) {
                client.StageBlockSharedAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockId), std::move(content),
            StageBlockOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto StageBlockAsync(std::string blockId, std::shared_ptr<const std::vector<char>> content,
                                       StageBlockOptions options, CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::StageBlockResult>(
            this,
            [](auto& client, auto&&... args) {
                client.StageBlockSharedAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockId), std::move(content),
            std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions> &&
                 !std::same_as<std::remove_cvref_t<CompletionToken>, StageBlockOptions>)
    [[nodiscard]] auto StageBlockAsync(std::string blockId, std::string content,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::StageBlockResult>(
            this,
            [](auto& client, auto&&... args) {
                client.StageBlockStringAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockId), StageBlockOptions{},
            std::move(content));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto StageBlockAsync(std::string blockId, std::string content, StageBlockOptions options,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::StageBlockResult>(
            this,
            [](auto& client, auto&&... args) {
                client.StageBlockStringAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockId), std::move(options),
            std::move(content));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadBlockBlobOptions> &&
                 !std::same_as<std::remove_cvref_t<CompletionToken>, StageBlockOptions>)
    [[nodiscard]] auto StageBlockAsync(std::string blockId, std::span<const std::byte> content,
                                       CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::StageBlockResult>(
            this,
            [](auto& client, auto&&... args) {
                client.StageBlockBytesAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockId), content,
            StageBlockOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto StageBlockAsync(std::string blockId, std::span<const std::byte> content,
                                       StageBlockOptions options, CompletionToken&& token = CompletionToken{},
                                       std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::StageBlockResult>(
            this,
            [](auto& client, auto&&... args) {
                client.StageBlockBytesAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockId), content,
            std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CommitBlockListOptions>)
    [[nodiscard]] auto CommitBlockListAsync(std::vector<std::string> blockIds,
                                            CompletionToken&& token = CompletionToken{},
                                            std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CommitBlockListResult>(
            this,
            [](auto& client, auto&&... args) {
                client.CommitBlockListAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockIds),
            CommitBlockListOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CommitBlockListAsync(std::vector<std::string> blockIds, CommitBlockListOptions options,
                                            CompletionToken&& token = CompletionToken{},
                                            std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CommitBlockListResult>(
            this,
            [](auto& client, auto&&... args) {
                client.CommitBlockListAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(blockIds), std::move(options));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    BasicBlockBlobClient(THttp& httpClient, std::shared_ptr<const Private::BasicBlobTarget<TCred>> target);

    template <class THandler>
    void UploadBytesAsyncImpl(std::span<const std::byte> content, const UploadBlockBlobOptions& options,
                              THandler completion, HttpRequestOptions requestOptions);
    // Moves the caller's std::string straight into the request body (no copy via a byte span).
    template <class THandler>
    void UploadStringAsyncImpl(std::string content, const UploadBlockBlobOptions& options, THandler completion,
                               HttpRequestOptions requestOptions);
    template <class THandler>
    void UploadSharedAsyncImpl(std::shared_ptr<const std::vector<char>> content, const UploadBlockBlobOptions& options,
                               THandler completion, HttpRequestOptions requestOptions);
    template <class THandler>
    void StageBlockSharedAsyncImpl(const std::string& blockId, std::shared_ptr<const std::vector<char>> content,
                                   const StageBlockOptions& options, THandler completion,
                                   HttpRequestOptions requestOptions);
    template <class THandler>
    void SendUploadRequest(HttpRequest request, const UploadBlockBlobOptions& options, THandler completion,
                           HttpRequestOptions requestOptions);
    template <class THandler>
    void StageBlockBytesAsyncImpl(const std::string& blockId, std::span<const std::byte> content,
                                  const StageBlockOptions& options, THandler completion,
                                  HttpRequestOptions requestOptions);
    template <class THandler>
    void StageBlockStringAsyncImpl(const std::string& blockId, const StageBlockOptions& options, std::string content,
                                   THandler completion, HttpRequestOptions requestOptions);
    template <class THandler>
    void SendStageBlockRequest(HttpRequest request, THandler completion, HttpRequestOptions requestOptions);
    template <class THandler>
    void CommitBlockListAsyncImpl(const std::vector<std::string>& blockIds, const CommitBlockListOptions& options,
                                  THandler completion, HttpRequestOptions requestOptions);
};

using BlockBlobClient = BasicBlockBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient

// Operation entry points are member templates, so their definitions must be visible to every user.
#include <AVEVA/AzureClient/Detail/BlockBlobClientImpl.hpp>
