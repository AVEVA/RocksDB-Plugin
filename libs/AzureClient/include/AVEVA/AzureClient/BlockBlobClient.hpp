// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/ProtocolConstants.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <istream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>

namespace AVEVA::AzureClient {
namespace Private {
constexpr std::string_view BlockBlobTypeValue = "BlockBlob";

[[nodiscard]] inline std::string EscapeXml(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char character : value) {
        switch (character) {
        case '&':
            escaped.append("&amp;");
            break;
        case '<':
            escaped.append("&lt;");
            break;
        case '>':
            escaped.append("&gt;");
            break;
        case '"':
            escaped.append("&quot;");
            break;
        case '\'':
            escaped.append("&apos;");
            break;
        default:
            escaped.push_back(character);
            break;
        }
    }
    return escaped;
}

[[nodiscard]] inline std::string BuildBlockListBody(const std::vector<std::string>& blockIds) {
    static constexpr std::string_view Prefix = R"(<?xml version="1.0" encoding="utf-8"?><BlockList>)";
    static constexpr std::string_view Suffix = "</BlockList>";
    static constexpr std::string_view LatestOpen = "<Latest>";
    static constexpr std::string_view LatestClose = "</Latest>";

    std::size_t reserveSize = Prefix.size() + Suffix.size();
    std::optional<std::size_t> expectedBlockIdLength;
    for (const auto& blockId : blockIds) {
        Private::ValidateBlockId(blockId);
        if (expectedBlockIdLength.has_value() && *expectedBlockIdLength != blockId.size()) {
            throw std::invalid_argument("All block IDs in a block list must have the same encoded length.");
        }

        expectedBlockIdLength = blockId.size();
        reserveSize += LatestOpen.size() + LatestClose.size() + blockId.size();
    }

    std::string body;
    body.reserve(reserveSize);
    body.append(Prefix);
    for (const auto& blockId : blockIds) {
        body.append(LatestOpen);
        body.append(EscapeXml(blockId));
        body.append(LatestClose);
    }
    body.append(Suffix);
    return body;
}

[[nodiscard]] inline Models::StageBlockResult ParseStageBlockResult(const HttpResponse& response) {
    Models::StageBlockResult result;
    result.ContentMd5 = std::string{FindHeaderValue(response, Private::ContentMd5HeaderName)};
    result.ContentCrc64 = std::string{FindHeaderValue(response, XMsContentCrc64HeaderName)};
    result.RequestId = std::string{FindHeaderValue(response, XMsRequestIdHeaderName)};
    return result;
}

void inline ApplyUploadOptions(HttpRequest& request, const UploadBlockBlobOptions& options) {
    Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
    Private::ApplyMetadata(request, options.Metadata);
    Private::AddHeaderIfNotEmpty(request, Private::XMsAccessTierHeaderName, options.AccessTier.ToString());
    Private::ApplyBlobRequestConditions(request, options.Conditions);
    Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
}

[[nodiscard]] inline HttpRequest BuildStageBlockRequest(const Private::BlobTarget& clientOptions,
                                                        std::string_view blockId,
                                                        const Models::BlobRequestConditions& conditions) {
    HttpRequest request = Private::BuildBlobRequest(clientOptions, HttpMethod::Put,
                                                    "comp=block&blockid=" + Private::UrlEncode(blockId, {}));
    // Put Block only supports the lease condition; access conditions belong on Put Block List (T08).
    Private::AddHeaderIfNotEmpty(request, Private::XMsLeaseIdHeaderName, conditions.LeaseId);
    return request;
}

// Throws std::invalid_argument for invalid or inconsistent block IDs.
[[nodiscard]] inline HttpRequest BuildCommitBlockListRequest(const Private::BlobTarget& clientOptions,
                                                             const std::vector<std::string>& blockIds,
                                                             const CommitBlockListOptions& options) {
    HttpRequest request = Private::BuildBlobRequest(clientOptions, HttpMethod::Put, "comp=blocklist");
    request.SetBody(BuildBlockListBody(blockIds));
    Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
    Private::ApplyMetadata(request, options.Metadata);
    Private::AddHeaderIfNotEmpty(request, Private::XMsAccessTierHeaderName, options.AccessTier.ToString());
    Private::ApplyBlobRequestConditions(request, options.Conditions);
    return request;
}

} // namespace Private

// Block blob operations; the common blob operations, lifetime, cancellation and completion-token
// contracts come from BlobClient.
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicBlockBlobClient : public BasicBlobClient<THttp, TCred> {
    using Base = BasicBlobClient<THttp, TCred>;

  public:
    using typename Base::DefaultCompletionToken;

    BasicBlockBlobClient(THttp& httpClient, const BlobClientOptions& options,
                         std::shared_ptr<TCred> credential = nullptr)
        : Base(httpClient, options, std::move(credential)) {}
    BasicBlockBlobClient(std::shared_ptr<THttp> httpClient, const BlobClientOptions& options,
                         std::shared_ptr<TCred> credential = nullptr)
        : Base(std::move(httpClient), options, std::move(credential)) {}

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
    BasicBlockBlobClient(THttp& httpClient, std::shared_ptr<const Private::BasicBlobTarget<TCred>> target,
                         std::shared_ptr<THttp> owner = nullptr)
        : Base(httpClient, std::move(target), std::move(owner)) {}

    template <class THandler>
    void UploadBytesAsyncImpl(std::span<const std::byte> content, const UploadBlockBlobOptions& options,
                              THandler completion, HttpRequestOptions requestOptions) {
        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, Private::BlockBlobTypeValue);
        request.SetBody(Private::BytesToString(content));
        SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    // Moves the caller's std::string straight into the request body (no copy via a byte span).
    template <class THandler>
    void UploadStringAsyncImpl(std::string content, const UploadBlockBlobOptions& options, THandler completion,
                               HttpRequestOptions requestOptions) {
        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, Private::BlockBlobTypeValue);
        request.SetBody(std::move(content));
        SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void UploadSharedAsyncImpl(std::shared_ptr<const std::vector<char>> content, const UploadBlockBlobOptions& options,
                               THandler completion, HttpRequestOptions requestOptions) {
        if (!content) {
            Private::PostCompletion(this->Http(), std::move(completion),
                                    Private::MakeError<Models::UploadBlockBlobResult>(
                                        std::make_error_code(std::errc::invalid_argument), "content must not be null"));
            return;
        }
        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, Private::BlockBlobTypeValue);
        const auto bytes = std::as_bytes(std::span<const char>(*content));
        request.SetBodyView(bytes, std::move(content));
        SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void StageBlockSharedAsyncImpl(const std::string& blockId, std::shared_ptr<const std::vector<char>> content,
                                   const StageBlockOptions& options, THandler completion,
                                   HttpRequestOptions requestOptions) {
        try {
            Private::ValidateBlockId(blockId);
        } catch (const std::invalid_argument&) {
            Private::PostCompletion(
                this->Http(), std::move(completion),
                Private::MakeError<Models::StageBlockResult>(std::make_error_code(std::errc::invalid_argument)));
            return;
        }
        if (!content) {
            Private::PostCompletion(this->Http(), std::move(completion),
                                    Private::MakeError<Models::StageBlockResult>(
                                        std::make_error_code(std::errc::invalid_argument), "content must not be null"));
            return;
        }

        HttpRequest request = Private::BuildStageBlockRequest(this->Target(), blockId, options.Conditions);
        Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
        const auto bytes = std::as_bytes(std::span<const char>(*content));
        request.SetBodyView(bytes, std::move(content));
        SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
    }

    template <class THandler>
    void SendUploadRequest(HttpRequest request, const UploadBlockBlobOptions& options, THandler completion,
                           HttpRequestOptions requestOptions) {
        Private::ApplyUploadOptions(request, options);

        Private::SendAuthorizedRequestAsync(
            this->Http(), this->Target(), std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
                Private::CompleteParsed<Models::UploadBlockBlobResult>(
                    error, std::move(response), Private::ParseETagAndLastModified<Models::UploadBlockBlobResult>,
                    std::move(completion));
            },
            requestOptions);
    }

    template <class THandler>
    void StageBlockBytesAsyncImpl(const std::string& blockId, std::span<const std::byte> content,
                                  const StageBlockOptions& options, THandler completion,
                                  HttpRequestOptions requestOptions) {
        try {
            Private::ValidateBlockId(blockId);
        } catch (const std::invalid_argument&) {
            Private::PostCompletion(
                this->Http(), std::move(completion),
                Private::MakeError<Models::StageBlockResult>(std::make_error_code(std::errc::invalid_argument)));
            return;
        }

        HttpRequest request = Private::BuildStageBlockRequest(this->Target(), blockId, options.Conditions);
        Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
        request.SetBody(Private::BytesToString(content));
        SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
    }

    template <class THandler>
    void StageBlockStringAsyncImpl(const std::string& blockId, const StageBlockOptions& options, std::string content,
                                   THandler completion, HttpRequestOptions requestOptions) {
        try {
            Private::ValidateBlockId(blockId);
        } catch (const std::invalid_argument&) {
            Private::PostCompletion(
                this->Http(), std::move(completion),
                Private::MakeError<Models::StageBlockResult>(std::make_error_code(std::errc::invalid_argument)));
            return;
        }

        HttpRequest request = Private::BuildStageBlockRequest(this->Target(), blockId, options.Conditions);
        Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
        request.SetBody(std::move(content));
        SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
    }

    template <class THandler>
    void SendStageBlockRequest(HttpRequest request, THandler completion, HttpRequestOptions requestOptions) {
        Private::SendAuthorizedRequestAsync(
            this->Http(), this->Target(), std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
                Private::CompleteParsed<Models::StageBlockResult>(
                    error, std::move(response), Private::ParseStageBlockResult, std::move(completion));
            },
            requestOptions);
    }

    template <class THandler>
    void CommitBlockListAsyncImpl(const std::vector<std::string>& blockIds, const CommitBlockListOptions& options,
                                  THandler completion, HttpRequestOptions requestOptions) {
        HttpRequest request;
        try {
            request = Private::BuildCommitBlockListRequest(this->Target(), blockIds, options);
        } catch (const std::invalid_argument&) {
            Private::PostCompletion(
                this->Http(), std::move(completion),
                Private::MakeError<Models::CommitBlockListResult>(std::make_error_code(std::errc::invalid_argument)));
            return;
        }

        Private::SendAuthorizedRequestAsync(
            this->Http(), this->Target(), std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
                Private::CompleteParsed<Models::CommitBlockListResult>(
                    error, std::move(response), Private::ParseETagAndLastModified<Models::CommitBlockListResult>,
                    std::move(completion));
            },
            requestOptions);
    }
};

using BlockBlobClient = BasicBlockBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
