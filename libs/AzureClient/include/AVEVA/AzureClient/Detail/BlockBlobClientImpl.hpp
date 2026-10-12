// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/ProtocolConstants.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

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
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
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
#include <utility>
#include <vector>

namespace AVEVA::AzureClient {
namespace Private::BlockBlobClientDetail {
using Private::FindHeaderValue;
using Private::XMsContentCrc64HeaderName;
using Private::XMsRequestIdHeaderName;

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

} // namespace Private::BlockBlobClientDetail

using namespace Private::BlockBlobClientDetail;

template <HttpClientLike THttp, TokenCredentialLike TCred>
BasicBlockBlobClient<THttp, TCred>::BasicBlockBlobClient(THttp& httpClient, const BlobClientOptions& options,
                                                         std::shared_ptr<TCred> credential)
    : Base(httpClient, options, std::move(credential)) {}

template <HttpClientLike THttp, TokenCredentialLike TCred>
BasicBlockBlobClient<THttp, TCred>::BasicBlockBlobClient(THttp& httpClient,
                                                         std::shared_ptr<const Private::BasicBlobTarget<TCred>> target)
    : Base(httpClient, std::move(target)) {}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::UploadBytesAsyncImpl(std::span<const std::byte> content,
                                                              const UploadBlockBlobOptions& options,
                                                              THandler completion, HttpRequestOptions requestOptions) {
    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
    Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
    request.SetBody(Private::BytesToString(content));
    SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::UploadSharedAsyncImpl(std::shared_ptr<const std::vector<char>> content,
                                                               const UploadBlockBlobOptions& options,
                                                               THandler completion, HttpRequestOptions requestOptions) {
    if (!content) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadBlockBlobResult>(
                                    std::make_error_code(std::errc::invalid_argument), "content must not be null"));
        return;
    }
    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
    Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
    const auto bytes = std::as_bytes(std::span<const char>(*content));
    request.SetBodyView(bytes, std::move(content));
    SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::StageBlockSharedAsyncImpl(const std::string& blockId,
                                                                   std::shared_ptr<const std::vector<char>> content,
                                                                   const StageBlockOptions& options,
                                                                   THandler completion,
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

    HttpRequest request = BuildStageBlockRequest(this->Target(), blockId, options.Conditions);
    Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
    const auto bytes = std::as_bytes(std::span<const char>(*content));
    request.SetBodyView(bytes, std::move(content));
    SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::UploadStringAsyncImpl(std::string content,
                                                               const UploadBlockBlobOptions& options,
                                                               THandler completion, HttpRequestOptions requestOptions) {
    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
    Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
    request.SetBody(std::move(content));
    SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::SendUploadRequest(HttpRequest request, const UploadBlockBlobOptions& options,
                                                           THandler completion, HttpRequestOptions requestOptions) {
    ApplyUploadOptions(request, options);

    Private::SendAuthorizedRequestAsync(
        this->Http(), this->Target(), std::move(request),
        [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
            Private::CompleteParsed<Models::UploadBlockBlobResult>(
                error, std::move(response), Private::ParseETagAndLastModified<Models::UploadBlockBlobResult>,
                std::move(completion));
        },
        requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::StageBlockBytesAsyncImpl(const std::string& blockId,
                                                                  std::span<const std::byte> content,
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

    HttpRequest request = BuildStageBlockRequest(this->Target(), blockId, options.Conditions);
    Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
    request.SetBody(Private::BytesToString(content));
    SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::StageBlockStringAsyncImpl(const std::string& blockId,
                                                                   const StageBlockOptions& options,
                                                                   std::string content, THandler completion,
                                                                   HttpRequestOptions requestOptions) {
    try {
        Private::ValidateBlockId(blockId);
    } catch (const std::invalid_argument&) {
        Private::PostCompletion(
            this->Http(), std::move(completion),
            Private::MakeError<Models::StageBlockResult>(std::make_error_code(std::errc::invalid_argument)));
        return;
    }

    HttpRequest request = BuildStageBlockRequest(this->Target(), blockId, options.Conditions);
    Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
    request.SetBody(std::move(content));
    SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::SendStageBlockRequest(HttpRequest request, THandler completion,
                                                               HttpRequestOptions requestOptions) {
    Private::SendAuthorizedRequestAsync(
        this->Http(), this->Target(), std::move(request),
        [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
            Private::CompleteParsed<Models::StageBlockResult>(error, std::move(response), ParseStageBlockResult,
                                                              std::move(completion));
        },
        requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicBlockBlobClient<THttp, TCred>::CommitBlockListAsyncImpl(const std::vector<std::string>& blockIds,
                                                                  const CommitBlockListOptions& options,
                                                                  THandler completion,
                                                                  HttpRequestOptions requestOptions) {
    HttpRequest request;
    try {
        request = BuildCommitBlockListRequest(this->Target(), blockIds, options);
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
} // namespace AVEVA::AzureClient
