// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient {
namespace Private::PageBlobClientDetail {
constexpr std::string_view PageBlobTypeValue = "PageBlob";
constexpr std::string_view PageWriteUpdateValue = "update";
constexpr std::string_view PageWriteClearValue = "clear";

[[nodiscard]] inline std::optional<std::string> ValidatePageAligned(std::uint64_t value, const char* fieldName) {
    if ((value % PageBlobPageSize) != 0U) {
        return std::string(fieldName) + " must be a multiple of 512 bytes.";
    }

    return std::nullopt;
}

// The x-ms-range value for a page write, or a validation message.
[[nodiscard]] inline std::expected<std::string, std::string> BuildPageWriteRangeHeaderValue(std::uint64_t offset,
                                                                                            std::uint64_t length) {
    if (length == 0U) {
        return std::unexpected("length must be greater than zero.");
    }
    if ((length - 1U) > (std::numeric_limits<std::uint64_t>::max() - offset)) {
        return std::unexpected("offset + length exceeds the maximum representable page range.");
    }

    return "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1U);
}
} // namespace Private::PageBlobClientDetail

using namespace Private::PageBlobClientDetail;

template <HttpClientLike THttp, TokenCredentialLike TCred>
BasicPageBlobClient<THttp, TCred>::BasicPageBlobClient(THttp& httpClient, const BlobClientOptions& options,
                                                       std::shared_ptr<TCred> credential)
    : Base(httpClient, options, std::move(credential)) {}

template <HttpClientLike THttp, TokenCredentialLike TCred>
BasicPageBlobClient<THttp, TCred>::BasicPageBlobClient(THttp& httpClient,
                                                       std::shared_ptr<const Private::BasicBlobTarget<TCred>> target)
    : Base(httpClient, std::move(target)) {}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::CreateAsyncImpl(std::uint64_t contentLength,
                                                        const CreatePageBlobOptions& options, THandler completion,
                                                        HttpRequestOptions requestOptions) {
    if (const auto validationError = ValidatePageAligned(contentLength, "contentLength"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::CreatePageBlobResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }
    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
    Private::AddHeader(request, Private::XMsBlobTypeHeaderName, PageBlobTypeValue);
    Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(contentLength));
    Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
    Private::ApplyMetadata(request, options.Metadata);
    Private::AddHeaderIfNotEmpty(request, Private::XMsAccessTierHeaderName, options.AccessTier.ToString());
    Private::ApplyBlobRequestConditions(request, options.Conditions);

    Private::SendAuthorizedRequestAsync(
        this->Http(), this->Target(), std::move(request),
        [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
            Private::CompleteParsed<Models::CreatePageBlobResult>(
                error, std::move(response), Private::ParseETagAndLastModified<Models::CreatePageBlobResult>,
                std::move(completion));
        },
        requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::UploadPagesBytesAsyncImpl(std::uint64_t offset,
                                                                  std::span<const std::byte> content,
                                                                  const UploadPagesOptions& options,
                                                                  THandler completion,
                                                                  HttpRequestOptions requestOptions) {
    UploadPagesCoreAsync(offset, content, nullptr, options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::UploadPagesSharedAsyncImpl(std::uint64_t offset,
                                                                   std::shared_ptr<const std::vector<char>> content,
                                                                   const UploadPagesOptions& options,
                                                                   THandler completion,
                                                                   HttpRequestOptions requestOptions) {
    if (!content) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), "content must not be null"));
        return;
    }
    const auto bytes = std::as_bytes(std::span<const char>(*content));
    UploadPagesCoreAsync(offset, bytes, std::move(content), options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::UploadPagesCoreAsync(std::uint64_t offset, std::span<const std::byte> content,
                                                             std::shared_ptr<const void> keepAlive,
                                                             const UploadPagesOptions& options, THandler completion,
                                                             HttpRequestOptions requestOptions) {
    if (const auto validationError = ValidatePageAligned(offset, "offset"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }
    if (const auto validationError = ValidatePageAligned(content.size(), "content size"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }

    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=page");
    Private::AddHeader(request, Private::XMsPageWriteHeaderName, PageWriteUpdateValue);
    const auto rangeHeader = BuildPageWriteRangeHeaderValue(offset, content.size());
    if (!rangeHeader.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), rangeHeader.error()));
        return;
    }
    Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
    Private::AddHeaderIfNotEmpty(request, Private::ContentMd5HeaderName, options.ContentMd5);
    if (keepAlive) {
        request.SetBodyView(content, std::move(keepAlive));
    } else {
        request.SetBody(Private::BytesToString(content));
    }
    SendUploadPagesRequest(std::move(request), options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::UploadPagesStringAsyncImpl(std::uint64_t offset, std::string content,
                                                                   const UploadPagesOptions& options,
                                                                   THandler completion,
                                                                   HttpRequestOptions requestOptions) {
    if (const auto validationError = ValidatePageAligned(offset, "offset"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }
    if (const auto validationError = ValidatePageAligned(content.size(), "content size"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }

    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=page");
    Private::AddHeader(request, Private::XMsPageWriteHeaderName, PageWriteUpdateValue);
    const auto rangeHeader = BuildPageWriteRangeHeaderValue(offset, content.size());
    if (!rangeHeader.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::UploadPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), rangeHeader.error()));
        return;
    }
    Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
    Private::AddHeaderIfNotEmpty(request, Private::ContentMd5HeaderName, options.ContentMd5);
    request.SetBody(std::move(content));
    SendUploadPagesRequest(std::move(request), options, std::move(completion), requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::SendUploadPagesRequest(HttpRequest request, const UploadPagesOptions& options,
                                                               THandler completion, HttpRequestOptions requestOptions) {
    Private::ApplyBlobRequestConditions(request, options.Conditions);

    Private::SendAuthorizedRequestAsync(
        this->Http(), this->Target(), std::move(request),
        [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
            Private::CompleteParsed<Models::UploadPagesResult>(
                error, std::move(response), Private::ParseETagAndLastModified<Models::UploadPagesResult>,
                std::move(completion));
        },
        requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::ClearPagesAsyncImpl(std::uint64_t offset, std::uint64_t length,
                                                            const ClearPagesOptions& options, THandler completion,
                                                            HttpRequestOptions requestOptions) {
    if (const auto validationError = ValidatePageAligned(offset, "offset"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::ClearPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }
    if (const auto validationError = ValidatePageAligned(length, "length"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::ClearPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }

    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=page");
    Private::AddHeader(request, Private::XMsPageWriteHeaderName, PageWriteClearValue);
    const auto rangeHeader = BuildPageWriteRangeHeaderValue(offset, length);
    if (!rangeHeader.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::ClearPagesResult>(
                                    std::make_error_code(std::errc::invalid_argument), rangeHeader.error()));
        return;
    }
    Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
    Private::ApplyBlobRequestConditions(request, options.Conditions);

    Private::SendAuthorizedRequestAsync(
        this->Http(), this->Target(), std::move(request),
        [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
            Private::CompleteParsed<Models::ClearPagesResult>(
                error, std::move(response), Private::ParseETagAndLastModified<Models::ClearPagesResult>,
                std::move(completion));
        },
        requestOptions);
}

template <HttpClientLike THttp, TokenCredentialLike TCred>
template <class THandler>
void BasicPageBlobClient<THttp, TCred>::ResizeAsyncImpl(std::uint64_t newSize, const ResizePageBlobOptions& options,
                                                        THandler completion, HttpRequestOptions requestOptions) {
    if (const auto validationError = ValidatePageAligned(newSize, "newSize"); validationError.has_value()) {
        Private::PostCompletion(this->Http(), std::move(completion),
                                Private::MakeError<Models::ResizePageBlobResult>(
                                    std::make_error_code(std::errc::invalid_argument), *validationError));
        return;
    }

    HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=properties");
    Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(newSize));
    Private::ApplyBlobRequestConditions(request, options.Conditions);

    Private::SendAuthorizedRequestAsync(
        this->Http(), this->Target(), std::move(request),
        [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
            Private::CompleteParsed<Models::ResizePageBlobResult>(
                error, std::move(response), Private::ParseETagAndLastModified<Models::ResizePageBlobResult>,
                std::move(completion));
        },
        requestOptions);
}
} // namespace AVEVA::AzureClient
