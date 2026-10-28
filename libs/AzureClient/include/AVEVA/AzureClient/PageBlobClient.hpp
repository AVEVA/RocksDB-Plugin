// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/awaitable.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <gsl/pointers>
#include <gsl/zstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient {
inline constexpr std::size_t PageBlobPageSize = 512;
// Largest range one Put Page From URL request can copy.
inline constexpr std::uint64_t MaxPagesFromUrlBytes = std::uint64_t{4} * 1024U * 1024U;

namespace Private {
constexpr std::string_view PageBlobTypeValue = "PageBlob";
constexpr std::string_view PageWriteUpdateValue = "update";

[[nodiscard]] std::optional<std::string> ValidatePageAligned(std::uint64_t value,
                                                             gsl::not_null<gsl::czstring> fieldName);

// The x-ms-range value for a page write, or a validation message.
[[nodiscard]] std::expected<std::string, std::string> BuildPageWriteRangeHeaderValue(std::uint64_t offset,
                                                                                     std::uint64_t length);
} // namespace Private

// Page blob operations; the common blob operations, lifetime and cancellation contracts come from BlobClient.
//
// Alignment: every `offset`, `length`, `contentLength` and `newSize` is a byte value and must be a
// multiple of PageBlobPageSize (512). Misaligned values are rejected before any request is sent: the
// operation yields std::errc::invalid_argument.
//
// Upload arguments are read (and the request built) when the operation is called, not when it is awaited, so a
// span only has to stay valid until the call returns.
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicPageBlobClient : public BasicBlobClient<THttp, TCred> {
    using Base = BasicBlobClient<THttp, TCred>;
    using Target = typename Base::Target;
    // A request built (and validated) eagerly, or the validation message.
    using Prepared = std::expected<HttpRequest, std::string>;

  public:
    BasicPageBlobClient(THttp& httpClient, const BlobClientOptions& options,
                        std::shared_ptr<TCred> credential = nullptr)
        : Base(httpClient, options, std::move(credential)) {}
    BasicPageBlobClient(std::shared_ptr<THttp> httpClient, const BlobClientOptions& options,
                        std::shared_ptr<TCred> credential = nullptr)
        : Base(std::move(httpClient), options, std::move(credential)) {}

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::CreatePageBlobResult>>
    CreateAsync(std::uint64_t contentLength, CreatePageBlobOptions options = {},
                std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return SendPrepared<Models::CreatePageBlobResult>(
            this->Http(), this->SharedTarget(), this->HttpOwner(), BuildCreate(contentLength, options),
            Private::ParseETagAndLastModified<Models::CreatePageBlobResult>, this->Resolve(std::move(requestOptions)),
            false, {});
    }

    // Suppressed-error contract: if the blob already exists nothing is changed, the expected is engaged,
    // Value() is default-constructed and Response::Error() carries the suppressed service error.
    // Replay ambiguity: when a transient failure after the request was sent causes an automatic retry, the retry can
    // find the blob this very call created and report it as already existing. An engaged Error() therefore means
    // "the blob exists", not "another caller created it".
    //
    // Only a LeaseId and an If-None-Match of "*" are accepted: any other validator could either let the PUT overwrite
    // an existing blob or make a 412 mean something other than "the blob exists".
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::CreatePageBlobResult>>
    CreateIfNotExistsAsync(std::uint64_t contentLength, CreatePageBlobOptions options = {},
                           std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        Prepared prepared;
        if (auto message = ValidateCreateIfNotExistsConditions(options.Conditions)) {
            prepared = std::unexpected(std::move(*message));
        } else {
            if (options.Conditions.IfNoneMatch.empty()) {
                options.Conditions.IfNoneMatch = "*";
            }
            prepared = BuildCreate(contentLength, options);
        }
        return SendPrepared<Models::CreatePageBlobResult>(
            this->Http(), this->SharedTarget(), this->HttpOwner(), std::move(prepared),
            Private::ParseETagAndLastModified<Models::CreatePageBlobResult>, this->Resolve(std::move(requestOptions)),
            true, {BlobStorageErrorCode::BlobAlreadyExists, BlobStorageErrorCode::ConditionNotMet});
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    UploadPagesAsync(std::uint64_t offset, std::string content, UploadPagesOptions options = {},
                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        auto prepared = BuildUploadPages(offset, content.size(), options);
        if (prepared) {
            prepared->SetBody(std::move(content));
        }
        return SendUpload(std::move(prepared), std::move(requestOptions));
    }

    // Zero-copy upload: the request body views `content`, which the request keeps alive until the HTTP
    // operation (including retries) has finished, so the caller may drop its reference immediately.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    UploadPagesAsync(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                     UploadPagesOptions options = {},
                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        if (!content) {
            return SendUpload(std::unexpected(std::string("content must not be null")), std::move(requestOptions));
        }
        const auto bytes = std::as_bytes(std::span<const char>(*content));
        auto prepared = BuildUploadPages(offset, bytes.size(), options);
        if (prepared) {
            prepared->SetBodyView(bytes, std::move(content));
        }
        return SendUpload(std::move(prepared), std::move(requestOptions));
    }

    // Copies `content` into the request body (use the shared-buffer overload above to avoid the copy); the span only
    // has to stay valid until this call returns.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    UploadPagesAsync(std::uint64_t offset, std::span<const std::byte> content, UploadPagesOptions options = {},
                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        auto prepared = BuildUploadPages(offset, content.size(), options);
        if (prepared) {
            prepared->SetBody(Private::BytesToString(content));
        }
        return SendUpload(std::move(prepared), std::move(requestOptions));
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::ResizePageBlobResult>>
    ResizeAsync(std::uint64_t newSize, ResizePageBlobOptions options = {},
                std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        Prepared prepared;
        if (auto message = Private::ValidatePageAligned(newSize, "newSize")) {
            prepared = std::unexpected(std::move(*message));
        } else {
            HttpRequest request = Private::BuildBlobRequest(this->GetTarget(), HttpMethod::Put, "comp=properties");
            Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(newSize));
            Private::ApplyBlobRequestConditions(request, options.Conditions);
            prepared = std::move(request);
        }
        return SendPrepared<Models::ResizePageBlobResult>(
            this->Http(), this->SharedTarget(), this->HttpOwner(), std::move(prepared),
            Private::ParseETagAndLastModified<Models::ResizePageBlobResult>, this->Resolve(std::move(requestOptions)),
            false, {});
    }

    // Lists the pages that hold data (Get Page Ranges). A truncated result carries NextMarker; pass it back as
    // options.Marker to continue. With Conditions.IfMatch the call fails with ConditionNotMet if the blob changed.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::GetPageRangesResult>>
    GetPageRangesAsync(GetPageRangesOptions options = {},
                       std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        std::vector<std::pair<std::string, std::string>> queryParameters{{"comp", "pagelist"}};
        if (!options.Marker.empty()) {
            queryParameters.emplace_back("marker", options.Marker);
        }
        HttpRequest request =
            Private::BuildBlobRequest(this->GetTarget(), HttpMethod::Get, Private::BuildQueryString(queryParameters));
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        return SendPrepared<Models::GetPageRangesResult>(
            this->Http(), this->SharedTarget(), this->HttpOwner(), Prepared(std::move(request)),
            Private::ParseGetPageRangesResult, this->Resolve(std::move(requestOptions)), false, {});
    }

    // Server-side copy (Put Page From URL): the service copies `length` bytes at `sourceOffset` of the blob at
    // `sourceUrl` into this blob at `offset`; the data never passes through the client. `offset`, `sourceOffset` and
    // `length` must be multiples of PageBlobPageSize and `length` at most MaxPagesFromUrlBytes (4 MiB); anything else
    // is rejected before a request is sent with std::errc::invalid_argument. The destination must already exist
    // with enough capacity. See UploadPagesFromUrlOptions for how the source is authorized.
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    UploadPagesFromUrlAsync(std::uint64_t offset, std::string sourceUrl, std::uint64_t sourceOffset,
                            std::uint64_t length, UploadPagesFromUrlOptions options = {},
                            std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return SendUpload(BuildUploadFromUrl(offset, std::move(sourceUrl), sourceOffset, length, options),
                          std::move(requestOptions));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    BasicPageBlobClient(THttp& httpClient, std::shared_ptr<const Target> target, std::shared_ptr<THttp> owner = nullptr)
        : Base(httpClient, std::move(target), std::move(owner)) {}

    template <class TModel, class TParser>
    static boost::asio::awaitable<BlobResult<TModel>>
    SendPrepared(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/, Prepared prepared,
                 TParser parser, AzureRequestOptions requestOptions, bool replaySafe,
                 std::vector<BlobStorageErrorCode> suppressed) {
        if (!prepared) {
            co_return Private::InvalidArgument(std::move(prepared.error()));
        }
        co_return co_await Private::SendAndParse<TModel>(http, *target, std::move(*prepared), parser,
                                                         std::move(requestOptions), replaySafe, std::move(suppressed));
    }

    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    SendUpload(Prepared prepared, std::optional<AzureRequestOptions> requestOptions) {
        return SendPrepared<Models::UploadPagesResult>(this->Http(), this->SharedTarget(), this->HttpOwner(),
                                                       std::move(prepared),
                                                       Private::ParseETagAndLastModified<Models::UploadPagesResult>,
                                                       this->Resolve(std::move(requestOptions)), false, {});
    }

    [[nodiscard]] Prepared BuildCreate(std::uint64_t contentLength, const CreatePageBlobOptions& options) const {
        if (auto message = Private::ValidatePageAligned(contentLength, "contentLength")) {
            return std::unexpected(std::move(*message));
        }
        HttpRequest request = Private::BuildBlobRequest(this->GetTarget(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, Private::PageBlobTypeValue);
        Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(contentLength));
        Private::ApplyMetadata(request, options.Metadata);
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        return request;
    }

    [[nodiscard]] static std::optional<std::string>
    ValidateCreateIfNotExistsConditions(const Models::BlobRequestConditions& conditions) {
        if (!conditions.IfNoneMatch.empty() && conditions.IfNoneMatch != "*") {
            return "CreateIfNotExists only accepts an If-None-Match condition of \"*\".";
        }
        if (!conditions.IfMatch.empty() || conditions.IfModifiedSince.has_value() ||
            conditions.IfUnmodifiedSince.has_value()) {
            return "CreateIfNotExists does not accept IfMatch, IfModifiedSince or IfUnmodifiedSince conditions.";
        }
        return std::nullopt;
    }

    // Validates and builds everything shared by the upload overloads except the body, which the caller attaches.
    [[nodiscard]] Prepared BuildUploadPages(std::uint64_t offset, std::size_t contentSize,
                                            const UploadPagesOptions& options) const {
        if (auto message = Private::ValidatePageAligned(offset, "offset")) {
            return std::unexpected(std::move(*message));
        }
        if (auto message = Private::ValidatePageAligned(contentSize, "content size")) {
            return std::unexpected(std::move(*message));
        }
        const auto rangeHeader = Private::BuildPageWriteRangeHeaderValue(offset, contentSize);
        if (!rangeHeader.has_value()) {
            return std::unexpected(rangeHeader.error());
        }
        HttpRequest request = Private::BuildBlobRequest(this->GetTarget(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, Private::PageWriteUpdateValue);
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        return request;
    }

    [[nodiscard]] Prepared BuildUploadFromUrl(std::uint64_t offset, std::string sourceUrl, std::uint64_t sourceOffset,
                                              std::uint64_t length, const UploadPagesFromUrlOptions& options) const {
        for (auto message : {Private::ValidatePageAligned(offset, "offset"),
                             Private::ValidatePageAligned(sourceOffset, "sourceOffset"),
                             Private::ValidatePageAligned(length, "length")}) {
            if (message) {
                return std::unexpected(std::move(*message));
            }
        }
        if (length > MaxPagesFromUrlBytes) {
            return std::unexpected(std::string("length must not exceed 4 MiB."));
        }
        if (sourceUrl.empty()) {
            return std::unexpected(std::string("sourceUrl must not be empty."));
        }
        const auto rangeHeader = Private::BuildPageWriteRangeHeaderValue(offset, length);
        if (!rangeHeader.has_value()) {
            return std::unexpected(rangeHeader.error());
        }
        const auto sourceRangeHeader = Private::BuildPageWriteRangeHeaderValue(sourceOffset, length);
        if (!sourceRangeHeader.has_value()) {
            return std::unexpected(sourceRangeHeader.error());
        }

        HttpRequest request = Private::BuildBlobRequest(this->GetTarget(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, Private::PageWriteUpdateValue);
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::AddHeader(request, Private::XMsCopySourceHeaderName, sourceUrl);
        Private::AddHeader(request, Private::XMsSourceRangeHeaderName, *sourceRangeHeader);
        Private::AddHeaderIfNotEmpty(request, Private::XMsSourceIfMatchHeaderName, options.SourceIfMatch);
        if (options.AuthorizeSourceWithClientCredential) {
            Private::AddHeader(request, Private::XMsCopySourceAuthorizationHeaderName,
                               Private::CopySourceClientCredentialMarker);
        }
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        return request;
    }
};

// The production client type.
using PageBlobClient = BasicPageBlobClient<HttpClient>;
extern template class BasicPageBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
