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
/// <summary>The size of a page; every page blob offset and length is a multiple of it.</summary>
inline constexpr std::size_t PageBlobPageSize = 512;
/// <summary>Largest range one Put Page From URL request can copy.</summary>
inline constexpr std::uint64_t MaxPagesFromUrlBytes = std::uint64_t{4} * 1024U * 1024U;

namespace Private {
/// <summary>The value of the x-ms-blob-type header that makes Put Blob create a page blob.</summary>
constexpr std::string_view PageBlobTypeValue = "PageBlob";
/// <summary>The value of the x-ms-page-write header that makes Put Page write the given range.</summary>
constexpr std::string_view PageWriteUpdateValue = "update";

/// <summary>
/// Checks that a byte value is a multiple of the page size.
/// </summary>
/// <param name="value">The offset or length to check.</param>
/// <param name="fieldName">The name that the validation message gives to the value.</param>
/// <returns>The validation message; empty when the value is aligned.</returns>
[[nodiscard]] std::optional<std::string> ValidatePageAligned(std::uint64_t value,
                                                             gsl::not_null<gsl::czstring> fieldName);

/// <summary>
/// Builds the x-ms-range value for a page write, or a validation message.
/// </summary>
/// <param name="offset">The first byte of the range.</param>
/// <param name="length">The number of bytes in the range.</param>
/// <returns>The header value, or the validation message when the range is not valid.</returns>
[[nodiscard]] std::expected<std::string, std::string> BuildPageWriteRangeHeaderValue(std::uint64_t offset,
                                                                                     std::uint64_t length);
} // namespace Private

/// <summary>
/// Page blob operations; the common blob operations, lifetime and cancellation contracts come from BlobClient.
/// </summary>
/// <remarks>
/// Alignment: every <c>offset</c>, <c>length</c>, <c>contentLength</c> and <c>newSize</c> is a byte value and must be
/// a multiple of PageBlobPageSize (512). Misaligned values are rejected before any request is sent: the
/// operation yields std::errc::invalid_argument.
///
/// Upload arguments are read (and the request built) when the operation is called, not when it is awaited, so a
/// span only has to stay valid until the call returns.
/// </remarks>
/// <typeparam name="THttp">The HTTP transport the client sends its requests with.</typeparam>
/// <typeparam name="TCred">The token credential the client authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred>
class BasicPageBlobClient : public BasicBlobClient<THttp, TCred> {
    using Base = BasicBlobClient<THttp, TCred>;
    using Target = typename Base::Target;
    /// <summary>A request built (and validated) eagerly, or the validation message.</summary>
    using Prepared = std::expected<HttpRequest, std::string>;

  public:
    /// <summary>
    /// Creates a client that borrows the HTTP client.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with; it must outlive this client and its
    /// operations.</param>
    /// <param name="options">The blob, endpoint and authentication settings.</param>
    /// <param name="credential">The token credential to authorize with; null when the options carry the
    /// authentication (SAS, Shared Key or a static bearer token).</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="options"/> are invalid.</exception>
    BasicPageBlobClient(THttp& httpClient, const BlobClientOptions& options,
                        std::shared_ptr<TCred> credential = nullptr)
        : Base(httpClient, options, std::move(credential)) {}

    /// <summary>
    /// Creates a client that shares ownership of the HTTP client for as long as this client or any operation started
    /// from it exists.
    /// </summary>
    /// <param name="httpClient">The HTTP client to send requests with.</param>
    /// <param name="options">The blob, endpoint and authentication settings.</param>
    /// <param name="credential">The token credential to authorize with; null when the options carry the
    /// authentication (SAS, Shared Key or a static bearer token).</param>
    /// <exception cref="std::invalid_argument">Thrown when <paramref name="httpClient"/> is null or
    /// <paramref name="options"/> are invalid.</exception>
    BasicPageBlobClient(std::shared_ptr<THttp> httpClient, const BlobClientOptions& options,
                        std::shared_ptr<TCred> credential = nullptr)
        : Base(std::move(httpClient), options, std::move(credential)) {}

    /// <summary>
    /// Creates the page blob. Unlike CreateIfNotExistsAsync it adds no condition of its own, so an existing blob is
    /// replaced unless <paramref name="options"/> carry a condition that prevents it.
    /// </summary>
    /// <param name="contentLength">The size of the blob in bytes; a multiple of PageBlobPageSize.</param>
    /// <param name="options">The metadata and request conditions of the new blob.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time of the blob, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::CreatePageBlobResult>>
    CreateAsync(std::uint64_t contentLength, CreatePageBlobOptions options = {},
                std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return SendPrepared<Models::CreatePageBlobResult>(
            this->Http(), this->SharedTarget(), this->HttpOwner(), BuildCreate(contentLength, options),
            Private::ParseETagAndLastModified<Models::CreatePageBlobResult>, this->Resolve(std::move(requestOptions)),
            ReplaySafety::Unsafe, {});
    }

    /// <summary>
    /// Creates the page blob unless it already exists.
    /// </summary>
    /// <remarks>
    /// Suppressed-error contract: if the blob already exists nothing is changed, the expected is engaged,
    /// Value() is default-constructed and Response::Error() carries the suppressed service error.
    /// Replay ambiguity: when a transient failure after the request was sent causes an automatic retry, the retry can
    /// find the blob this very call created and report it as already existing. An engaged Error() therefore means
    /// "the blob exists", not "another caller created it".
    ///
    /// Only a LeaseId and an If-None-Match of "*" are accepted: any other validator could either let the PUT overwrite
    /// an existing blob or make a 412 mean something other than "the blob exists".
    /// </remarks>
    /// <param name="contentLength">The size of the blob in bytes; a multiple of PageBlobPageSize.</param>
    /// <param name="options">The metadata and request conditions of the new blob.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time of the blob, or the error that failed the request.</returns>
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
            ReplaySafety::Safe, {BlobStorageErrorCode::BlobAlreadyExists, BlobStorageErrorCode::ConditionNotMet});
    }

    /// <summary>
    /// Writes <paramref name="content"/> to the blob starting at <paramref name="offset"/>.
    /// </summary>
    /// <param name="offset">The first byte to write; a multiple of PageBlobPageSize.</param>
    /// <param name="content">The bytes to write; its size must be a multiple of PageBlobPageSize.</param>
    /// <param name="options">The request conditions of the write.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time after the write, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    UploadPagesAsync(std::uint64_t offset, std::string content, UploadPagesOptions options = {},
                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        auto prepared = BuildUploadPages(offset, content.size(), options);
        if (prepared) {
            prepared->SetBody(std::move(content));
        }
        return SendUpload(std::move(prepared), std::move(requestOptions));
    }

    /// <summary>
    /// Zero-copy upload: the request body views <paramref name="content"/>, which the request keeps alive until the
    /// HTTP operation (including retries) has finished, so the caller may drop its reference immediately.
    /// </summary>
    /// <param name="offset">The first byte to write; a multiple of PageBlobPageSize.</param>
    /// <param name="content">The bytes to write; its size must be a multiple of PageBlobPageSize. A null pointer
    /// is rejected with std::errc::invalid_argument.</param>
    /// <param name="options">The request conditions of the write.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time after the write, or the error that failed the request.</returns>
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

    /// <summary>
    /// Copies <paramref name="content"/> into the request body (use the shared-buffer overload above to avoid the
    /// copy); the span only has to stay valid until this call returns.
    /// </summary>
    /// <param name="offset">The first byte to write; a multiple of PageBlobPageSize.</param>
    /// <param name="content">The bytes to write; its size must be a multiple of PageBlobPageSize.</param>
    /// <param name="options">The request conditions of the write.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time after the write, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    UploadPagesAsync(std::uint64_t offset, std::span<const std::byte> content, UploadPagesOptions options = {},
                     std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        auto prepared = BuildUploadPages(offset, content.size(), options);
        if (prepared) {
            prepared->SetBody(Private::BytesToString(content));
        }
        return SendUpload(std::move(prepared), std::move(requestOptions));
    }

    /// <summary>
    /// Changes the size of the blob.
    /// </summary>
    /// <param name="newSize">The new size in bytes; a multiple of PageBlobPageSize.</param>
    /// <param name="options">The request conditions of the resize.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time after the resize, or the error that failed the request.</returns>
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
            ReplaySafety::Unsafe, {});
    }

    /// <summary>
    /// Lists the pages that hold data (Get Page Ranges). A truncated result carries NextMarker; pass it back as
    /// options.Marker to continue. With Conditions.IfMatch the call fails with ConditionNotMet if the blob changed.
    /// </summary>
    /// <param name="options">The continuation marker and request conditions.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>One page of the ranges that hold data, or the error that failed the request.</returns>
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
            Private::ParseGetPageRangesResult, this->Resolve(std::move(requestOptions)), ReplaySafety::Unsafe, {});
    }

    /// <summary>
    /// Server-side copy (Put Page From URL): the service copies <paramref name="length"/> bytes at
    /// <paramref name="sourceOffset"/> of the blob at <paramref name="sourceUrl"/> into this blob at
    /// <paramref name="offset"/>; the data never passes through the client. <paramref name="offset"/>,
    /// <paramref name="sourceOffset"/> and <paramref name="length"/> must be multiples of PageBlobPageSize and
    /// <paramref name="length"/> at most MaxPagesFromUrlBytes (4 MiB); anything else is rejected before a request is
    /// sent with std::errc::invalid_argument. The destination must already exist with enough capacity. See
    /// UploadPagesFromUrlOptions for how the source is authorized.
    /// </summary>
    /// <param name="offset">The first byte of this blob to write.</param>
    /// <param name="sourceUrl">The URL of the source blob.</param>
    /// <param name="sourceOffset">The first byte of the source to copy.</param>
    /// <param name="length">The number of bytes to copy.</param>
    /// <param name="options">How the source is authorized and the request conditions.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time after the write, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    UploadPagesFromUrlAsync(std::uint64_t offset, std::string sourceUrl, std::uint64_t sourceOffset,
                            std::uint64_t length, UploadPagesFromUrlOptions options = {},
                            std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return SendUpload(BuildUploadFromUrl(offset, std::move(sourceUrl), sourceOffset, length, options),
                          std::move(requestOptions));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    /// <summary>Creates a client for a target already validated by the container client that makes it.</summary>
    /// <param name="httpClient">The HTTP client to send requests with.</param>
    /// <param name="target">The validated blob target.</param>
    /// <param name="owner">The shared owner of the HTTP client; null when it is only borrowed.</param>
    BasicPageBlobClient(THttp& httpClient, std::shared_ptr<const Target> target, std::shared_ptr<THttp> owner = nullptr)
        : Base(httpClient, std::move(target), std::move(owner)) {}

    /// <summary>
    /// Sends a request built by one of the Build functions, or fails with its validation message.
    /// </summary>
    /// <typeparam name="TModel">The model the response is parsed into.</typeparam>
    /// <typeparam name="TParser">The callable that parses the response.</typeparam>
    /// <param name="http">The HTTP client to send the request with.</param>
    /// <param name="target">The blob the request addresses.</param>
    /// <param name="prepared">The request, or the validation message that fails the operation.</param>
    /// <param name="parser">Parses the response into <typeparamref name="TModel"/>.</param>
    /// <param name="requestOptions">The resolved per-request options.</param>
    /// <param name="replaySafe">Whether the request may be sent again after a failure that leaves its outcome
    /// unknown.</param>
    /// <param name="suppressed">Service error codes that count as success, with the error attached to the
    /// response.</param>
    /// <returns>The parsed response, or the validation or request error.</returns>
    template <class TModel, class TParser>
    static boost::asio::awaitable<BlobResult<TModel>>
    SendPrepared(THttp& http, std::shared_ptr<const Target> target, std::shared_ptr<THttp> /*owner*/, Prepared prepared,
                 TParser parser, AzureRequestOptions requestOptions, ReplaySafety replaySafe,
                 std::vector<BlobStorageErrorCode> suppressed) {
        if (!prepared) {
            co_return Private::InvalidArgument(std::move(prepared.error()));
        }
        co_return co_await Private::SendAndParse<TModel>(http, *target, std::move(*prepared), parser,
                                                         std::move(requestOptions), replaySafe, std::move(suppressed));
    }

    /// <summary>Sends a page upload request with the retry rules shared by the upload overloads.</summary>
    /// <param name="prepared">The request, or the validation message that fails the operation.</param>
    /// <param name="requestOptions">Per-request options; the client's defaults apply when empty.</param>
    /// <returns>The ETag and last-modified time after the write, or the error that failed the request.</returns>
    [[nodiscard]] boost::asio::awaitable<BlobResult<Models::UploadPagesResult>>
    SendUpload(Prepared prepared, std::optional<AzureRequestOptions> requestOptions) {
        return SendPrepared<Models::UploadPagesResult>(
            this->Http(), this->SharedTarget(), this->HttpOwner(), std::move(prepared),
            Private::ParseETagAndLastModified<Models::UploadPagesResult>, this->Resolve(std::move(requestOptions)),
            ReplaySafety::Unsafe, {});
    }

    /// <summary>Validates and builds the Put Blob request for a new page blob.</summary>
    /// <param name="contentLength">The size of the blob in bytes.</param>
    /// <param name="options">The metadata and request conditions.</param>
    /// <returns>The request, or the validation message when <paramref name="contentLength"/> is misaligned.</returns>
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

    /// <summary>
    /// Checks that the conditions cannot make CreateIfNotExistsAsync overwrite an existing blob or give a 412 another
    /// meaning.
    /// </summary>
    /// <param name="conditions">The conditions the caller supplied.</param>
    /// <returns>The validation message; empty when the conditions are acceptable.</returns>
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

    /// <summary>
    /// Validates and builds everything shared by the upload overloads except the body, which the caller attaches.
    /// </summary>
    /// <param name="offset">The first byte to write.</param>
    /// <param name="contentSize">The number of bytes the caller will attach.</param>
    /// <param name="options">The request conditions.</param>
    /// <returns>The request, or the validation message when a value is misaligned or out of range.</returns>
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

    /// <summary>Validates and builds the Put Page From URL request.</summary>
    /// <param name="offset">The first byte of this blob to write.</param>
    /// <param name="sourceUrl">The URL of the source blob.</param>
    /// <param name="sourceOffset">The first byte of the source to copy.</param>
    /// <param name="length">The number of bytes to copy.</param>
    /// <param name="options">How the source is authorized and the request conditions.</param>
    /// <returns>The request, or the validation message when a value is misaligned, too large or empty.</returns>
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
        if (options.SourceAuth == SourceAuthorization::ClientCredential) {
            Private::AddHeader(request, Private::XMsCopySourceAuthorizationHeaderName,
                               Private::CopySourceClientCredentialMarker);
        }
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        return request;
    }
};

/// <summary>The production client type.</summary>
using PageBlobClient = BasicPageBlobClient<HttpClient>;
/// <summary>Explicit instantiation declaration of the production client; its members are instantiated
/// elsewhere.</summary>
extern template class BasicPageBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
