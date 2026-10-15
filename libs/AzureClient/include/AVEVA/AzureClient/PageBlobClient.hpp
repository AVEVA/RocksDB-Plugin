// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/Models/BlobModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

namespace AVEVA::AzureClient {
inline constexpr std::size_t PageBlobPageSize = 512;
// Largest range one Put Page From URL request can copy.
inline constexpr std::uint64_t MaxPagesFromUrlBytes = std::uint64_t{4} * 1024U * 1024U;

namespace Private {
constexpr std::string_view PageBlobTypeValue = "PageBlob";
constexpr std::string_view PageWriteUpdateValue = "update";

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
} // namespace Private

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
                        std::shared_ptr<TCred> credential = nullptr)
        : Base(httpClient, options, std::move(credential)) {}
    BasicPageBlobClient(std::shared_ptr<THttp> httpClient, const BlobClientOptions& options,
                        std::shared_ptr<TCred> credential = nullptr)
        : Base(std::move(httpClient), options, std::move(credential)) {}

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreatePageBlobOptions>)
    [[nodiscard]] auto CreateAsync(std::uint64_t contentLength, CompletionToken&& token = CompletionToken{},
                                   std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return CreateAsync(contentLength, CreatePageBlobOptions{}, std::forward<CompletionToken>(token),
                           std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateAsync(std::uint64_t contentLength, CreatePageBlobOptions options,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreatePageBlobResult>(
            this, [](auto& client, auto&&... args) { client.CreateAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), contentLength, std::move(options));
    }

    // Suppressed-error contract: if the blob already exists nothing is changed, the expected is engaged,
    // Value() is default-constructed and Response::Error() carries the suppressed service error.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, CreatePageBlobOptions>)
    [[nodiscard]] auto CreateIfNotExistsAsync(std::uint64_t contentLength, CompletionToken&& token = CompletionToken{},
                                              std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return CreateIfNotExistsAsync(contentLength, CreatePageBlobOptions{}, std::forward<CompletionToken>(token),
                                      std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto CreateIfNotExistsAsync(std::uint64_t contentLength, CreatePageBlobOptions options,
                                              CompletionToken&& token = CompletionToken{},
                                              std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::CreatePageBlobResult>(
            this,
            [](auto& client, auto&&... args) {
                client.CreateIfNotExistsAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), contentLength, std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesOptions>)
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::string content,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return UploadPagesAsync(offset, std::move(content), UploadPagesOptions{}, std::forward<CompletionToken>(token),
                                std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::string content, UploadPagesOptions options,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
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
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return UploadPagesAsync(offset, std::move(content), UploadPagesOptions{}, std::forward<CompletionToken>(token),
                                std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                                        UploadPagesOptions options, CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesSharedAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, std::move(content),
            std::move(options));
    }

    // Copies `content` into the request body (use the shared-buffer overload above to avoid the copy). `content` is
    // read when the operation is *initiated*; it must stay valid until then. With the default
    // deferred completion token, initiation happens when the returned operation is `co_await`ed or
    // launched, which may be long after this call returns. See the README lifetime section.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesOptions>)
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::span<const std::byte> content,
                                        CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return UploadPagesAsync(offset, content, UploadPagesOptions{}, std::forward<CompletionToken>(token),
                                std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadPagesAsync(std::uint64_t offset, std::span<const std::byte> content,
                                        UploadPagesOptions options, CompletionToken&& token = CompletionToken{},
                                        std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesBytesAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, content, std::move(options));
    }

    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, ResizePageBlobOptions> &&
                 !std::same_as<std::remove_cvref_t<CompletionToken>, CreatePageBlobOptions>)
    [[nodiscard]] auto ResizeAsync(std::uint64_t newSize, CompletionToken&& token = CompletionToken{},
                                   std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return ResizeAsync(newSize, ResizePageBlobOptions{}, std::forward<CompletionToken>(token),
                           std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto ResizeAsync(std::uint64_t newSize, ResizePageBlobOptions options,
                                   CompletionToken&& token = CompletionToken{},
                                   std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::ResizePageBlobResult>(
            this, [](auto& client, auto&&... args) { client.ResizeAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), newSize, std::move(options));
    }

    // Lists the pages that hold data (Get Page Ranges). A truncated result carries NextMarker; pass it back as
    // options.Marker to continue. With Conditions.IfMatch the call fails with ConditionNotMet if the blob changed.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, GetPageRangesOptions>)
    [[nodiscard]] auto GetPageRangesAsync(CompletionToken&& token = CompletionToken{},
                                          std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return GetPageRangesAsync(GetPageRangesOptions{}, std::forward<CompletionToken>(token),
                                  std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto GetPageRangesAsync(GetPageRangesOptions options, CompletionToken&& token = CompletionToken{},
                                          std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::GetPageRangesResult>(
            this,
            [](auto& client, auto&&... args) { client.GetPageRangesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

    // Server-side copy (Put Page From URL): the service copies `length` bytes at `sourceOffset` of the blob at
    // `sourceUrl` into this blob at `offset`; the data never passes through the client. `offset`, `sourceOffset` and
    // `length` must be multiples of PageBlobPageSize and `length` at most MaxPagesFromUrlBytes (4 MiB); anything else
    // is rejected before a request is sent with std::errc::invalid_argument. The destination must already exist
    // with enough capacity. See UploadPagesFromUrlOptions for how the source is authorized.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, UploadPagesFromUrlOptions>)
    [[nodiscard]] auto UploadPagesFromUrlAsync(std::uint64_t offset, std::string sourceUrl, std::uint64_t sourceOffset,
                                               std::uint64_t length, CompletionToken&& token = CompletionToken{},
                                               std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return UploadPagesFromUrlAsync(offset, std::move(sourceUrl), sourceOffset, length, UploadPagesFromUrlOptions{},
                                       std::forward<CompletionToken>(token), std::move(requestOptions));
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto UploadPagesFromUrlAsync(std::uint64_t offset, std::string sourceUrl, std::uint64_t sourceOffset,
                                               std::uint64_t length, UploadPagesFromUrlOptions options,
                                               CompletionToken&& token = CompletionToken{},
                                               std::optional<AzureRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::UploadPagesResult>(
            this,
            [](auto& client, auto&&... args) {
                client.UploadPagesFromUrlAsyncImpl(std::forward<decltype(args)>(args)...);
            },
            std::forward<CompletionToken>(token), std::move(requestOptions), offset, std::move(sourceUrl), sourceOffset,
            length, std::move(options));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    friend struct Private::ClientSnapshotAccess;
    BasicPageBlobClient(THttp& httpClient, std::shared_ptr<const Private::BasicBlobTarget<TCred>> target,
                        std::shared_ptr<THttp> owner = nullptr)
        : Base(httpClient, std::move(target), std::move(owner)) {}

    [[nodiscard]] BasicPageBlobClient MakeSnapshot() const {
        return BasicPageBlobClient{this->Http(), this->SharedTarget(), this->HttpOwner()};
    }

    template <class THandler>
    void GetPageRangesAsyncImpl(const GetPageRangesOptions& options, THandler completion,
                                AzureRequestOptions requestOptions) {
        const auto fail = [&](std::string message) {
            Private::FailInvalidArgument<Models::GetPageRangesResult>(this->Http(), std::move(completion),
                                                                      std::move(message));
        };
        std::optional<std::string> rangeHeader;
        if (options.Range.has_value()) {
            if (Private::ValidateOrFail<Models::GetPageRangesResult>(
                    this->Http(), completion, Private::ValidatePageAligned(options.Range->Offset, "offset"))) {
                return;
            }
            if (!options.Range->Length.has_value()) {
                // An open-ended window is the same as no window for a whole-blob scan from the offset.
                rangeHeader = "bytes=" + std::to_string(options.Range->Offset) + "-";
            } else {
                if (Private::ValidateOrFail<Models::GetPageRangesResult>(
                        this->Http(), completion, Private::ValidatePageAligned(*options.Range->Length, "length"))) {
                    return;
                }
                const auto header =
                    Private::BuildPageWriteRangeHeaderValue(options.Range->Offset, *options.Range->Length);
                if (!header.has_value()) {
                    return fail(header.error());
                }
                rangeHeader = *header;
            }
        }

        std::vector<std::pair<std::string, std::string>> queryParameters{{"comp", "pagelist"}};
        if (!options.Marker.empty()) {
            queryParameters.emplace_back("marker", options.Marker);
        }
        if (options.MaxResults.has_value()) {
            queryParameters.emplace_back("maxresults", std::to_string(*options.MaxResults));
        }
        HttpRequest request =
            Private::BuildBlobRequest(this->Target(), HttpMethod::Get, Private::BuildQueryString(queryParameters));
        if (rangeHeader.has_value()) {
            Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        }
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAndParse<Models::GetPageRangesResult>(this->Http(), this->Target(), std::move(request),
                                                           Private::ParseGetPageRangesResult, std::move(completion),
                                                           requestOptions);
    }

    [[nodiscard]] HttpRequest BuildCreateRequest(std::uint64_t contentLength,
                                                 const CreatePageBlobOptions& options) const {
        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, Private::PageBlobTypeValue);
        Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(contentLength));
        Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
        Private::ApplyMetadata(request, options.Metadata);
        Private::AddHeaderIfNotEmpty(request, Private::XMsAccessTierHeaderName, options.AccessTier.ToString());
        Private::ApplyBlobRequestConditions(request, options.Conditions);
        return request;
    }

    // Create with If-None-Match: * so an existing blob is left untouched; BlobAlreadyExists (409) and
    // ConditionNotMet (412) are suppressed per the *IfNotExists contract (Response::Error() stays engaged).
    template <class THandler>
    void CreateIfNotExistsAsyncImpl(std::uint64_t contentLength, CreatePageBlobOptions options, THandler completion,
                                    AzureRequestOptions requestOptions) {
        if (Private::ValidateOrFail<Models::CreatePageBlobResult>(
                this->Http(), completion, Private::ValidatePageAligned(contentLength, "contentLength"))) {
            return;
        }
        if (options.Conditions.IfNoneMatch.empty()) {
            options.Conditions.IfNoneMatch = "*";
        }
        Private::SendAndParse<Models::CreatePageBlobResult>(
            this->Http(), this->Target(), BuildCreateRequest(contentLength, options),
            Private::ParseETagAndLastModified<Models::CreatePageBlobResult>,
            Private::SuppressErrors<Models::CreatePageBlobResult>(
                std::move(completion),
                {BlobStorageErrorCode::BlobAlreadyExists, BlobStorageErrorCode::ConditionNotMet}),
            requestOptions, true);
    }

    template <class THandler>
    void CreateAsyncImpl(std::uint64_t contentLength, const CreatePageBlobOptions& options, THandler completion,
                         AzureRequestOptions requestOptions) {
        if (Private::ValidateOrFail<Models::CreatePageBlobResult>(
                this->Http(), completion, Private::ValidatePageAligned(contentLength, "contentLength"))) {
            return;
        }
        Private::SendAndParse<Models::CreatePageBlobResult>(
            this->Http(), this->Target(), BuildCreateRequest(contentLength, options),
            Private::ParseETagAndLastModified<Models::CreatePageBlobResult>, std::move(completion), requestOptions);
    }

    template <class THandler>
    void UploadPagesBytesAsyncImpl(std::uint64_t offset, std::span<const std::byte> content,
                                   const UploadPagesOptions& options, THandler completion,
                                   AzureRequestOptions requestOptions) {
        UploadPagesCoreAsync(offset, content, nullptr, options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void UploadPagesSharedAsyncImpl(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                                    const UploadPagesOptions& options, THandler completion,
                                    AzureRequestOptions requestOptions) {
        if (!content) {
            Private::FailInvalidArgument<Models::UploadPagesResult>(this->Http(), std::move(completion),
                                                                    "content must not be null");
            return;
        }
        const auto bytes = std::as_bytes(std::span<const char>(*content));
        UploadPagesCoreAsync(offset, bytes, std::move(content), options, std::move(completion), requestOptions);
    }

    // Shared by the byte-span and shared-buffer uploads; a non-null `keepAlive` makes the body a view of
    // `content` instead of a copy.
    template <class THandler>
    void UploadPagesCoreAsync(std::uint64_t offset, std::span<const std::byte> content,
                              std::shared_ptr<const void> keepAlive, const UploadPagesOptions& options,
                              THandler completion, AzureRequestOptions requestOptions) {
        auto request = BuildUploadPagesRequest(offset, content.size(), options, completion);
        if (!request.has_value()) {
            return;
        }
        if (keepAlive) {
            request->SetBodyView(content, std::move(keepAlive));
        } else {
            request->SetBody(Private::BytesToString(content));
        }
        SendUploadPagesRequest(std::move(*request), options, std::move(completion), requestOptions);
    }

    // Moves the caller's std::string straight into the request body (no copy via a byte span).
    template <class THandler>
    void UploadPagesStringAsyncImpl(std::uint64_t offset, std::string content, const UploadPagesOptions& options,
                                    THandler completion, AzureRequestOptions requestOptions) {
        auto request = BuildUploadPagesRequest(offset, content.size(), options, completion);
        if (!request.has_value()) {
            return;
        }
        request->SetBody(std::move(content));
        SendUploadPagesRequest(std::move(*request), options, std::move(completion), requestOptions);
    }

    // Validates and builds the headers shared by every upload overload. On a validation failure it posts the
    // error to `completion` (which is left usable only for that purpose) and returns nullopt.
    template <class THandler>
    [[nodiscard]] std::optional<HttpRequest> BuildUploadPagesRequest(std::uint64_t offset, std::size_t contentSize,
                                                                     const UploadPagesOptions& options,
                                                                     THandler& completion) {
        if (Private::ValidateOrFail<Models::UploadPagesResult>(this->Http(), completion,
                                                               Private::ValidatePageAligned(offset, "offset")) ||
            Private::ValidateOrFail<Models::UploadPagesResult>(
                this->Http(), completion, Private::ValidatePageAligned(contentSize, "content size"))) {
            return std::nullopt;
        }
        const auto rangeHeader = Private::BuildPageWriteRangeHeaderValue(offset, contentSize);
        if (!rangeHeader.has_value()) {
            Private::FailInvalidArgument<Models::UploadPagesResult>(this->Http(), std::move(completion),
                                                                    rangeHeader.error());
            return std::nullopt;
        }

        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, Private::PageWriteUpdateValue);
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::AddHeaderIfNotEmpty(request, Private::ContentMd5HeaderName, options.ContentMd5);
        return request;
    }

    template <class THandler>
    void SendUploadPagesRequest(HttpRequest request, const UploadPagesOptions& options, THandler completion,
                                AzureRequestOptions requestOptions) {
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAndParse<Models::UploadPagesResult>(this->Http(), this->Target(), std::move(request),
                                                         Private::ParseETagAndLastModified<Models::UploadPagesResult>,
                                                         std::move(completion), requestOptions);
    }

    template <class THandler>
    void UploadPagesFromUrlAsyncImpl(std::uint64_t offset, std::string sourceUrl, std::uint64_t sourceOffset,
                                     std::uint64_t length, const UploadPagesFromUrlOptions& options,
                                     THandler completion, AzureRequestOptions requestOptions) {
        const auto fail = [&](std::string message) {
            Private::FailInvalidArgument<Models::UploadPagesResult>(this->Http(), std::move(completion),
                                                                    std::move(message));
        };
        if (Private::ValidateOrFail<Models::UploadPagesResult>(this->Http(), completion,
                                                               Private::ValidatePageAligned(offset, "offset")) ||
            Private::ValidateOrFail<Models::UploadPagesResult>(
                this->Http(), completion, Private::ValidatePageAligned(sourceOffset, "sourceOffset")) ||
            Private::ValidateOrFail<Models::UploadPagesResult>(this->Http(), completion,
                                                               Private::ValidatePageAligned(length, "length"))) {
            return;
        }
        if (length > MaxPagesFromUrlBytes) {
            return fail("length must not exceed 4 MiB.");
        }
        if (sourceUrl.empty()) {
            return fail("sourceUrl must not be empty.");
        }
        const auto rangeHeader = Private::BuildPageWriteRangeHeaderValue(offset, length);
        if (!rangeHeader.has_value()) {
            return fail(rangeHeader.error());
        }
        const auto sourceRangeHeader = Private::BuildPageWriteRangeHeaderValue(sourceOffset, length);
        if (!sourceRangeHeader.has_value()) {
            return fail(sourceRangeHeader.error());
        }

        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, Private::PageWriteUpdateValue);
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::AddHeader(request, Private::XMsCopySourceHeaderName, sourceUrl);
        Private::AddHeader(request, Private::XMsSourceRangeHeaderName, *sourceRangeHeader);
        Private::AddHeaderIfNotEmpty(request, Private::XMsSourceIfMatchHeaderName, options.SourceIfMatch);
        if (!options.SourceAuthorization.empty()) {
            Private::AddHeader(request, Private::XMsCopySourceAuthorizationHeaderName, options.SourceAuthorization);
        } else if (options.AuthorizeSourceWithClientCredential) {
            Private::AddHeader(request, Private::XMsCopySourceAuthorizationHeaderName,
                               Private::CopySourceClientCredentialMarker);
        }
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAndParse<Models::UploadPagesResult>(this->Http(), this->Target(), std::move(request),
                                                         Private::ParseETagAndLastModified<Models::UploadPagesResult>,
                                                         std::move(completion), requestOptions);
    }

    template <class THandler>
    void ResizeAsyncImpl(std::uint64_t newSize, const ResizePageBlobOptions& options, THandler completion,
                         AzureRequestOptions requestOptions) {
        if (Private::ValidateOrFail<Models::ResizePageBlobResult>(this->Http(), completion,
                                                                  Private::ValidatePageAligned(newSize, "newSize"))) {
            return;
        }

        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=properties");
        Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(newSize));
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAndParse<Models::ResizePageBlobResult>(
            this->Http(), this->Target(), std::move(request),
            Private::ParseETagAndLastModified<Models::ResizePageBlobResult>, std::move(completion), requestOptions);
    }
};

// The production client type.
using PageBlobClient = BasicPageBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
