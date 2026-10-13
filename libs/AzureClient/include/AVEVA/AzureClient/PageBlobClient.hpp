// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

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
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
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

namespace Private {
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

    // Lists the pages that hold data (Get Page Ranges). A truncated result carries NextMarker; pass it back as
    // options.Marker to continue. With Conditions.IfMatch the call fails with ConditionNotMet if the blob changed.
    template <class CompletionToken = DefaultCompletionToken>
        requires(!std::same_as<std::remove_cvref_t<CompletionToken>, GetPageRangesOptions>)
    [[nodiscard]] auto GetPageRangesAsync(CompletionToken&& token = CompletionToken{},
                                          std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::GetPageRangesResult>(
            this,
            [](auto& client, auto&&... args) { client.GetPageRangesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), GetPageRangesOptions{});
    }

    template <class CompletionToken = DefaultCompletionToken>
    [[nodiscard]] auto GetPageRangesAsync(GetPageRangesOptions options, CompletionToken&& token = CompletionToken{},
                                          std::optional<HttpRequestOptions> requestOptions = std::nullopt) {
        return Private::InitiateClientOperation<Models::GetPageRangesResult>(
            this,
            [](auto& client, auto&&... args) { client.GetPageRangesAsyncImpl(std::forward<decltype(args)>(args)...); },
            std::forward<CompletionToken>(token), std::move(requestOptions), std::move(options));
    }

  private:
    template <HttpClientLike, TokenCredentialLike> friend class BasicBlobContainerClient;
    BasicPageBlobClient(THttp& httpClient, std::shared_ptr<const Private::BasicBlobTarget<TCred>> target)
        : Base(httpClient, std::move(target)) {}

    template <class THandler>
    void GetPageRangesAsyncImpl(const GetPageRangesOptions& options, THandler completion,
                                HttpRequestOptions requestOptions) {
        const auto fail = [&](std::string message) {
            Private::PostCompletion(this->Http(), std::move(completion),
                                    Private::MakeError<Models::GetPageRangesResult>(
                                        std::make_error_code(std::errc::invalid_argument), std::move(message)));
        };
        std::optional<std::string> rangeHeader;
        if (options.Range.has_value()) {
            if (const auto error = Private::ValidatePageAligned(options.Range->Offset, "offset")) {
                return fail(*error);
            }
            if (!options.Range->Length.has_value()) {
                // An open-ended window is the same as no window for a whole-blob scan from the offset.
                rangeHeader = "bytes=" + std::to_string(options.Range->Offset) + "-";
            } else {
                if (const auto error = Private::ValidatePageAligned(*options.Range->Length, "length")) {
                    return fail(*error);
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

        Private::SendAuthorizedRequestAsync(
            this->Http(), this->Target(), std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable {
                Private::CompleteParsed<Models::GetPageRangesResult>(error, std::move(response),
                                                                     Private::ParseGetPageRangesResult,
                                                                     std::move(completion));
            },
            requestOptions);
    }

    template <class THandler>
    void CreateAsyncImpl(std::uint64_t contentLength, const CreatePageBlobOptions& options, THandler completion,
                         HttpRequestOptions requestOptions) {
        if (const auto validationError = Private::ValidatePageAligned(contentLength, "contentLength");
            validationError.has_value()) {
            Private::PostCompletion(this->Http(), std::move(completion),
                                    Private::MakeError<Models::CreatePageBlobResult>(
                                        std::make_error_code(std::errc::invalid_argument), *validationError));
            return;
        }
        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, Private::PageBlobTypeValue);
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

    template <class THandler>
    void UploadPagesBytesAsyncImpl(std::uint64_t offset, std::span<const std::byte> content,
                                   const UploadPagesOptions& options, THandler completion,
                                   HttpRequestOptions requestOptions) {
        UploadPagesCoreAsync(offset, content, nullptr, options, std::move(completion), requestOptions);
    }

    template <class THandler>
    void UploadPagesSharedAsyncImpl(std::uint64_t offset, std::shared_ptr<const std::vector<char>> content,
                                    const UploadPagesOptions& options, THandler completion,
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

    // Shared by the byte-span and shared-buffer uploads; a non-null `keepAlive` makes the body a view of
    // `content` instead of a copy.
    template <class THandler>
    void UploadPagesCoreAsync(std::uint64_t offset, std::span<const std::byte> content,
                              std::shared_ptr<const void> keepAlive, const UploadPagesOptions& options,
                              THandler completion, HttpRequestOptions requestOptions) {
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
                                    THandler completion, HttpRequestOptions requestOptions) {
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
        const auto fail = [&](std::string_view message) {
            Private::PostCompletion(this->Http(), std::move(completion),
                                    Private::MakeError<Models::UploadPagesResult>(
                                        std::make_error_code(std::errc::invalid_argument), std::string{message}));
            return std::nullopt;
        };
        if (const auto validationError = Private::ValidatePageAligned(offset, "offset");
            validationError.has_value()) {
            return fail(*validationError);
        }
        if (const auto validationError = Private::ValidatePageAligned(contentSize, "content size");
            validationError.has_value()) {
            return fail(*validationError);
        }
        const auto rangeHeader = Private::BuildPageWriteRangeHeaderValue(offset, contentSize);
        if (!rangeHeader.has_value()) {
            return fail(rangeHeader.error());
        }

        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, Private::PageWriteUpdateValue);
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::AddHeaderIfNotEmpty(request, Private::ContentMd5HeaderName, options.ContentMd5);
        return request;
    }

    template <class THandler>
    void SendUploadPagesRequest(HttpRequest request, const UploadPagesOptions& options, THandler completion,
                                HttpRequestOptions requestOptions) {
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

    template <class THandler>
    void ClearPagesAsyncImpl(std::uint64_t offset, std::uint64_t length, const ClearPagesOptions& options,
                             THandler completion, HttpRequestOptions requestOptions) {
        if (const auto validationError = Private::ValidatePageAligned(offset, "offset");
            validationError.has_value()) {
            Private::PostCompletion(this->Http(), std::move(completion),
                                    Private::MakeError<Models::ClearPagesResult>(
                                        std::make_error_code(std::errc::invalid_argument), *validationError));
            return;
        }
        if (const auto validationError = Private::ValidatePageAligned(length, "length");
            validationError.has_value()) {
            Private::PostCompletion(this->Http(), std::move(completion),
                                    Private::MakeError<Models::ClearPagesResult>(
                                        std::make_error_code(std::errc::invalid_argument), *validationError));
            return;
        }

        HttpRequest request = Private::BuildBlobRequest(this->Target(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::        XMsPageWriteHeaderName, Private::PageWriteClearValue);
                const auto rangeHeader = Private::BuildPageWriteRangeHeaderValue(offset, length);
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

    template <class THandler>
    void ResizeAsyncImpl(std::uint64_t newSize, const ResizePageBlobOptions& options, THandler completion,
                         HttpRequestOptions requestOptions) {
        if (const auto validationError = Private::ValidatePageAligned(newSize, "newSize");
            validationError.has_value()) {
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
};

using PageBlobClient = BasicPageBlobClient<HttpClient>;
} // namespace AVEVA::AzureClient
