#include <AVEVA/AzureClient/PageBlobClient.hpp>

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "BlobRequestHelpers.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <algorithm>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <limits>
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

namespace AVEVA::AzureClient
{
    namespace
    {
        constexpr std::string_view PageBlobTypeValue = "PageBlob";
        constexpr std::string_view PageWriteUpdateValue = "update";
        constexpr std::string_view PageWriteClearValue = "clear";

        [[nodiscard]] std::optional<std::string> ValidatePageAligned(std::uint64_t value, const char* fieldName)
        {
            if ((value % PageBlobPageSize) != 0U)
            {
                return std::string(fieldName) + " must be a multiple of 512 bytes.";
            }

            return std::nullopt;
        }

        // The x-ms-range value for a page write, or a validation message.
        [[nodiscard]] std::expected<std::string, std::string> BuildPageWriteRangeHeaderValue(std::uint64_t offset,
            std::uint64_t length)
        {
            if (length == 0U)
            {
                return std::unexpected("length must be greater than zero.");
            }
            if ((length - 1U) > (std::numeric_limits<std::uint64_t>::max() - offset))
            {
                return std::unexpected("offset + length exceeds the maximum representable page range.");
            }

            return "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1U);
        }
    } // namespace

    PageBlobClient::PageBlobClient(IHttpClient& httpClient, const BlobClientOptions& options)
        : BlobClient(httpClient, options)
    {
    }

    PageBlobClient::PageBlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target)
        : BlobClient(httpClient, std::move(target))
    {
    }

    void PageBlobClient::CreateAsyncImpl(std::uint64_t contentLength,
        const CreatePageBlobOptions& options,
        CreateCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (const auto validationError = ValidatePageAligned(contentLength, "contentLength");
            validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::CreatePageBlobResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, PageBlobTypeValue);
        Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(contentLength));
        Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
        Private::ApplyMetadata(request, options.Metadata);
        Private::AddHeaderIfNotEmpty(request, Private::XMsAccessTierHeaderName, options.AccessTier.ToString());
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::CreatePageBlobResult>(error,
                std::move(response),
                Private::ParseETagAndLastModified<Models::CreatePageBlobResult>,
                std::move(completion));
        },
            requestOptions);
    }

    void PageBlobClient::UploadPagesBytesAsyncImpl(std::uint64_t offset,
        std::span<const std::byte> content,
        const UploadPagesOptions& options,
        UploadPagesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (const auto validationError = ValidatePageAligned(offset, "offset"); validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }
        if (const auto validationError = ValidatePageAligned(content.size(), "content size");
            validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }

        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, PageWriteUpdateValue);
        const auto rangeHeader = BuildPageWriteRangeHeaderValue(offset, content.size());
        if (!rangeHeader.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    rangeHeader.error()));
            return;
        }
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::AddHeaderIfNotEmpty(request, Private::ContentMd5HeaderName, options.ContentMd5);
        request.SetBody(Private::BytesToString(content));
        SendUploadPagesRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    void PageBlobClient::UploadPagesStringAsyncImpl(std::uint64_t offset,
        std::string content,
        const UploadPagesOptions& options,
        UploadPagesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (const auto validationError = ValidatePageAligned(offset, "offset"); validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }
        if (const auto validationError = ValidatePageAligned(content.size(), "content size");
            validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }

        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, PageWriteUpdateValue);
        const auto rangeHeader = BuildPageWriteRangeHeaderValue(offset, content.size());
        if (!rangeHeader.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    rangeHeader.error()));
            return;
        }
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::AddHeaderIfNotEmpty(request, Private::ContentMd5HeaderName, options.ContentMd5);
        request.SetBody(std::move(content));
        SendUploadPagesRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    void PageBlobClient::SendUploadPagesRequest(HttpRequest request,
        const UploadPagesOptions& options,
        UploadPagesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::UploadPagesResult>(error,
                std::move(response),
                Private::ParseETagAndLastModified<Models::UploadPagesResult>,
                std::move(completion));
        },
            requestOptions);
    }

    void PageBlobClient::ClearPagesAsyncImpl(std::uint64_t offset,
        std::uint64_t length,
        const ClearPagesOptions& options,
        ClearPagesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (const auto validationError = ValidatePageAligned(offset, "offset"); validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::ClearPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }
        if (const auto validationError = ValidatePageAligned(length, "length"); validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::ClearPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }

        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put, "comp=page");
        Private::AddHeader(request, Private::XMsPageWriteHeaderName, PageWriteClearValue);
        const auto rangeHeader = BuildPageWriteRangeHeaderValue(offset, length);
        if (!rangeHeader.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::ClearPagesResult>(std::make_error_code(std::errc::invalid_argument),
                    rangeHeader.error()));
            return;
        }
        Private::AddHeader(request, Private::XMsRangeHeaderName, *rangeHeader);
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::ClearPagesResult>(error,
                std::move(response),
                Private::ParseETagAndLastModified<Models::ClearPagesResult>,
                std::move(completion));
        },
            requestOptions);
    }

    void PageBlobClient::ResizeAsyncImpl(std::uint64_t newSize,
        const ResizePageBlobOptions& options,
        ResizeCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (const auto validationError = ValidatePageAligned(newSize, "newSize"); validationError.has_value())
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::ResizePageBlobResult>(std::make_error_code(std::errc::invalid_argument),
                    *validationError));
            return;
        }

        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put, "comp=properties");
        Private::AddHeader(request, Private::XMsBlobContentLengthHeaderName, std::to_string(newSize));
        Private::ApplyBlobRequestConditions(request, options.Conditions);

        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::ResizePageBlobResult>(error,
                std::move(response),
                Private::ParseETagAndLastModified<Models::ResizePageBlobResult>,
                std::move(completion));
        },
            requestOptions);
    }

    namespace
    {
        using PageRangesHandler =
            std::move_only_function<void(std::expected<Response<Models::GetPageRangesResult>, BlobStorageError>)>;

        // Requests a single page of the page list, starting at options.Marker.
        void GetPageRangesPage(IHttpClient& httpClient,
            const std::shared_ptr<const Private::BlobTarget>& target,
            const GetPageRangesOptions& options,
            PageRangesHandler completion,
            HttpRequestOptions requestOptions)
        {
            std::vector<std::pair<std::string, std::string>> queryParameters{{"comp", "pagelist"}};
            if (!options.Marker.empty())
            {
                queryParameters.emplace_back("marker", options.Marker);
            }
            if (options.MaxResults.has_value())
            {
                queryParameters.emplace_back("maxresults", std::to_string(*options.MaxResults));
            }
            HttpRequest request =
                Private::BuildBlobRequest(*target, HttpMethod::Get, Private::BuildQueryString(queryParameters));
            if (options.Range.has_value())
            {
                try
                {
                    Private::AddHeader(request,
                        Private::XMsRangeHeaderName,
                        Private::BuildRangeHeaderValue(*options.Range));
                }
                catch (const std::invalid_argument&)
                {
                    Private::PostCompletion(httpClient,
                        std::move(completion),
                        Private::MakeError<Models::GetPageRangesResult>(
                            std::make_error_code(std::errc::invalid_argument)));
                    return;
                }
            }
            Private::ApplyBlobRequestConditions(request, options.Conditions);

            Private::SendAuthorizedRequestAsync(httpClient,
                *target,
                std::move(request),
                [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
            {
                Private::CompleteParsed<Models::GetPageRangesResult>(error,
                    std::move(response),
                    [](const HttpResponse& value)
                {
                    return Private::ParseGetPageRangesResultXml(value.GetBody());
                },
                    std::move(completion));
            },
                requestOptions);
        }
    } // namespace

    // Follows NextMarker so a fragmented blob never yields a silently truncated page list.
    //
    // Later pages start on the transport thread, so each page gets its own cancellation signal instead of
    // re-assigning the caller's slot (which the caller may emit on at the same time). One handler on the caller's
    // slot forwards to the signal of the page in flight.
    void PageBlobClient::GetPageRangesAsyncImpl(GetPageRangesOptions options,
        GetPageRangesCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        struct CancellationRelay
        {
            std::mutex Mutex;
            bool Cancelled = false;
            boost::asio::cancellation_type_t Type = boost::asio::cancellation_type::none;
            std::shared_ptr<boost::asio::cancellation_signal> Current;
        };

        std::shared_ptr<CancellationRelay> relay;
        if (auto parentSlot = requestOptions.GetCancellationSlot(); parentSlot.is_connected())
        {
            relay = std::make_shared<CancellationRelay>();
            parentSlot.assign([relay](boost::asio::cancellation_type_t type)
            {
                std::shared_ptr<boost::asio::cancellation_signal> current;
                {
                    const std::scoped_lock lock(relay->Mutex);
                    relay->Cancelled = true;
                    relay->Type = type;
                    current = relay->Current;
                }
                if (current)
                {
                    current->emit(type);
                }
            });
        }

        std::string marker = options.Marker;
        auto collector = std::make_shared<Private::PageCollector<Models::GetPageRangesResult>>(
            [httpClient = &HttpClient(), target = SharedTarget(), options = std::move(options), requestOptions, relay](
                std::string pageMarker, PageRangesHandler pageCompletion) mutable
        {
            options.Marker = std::move(pageMarker);
            if (!relay)
            {
                GetPageRangesPage(*httpClient, target, options, std::move(pageCompletion), requestOptions);
                return;
            }

            auto pageSignal = std::make_shared<boost::asio::cancellation_signal>();
            HttpRequestOptions pageOptions = requestOptions;
            pageOptions.SetCancellationSlot(pageSignal->slot());
            GetPageRangesPage(*httpClient, target, options, std::move(pageCompletion), pageOptions);

            // Published only after the page installed its handler, so emitting never races with that assignment.
            bool cancelled = false;
            boost::asio::cancellation_type_t type = boost::asio::cancellation_type::none;
            {
                const std::scoped_lock lock(relay->Mutex);
                relay->Current = pageSignal;
                cancelled = relay->Cancelled;
                type = relay->Type;
            }
            if (cancelled)
            {
                pageSignal->emit(type);
            }
        },
            [](Models::GetPageRangesResult& accumulated, Models::GetPageRangesResult page)
        {
            std::ranges::move(page.PageRanges, std::back_inserter(accumulated.PageRanges));
        },
            std::move(completion));
        collector->Start(std::move(marker));
    }
} // namespace AVEVA::AzureClient
