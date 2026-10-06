#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>

#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include "BlobRequestHelpers.hpp"
#include "ProtocolConstants.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/any_io_executor.hpp>
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

namespace AVEVA::AzureClient
{
    namespace
    {
        using Private::FindHeaderValue;
        using Private::XMsContentCrc64HeaderName;
        using Private::XMsRequestIdHeaderName;

        constexpr std::string_view BlockBlobTypeValue = "BlockBlob";

        [[nodiscard]] std::string EscapeXml(std::string_view value)
        {
            std::string escaped;
            escaped.reserve(value.size());
            for (const char character : value)
            {
                switch (character)
                {
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

        [[nodiscard]] std::string BuildBlockListBody(const std::vector<std::string>& blockIds)
        {
            static constexpr std::string_view Prefix = R"(<?xml version="1.0" encoding="utf-8"?><BlockList>)";
            static constexpr std::string_view Suffix = "</BlockList>";
            static constexpr std::string_view LatestOpen = "<Latest>";
            static constexpr std::string_view LatestClose = "</Latest>";

            std::size_t reserveSize = Prefix.size() + Suffix.size();
            std::optional<std::size_t> expectedBlockIdLength;
            for (const auto& blockId : blockIds)
            {
                Private::ValidateBlockId(blockId);
                if (expectedBlockIdLength.has_value() && *expectedBlockIdLength != blockId.size())
                {
                    throw std::invalid_argument("All block IDs in a block list must have the same encoded length.");
                }

                expectedBlockIdLength = blockId.size();
                reserveSize += LatestOpen.size() + LatestClose.size() + blockId.size();
            }

            std::string body;
            body.reserve(reserveSize);
            body.append(Prefix);
            for (const auto& blockId : blockIds)
            {
                body.append(LatestOpen);
                body.append(EscapeXml(blockId));
                body.append(LatestClose);
            }
            body.append(Suffix);
            return body;
        }

        [[nodiscard]] Models::StageBlockResult ParseStageBlockResult(const HttpResponse& response)
        {
            Models::StageBlockResult result;
            result.ContentMd5 = std::string{FindHeaderValue(response, Private::ContentMd5HeaderName)};
            result.ContentCrc64 = std::string{FindHeaderValue(response, XMsContentCrc64HeaderName)};
            result.RequestId = std::string{FindHeaderValue(response, XMsRequestIdHeaderName)};
            return result;
        }

        void ApplyUploadOptions(HttpRequest& request, const UploadBlockBlobOptions& options)
        {
            Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
            Private::ApplyMetadata(request, options.Metadata);
            Private::AddHeaderIfNotEmpty(request, Private::XMsAccessTierHeaderName, options.AccessTier.ToString());
            Private::ApplyBlobRequestConditions(request, options.Conditions);
            Private::ApplyTransactionalHashes(request,
                options.TransactionalContentMd5,
                options.TransactionalContentCrc64);
        }

        [[nodiscard]] HttpRequest BuildStageBlockRequest(const Private::BlobTarget& clientOptions,
            std::string_view blockId,
            const Models::BlobRequestConditions& conditions)
        {
            HttpRequest request = Private::BuildBlobRequest(clientOptions,
                HttpMethod::Put,
                "comp=block&blockid=" + Private::UrlEncode(blockId, {}));
            // Put Block only supports the lease condition; access conditions belong on Put Block List (T08).
            Private::AddHeaderIfNotEmpty(request, Private::XMsLeaseIdHeaderName, conditions.LeaseId);
            return request;
        }

        // Throws std::invalid_argument for invalid or inconsistent block IDs.
        [[nodiscard]] HttpRequest BuildCommitBlockListRequest(const Private::BlobTarget& clientOptions,
            const std::vector<std::string>& blockIds,
            const CommitBlockListOptions& options)
        {
            HttpRequest request = Private::BuildBlobRequest(clientOptions, HttpMethod::Put, "comp=blocklist");
            request.SetBody(BuildBlockListBody(blockIds));
            Private::ApplyBlobHttpHeadersForUpload(request, options.HttpHeaders);
            Private::ApplyMetadata(request, options.Metadata);
            Private::AddHeaderIfNotEmpty(request, Private::XMsAccessTierHeaderName, options.AccessTier.ToString());
            Private::ApplyBlobRequestConditions(request, options.Conditions);
            return request;
        }

    } // namespace

    BlockBlobClient::BlockBlobClient(IHttpClient& httpClient, const BlobClientOptions& options)
        : BlobClient(httpClient, options)
    {
    }

    BlockBlobClient::BlockBlobClient(IHttpClient& httpClient, std::shared_ptr<const Private::BlobTarget> target)
        : BlobClient(httpClient, std::move(target))
    {
    }

    void BlockBlobClient::UploadBytesAsyncImpl(std::span<const std::byte> content,
        const UploadBlockBlobOptions& options,
        UploadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
        request.SetBody(Private::BytesToString(content));
        SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    void BlockBlobClient::UploadStringAsyncImpl(std::string content,
        const UploadBlockBlobOptions& options,
        UploadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
        request.SetBody(std::move(content));
        SendUploadRequest(std::move(request), options, std::move(completion), requestOptions);
    }

    void BlockBlobClient::SendUploadRequest(HttpRequest request,
        const UploadBlockBlobOptions& options,
        UploadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        ApplyUploadOptions(request, options);

        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::UploadBlockBlobResult>(error,
                std::move(response),
                Private::ParseETagAndLastModified<Models::UploadBlockBlobResult>,
                std::move(completion));
        },
            requestOptions);
    }

    void BlockBlobClient::StageBlockBytesAsyncImpl(const std::string& blockId,
        std::span<const std::byte> content,
        const StageBlockOptions& options,
        StageBlockCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        try
        {
            Private::ValidateBlockId(blockId);
        }
        catch (const std::invalid_argument&)
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::StageBlockResult>(std::make_error_code(std::errc::invalid_argument)));
            return;
        }

        HttpRequest request = BuildStageBlockRequest(Target(), blockId, options.Conditions);
        Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
        request.SetBody(Private::BytesToString(content));
        SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
    }

    void BlockBlobClient::StageBlockStringAsyncImpl(const std::string& blockId,
        const StageBlockOptions& options,
        std::string content,
        StageBlockCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        try
        {
            Private::ValidateBlockId(blockId);
        }
        catch (const std::invalid_argument&)
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::StageBlockResult>(std::make_error_code(std::errc::invalid_argument)));
            return;
        }

        HttpRequest request = BuildStageBlockRequest(Target(), blockId, options.Conditions);
        Private::ApplyTransactionalHashes(request, options.TransactionalContentMd5, options.TransactionalContentCrc64);
        request.SetBody(std::move(content));
        SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
    }

    void BlockBlobClient::SendStageBlockRequest(HttpRequest request,
        StageBlockCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::StageBlockResult>(error,
                std::move(response),
                ParseStageBlockResult,
                std::move(completion));
        },
            requestOptions);
    }

    void BlockBlobClient::CommitBlockListAsyncImpl(const std::vector<std::string>& blockIds,
        const CommitBlockListOptions& options,
        CommitBlockListCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request;
        try
        {
            request = BuildCommitBlockListRequest(Target(), blockIds, options);
        }
        catch (const std::invalid_argument&)
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::CommitBlockListResult>(std::make_error_code(std::errc::invalid_argument)));
            return;
        }

        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::CommitBlockListResult>(error,
                std::move(response),
                Private::ParseETagAndLastModified<Models::CommitBlockListResult>,
                std::move(completion));
        },
            requestOptions);
    }
} // namespace AVEVA::AzureClient
