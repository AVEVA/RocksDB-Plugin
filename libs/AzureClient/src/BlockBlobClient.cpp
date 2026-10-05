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
#include "TestHooks.hpp"

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

#ifdef AVEVA_AZURE_CLIENT_TESTING
        [[nodiscard]] std::atomic<std::size_t>& TotalUploadBufferAllocations() noexcept
        {
            static std::atomic<std::size_t> value{0};
            return value;
        }

        [[nodiscard]] std::atomic<std::size_t>& CurrentUploadBufferBytes() noexcept
        {
            static std::atomic<std::size_t> value{0};
            return value;
        }

        [[nodiscard]] std::atomic<std::size_t>& PeakUploadBufferBytes() noexcept
        {
            static std::atomic<std::size_t> value{0};
            return value;
        }

#endif

        // Fixed-capacity, uninitialized upload block buffer. Construction/destruction feed the
        // TestHooks counters (test builds only) so tests can verify the bounded-pool guarantees of UploadFromAsync.
        class UploadBuffer
        {
          public:
            explicit UploadBuffer(std::size_t capacity) : m_data(Allocator{}.allocate(capacity)), m_capacity(capacity)
            {
#ifdef AVEVA_AZURE_CLIENT_TESTING
                TotalUploadBufferAllocations().fetch_add(1, std::memory_order_acq_rel);
                const std::size_t current =
                    CurrentUploadBufferBytes().fetch_add(capacity, std::memory_order_acq_rel) + capacity;
                std::size_t peak = PeakUploadBufferBytes().load(std::memory_order_acquire);
                while (current > peak &&
                       !PeakUploadBufferBytes().compare_exchange_weak(peak, current, std::memory_order_acq_rel))
                {
                }
#endif
            }

            ~UploadBuffer()
            {
                Allocator{}.deallocate(m_data, m_capacity);
#ifdef AVEVA_AZURE_CLIENT_TESTING
                CurrentUploadBufferBytes().fetch_sub(m_capacity, std::memory_order_acq_rel);
#endif
            }

            UploadBuffer(const UploadBuffer&) = delete;
            UploadBuffer& operator=(const UploadBuffer&) = delete;
            UploadBuffer(UploadBuffer&&) = delete;
            UploadBuffer& operator=(UploadBuffer&&) = delete;

            [[nodiscard]] std::span<std::byte> Span() const noexcept
            {
                return {m_data, m_capacity};
            }

          private:
            // Raw storage so the block is left uninitialized; UploadBuffer is the sole owner and is neither
            // copyable nor movable.
            using Allocator = std::allocator<std::byte>;

            std::byte* m_data;
            std::size_t m_capacity;
        };

        // Reads up to destination.size() bytes, returning the number of bytes read.
        [[nodiscard]] std::size_t ReadInto(std::istream& stream, std::span<std::byte> destination)
        {
            if (destination.empty())
            {
                return 0U;
            }
            stream.read(Private::AsChars(destination).data(), static_cast<std::streamsize>(destination.size()));
            return static_cast<std::size_t>(stream.gcount());
        }

        // A failure that is not just running out of data (eofbit alone is a clean end of stream).
        [[nodiscard]] bool HasReadError(const std::istream& stream) noexcept
        {
            return stream.bad() || (stream.fail() && !stream.eof());
        }

        // A short read means the source is drained; after a full block, look ahead through the
        // stream buffer so the stream's own state bits are not modified.
        [[nodiscard]] bool IsDrained(std::istream& stream, std::size_t read, std::size_t blockSize)
        {
            return read < blockSize || stream.rdbuf()->sgetc() == std::char_traits<char>::eof();
        }

        [[nodiscard]] std::optional<BlobStorageError> ToFailure(std::error_code error, const HttpResponse& response)
        {
            Private::RequestFailure const failure = Private::DetermineBlobStorageFailure(error, response);
            if (!failure.Error)
            {
                return std::nullopt;
            }
            BlobStorageError details = failure.Details.value_or(Private::MakeClientError(failure.Error));
            details.Code = failure.Error;
            return details;
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

#ifdef AVEVA_AZURE_CLIENT_TESTING
    // Test hooks to observe internal allocation behaviour during unit tests.
    namespace TestHooks
    {
        void ResetUploadBufferAllocationCounter()
        {
            TotalUploadBufferAllocations().store(0, std::memory_order_release);
            CurrentUploadBufferBytes().store(0, std::memory_order_release);
            PeakUploadBufferBytes().store(0, std::memory_order_release);
        }

        std::size_t GetTotalUploadBufferAllocations()
        {
            return TotalUploadBufferAllocations().load(std::memory_order_acquire);
        }

        void ResetPeakUploadBufferBytes()
        {
            CurrentUploadBufferBytes().store(0, std::memory_order_release);
            PeakUploadBufferBytes().store(0, std::memory_order_release);
        }

        std::size_t GetPeakUploadBufferBytes()
        {
            return PeakUploadBufferBytes().load(std::memory_order_acquire);
        }
    } // namespace TestHooks
#endif

    BlockBlobClient::BlockBlobClient(IHttpClient& httpClient, const BlobClientOptions& options)
        : BlobClient(httpClient, options)
    {
    }

    BlockBlobClient BlockBlobClient::WithSnapshot(std::string snapshot) const
    {
        Private::BlobTarget target = Target();
        target.Snapshot = std::move(snapshot);
        target.VersionId.clear();
        return BlockBlobClient{HttpClient(), std::make_shared<const Private::BlobTarget>(std::move(target))};
    }

    BlockBlobClient BlockBlobClient::WithVersionId(std::string versionId) const
    {
        Private::BlobTarget target = Target();
        target.VersionId = std::move(versionId);
        target.Snapshot.clear();
        return BlockBlobClient{HttpClient(), std::make_shared<const Private::BlobTarget>(std::move(target))};
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

    void BlockBlobClient::UploadSharedBytesAsyncImpl(std::shared_ptr<const std::vector<std::byte>> content,
        const UploadBlockBlobOptions& options,
        UploadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (!content)
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadBlockBlobResult>(std::make_error_code(std::errc::invalid_argument),
                    "Upload content must not be null."));
            return;
        }

        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
        request.SetBodyView(std::span<const std::byte>{*content});
        // The buffer must outlive every retry attempt, so the completion keeps it alive.
        SendUploadRequest(std::move(request),
            options,
            [content = std::move(content), completion = std::move(completion)](auto result) mutable
        {
            completion(std::move(result));
        },
            requestOptions);
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

    void BlockBlobClient::StageBlockFromUriAsyncImpl(const std::string& blockId,
        const std::string& sourceUri,
        StageBlockFromUriOptions options,
        StageBlockCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        bool invalidRange =
            options.SourceLength.has_value() && (*options.SourceLength == 0 || !options.SourceOffset.has_value());
        bool invalidBlockId = false;
        try
        {
            Private::ValidateBlockId(blockId);
        }
        catch (const std::invalid_argument&)
        {
            invalidBlockId = true;
        }
        std::string sourceRange;
        if (!invalidRange && options.SourceOffset.has_value())
        {
            try
            {
                sourceRange = Private::BuildRangeHeaderValue(
                    Models::BlobByteRange{.Offset = *options.SourceOffset, .Length = options.SourceLength});
            }
            catch (const std::invalid_argument&)
            {
                invalidRange = true;
            }
        }
        if (invalidBlockId || invalidRange || sourceUri.empty())
        {
            std::string message = "Source URI must not be empty.";
            if (invalidBlockId)
            {
                message = "Invalid block ID.";
            }
            else if (invalidRange)
            {
                message = "SourceLength requires SourceOffset and must be non-zero.";
            }
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::StageBlockResult>(std::make_error_code(std::errc::invalid_argument),
                    message));
            return;
        }

        HttpRequest request = BuildStageBlockRequest(Target(), blockId, options.Conditions);
        Private::AddHeader(request, Private::XMsCopySourceHeaderName, sourceUri);
        if (!sourceRange.empty())
        {
            Private::AddHeader(request, Private::XMsSourceRangeHeaderName, sourceRange);
        }
        Private::AddHeaderIfNotEmpty(request, Private::XMsSourceContentMd5HeaderName, options.SourceContentMd5);
        SendStageBlockRequest(std::move(request), std::move(completion), requestOptions);
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

    void BlockBlobClient::GetBlockListAsyncImpl(GetBlockListCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Get, "comp=blocklist&blocklisttype=all");
        Private::SendAuthorizedRequestAsync(HttpClient(),
            Target(),
            std::move(request),
            [completion = std::move(completion)](std::error_code error, HttpResponse response) mutable
        {
            Private::CompleteParsed<Models::GetBlockListResult>(error,
                std::move(response),
                [](const HttpResponse& value)
            {
                return Private::ParseGetBlockListResultXml(value.GetBody());
            },
                std::move(completion));
        },
            requestOptions);
    }

    void BlockBlobClient::CreateIfNotExistsAsyncImpl(UploadBlockBlobOptions options,
        UploadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (options.Conditions.IfNoneMatch.empty())
        {
            options.Conditions.IfNoneMatch = "*";
        }
        HttpRequest request = Private::BuildBlobRequest(Target(), HttpMethod::Put);
        Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
        request.SetBody(std::string{});
        ApplyUploadOptions(request, options);
        // The blob already existing is the expected outcome of a conditional create: succeed with the real
        // response, keeping the suppressed error's diagnostics in Response<T>::Error().
        Private::SendAndParse<Models::UploadBlockBlobResult>(HttpClient(),
            Target(),
            std::move(request),
            Private::ParseETagAndLastModified<Models::UploadBlockBlobResult>,
            Private::SuppressErrors<Models::UploadBlockBlobResult>(std::move(completion),
                {BlobStorageErrorCode::BlobAlreadyExists, BlobStorageErrorCode::ConditionNotMet}),
            requestOptions);
    }

    namespace
    {
        constexpr std::size_t MaxBlocksPerBlob = Private::BlobTransferLimits::MaxBlocksPerBlob;
        constexpr std::uint64_t MaxStageBlockBytes = Private::BlobTransferLimits::MaxStageBlockBytes;
        constexpr std::uint64_t MaxPutBlobBytes = Private::BlobTransferLimits::MaxPutBlobBytes;

        // One UploadFromAsync operation (stream or file). All mutable state is owned here and guarded
        // by m_mutex; every transport completion and cancellation request is funnelled through m_strand,
        // so state transitions never run re-entrantly inside SendAsync or a cancellation emit.
        class UploadFromOperation final : public std::enable_shared_from_this<UploadFromOperation>
        {
          public:
            using Completion = BlockBlobClient::UploadCompletionHandler;
            using Result = std::expected<Response<Models::UploadBlockBlobResult>, BlobStorageError>;

            UploadFromOperation(IHttpClient& httpClient,
                Private::BlobTarget clientOptions,
                UploadFromOptions options,
                HttpRequestOptions requestOptions,
                Completion completion,
                std::istream& stream,
                std::shared_ptr<std::istream> ownedStream,
                std::optional<std::uint64_t> knownSize)
                : m_httpClient(httpClient), m_clientOptions(std::move(clientOptions)), m_options(std::move(options)),
                  m_requestOptions(requestOptions), m_completion(std::move(completion)), m_stream(stream),
                  m_ownedStream(std::move(ownedStream)), m_knownSize(knownSize),
                  m_strand(boost::asio::make_strand(httpClient.get_executor())),
                  m_parentSlot(m_requestOptions.GetCancellationSlot()), m_blockSize(m_options.BlockSize),
                  m_concurrency(std::max<std::size_t>(1U, m_options.Concurrency))
            {
                m_stageConditions.LeaseId = m_options.UploadOptions.Conditions.LeaseId;
                m_blockIdPrefix = Private::CreateUploadBlockIdPrefix();
            }

            void Start()
            {
                std::unique_lock lock(m_mutex);
                m_starting = true;
                try
                {
                    StartLocked(lock);
                }
                catch (...)
                {
                    if (!lock.owns_lock())
                    {
                        lock.lock();
                    }
                    Finish(lock, std::unexpected(Private::MakeFailureFromCurrentException()));
                }
                m_starting = false;
            }

          private:
            void StartLocked(std::unique_lock<std::mutex>& lock)
            {
                if (m_knownSize.has_value())
                {
                    const std::uint64_t threshold =
                        std::min<std::uint64_t>(m_options.SingleUploadThreshold, MaxPutBlobBytes);
                    if (*m_knownSize <= std::max<std::uint64_t>(threshold, m_blockSize))
                    {
                        auto buffer = std::make_unique<UploadBuffer>(static_cast<std::size_t>(*m_knownSize));
                        const std::size_t read = ReadInto(m_stream, buffer->Span());
                        // A short read means the file shrank after its size was captured; uploading it would
                        // silently store a truncated blob.
                        if (HasReadError(m_stream) || read != buffer->Span().size())
                        {
                            FailEarly(lock, std::errc::io_error);
                            return;
                        }
                        PutSingleBlob(std::move(buffer), read);
                        return;
                    }

                    const std::uint64_t blocksNeeded = (*m_knownSize + m_blockSize - 1U) / m_blockSize;
                    if (blocksNeeded > MaxBlocksPerBlob)
                    {
                        const std::uint64_t grown = (*m_knownSize + MaxBlocksPerBlob - 1U) / MaxBlocksPerBlob;
                        if (grown > MaxStageBlockBytes)
                        {
                            FailEarly(lock, std::errc::invalid_argument);
                            return;
                        }
                        m_blockSize = static_cast<std::size_t>(grown);
                    }
                }

                std::unique_ptr<UploadBuffer> first = AcquireBuffer();
                const std::size_t read = ReadInto(m_stream, first->Span());
                if (HasReadError(m_stream))
                {
                    FailEarly(lock, std::errc::io_error);
                    return;
                }
                const bool atEnd = IsDrained(m_stream, read, m_blockSize);
                if (atEnd)
                {
                    PutSingleBlob(std::move(first), read);
                    return;
                }

                if (m_parentSlot.is_connected())
                {
                    std::weak_ptr<UploadFromOperation> const weak = weak_from_this();
                    m_parentSlot.assign([weak](boost::asio::cancellation_type_t)
                    {
                        if (auto self = weak.lock())
                        {
                            boost::asio::post(self->m_strand,
                                [self]()
                            {
                                self->OnCancelRequested();
                            });
                        }
                    });
                }

                StageBlock(std::move(first), read);
                PumpLocked(lock);
            }

            void FailEarly(std::unique_lock<std::mutex>& lock, std::errc error)
            {
                m_done = true;
                Completion completion = std::move(m_completion);
                lock.unlock();
                Private::PostCompletion(m_httpClient,
                    std::move(completion),
                    Private::MakeError<Models::UploadBlockBlobResult>(std::make_error_code(error)));
                lock.lock();
            }

            void PutSingleBlob(std::unique_ptr<UploadBuffer> buffer, std::size_t size)
            {
                HttpRequest request = Private::BuildBlobRequest(m_clientOptions, HttpMethod::Put);
                Private::AddHeader(request, Private::XMsBlobTypeHeaderName, BlockBlobTypeValue);
                ApplyUploadOptions(request, m_options.UploadOptions);
                request.SetBodyView(buffer->Span().first(size));
                m_done = true;

                Private::SendAuthorizedRequestAsync(m_httpClient,
                    m_clientOptions,
                    std::move(request),
                    [self = shared_from_this(), buffer = std::move(buffer)](std::error_code error,
                        HttpResponse response) mutable
                {
                    buffer.reset();
                    Private::CompleteParsed<Models::UploadBlockBlobResult>(error,
                        std::move(response),
                        Private::ParseETagAndLastModified<Models::UploadBlockBlobResult>,
                        std::move(self->m_completion));
                },
                    m_requestOptions);
            }

            [[nodiscard]] std::unique_ptr<UploadBuffer> AcquireBuffer()
            {
                if (!m_freeBuffers.empty())
                {
                    std::unique_ptr<UploadBuffer> buffer = std::move(m_freeBuffers.back());
                    m_freeBuffers.pop_back();
                    return buffer;
                }
                return std::make_unique<UploadBuffer>(m_blockSize);
            }

            // Reads and stages blocks until Concurrency requests are in flight or the source is drained.
            void IssueMore()
            {
                try
                {
                    IssueMoreUnchecked();
                }
                catch (...)
                {
                    Fail(Private::MakeFailureFromCurrentException());
                }
            }

            void IssueMoreUnchecked()
            {
                while (!m_failure.has_value() && !m_endOfStream && m_inFlight < m_concurrency)
                {
                    if (m_blockIds.size() >= MaxBlocksPerBlob)
                    {
                        Fail(Private::MakeClientError(std::make_error_code(std::errc::invalid_argument),
                            "The source exceeds the 50,000-block limit for the configured BlockSize."));
                        return;
                    }

                    std::unique_ptr<UploadBuffer> buffer = AcquireBuffer();
                    const std::size_t read = ReadInto(m_stream, buffer->Span());
                    if (HasReadError(m_stream))
                    {
                        m_freeBuffers.push_back(std::move(buffer));
                        Fail(Private::MakeClientError(std::make_error_code(std::errc::io_error),
                            "Failed to read the upload source."));
                        return;
                    }
                    const bool atEnd = IsDrained(m_stream, read, m_blockSize);
                    m_endOfStream = atEnd;
                    if (read == 0U)
                    {
                        m_freeBuffers.push_back(std::move(buffer));
                        return;
                    }
                    StageBlock(std::move(buffer), read);
                }
            }

            void StageBlock(std::unique_ptr<UploadBuffer> buffer, std::size_t size)
            {
                const std::size_t index = m_blockIds.size();
                m_blockIds.push_back(Private::EncodeUploadBlockId(m_blockIdPrefix, index));

                HttpRequest request = BuildStageBlockRequest(m_clientOptions, m_blockIds.back(), m_stageConditions);
                request.SetBodyView(buffer->Span().first(size));

                auto signal = std::make_unique<boost::asio::cancellation_signal>();
                HttpRequestOptions requestOptions = m_requestOptions;
                requestOptions.SetCancellationSlot(signal->slot());
                m_stageSignals.emplace(index, std::move(signal));
                ++m_inFlight;

                Private::SendAuthorizedRequestAsync(m_httpClient,
                    m_clientOptions,
                    std::move(request),
                    [self = shared_from_this(), index, buffer = std::move(buffer)](std::error_code error,
                        const HttpResponse& response) mutable
                {
                    std::optional<BlobStorageError> failure = ToFailure(error, response);
                    boost::asio::post(self->m_strand,
                        [self, index, buffer = std::move(buffer), failure = std::move(failure)]() mutable
                    {
                        self->OnStageComplete(index, std::move(buffer), std::move(failure));
                    });
                },
                    requestOptions);
            }

            void OnStageComplete(std::size_t index,
                std::unique_ptr<UploadBuffer> buffer,
                std::optional<BlobStorageError> failure)
            {
                std::unique_lock lock(m_mutex);
                --m_inFlight;
                m_stageSignals.erase(index);
                m_freeBuffers.push_back(std::move(buffer));
                if (failure.has_value())
                {
                    Fail(std::move(*failure));
                }
                PumpLocked(lock);
            }

            void OnCancelRequested()
            {
                std::unique_lock lock(m_mutex);
                if (m_done)
                {
                    return;
                }
                Fail(Private::MakeClientError(std::make_error_code(std::errc::operation_canceled),
                    "The operation was canceled."));
                PumpLocked(lock);
            }

            // Records the first failure and cancels every in-flight child request. Completion is
            // deferred until all of them have drained (see PumpLocked).
            void Fail(BlobStorageError error)
            {
                if (m_failure.has_value())
                {
                    return;
                }
                m_failure = std::move(error);
                for (auto& [index, signal] : m_stageSignals)
                {
                    signal->emit(boost::asio::cancellation_type::terminal);
                }
                if (m_committing)
                {
                    m_commitSignal.emit(boost::asio::cancellation_type::terminal);
                }
            }

            void PumpLocked(std::unique_lock<std::mutex>& lock)
            {
                if (m_done || m_committing)
                {
                    return;
                }
                IssueMore();
                if (m_inFlight != 0U)
                {
                    return;
                }
                if (m_failure.has_value())
                {
                    Finish(lock, std::unexpected(std::move(*m_failure)));
                    return;
                }
                if (m_endOfStream)
                {
                    Commit(lock);
                }
            }

            void Commit(std::unique_lock<std::mutex>& lock)
            {
                CommitBlockListOptions commitOptions;
                commitOptions.HttpHeaders = m_options.UploadOptions.HttpHeaders;
                commitOptions.Metadata = m_options.UploadOptions.Metadata;
                commitOptions.AccessTier = m_options.UploadOptions.AccessTier;
                commitOptions.Conditions = m_options.UploadOptions.Conditions;

                HttpRequest request;
                try
                {
                    request = BuildCommitBlockListRequest(m_clientOptions, m_blockIds, commitOptions);
                }
                catch (...)
                {
                    Finish(lock, std::unexpected(Private::MakeFailureFromCurrentException()));
                    return;
                }

                m_committing = true;
                HttpRequestOptions requestOptions = m_requestOptions;
                requestOptions.SetCancellationSlot(m_commitSignal.slot());
                Private::SendAuthorizedRequestAsync(m_httpClient,
                    m_clientOptions,
                    std::move(request),
                    [self = shared_from_this()](std::error_code error, HttpResponse response) mutable
                {
                    boost::asio::post(self->m_strand,
                        [self, error, response = std::move(response)]() mutable
                    {
                        self->OnCommitComplete(error, std::move(response));
                    });
                },
                    requestOptions);
            }

            void OnCommitComplete(std::error_code error, HttpResponse response)
            {
                std::unique_lock lock(m_mutex);
                if (std::optional<BlobStorageError> failure = ToFailure(error, response); failure.has_value())
                {
                    Finish(lock, std::unexpected(std::move(*failure)));
                    return;
                }

                Models::UploadBlockBlobResult result;
                try
                {
                    result = Private::ParseETagAndLastModified<Models::UploadBlockBlobResult>(response);
                }
                catch (const std::exception& exception)
                {
                    Private::RequestFailure failure = Private::MakeInvalidResponseFailure(response, exception.what());
                    Finish(lock,
                        Private::ToExpected(failure.Error,
                            Response<Models::UploadBlockBlobResult>{{},
                                std::move(response),
                                std::move(failure.Details)}));
                    return;
                }
                Finish(lock, Response<Models::UploadBlockBlobResult>{std::move(result), std::move(response)});
            }

            void Finish(std::unique_lock<std::mutex>& lock, Result result)
            {
                if (m_done)
                {
                    return;
                }
                m_done = true;
                m_freeBuffers.clear();
                Completion completion = std::move(m_completion);
                const bool starting = m_starting;
                lock.unlock();
                if (starting)
                {
                    Private::PostCompletion(m_httpClient, std::move(completion), std::move(result));
                }
                else
                {
                    completion(std::move(result));
                }
                lock.lock();
            }

            IHttpClient& m_httpClient;
            Private::BlobTarget m_clientOptions;
            UploadFromOptions m_options;
            HttpRequestOptions m_requestOptions;
            Completion m_completion;
            std::istream& m_stream;
            std::shared_ptr<std::istream> m_ownedStream;
            std::optional<std::uint64_t> m_knownSize;
            boost::asio::strand<boost::asio::any_io_executor> m_strand;
            boost::asio::cancellation_slot m_parentSlot;
            Models::BlobRequestConditions m_stageConditions;

            std::mutex m_mutex;
            std::size_t m_blockSize;
            std::string m_blockIdPrefix;
            std::size_t m_concurrency;
            std::vector<std::string> m_blockIds;
            std::vector<std::unique_ptr<UploadBuffer>> m_freeBuffers;
            std::map<std::size_t, std::unique_ptr<boost::asio::cancellation_signal>> m_stageSignals;
            boost::asio::cancellation_signal m_commitSignal;
            std::size_t m_inFlight = 0U;
            std::optional<BlobStorageError> m_failure;
            bool m_endOfStream = false;
            bool m_committing = false;
            bool m_done = false;
            bool m_starting = false;
        };
    } // namespace

    void BlockBlobClient::UploadFromFileAsyncImpl(const std::filesystem::path& path,
        UploadFromOptions options,
        UploadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (options.BlockSize == 0U || options.BlockSize > MaxStageBlockBytes)
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadBlockBlobResult>(std::make_error_code(std::errc::invalid_argument),
                    "BlockSize must be between 1 byte and 4000 MiB."));
            return;
        }

        auto file = std::make_shared<std::ifstream>(path, std::ios::binary);
        if (!*file)
        {
            // errno is not guaranteed to be set by ifstream, so ask the filesystem why the open failed.
            std::error_code openError;
            const auto status = std::filesystem::status(path, openError);
            if (status.type() == std::filesystem::file_type::not_found)
            {
                openError = std::make_error_code(std::errc::no_such_file_or_directory);
            }
            else if (!openError)
            {
                openError = std::make_error_code(std::errc::permission_denied);
            }
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadBlockBlobResult>(openError, "Failed to open the upload source file."));
            return;
        }

        std::optional<std::uint64_t> knownSize;
        std::error_code sizeError;
        if (std::filesystem::is_regular_file(path, sizeError))
        {
            const std::uintmax_t size = std::filesystem::file_size(path, sizeError);
            if (sizeError)
            {
                Private::PostCompletion(HttpClient(),
                    std::move(completion),
                    Private::MakeError<Models::UploadBlockBlobResult>(sizeError,
                        "Failed to determine the upload source file size."));
                return;
            }
            if (size > std::numeric_limits<std::uint64_t>::max())
            {
                Private::PostCompletion(HttpClient(),
                    std::move(completion),
                    Private::MakeError<Models::UploadBlockBlobResult>(std::make_error_code(std::errc::file_too_large)));
                return;
            }
            knownSize = static_cast<std::uint64_t>(size);
        }

        std::istream& input = *file;
        std::make_shared<UploadFromOperation>(HttpClient(),
            Target(),
            std::move(options),
            requestOptions,
            std::move(completion),
            input,
            std::move(file),
            knownSize)
            ->Start();
    }

    void BlockBlobClient::UploadFromStreamAsyncImpl(std::istream& stream,
        UploadFromOptions options,
        UploadCompletionHandler completion,
        HttpRequestOptions requestOptions)
    {
        if (options.BlockSize == 0U || options.BlockSize > MaxStageBlockBytes)
        {
            Private::PostCompletion(HttpClient(),
                std::move(completion),
                Private::MakeError<Models::UploadBlockBlobResult>(std::make_error_code(std::errc::invalid_argument),
                    "BlockSize must be between 1 byte and 4000 MiB."));
            return;
        }

        std::make_shared<UploadFromOperation>(HttpClient(),
            Target(),
            std::move(options),
            requestOptions,
            std::move(completion),
            stream,
            nullptr,
            std::nullopt)
            ->Start();
    }

} // namespace AVEVA::AzureClient
