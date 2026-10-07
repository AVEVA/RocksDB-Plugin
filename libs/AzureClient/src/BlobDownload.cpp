#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
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
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/post.hpp>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <ios>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient::Private
{
    namespace
    {
        constexpr std::size_t DefaultDownloadChunkSize = BlobTransferLimits::DefaultDownloadChunkSize;
        constexpr std::uint64_t ResponseBodySlack = BlobTransferLimits::ResponseBodySlack;

        // Tags the chunk length with its own type so it's never adjacent-and-same-type with the
        // chunk offset parameter.
        struct ChunkLengthTag
        {
        };

        using ChunkLength = NumericLabel<std::uint64_t, ChunkLengthTag>;

        [[nodiscard]] BlobStorageError MakeFailure(std::errc error, std::string message = {})
        {
            const std::error_code code = std::make_error_code(error);
            return MakeClientError(code, std::move(message));
        }

        // Preserves the real error category so operational failures (disk full, permission denied)
        // are not reported as caller input errors.
        [[nodiscard]] BlobStorageError MakeFailure(std::error_code error, std::string message)
        {
            return MakeClientError(error, std::move(message));
        }

        // errno is the only cause a failed stream open exposes.
        [[nodiscard]] std::error_code LastOpenError() noexcept
        {
            return errno != 0 ? std::error_code{errno, std::generic_category()}
                              : std::make_error_code(std::errc::io_error);
        }

        [[nodiscard]] BlobStorageError MakeInvalidResponse(std::string message)
        {
            return MakeClientError(make_error_code(BlobStorageErrorCode::InvalidResponse), std::move(message));
        }

        [[nodiscard]] std::optional<BlobStorageError> ToFailure(std::error_code error, const HttpResponse& response)
        {
            RequestFailure const failure = DetermineBlobStorageFailure(error, response);
            if (!failure.Error)
            {
                return std::nullopt;
            }
            BlobStorageError details = failure.Details.value_or(MakeClientError(failure.Error));
            details.Code = failure.Error;
            return details;
        }

        [[nodiscard]] std::string TakeBody(HttpResponse& response)
        {
            std::string body = std::move(response).GetBody();
            return body;
        }

        // Destination of the downloaded bytes, always written in offset order.
        class DownloadSink
        {
          public:
            DownloadSink() = default;
            DownloadSink(const DownloadSink&) = delete;
            DownloadSink& operator=(const DownloadSink&) = delete;
            DownloadSink(DownloadSink&&) = delete;
            DownloadSink& operator=(DownloadSink&&) = delete;
            virtual ~DownloadSink() = default;

            virtual void Reserve(std::uint64_t /*size*/)
            {
            }

            [[nodiscard]] virtual bool Write(std::string data) = 0;
        };

        class StreamSink final : public DownloadSink
        {
          public:
            explicit StreamSink(std::ostream& stream) : m_stream(stream)
            {
            }

            [[nodiscard]] bool Write(std::string data) override
            {
                m_stream.write(data.data(), static_cast<std::streamsize>(data.size()));
                return m_stream.good();
            }

          private:
            std::ostream& m_stream;
        };

        class FileSink final : public DownloadSink
        {
          public:
            explicit FileSink(std::shared_ptr<std::ofstream> file) : m_file(std::move(file))
            {
            }

            [[nodiscard]] bool Write(std::string data) override
            {
                m_file->write(data.data(), static_cast<std::streamsize>(data.size()));
                return m_file->good();
            }

          private:
            std::shared_ptr<std::ofstream> m_file;
        };

        class StringSink final : public DownloadSink
        {
          public:
            void Reserve(std::uint64_t size) override
            {
                if (size > m_data.max_size())
                {
                    throw std::length_error("Blob is too large to download into memory.");
                }
                m_data.reserve(static_cast<std::size_t>(size));
            }

            [[nodiscard]] bool Write(std::string data) override
            {
                if (m_data.empty() && m_data.capacity() < data.size())
                {
                    m_data = std::move(data);
                }
                else
                {
                    m_data.append(data);
                }
                return true;
            }

            [[nodiscard]] std::string Take() noexcept
            {
                return std::move(m_data);
            }

          private:
            std::string m_data;
        };

        struct DownloadSummary
        {
            HttpResponse FirstResponse; // headers of the first response; its body has been consumed
            Models::BlobProperties Properties;
            std::uint64_t BytesWritten = 0;
            std::optional<Models::BlobByteRange> ContentRange;
        };

        using SummaryResult = std::expected<DownloadSummary, BlobStorageError>;

        // One download (to a stream, file, or memory). The first ranged GET learns the blob size and
        // ETag; the rest is fetched in ChunkSize ranges with up to Concurrency requests in flight and
        // written strictly in order through a reorder window of at most Concurrency chunks. Later
        // chunks carry If-Match: <first ETag> so a concurrent overwrite fails instead of tearing.
        //
        // All state is owned here. Transport completions and cancellation requests are queued as
        // events and processed one at a time by whichever thread queued first (a trampoline), so
        // handlers never run re-entrantly inside SendAsync or a cancellation emit, and completions
        // that the transport delivers inline are processed inline (no extra executor hop).
        class DownloadOperation final : public std::enable_shared_from_this<DownloadOperation>
        {
          public:
            using Completion = std::move_only_function<void(SummaryResult)>;

            DownloadOperation(IHttpClient& httpClient,
                BlobTarget clientOptions,
                DownloadToOptions options,
                HttpRequestOptions requestOptions,
                std::shared_ptr<DownloadSink> sink,
                Completion completion)
                : m_httpClient(httpClient), m_clientOptions(std::move(clientOptions)), m_options(std::move(options)),
                  m_requestOptions(requestOptions), m_sink(std::move(sink)), m_completion(std::move(completion)),
                  m_parentSlot(m_requestOptions.GetCancellationSlot()),
                  m_chunkSize(m_options.ChunkSize == 0U ? DefaultDownloadChunkSize : m_options.ChunkSize),
                  m_concurrency(std::max<std::size_t>(1U, m_options.Concurrency))
            {
            }

            void Start()
            {
                std::unique_lock lock(m_mutex);
                m_processing = true;
                m_initiating = true;
                lock.unlock();

                Begin();

                lock.lock();
                Drain(lock);
                m_initiating = false;
            }

          private:
            enum class RequestKind : std::uint8_t
            {
                Probe,
                Full,
                Chunk
            };

            struct InFlightRequest
            {
                RequestKind Kind = RequestKind::Chunk;
                std::uint64_t Offset = 0;
                std::uint64_t Length = 0;
                std::unique_ptr<boost::asio::cancellation_signal> Signal;
            };

            struct Event
            {
                bool Cancel = false;
                std::uint64_t RequestId = 0;
                std::error_code Error;
                HttpResponse Response;
            };

            void Enqueue(Event event)
            {
                std::unique_lock lock(m_mutex);
                m_events.push_back(std::move(event));
                if (m_processing)
                {
                    return;
                }
                m_processing = true;
                Drain(lock);
            }

            void Drain(std::unique_lock<std::mutex>& lock)
            {
                while (!m_events.empty())
                {
                    Event event = std::move(m_events.front());
                    m_events.pop_front();
                    lock.unlock();
                    Handle(std::move(event));
                    lock.lock();
                }
                m_processing = false;
            }

            void Begin()
            {
                m_begin = m_options.Range.has_value() ? m_options.Range->Offset : 0U;
                if (m_options.Range.has_value() && m_options.Range->Length.has_value())
                {
                    const std::uint64_t length = *m_options.Range->Length;
                    if (length == 0U || length > std::numeric_limits<std::uint64_t>::max() - m_begin)
                    {
                        Fail(MakeFailure(std::errc::invalid_argument,
                            "Download range length must be non-zero and must not overflow."));
                        MaybeFinish();
                        return;
                    }
                    m_end = m_begin + length;
                }

                if (m_parentSlot.is_connected())
                {
                    std::weak_ptr<DownloadOperation> const weak = weak_from_this();
                    IHttpClient const* httpClient = &m_httpClient;
                    m_parentSlot.assign([weak, httpClient](boost::asio::cancellation_type_t)
                    {
                        boost::asio::post(httpClient->get_executor(),
                            [weak]()
                        {
                            if (auto self = weak.lock())
                            {
                                self->Enqueue(Event{.Cancel = true, .RequestId = 0, .Error = {}, .Response = {}});
                            }
                        });
                    });
                }

                const std::uint64_t probeLength =
                    m_end.has_value() ? std::min<std::uint64_t>(m_chunkSize, *m_end - m_begin) : m_chunkSize;
                Send(RequestKind::Probe, m_begin, probeLength, m_options.Conditions);
                MaybeFinish();
            }

            void Send(RequestKind kind,
                std::uint64_t offset,
                std::uint64_t length,
                const Models::BlobRequestConditions& conditions)
            {
                HttpRequest request;
                HttpRequestOptions requestOptions = m_requestOptions;
                try
                {
                    request = BuildBlobRequest(m_clientOptions, HttpMethod::Get);
                    if (kind != RequestKind::Full)
                    {
                        AddHeader(request,
                            RangeHeaderName,
                            BuildRangeHeaderValue(Models::BlobByteRange{.Offset = offset, .Length = length}));
                        const std::uint64_t limit =
                            length > std::numeric_limits<std::uint64_t>::max() - ResponseBodySlack
                                ? std::numeric_limits<std::uint64_t>::max()
                                : length + ResponseBodySlack;
                        requestOptions.SetResponseBodyLimit(std::max(requestOptions.GetResponseBodyLimit(), limit));
                    }
                    ApplyBlobRequestConditions(request, conditions);
                }
                catch (...)
                {
                    Fail(MakeFailureFromCurrentException());
                    return;
                }

                auto signal = std::make_unique<boost::asio::cancellation_signal>();
                requestOptions.SetCancellationSlot(signal->slot());
                const std::uint64_t id = m_nextRequestId++;
                m_inFlight.emplace(id,
                    InFlightRequest{.Kind = kind, .Offset = offset, .Length = length, .Signal = std::move(signal)});
                if (kind == RequestKind::Chunk)
                {
                    ++m_chunksInFlight;
                }

                SendAuthorizedRequestAsync(m_httpClient,
                    m_clientOptions,
                    std::move(request),
                    [self = shared_from_this(), id](std::error_code error, HttpResponse response) mutable
                {
                    self->Enqueue(Event{.RequestId = id, .Error = error, .Response = std::move(response)});
                },
                    requestOptions);
            }

            void Handle(Event event)
            {
                if (event.Cancel)
                {
                    if (!m_done)
                    {
                        Fail(MakeFailure(std::errc::operation_canceled, "The operation was canceled."));
                        MaybeFinish();
                    }
                    return;
                }

                auto node = m_inFlight.extract(event.RequestId);
                if (node.empty())
                {
                    return;
                }
                const InFlightRequest& request = node.mapped();
                if (request.Kind == RequestKind::Chunk)
                {
                    --m_chunksInFlight;
                }

                if (!m_failure.has_value())
                {
                    switch (request.Kind)
                    {
                    case RequestKind::Probe:
                        OnProbe(event.Error, std::move(event.Response), request.Length);
                        break;
                    case RequestKind::Full:
                        OnFull(event.Error, std::move(event.Response));
                        break;
                    case RequestKind::Chunk:
                        OnChunk(event.Error, std::move(event.Response), request.Offset, request.Length);
                        break;
                    }
                }

                Pump();
                MaybeFinish();
            }

            [[nodiscard]] bool CaptureFirstResponse(HttpResponse& response)
            {
                try
                {
                    m_summary.Properties = ParseBlobProperties(response);
                }
                catch (const std::exception&)
                {
                    Fail(MakeInvalidResponse("Failed to parse the download response headers."));
                    return false;
                }
                m_etag = std::string{FindHeaderValue(response, ETagHeaderName)};
                return true;
            }

            void OnProbe(std::error_code error, HttpResponse response, std::uint64_t probeLength)
            {
                // A 0-byte blob rejects any range with 416; fall back to one un-ranged GET. That is only
                // equivalent when the request starts at byte 0 and has no length limit.
                const bool unboundedFromStart = !m_options.Range.has_value() ||
                                                (m_begin == 0U && !m_options.Range->Length.has_value());
                if (!error && response.GetStatus() == HttpStatusRangeNotSatisfiable && unboundedFromStart)
                {
                    Send(RequestKind::Full, m_begin, 0U, m_options.Conditions);
                    return;
                }
                if (auto failure = ToFailure(error, response); failure.has_value())
                {
                    Fail(std::move(*failure));
                    return;
                }
                if (!CaptureFirstResponse(response))
                {
                    return;
                }

                if (response.GetStatus() != HttpStatusPartialContent)
                {
                    // A 200 means the Range header was ignored and the body is the whole blob from byte 0.
                    if (m_begin != 0U)
                    {
                        Fail(MakeInvalidResponse("The service ignored the requested range."));
                        return;
                    }
                    std::string body = TakeBody(response);
                    if (m_end.has_value() && body.size() > *m_end)
                    {
                        body.resize(static_cast<std::size_t>(*m_end));
                    }
                    m_end = body.size();
                    m_summary.FirstResponse = std::move(response);
                    Deliver(0U, std::move(body));
                    return;
                }

                std::optional<std::uint64_t> total;
                if (const std::string_view header = FindHeaderValue(response, "Content-Range"); !header.empty())
                {
                    const auto parsed = ParseContentRange(header);
                    if (!parsed.has_value() || parsed->Start != m_begin)
                    {
                        Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
                        return;
                    }
                    total = parsed->Total;
                }

                std::string body = TakeBody(response);
                if (total.has_value())
                {
                    m_end = m_end.has_value() ? std::min(*m_end, *total) : *total;
                }
                else if (body.size() < probeLength)
                {
                    m_end = m_begin + body.size();
                }
                else if (!m_end.has_value())
                {
                    Fail(MakeInvalidResponse("The download response does not report the blob size."));
                    return;
                }

                if (*m_end < m_begin || body.size() != std::min<std::uint64_t>(probeLength, *m_end - m_begin))
                {
                    Fail(MakeInvalidResponse("The download response has an unexpected length."));
                    return;
                }

                m_ranged = true;
                m_summary.FirstResponse = std::move(response);
                try
                {
                    m_sink->Reserve(*m_end - m_begin);
                }
                catch (const std::exception&)
                {
                    Fail(MakeFailure(std::errc::not_enough_memory));
                    return;
                }

                m_chunkConditions = m_options.Conditions;
                if (m_chunkConditions.IfMatch.empty())
                {
                    m_chunkConditions.IfMatch = m_etag;
                }
                m_next = m_begin + body.size();
                Deliver(m_begin, std::move(body));
            }

            void OnFull(std::error_code error, HttpResponse response)
            {
                if (auto failure = ToFailure(error, response); failure.has_value())
                {
                    Fail(std::move(*failure));
                    return;
                }
                if (!CaptureFirstResponse(response))
                {
                    return;
                }
                std::string body = TakeBody(response);
                m_end = m_begin + body.size();
                m_summary.FirstResponse = std::move(response);
                Deliver(m_begin, std::move(body));
            }

            void OnChunk(std::error_code error, HttpResponse response, std::uint64_t offset, ChunkLength length)
            {
                if (auto failure = ToFailure(error, response); failure.has_value())
                {
                    Fail(std::move(*failure));
                    return;
                }
                std::string body = TakeBody(response);
                if (response.GetStatus() != HttpStatusPartialContent || body.size() != length.Value)
                {
                    Fail(MakeInvalidResponse("The download response has an unexpected length."));
                    return;
                }
                // A server that answers a different range would otherwise be written at the wrong offset.
                if (const std::string_view header = FindHeaderValue(response, "Content-Range"); !header.empty())
                {
                    const auto parsed = ParseContentRange(header);
                    if (!parsed.has_value() || parsed->Unsatisfied || parsed->Start != offset)
                    {
                        Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
                        return;
                    }
                }
                Deliver(offset, std::move(body));
            }

            // Writes `data` if it is next in order, then any buffered chunks that follow it.
            void Deliver(std::uint64_t offset, std::string data)
            {
                if (offset != m_written + m_begin)
                {
                    m_ready.emplace(offset, std::move(data));
                    return;
                }
                if (!WriteToSink(std::move(data)))
                {
                    return;
                }
                for (auto it = m_ready.find(m_written + m_begin); it != m_ready.end();
                    it = m_ready.find(m_written + m_begin))
                {
                    std::string next = std::move(it->second);
                    m_ready.erase(it);
                    if (!WriteToSink(std::move(next)))
                    {
                        return;
                    }
                }
            }

            [[nodiscard]] bool WriteToSink(std::string data)
            {
                const std::size_t size = data.size();
                if (!m_sink->Write(std::move(data)))
                {
                    Fail(MakeFailure(std::errc::io_error, "Failed to write the downloaded data."));
                    return false;
                }
                m_written += size;
                return true;
            }

            void Pump()
            {
                if (m_failure.has_value() || m_done || !m_ranged)
                {
                    return;
                }
                while (m_next < *m_end && m_chunksInFlight + m_ready.size() < m_concurrency && !m_failure.has_value())
                {
                    const std::uint64_t length = std::min<std::uint64_t>(m_chunkSize, *m_end - m_next);
                    const std::uint64_t offset = m_next;
                    m_next += length;
                    Send(RequestKind::Chunk, offset, length, m_chunkConditions);
                }
            }

            // Records the first failure and cancels every in-flight request; completion waits for
            // them to drain (MaybeFinish).
            void Fail(BlobStorageError error)
            {
                if (m_failure.has_value())
                {
                    return;
                }
                m_failure = std::move(error);
                m_ready.clear();
                for (auto& [id, request] : m_inFlight)
                {
                    request.Signal->emit(boost::asio::cancellation_type::terminal);
                }
            }

            void MaybeFinish()
            {
                if (m_done || !m_inFlight.empty())
                {
                    return;
                }
                if (!m_failure.has_value() && (!m_end.has_value() || m_begin + m_written != *m_end))
                {
                    Fail(MakeInvalidResponse("The download ended before all data was received."));
                }
                if (m_failure.has_value())
                {
                    Finish(std::unexpected(std::move(*m_failure)));
                    return;
                }

                m_summary.BytesWritten = m_written;
                m_summary.Properties.ContentLength = m_written;
                if (m_ranged)
                {
                    m_summary.ContentRange = Models::BlobByteRange{.Offset = m_begin, .Length = m_written};
                }
                Finish(std::move(m_summary));
            }

            void Finish(SummaryResult result)
            {
                m_done = true;
                Completion completion = std::move(m_completion);
                if (m_initiating)
                {
                    PostCompletion(m_httpClient, std::move(completion), std::move(result));
                }
                else
                {
                    completion(std::move(result));
                }
            }

            IHttpClient& m_httpClient;
            BlobTarget m_clientOptions;
            DownloadToOptions m_options;
            HttpRequestOptions m_requestOptions;
            std::shared_ptr<DownloadSink> m_sink;
            Completion m_completion;
            boost::asio::cancellation_slot m_parentSlot;
            std::size_t m_chunkSize;
            std::size_t m_concurrency;

            std::mutex m_mutex;
            std::deque<Event> m_events;
            bool m_processing = false;
            bool m_initiating = false;

            // Only touched by the thread currently draining events.
            std::map<std::uint64_t, InFlightRequest> m_inFlight;
            std::uint64_t m_nextRequestId = 0;
            std::size_t m_chunksInFlight = 0;
            std::map<std::uint64_t, std::string> m_ready;
            Models::BlobRequestConditions m_chunkConditions;
            std::string m_etag;
            std::uint64_t m_begin = 0;
            std::optional<std::uint64_t> m_end;
            std::uint64_t m_next = 0;
            std::uint64_t m_written = 0;
            bool m_ranged = false;
            std::optional<BlobStorageError> m_failure;
            DownloadSummary m_summary;
            bool m_done = false;
        };

        [[nodiscard]] Response<Models::DownloadBlobToResult> ToDownloadToResponse(DownloadSummary summary)
        {
            Models::DownloadBlobToResult result;
            result.Properties = std::move(summary.Properties);
            result.BytesWritten = summary.BytesWritten;
            result.ContentRange = summary.ContentRange;
            return Response<Models::DownloadBlobToResult>{std::move(result),
                std::move(summary.FirstResponse),
                std::nullopt};
        }

        void StartDownload(IHttpClient& httpClient,
            const BlobTarget& options,
            DownloadToOptions operationOptions,
            std::shared_ptr<DownloadSink> sink,
            DownloadOperation::Completion completion,
            HttpRequestOptions requestOptions)
        {
            std::make_shared<DownloadOperation>(httpClient,
                options,
                std::move(operationOptions),
                requestOptions,
                std::move(sink),
                std::move(completion))
                ->Start();
        }
    } // namespace

    DownloadToOptions ToDownloadToOptions(DownloadBlobOptions options)
    {
        DownloadToOptions result;
        result.Range = options.Range;
        result.Conditions = std::move(options.Conditions);
        return result;
    }

    void DownloadBlobAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        DownloadBlobOptions operationOptions,
        DownloadCompletion completion,
        HttpRequestOptions requestOptions)
    {
        auto sink = std::make_shared<StringSink>();
        StartDownload(httpClient,
            options,
            ToDownloadToOptions(std::move(operationOptions)),
            sink,
            [sink, completion = std::move(completion)](SummaryResult result) mutable
        {
            if (!result.has_value())
            {
                completion(std::unexpected(std::move(result).error()));
                return;
            }
            Models::DownloadBlobResult value;
            value.Content = sink->Take();
            value.Properties = std::move(result->Properties);
            value.ContentRange = result->ContentRange;
            completion(
                Response<Models::DownloadBlobResult>{std::move(value), std::move(result->FirstResponse), std::nullopt});
        },
            requestOptions);
    }

    void DownloadBlobToAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        DownloadToOptions operationOptions,
        std::ostream& stream,
        DownloadToCompletion completion,
        HttpRequestOptions requestOptions)
    {
        StartDownload(httpClient,
            options,
            std::move(operationOptions),
            std::make_shared<StreamSink>(stream),
            [completion = std::move(completion)](SummaryResult result) mutable
        {
            if (!result.has_value())
            {
                completion(std::unexpected(std::move(result).error()));
                return;
            }
            completion(ToDownloadToResponse(std::move(*result)));
        },
            requestOptions);
    }

    void DownloadBlobToFileAsync(IHttpClient& httpClient,
        const BlobTarget& options,
        DownloadToOptions operationOptions,
        const std::filesystem::path& path,
        DownloadToCompletion completion,
        HttpRequestOptions requestOptions)
    {
        std::error_code statusError;
        if (std::filesystem::is_directory(path, statusError))
        {
            PostCompletion(httpClient,
                std::move(completion),
                std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>{std::unexpect,
                    MakeFailure(std::make_error_code(std::errc::is_a_directory),
                        "The destination path is a directory.")});
            return;
        }

        // Download into a sibling temporary file and rename it over the target only on success, so a
        // failed download never leaves a truncated target behind.
        std::filesystem::path tempPath = path;
        tempPath += ".partial-" + CreateClientRequestId();
        errno = 0;
        auto file = std::make_shared<std::ofstream>(tempPath, std::ios::binary | std::ios::trunc);
        if (!*file)
        {
            PostCompletion(httpClient,
                std::move(completion),
                std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>{std::unexpect,
                    MakeFailure(LastOpenError(), "Failed to open destination file.")});
            return;
        }

        StartDownload(httpClient,
            options,
            std::move(operationOptions),
            std::make_shared<FileSink>(file),
            [file, tempPath, target = path, completion = std::move(completion)](SummaryResult result) mutable
        {
            file->close();
            std::error_code ignored;
            if (!result.has_value())
            {
                std::filesystem::remove(tempPath, ignored);
                completion(std::unexpected(std::move(result).error()));
                return;
            }
            if (file->fail())
            {
                std::filesystem::remove(tempPath, ignored);
                completion(std::unexpected(MakeFailure(std::errc::io_error, "I/O error writing destination file.")));
                return;
            }
            std::error_code renameError;
            if (std::filesystem::is_directory(target, renameError))
            {
                std::filesystem::remove(tempPath, ignored);
                completion(std::unexpected(MakeFailure(std::make_error_code(std::errc::is_a_directory),
                    "The destination path is a directory.")));
                return;
            }
            renameError.clear();
            std::filesystem::rename(tempPath, target, renameError);
            if (renameError)
            {
                std::filesystem::remove(tempPath, ignored);
                completion(std::unexpected(
                    MakeFailure(renameError, "Failed to rename temporary download file to destination.")));
                return;
            }
            completion(ToDownloadToResponse(std::move(*result)));
        },
            requestOptions);
    }
} // namespace AVEVA::AzureClient::Private
