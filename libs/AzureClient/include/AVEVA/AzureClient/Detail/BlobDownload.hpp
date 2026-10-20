// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "AVEVA/AzureClient/Detail/AsyncInitiation.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/DownloadSinks.hpp>
#include <AVEVA/AzureClient/Detail/ProtocolConstants.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/post.hpp>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
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
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

namespace AVEVA::AzureClient::Private {
namespace DownloadDetail {
// Runs a blocking task off the I/O thread: on the caller's scheduler, else the HTTP client's, else a short-lived
// thread (so no process-wide pool outlives the clients).
template <class THttp>
void RunOnWriter(THttp& httpClient, const DownloadWriteScheduler& scheduler, std::move_only_function<void()> task) {
    if (scheduler) {
        scheduler(std::move(task));
    } else if constexpr (requires {
                             { httpClient.GetDownloadWriter() } -> std::convertible_to<DownloadWriteScheduler>;
                         }) {
        DownloadWriteScheduler{httpClient.GetDownloadWriter()}(std::move(task));
    } else {
        std::thread(std::move(task)).detach();
    }
}

inline constexpr std::size_t DefaultDownloadChunkSize = BlobTransferLimits::DefaultDownloadChunkSize;
inline constexpr std::uint64_t ResponseBodySlack = BlobTransferLimits::ResponseBodySlack;
inline constexpr std::uint64_t MaxBufferedDownloadBytes = BlobTransferLimits::MaxBufferedDownloadBytes;

// Tags the chunk length with its own type so it's never adjacent-and-same-type with the
// chunk offset parameter.
struct ChunkLengthTag {};

using ChunkLength = NumericLabel<std::uint64_t, ChunkLengthTag>;

// Range-end arithmetic must never wrap: a wrapped end would make the download look complete or place data at the
// wrong offset.
[[nodiscard]] constexpr std::optional<std::uint64_t> CheckedAdd(std::uint64_t lhs, std::uint64_t rhs) noexcept {
    if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs) {
        return std::nullopt;
    }
    return lhs + rhs;
}

[[nodiscard]] BlobStorageError MakeFailure(std::errc error, std::string message = {});

// Preserves the real error category so operational failures (disk full, permission denied)
// are not reported as caller input errors.
[[nodiscard]] BlobStorageError MakeFailure(std::error_code error, std::string message);

// errno is the only cause a failed stream open exposes.
[[nodiscard]] std::error_code LastOpenError() noexcept;

[[nodiscard]] BlobStorageError MakeInvalidResponse(std::string message);

// A sink may throw anything; an exception must never unwind through the event loop, so it counts as a failed write.
[[nodiscard]] inline bool TryWrite(DownloadSink& sink, std::string data) noexcept {
    try {
        return sink.Write(std::move(data));
    } catch (...) {
        return false;
    }
}

[[nodiscard]] std::optional<BlobStorageError> ToFailure(std::error_code error, const HttpResponse& response);

[[nodiscard]] std::string TakeBody(HttpResponse& response);

struct DownloadSummary {
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
template <HttpClientLike THttp, class TCred, class TDone>
class DownloadOperation final : public std::enable_shared_from_this<DownloadOperation<THttp, TCred, TDone>> {
  public:
    DownloadOperation(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> clientOptions,
                      DownloadToOptions options, AzureRequestOptions requestOptions, std::shared_ptr<DownloadSink> sink,
                      TDone completion)
        : m_httpClient(httpClient), m_clientOptions(std::move(clientOptions)), m_options(std::move(options)),
          m_requestOptions(requestOptions), m_sink(std::move(sink)), m_completion(std::move(completion)),
          m_parentSlot(m_requestOptions.GetCancellationSlot()),
          m_chunkSize(m_options.ChunkSize == 0U ? DefaultDownloadChunkSize : m_options.ChunkSize),
          m_concurrency(std::min(std::max<std::size_t>(1U, m_options.Concurrency),
                                 std::max<std::size_t>(1U, MaxBufferedDownloadBytes / m_chunkSize))) {}

    void Start() {
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
    enum class RequestKind : std::uint8_t { Probe, Full, Chunk };

    struct InFlightRequest {
        RequestKind Kind = RequestKind::Chunk;
        std::uint64_t Offset = 0;
        std::uint64_t Length = 0;
        std::unique_ptr<boost::asio::cancellation_signal> Signal;
    };

    struct Event {
        bool Cancel = false;
        std::uint64_t RequestId = 0;
        std::error_code Error;
        HttpResponse Response;
        bool WriteDone = false;
        bool WriteOk = true;
        std::size_t WriteSize = 0;
    };

    void Enqueue(Event event) {
        std::unique_lock lock(m_mutex);
        m_events.push_back(std::move(event));
        if (m_processing) {
            return;
        }
        m_processing = true;
        Drain(lock);
    }

    void Drain(std::unique_lock<std::mutex>& lock) {
        while (!m_events.empty()) {
            Event event = std::move(m_events.front());
            m_events.pop_front();
            lock.unlock();
            Handle(std::move(event));
            lock.lock();
        }
        m_processing = false;
    }

    void Begin() {
        m_begin = m_options.Range.has_value() ? m_options.Range->Offset : 0U;
        if (m_options.Range.has_value() && m_options.Range->Length.has_value()) {
            const std::uint64_t length = *m_options.Range->Length;
            if (length == 0U || length > std::numeric_limits<std::uint64_t>::max() - m_begin) {
                Fail(MakeFailure(std::errc::invalid_argument,
                                 "Download range length must be non-zero and must not overflow."));
                MaybeFinish();
                return;
            }
            m_end = m_begin + length;
        }

        if (m_parentSlot.is_connected()) {
            std::weak_ptr<DownloadOperation> const weak = this->weak_from_this();
            THttp const* httpClient = &m_httpClient;
            m_parentSlot.assign([weak, httpClient](boost::asio::cancellation_type_t) {
                boost::asio::post(httpClient->get_executor(), [weak]() {
                    if (auto self = weak.lock()) {
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

    void Send(RequestKind kind, std::uint64_t offset, std::uint64_t length,
              const Models::BlobRequestConditions& conditions) {
        HttpRequest request;
        AzureRequestOptions requestOptions = m_requestOptions;
        try {
            request = BuildBlobRequest(*m_clientOptions, HttpMethod::Get);
            if (kind != RequestKind::Full) {
                AddHeader(request, RangeHeaderName,
                          BuildRangeHeaderValue(Models::BlobByteRange{.Offset = offset, .Length = length}));
                const std::uint64_t limit = length > std::numeric_limits<std::uint64_t>::max() - ResponseBodySlack
                                                ? std::numeric_limits<std::uint64_t>::max()
                                                : length + ResponseBodySlack;
                requestOptions.SetResponseBodyLimit(std::max(requestOptions.GetResponseBodyLimit(), limit));
            }
            ApplyBlobRequestConditions(request, conditions);
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
            return;
        }

        auto signal = std::make_unique<boost::asio::cancellation_signal>();
        requestOptions.SetCancellationSlot(signal->slot());
        const std::uint64_t id = m_nextRequestId++;
        m_inFlight.emplace(
            id, InFlightRequest{.Kind = kind, .Offset = offset, .Length = length, .Signal = std::move(signal)});
        if (kind == RequestKind::Chunk) {
            ++m_chunksInFlight;
        }

        SendAuthorizedRequestAsync(
            m_httpClient, *m_clientOptions, std::move(request),
            [self = this->shared_from_this(), id](std::error_code error, HttpResponse response) mutable {
                self->Enqueue(Event{.RequestId = id, .Error = error, .Response = std::move(response)});
            },
            requestOptions);
    }

    void Handle(Event event) {
        if (event.Cancel) {
            if (!m_done) {
                Fail(MakeFailure(std::errc::operation_canceled, "The operation was canceled."));
                MaybeFinish();
            }
            return;
        }

        if (event.WriteDone) {
            OnWriteDone(event);
            Pump();
            MaybeFinish();
            return;
        }

        auto node = m_inFlight.extract(event.RequestId);
        if (node.empty()) {
            return;
        }
        const InFlightRequest& request = node.mapped();
        if (request.Kind == RequestKind::Chunk) {
            --m_chunksInFlight;
        }

        if (!m_failure.has_value()) {
            switch (request.Kind) {
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

    [[nodiscard]] bool CaptureFirstResponse(HttpResponse& response) {
        try {
            m_summary.Properties = ParseBlobProperties(response);
        } catch (const std::exception&) {
            Fail(MakeInvalidResponse("Failed to parse the download response headers."));
            return false;
        }
        m_etag = std::string{FindHeaderValue(response, ETagHeaderName)};
        return true;
    }

    void OnProbe(std::error_code error, HttpResponse response, std::uint64_t probeLength) {
        // A 0-byte blob rejects any range with 416; fall back to one un-ranged GET. That is only
        // equivalent when the request starts at byte 0 and has no length limit.
        const bool unboundedFromStart =
            !m_options.Range.has_value() || (m_begin == 0U && !m_options.Range->Length.has_value());
        if (!error && response.GetStatus() == HttpStatusRangeNotSatisfiable && unboundedFromStart) {
            Send(RequestKind::Full, m_begin, 0U, m_options.Conditions);
            return;
        }
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            return;
        }
        if (!CaptureFirstResponse(response)) {
            return;
        }

        if (response.GetStatus() != HttpStatusPartialContent) {
            // A 200 means the Range header was ignored and the body is the whole blob from byte 0.
            if (m_begin != 0U) {
                Fail(MakeInvalidResponse("The service ignored the requested range."));
                return;
            }
            std::string body = TakeBody(response);
            if (m_end.has_value() && body.size() > *m_end) {
                body.resize(static_cast<std::size_t>(*m_end));
            }
            m_end = body.size();
            m_summary.FirstResponse = std::move(response);
            if (ExceedsMaxSize(body.size())) {
                return;
            }
            Deliver(0U, std::move(body));
            return;
        }

        // A 206 without a valid Content-Range cannot be placed or validated; refuse it rather than guess.
        const std::string_view rangeHeader = FindHeaderValue(response, "Content-Range");
        const auto parsedRange = rangeHeader.empty() ? std::nullopt : ParseContentRange(rangeHeader);
        if (!parsedRange.has_value() || parsedRange->Unsatisfied || parsedRange->Start != m_begin ||
            parsedRange->End < parsedRange->Start) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            return;
        }
        const std::optional<std::uint64_t> total = parsedRange->Total;
        m_total = total;

        std::string body = TakeBody(response);
        if (body.empty() || parsedRange->End - parsedRange->Start != body.size() - 1U) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            return;
        }
        if (total.has_value()) {
            m_end = m_end.has_value() ? std::min(*m_end, *total) : *total;
        } else if (body.size() < probeLength) {
            m_end = CheckedAdd(m_begin, body.size());
            if (!m_end.has_value()) {
                Fail(MakeInvalidResponse("The download response range overflows."));
                return;
            }
        } else if (!m_end.has_value()) {
            Fail(MakeInvalidResponse("The download response does not report the blob size."));
            return;
        }

        if (*m_end < m_begin || body.size() != std::min<std::uint64_t>(probeLength, *m_end - m_begin)) {
            Fail(MakeInvalidResponse("The download response has an unexpected length."));
            return;
        }

        m_ranged = true;
        m_summary.FirstResponse = std::move(response);
        if (ExceedsMaxSize(*m_end - m_begin)) {
            return;
        }
        try {
            m_sink->Reserve(*m_end - m_begin);
        } catch (const std::exception&) {
            Fail(MakeFailure(std::errc::not_enough_memory));
            return;
        } catch (...) {
            Fail(MakeFailure(std::errc::io_error, "The download sink failed to reserve space."));
            return;
        }

        m_chunkConditions = m_options.Conditions;
        if (m_chunkConditions.IfMatch.empty()) {
            m_chunkConditions.IfMatch = m_etag;
        }
        const auto next = CheckedAdd(m_begin, body.size());
        if (!next.has_value()) {
            Fail(MakeInvalidResponse("The download response range overflows."));
            return;
        }
        m_next = *next;
        Deliver(m_begin, std::move(body));
    }

    void OnFull(std::error_code error, HttpResponse response) {
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            return;
        }
        if (!CaptureFirstResponse(response)) {
            return;
        }
        std::string body = TakeBody(response);
        m_end = CheckedAdd(m_begin, body.size());
        if (!m_end.has_value()) {
            Fail(MakeInvalidResponse("The download response range overflows."));
            return;
        }
        m_summary.FirstResponse = std::move(response);
        if (ExceedsMaxSize(body.size())) {
            return;
        }
        Deliver(m_begin, std::move(body));
    }

    // Fails the download when `size` is over the caller's limit; true means it failed.
    [[nodiscard]] bool ExceedsMaxSize(std::uint64_t size) {
        if (!m_options.MaxSize.has_value() || size <= *m_options.MaxSize) {
            return false;
        }
        Fail(MakeFailure(std::errc::file_too_large, "The download is larger than the configured maximum size."));
        return true;
    }

    void OnChunk(std::error_code error, HttpResponse response, std::uint64_t offset, ChunkLength length) {
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            return;
        }
        std::string body = TakeBody(response);
        if (response.GetStatus() != HttpStatusPartialContent || body.size() != length.Value) {
            Fail(MakeInvalidResponse("The download response has an unexpected length."));
            return;
        }
        // A server that answers a different range would otherwise be written at the wrong offset.
        if (const std::string_view header = FindHeaderValue(response, "Content-Range"); !header.empty()) {
            const auto parsed = ParseContentRange(header);
            if (!parsed.has_value() || parsed->Unsatisfied || parsed->Start != offset || parsed->End < parsed->Start ||
                body.empty() || parsed->End - parsed->Start != body.size() - 1U ||
                (m_total.has_value() && parsed->Total.has_value() && *parsed->Total != *m_total)) {
                Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
                return;
            }
        } else {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            return;
        }
        Deliver(offset, std::move(body));
    }

    // Writes `data` (or queues it behind earlier gaps), then everything that has become contiguous.
    void Deliver(std::uint64_t offset, std::string data) {
        m_ready.emplace(offset, std::move(data));
        WriteReady();
    }

    // Sinks that block on the disk are written from the writer scheduler (see RunOnWriter) so a slow
    // write never stalls the I/O thread that is also servicing every other connection. One write is in
    // flight at a time, which keeps the sink in offset order; completion re-enters on the I/O executor.
    void WriteReady() {
        while (!m_writing && !m_failure.has_value()) {
            const auto cursor = CheckedAdd(m_written, m_begin);
            if (!cursor.has_value()) {
                Fail(MakeInvalidResponse("The download write offset overflows."));
                return;
            }
            const auto it = m_ready.find(*cursor);
            if (it == m_ready.end()) {
                return;
            }
            std::string data = std::move(it->second);
            m_ready.erase(it);

            if (m_sink->WritesInline()) {
                if (!WriteToSink(std::move(data))) {
                    return;
                }
                continue;
            }

            // Take every chunk that is already contiguous so one writer task (and one hop back to the
            // I/O executor) covers the whole run instead of one pair of hops per chunk.
            std::vector<std::string> batch;
            batch.reserve(m_ready.size() + 1U);
            batch.push_back(std::move(data));
            std::uint64_t next = *cursor;
            while (true) {
                const auto advanced = CheckedAdd(next, batch.back().size());
                if (!advanced.has_value()) {
                    Fail(MakeInvalidResponse("The download write offset overflows."));
                    return;
                }
                next = *advanced;
                const auto following = m_ready.find(next);
                if (following == m_ready.end()) {
                    break;
                }
                batch.push_back(std::move(following->second));
                m_ready.erase(following);
            }

            m_writing = true;
            m_writingChunks = batch.size();
            RunOnWriter(m_httpClient, m_options.WriteScheduler,
                        [self = this->shared_from_this(), batch = std::move(batch)]() mutable {
                            std::size_t size = 0U;
                            bool ok = true;
                            for (auto& chunk : batch) {
                                const std::size_t chunkSize = chunk.size();
                                ok = TryWrite(*self->m_sink, std::move(chunk));
                                if (!ok) {
                                    break;
                                }
                                size += chunkSize;
                            }
                            boost::asio::post(self->m_httpClient.get_executor(), [self, ok, size]() {
                                self->Enqueue(Event{.WriteDone = true, .WriteOk = ok, .WriteSize = size});
                            });
                        });
            return;
        }
    }

    void OnWriteDone(const Event& event) {
        m_writing = false;
        m_writingChunks = 0U;
        if (!event.WriteOk) {
            Fail(MakeFailure(std::errc::io_error, "Failed to write the downloaded data."));
            return;
        }
        m_written += event.WriteSize;
        WriteReady();
    }

    [[nodiscard]] bool WriteToSink(std::string data) {
        const std::size_t size = data.size();
        if (!TryWrite(*m_sink, std::move(data))) {
            Fail(MakeFailure(std::errc::io_error, "Failed to write the downloaded data."));
            return false;
        }
        m_written += size;
        return true;
    }

    void Pump() {
        if (m_failure.has_value() || m_done || !m_ranged) {
            return;
        }
        while (m_next < *m_end && m_chunksInFlight + m_ready.size() + m_writingChunks < m_concurrency &&
               !m_failure.has_value()) {
            const std::uint64_t length = std::min<std::uint64_t>(m_chunkSize, *m_end - m_next);
            const std::uint64_t offset = m_next;
            m_next += length;
            Send(RequestKind::Chunk, offset, length, m_chunkConditions);
        }
    }

    // Records the first failure and cancels every in-flight request; completion waits for
    // them to drain (MaybeFinish).
    void Fail(BlobStorageError error) {
        if (m_failure.has_value()) {
            return;
        }
        m_failure = std::move(error);
        m_ready.clear();
        for (auto& [id, request] : m_inFlight) {
            request.Signal->emit(boost::asio::cancellation_type::terminal);
        }
    }

    void MaybeFinish() {
        if (m_done || !m_inFlight.empty() || m_writing) {
            return;
        }
        if (!m_failure.has_value() && (!m_end.has_value() || *m_end < m_begin || m_written != *m_end - m_begin)) {
            Fail(MakeInvalidResponse("The download ended before all data was received."));
        }
        if (m_failure.has_value()) {
            Finish(std::unexpected(std::move(*m_failure)));
            return;
        }

        m_summary.BytesWritten = m_written;
        m_summary.Properties.ContentLength = m_written;
        if (m_ranged) {
            m_summary.ContentRange = Models::BlobByteRange{.Offset = m_begin, .Length = m_written};
        }
        Finish(std::move(m_summary));
    }

    void Finish(SummaryResult result) {
        m_done = true;
        TDone completion = std::move(m_completion);
        if (m_initiating) {
            PostCompletion(m_httpClient, std::move(completion), std::move(result));
        } else {
            completion(std::move(result));
        }
    }

    THttp& m_httpClient;
    std::shared_ptr<const BasicBlobTarget<TCred>> m_clientOptions;
    DownloadToOptions m_options;
    AzureRequestOptions m_requestOptions;
    std::shared_ptr<DownloadSink> m_sink;
    TDone m_completion;
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
    std::optional<std::uint64_t> m_total;
    std::uint64_t m_next = 0;
    std::uint64_t m_written = 0;
    bool m_ranged = false;
    bool m_writing = false;
    std::size_t m_writingChunks = 0;
    std::optional<BlobStorageError> m_failure;
    DownloadSummary m_summary;
    bool m_done = false;
};

[[nodiscard]] Response<Models::DownloadBlobToResult> ToDownloadToResponse(DownloadSummary summary);

template <HttpClientLike THttp, class TCred, class TDone>
void StartDownload(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                   DownloadToOptions operationOptions, std::shared_ptr<DownloadSink> sink, TDone completion,
                   AzureRequestOptions requestOptions) {
    std::make_shared<DownloadOperation<THttp, TCred, TDone>>(httpClient, std::move(options),
                                                             std::move(operationOptions), requestOptions,
                                                             std::move(sink), std::move(completion))
        ->Start();
}
// Closes the temporary file and, on success, renames it over `target`; any failure removes the temporary file.
[[nodiscard]] std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>
FinalizeFileDownload(std::ofstream& file, const std::filesystem::path& tempPath, const std::filesystem::path& target,
                     SummaryResult result);
} // namespace DownloadDetail

template <HttpClientLike THttp, class TCred, class TCompletion>
void DownloadBlobAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                       DownloadBlobOptions operationOptions, TCompletion completion,
                       AzureRequestOptions requestOptions) {
    auto sink = std::make_shared<StringSink>();
    DownloadDetail::StartDownload(
        httpClient, std::move(options), ToDownloadToOptions(std::move(operationOptions)), sink,
        [sink, completion = std::move(completion)](DownloadDetail::SummaryResult result) mutable {
            if (!result.has_value()) {
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

template <HttpClientLike THttp, class TCred, class TCompletion>
void DownloadBlobToSpanAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                             DownloadToOptions operationOptions, std::span<char> destination, TCompletion completion,
                             AzureRequestOptions requestOptions) {
    DownloadDetail::StartDownload(
        httpClient, std::move(options), std::move(operationOptions), std::make_shared<SpanSink>(destination),
        [completion = std::move(completion)](DownloadDetail::SummaryResult result) mutable {
            if (!result.has_value()) {
                completion(std::unexpected(std::move(result).error()));
                return;
            }
            completion(DownloadDetail::ToDownloadToResponse(std::move(*result)));
        },
        requestOptions);
}

// Every blocking filesystem call (directory check, open, close, rename, remove) runs on the write scheduler (see
// RunOnWriter), never on the thread that initiates the download or on the transport thread that completes it. The
// handler then completes on its associated executor.
template <HttpClientLike THttp, class TCred, class TCompletion>
void DownloadBlobToFileAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                             DownloadToOptions operationOptions, const std::filesystem::path& path,
                             TCompletion completion, AzureRequestOptions requestOptions) {
    using FileResult = std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>;
    auto scheduler = operationOptions.WriteScheduler;
    DownloadDetail::RunOnWriter(
        httpClient, scheduler,
        [&httpClient, scheduler, options, operationOptions = std::move(operationOptions), path,
         completion = std::move(completion), requestOptions]() mutable {
            std::error_code statusError;
            if (std::filesystem::is_directory(path, statusError)) {
                PostCompletion(httpClient, std::move(completion),
                               FileResult{std::unexpect,
                                          DownloadDetail::MakeFailure(std::make_error_code(std::errc::is_a_directory),
                                                                      "The destination path is a directory.")});
                return;
            }

            // Download into a sibling temporary file and rename it over the target only on success, so a
            // failed download never leaves a truncated target behind.
            std::filesystem::path tempPath = path;
            tempPath += ".partial-" + CreateClientRequestId();
            errno = 0;
            auto file = std::make_shared<std::ofstream>(tempPath, std::ios::binary | std::ios::trunc);
            if (!*file) {
                PostCompletion(
                    httpClient, std::move(completion),
                    FileResult{std::unexpect, DownloadDetail::MakeFailure(DownloadDetail::LastOpenError(),
                                                                          "Failed to open destination file.")});
                return;
            }

            // The chunk requests are started from the I/O executor, like any other download.
            boost::asio::post(
                httpClient.get_executor(),
                [&httpClient, scheduler = std::move(scheduler), options = std::move(options),
                 operationOptions = std::move(operationOptions), file, tempPath = std::move(tempPath),
                 target = std::move(path), completion = std::move(completion), requestOptions]() mutable {
                    DownloadDetail::StartDownload(
                        httpClient, std::move(options), std::move(operationOptions), std::make_shared<FileSink>(file),
                        [&httpClient, scheduler = std::move(scheduler), file, tempPath = std::move(tempPath),
                         target = std::move(target),
                         completion = std::move(completion)](DownloadDetail::SummaryResult result) mutable {
                            DownloadDetail::RunOnWriter(
                                httpClient, scheduler,
                                [&httpClient, file, tempPath = std::move(tempPath), target = std::move(target),
                                 completion = std::move(completion), result = std::move(result)]() mutable {
                                    PostCompletion(httpClient, std::move(completion),
                                                   DownloadDetail::FinalizeFileDownload(*file, tempPath, target,
                                                                                        std::move(result)));
                                });
                        },
                        requestOptions);
                });
        });
}
} // namespace AVEVA::AzureClient::Private
