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
#include <AVEVA/AzureClient/Detail/AggregateDeadline.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/post.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <gsl/pointers>
#include <gsl/util>
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
#include <vector>

namespace AVEVA::AzureClient::Private {
namespace DownloadDetail {
// Runs a blocking task off the I/O thread: on the caller's scheduler, else the HTTP client's, else a short-lived
// thread for custom transports without one (so no process-wide pool outlives the clients).
template <class THttp, class TTask>
void RunOnWriter(THttp& httpClient, const std::optional<DownloadWriteScheduler>& scheduler, TTask task) {
    if (scheduler) {
        boost::asio::post(*scheduler, std::move(task));
    } else if constexpr (requires { boost::asio::post(httpClient.GetDownloadWriter(), std::move(task)); }) {
        boost::asio::post(httpClient.GetDownloadWriter(), std::move(task));
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
[[nodiscard]] inline bool TryWrite(DownloadSinkLike auto& sink, std::string data) noexcept {
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

// An internal completion adapter never reaches a user handler and does not throw, so it may run inline on the
// initiating thread; only a user's completion has to be deferred to its executor.
template <class T>
concept InternalCompletion = requires { requires T::IsInternal; };

// One download (to a stream, file, or memory). The first ranged GET learns the blob size and
// ETag; the rest is fetched in ChunkSize ranges with up to Concurrency requests in flight and
// written strictly in order through a reorder window of at most Concurrency chunks. Later
// chunks carry If-Match: <first ETag> so a concurrent overwrite fails instead of tearing.
//
// All state is owned here. Transport completions and cancellation requests are queued as
// events and processed one at a time by whichever thread queued first (a trampoline), so
// handlers never run re-entrantly inside SendAsync or a cancellation emit, and completions
// that the transport delivers inline are processed inline (no extra executor hop).
template <HttpClientLike THttp, class TCred, DownloadSinkLike TSink, class TDone>
class DownloadOperation final : public std::enable_shared_from_this<DownloadOperation<THttp, TCred, TSink, TDone>> {
  public:
    DownloadOperation(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> clientOptions,
                      DownloadToOptions options, AzureRequestOptions requestOptions, TSink sink, TDone completion,
                      std::chrono::steady_clock::time_point start)
        : m_httpClient(httpClient), m_clientOptions(std::move(clientOptions)), m_options(std::move(options)),
          m_requestOptions(requestOptions), m_deadline(m_requestOptions, start), m_sink(std::move(sink)),
          m_completion(std::move(completion)), m_parentSlot(m_requestOptions.GetCancellationSlot()),
          m_chunkSize(m_options.ChunkSize == 0U ? DefaultDownloadChunkSize : m_options.ChunkSize),
          m_concurrency(std::min(std::max<std::size_t>(1U, m_options.Concurrency),
                                 std::max<std::size_t>(1U, MaxBufferedDownloadBytes / m_chunkSize))) {}

    void Start() {
        // Must precede everything that can queue an event: queueing is then allocation-free (see ReserveEvents).
        ReserveEvents(2U);
        std::unique_lock lock(m_mutex);
        m_processing = true;
        m_initiating = true;
        lock.unlock();

        Guarded([this] { Begin(); });

        lock.lock();
        Drain(lock);
        m_initiating = false;
    }

  private:
    // An internal throw (allocation, request setup, writer scheduling) becomes the download's failure instead of
    // escaping onto the host event loop. Once the download has completed a throw can only come from the caller's
    // completion, which is not a storage failure and propagates after the runner state is restored.
    template <class TStep> void Guarded(TStep&& step) {
        try {
            step();
        } catch (...) {
            if (m_done) {
                std::lock_guard lock(m_mutex);
                m_processing = false;
                throw;
            }
            Fail(MakeFailureFromCurrentException());
            try {
                MaybeFinish();
            } catch (...) {
                std::lock_guard lock(m_mutex);
                m_processing = false;
                throw;
            }
        }
    }

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
        std::optional<BlobStorageError> WriteError;
    };

    // Queueing an event must not fail: a lost completion, write-done or cancellation would strand the download. The
    // pending events are bounded (one per in-flight request, one write-done, one cancel), so room for them is
    // reserved up front, on a path that can still fail the download, and Enqueue only ever fills it.
    void ReserveEvents(std::size_t extra) {
        std::lock_guard lock(m_mutex);
        CompactEvents();
        const std::size_t needed = m_events.size() + extra;
        if (m_events.capacity() < needed) {
            m_events.reserve(needed);
        }
    }

    // Reclaims the consumed prefix without allocating.
    void CompactEvents() {
        if (m_head != 0U) {
            m_events.erase(m_events.begin(), m_events.begin() + static_cast<std::ptrdiff_t>(m_head));
            m_head = 0U;
        }
    }

    void Enqueue(Event event) {
        std::unique_lock lock(m_mutex);
        if (m_events.size() == m_events.capacity()) {
            CompactEvents();
        }
        m_events.push_back(std::move(event));
        if (m_processing) {
            return;
        }
        m_processing = true;
        Drain(lock);
    }

    void Drain(std::unique_lock<std::mutex>& lock) {
        while (m_head < m_events.size()) {
            Event event = std::move(m_events[m_head]);
            ++m_head;
            lock.unlock();
            Guarded([&] { Handle(std::move(event)); });
            lock.lock();
        }
        m_events.clear();
        m_head = 0U;
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
            // The handler outlives the operation when the caller's explicit slot stays connected, so it must not
            // reach the transport: it keeps a copy of the executor and checks the operation first.
            m_parentSlot.assign([weak, executor = m_httpClient.get_executor()](boost::asio::cancellation_type_t type) {
                if (!IsAbortingCancellation(type) || weak.expired()) {
                    return;
                }
                // One queued cancel is enough, and it keeps the bound on pending events.
                if (const auto self = weak.lock(); !self || self->m_cancelPosted.exchange(true)) {
                    return;
                }
                boost::asio::post(executor, [weak]() {
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
        // Every request gets only what is left of the whole download's budget, and none starts once it is spent.
        if (!m_deadline.Apply(requestOptions)) {
            Fail(MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
            return;
        }
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
        ReserveEvents(m_inFlight.size() + 3U);
        const std::uint64_t id = m_nextRequestId++;
        m_inFlight.emplace(
            id, InFlightRequest{.Kind = kind, .Offset = offset, .Length = length, .Signal = std::move(signal)});
        if (kind == RequestKind::Chunk) {
            ++m_chunksInFlight;
        }

        try {
            SendAuthorizedRequestAsync(
                m_httpClient, *m_clientOptions, std::move(request),
                [self = this->shared_from_this(), id](std::error_code error, HttpResponse response) mutable {
                    self->Enqueue(Event{.RequestId = id, .Error = error, .Response = std::move(response)});
                },
                requestOptions);
        } catch (...) {
            // The request was not accepted, so no completion will ever drain this entry.
            if (m_inFlight.erase(id) != 0U && kind == RequestKind::Chunk) {
                --m_chunksInFlight;
            }
            Fail(MakeFailureFromCurrentException());
        }
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
                body.resize(gsl::narrow_cast<std::size_t>(*m_end));
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
            m_sink.Reserve(*m_end - m_begin);
        } catch (const std::exception&) {
            Fail(MakeFailure(std::errc::not_enough_memory));
            return;
        } catch (...) {
            Fail(MakeFailure(std::errc::io_error, "The download sink failed to reserve space."));
            return;
        }

        m_chunkConditions = m_options.Conditions;
        // A wildcard or list only tests existence, so it cannot keep later chunks on one blob version.
        if (!m_etag.empty()) {
            m_chunkConditions.IfMatch = m_etag;
        } else if (m_chunkConditions.IfMatch == "*" || m_chunkConditions.IfMatch.find(',') != std::string::npos) {
            m_chunkConditions.IfMatch.clear();
        }
        const auto next = CheckedAdd(m_begin, body.size());
        if (!next.has_value()) {
            Fail(MakeInvalidResponse("The download response range overflows."));
            return;
        }
        // Without a validator the remaining chunks would be unconditional and could mix blob versions.
        if (*next < *m_end && m_chunkConditions.IfMatch.empty()) {
            Fail(MakeInvalidResponse("The download response has no ETag to keep later chunks on one blob version."));
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

            if constexpr (TSink::WritesInline) {
                if (!WriteToSink(std::move(data))) {
                    return;
                }
                continue;
            } else {
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
                try {
                    RunOnWriter(m_httpClient, m_options.WriteScheduler,
                                [self = this->shared_from_this(), batch = std::move(batch)]() mutable {
                                    std::size_t size = 0U;
                                    bool ok = true;
                                    for (auto& chunk : batch) {
                                        const std::size_t chunkSize = chunk.size();
                                        ok = TryWrite(self->m_sink, std::move(chunk));
                                        if (!ok) {
                                            break;
                                        }
                                        size += chunkSize;
                                    }
                                    PostWriteDone(self, ok, size);
                                });
                } catch (...) {
                    // Scheduling failed, so no write-done event will ever clear the writing state.
                    m_writing = false;
                    m_writingChunks = 0U;
                    Fail(MakeFailureFromCurrentException());
                }
                return;
            }
        }
    }

    // Runs on a writer thread. The hop back to the I/O executor is what normally clears `m_writing`; if it cannot be
    // scheduled the event is queued from here instead, so the failure still completes the download exactly once
    // rather than escaping the worker and stranding the write.
    static void PostWriteDone(const std::shared_ptr<DownloadOperation>& self, bool ok, std::size_t size) noexcept {
        std::optional<BlobStorageError> failure;
        try {
            boost::asio::post(self->m_httpClient.get_executor(), [self, ok, size]() {
                self->Enqueue(Event{.WriteDone = true, .WriteOk = ok, .WriteSize = size});
            });
            return;
        } catch (...) {
            try {
                failure = MakeFailureFromCurrentException();
            } catch (...) {
                // The generic write failure is reported instead.
            }
        }
        try {
            self->Enqueue(
                Event{.WriteDone = true, .WriteOk = false, .WriteSize = size, .WriteError = std::move(failure)});
        } catch (...) {
            // Nothing is left to report the failure to; the worker must still not unwind.
        }
    }

    void OnWriteDone(const Event& event) {
        m_writing = false;
        m_writingChunks = 0U;
        if (!event.WriteOk) {
            Fail(event.WriteError.value_or(MakeFailure(std::errc::io_error, "Failed to write the downloaded data.")));
            return;
        }
        m_written += event.WriteSize;
        WriteReady();
    }

    [[nodiscard]] bool WriteToSink(std::string data) {
        const std::size_t size = data.size();
        if (!TryWrite(m_sink, std::move(data))) {
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
        // The last write can outlast the budget after the final response arrived in time.
        if (!m_failure.has_value() && m_deadline.Expired()) {
            Fail(MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
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
        // A completion that takes the sink gets it moved in, so it can outlive the operation (file finalization,
        // string result).
        auto completion = [&]() {
            if constexpr (std::invocable<TDone&, SummaryResult, TSink>) {
                return [done = std::move(m_completion), sink = std::move(m_sink)](SummaryResult value) mutable {
                    done(std::move(value), std::move(sink));
                };
            } else {
                return std::move(m_completion);
            }
        }();
        if (m_initiating && !InternalCompletion<TDone>) {
            PostCompletion(m_httpClient, std::move(completion), std::move(result));
        } else {
            completion(std::move(result));
        }
    }

    THttp& m_httpClient;
    std::shared_ptr<const BasicBlobTarget<TCred>> m_clientOptions;
    DownloadToOptions m_options;
    AzureRequestOptions m_requestOptions;
    AggregateDeadline m_deadline;
    TSink m_sink;
    TDone m_completion;
    boost::asio::cancellation_slot m_parentSlot;
    std::size_t m_chunkSize;
    std::size_t m_concurrency;

    std::mutex m_mutex;
    std::vector<Event> m_events;
    std::size_t m_head = 0;
    std::atomic<bool> m_cancelPosted{false};
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

template <HttpClientLike THttp, class TCred, DownloadSinkLike TSink, class TDone>
void StartDownload(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                   DownloadToOptions operationOptions, TSink sink, TDone completion, AzureRequestOptions requestOptions,
                   std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now()) {
    std::make_shared<DownloadOperation<THttp, TCred, TSink, TDone>>(httpClient, std::move(options),
                                                                    std::move(operationOptions), requestOptions,
                                                                    std::move(sink), std::move(completion), start)
        ->Start();
}
// Closes the temporary file and, on success, renames it over `target`; any failure removes the temporary file.
[[nodiscard]] std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>
FinalizeFileDownload(std::ofstream& file, const std::filesystem::path& tempPath, const std::filesystem::path& target,
                     SummaryResult result);

using FileResult = std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>;

// What a file download owns across its writer and I/O-executor hand-offs. It is shared so a hand-off that throws
// while being scheduled cannot take the user's completion or the temporary file's path down with the discarded task,
// and `Completed` keeps the completion exactly-once however many paths race to fail the download.
template <class TCompletion> struct FileDownloadState {
    FileDownloadState(std::optional<DownloadWriteScheduler> scheduler, std::filesystem::path targetPath,
                      TCompletion userCompletion, const AzureRequestOptions& requestOptions,
                      std::chrono::steady_clock::time_point start)
        : Scheduler(std::move(scheduler)), Deadline(requestOptions, start), Target(std::move(targetPath)),
          Completion(std::move(userCompletion)) {}

    std::optional<DownloadWriteScheduler> Scheduler;
    AggregateDeadline Deadline;
    std::filesystem::path TempPath;
    std::filesystem::path Target;
    TCompletion Completion;
    std::optional<FileSink> Sink;
    std::optional<SummaryResult> Result;
    std::optional<FileResult> Final;
    std::atomic<bool> Completed{false};

    // The caller's slot is owned here for the whole operation. The inner download listens to `Signal` instead, and
    // `Canceled` remembers an abort that arrives while no inner download exists yet (queued file preparation) or
    // after it finished (queued finalization). `SignalMutex` serializes the inner download installing its handler
    // with an abort being forwarded.
    std::atomic<bool> Canceled{false};
    std::mutex SignalMutex;
    boost::asio::cancellation_signal Signal;
};

[[nodiscard]] inline BlobStorageError CurrentFailure() {
    try {
        return MakeFailureFromCurrentException();
    } catch (...) {
        return MakeFailure(std::errc::not_enough_memory);
    }
}

// Delivers the user's completion once, on its associated executor. The handler stays owned by `state` until it is
// invoked, so a post that cannot be scheduled falls back to invoking it on this thread rather than losing it.
template <HttpClientLike THttp, class TState>
void CompleteFileDownload(THttp& httpClient, const std::shared_ptr<TState>& state, FileResult result) noexcept {
    if (state->Completed.exchange(true)) {
        return;
    }
    const auto deliver = [state]() { std::move(state->Completion)(std::move(*state->Final)); };
    try {
        state->Final.emplace(std::move(result));
        boost::asio::post(boost::asio::get_associated_executor(state->Completion, httpClient.get_executor()), deliver);
        return;
    } catch (...) {
    }
    if (!state->Final.has_value()) {
        return;
    }
    try {
        deliver();
    } catch (...) {
    }
}

// Closes the temporary file, removes it and reports `error`. Blocks on the disk, so it runs on a writer thread (or
// before any file exists).
template <HttpClientLike THttp, class TState>
void AbandonFileDownload(THttp& httpClient, const std::shared_ptr<TState>& state, BlobStorageError error) noexcept {
    state->Sink.reset();
    if (!state->TempPath.empty()) {
        std::error_code ignored;
        std::filesystem::remove(state->TempPath, ignored);
    }
    CompleteFileDownload(httpClient, state, FileResult{std::unexpect, std::move(error)});
}

// As AbandonFileDownload, for a thread that must not block: it hops to a writer. Only when that hop fails too does
// the cleanup run here, because leaking the partial file and the completion would be worse.
template <HttpClientLike THttp, class TState>
void AbandonFileDownloadFromIo(THttp& httpClient, const std::shared_ptr<TState>& state,
                               const BlobStorageError& error) noexcept {
    try {
        RunOnWriter(httpClient, state->Scheduler,
                    [&httpClient, state, error]() { AbandonFileDownload(httpClient, state, error); });
        return;
    } catch (...) {
    }
    AbandonFileDownload(httpClient, state, error);
}

// The last writer-thread step: closes the file and renames it over the target (see FinalizeFileDownload).
template <HttpClientLike THttp, class TState>
void FinishFileDownloadOnWriter(THttp& httpClient, const std::shared_ptr<TState>& state) noexcept {
    try {
        // The commit point: once the rename starts the destination is published and a cancellation is too late.
        if (state->Canceled.load() && state->Result->has_value()) {
            AbandonFileDownload(httpClient, state,
                                MakeFailure(std::errc::operation_canceled, "The operation was canceled."));
            return;
        }
        if (state->Result->has_value() && state->Deadline.Expired()) {
            AbandonFileDownload(httpClient, state,
                                MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
            return;
        }
        auto result =
            FinalizeFileDownload(state->Sink->File(), state->TempPath, state->Target, std::move(*state->Result));
        state->Sink.reset();
        CompleteFileDownload(httpClient, state, std::move(result));
    } catch (...) {
        AbandonFileDownload(httpClient, state, CurrentFailure());
    }
}

// Receives the network phase's outcome on the I/O thread and schedules finalization on a writer. It is internal
// (see InternalCompletion): it never throws, and a failure to schedule becomes the download's error.
template <HttpClientLike THttp, class TState> struct FileFinalizer {
    static constexpr bool IsInternal = true;

    gsl::not_null<THttp*> Http;
    std::shared_ptr<TState> State;

    void operator()(SummaryResult result, FileSink sink) noexcept {
        try {
            State->Sink.emplace(std::move(sink));
            State->Result.emplace(std::move(result));
            RunOnWriter(*Http, State->Scheduler,
                        [http = Http, state = State]() { FinishFileDownloadOnWriter(*http, state); });
        } catch (...) {
            AbandonFileDownloadFromIo(*Http, State, CurrentFailure());
        }
    }
};
} // namespace DownloadDetail

template <HttpClientLike THttp, class TCred, class TCompletion>
void DownloadBlobAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                       DownloadBlobOptions operationOptions, TCompletion completion,
                       AzureRequestOptions requestOptions) {
    DownloadDetail::StartDownload(
        httpClient, std::move(options), ToDownloadToOptions(std::move(operationOptions)), StringSink{},
        [completion = std::move(completion)](DownloadDetail::SummaryResult result, StringSink sink) mutable {
            if (!result.has_value()) {
                completion(std::unexpected(std::move(result).error()));
                return;
            }
            Models::DownloadBlobResult value;
            value.Content = sink.Take();
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
        httpClient, std::move(options), std::move(operationOptions), SpanSink{destination},
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
    using State = DownloadDetail::FileDownloadState<TCompletion>;
    // The budget covers the hand-offs to the writer and back, so it starts here rather than at the first GET.
    const auto start = std::chrono::steady_clock::now();
    // Holds the completion and temporary path for the whole operation, so no hand-off can lose them if it throws.
    auto state =
        std::make_shared<State>(operationOptions.WriteScheduler, path, std::move(completion), requestOptions, start);
    auto scheduler = operationOptions.WriteScheduler;
    if (auto parentSlot = requestOptions.GetCancellationSlot(); parentSlot.is_connected()) {
        std::weak_ptr<State> const weak = state;
        // The handler may outlive the operation (an explicit slot stays connected), so it holds only the weak state.
        parentSlot.assign([weak](boost::asio::cancellation_type_t type) {
            if (!IsAbortingCancellation(type)) {
                return;
            }
            if (auto self = weak.lock()) {
                self->Canceled.store(true);
                std::lock_guard lock(self->SignalMutex);
                self->Signal.emit(type);
            }
        });
        requestOptions.SetCancellationSlot(state->Signal.slot());
    }
    try {
        DownloadDetail::RunOnWriter(
            httpClient, scheduler,
            [&httpClient, state, start, options, operationOptions = std::move(operationOptions),
             requestOptions]() mutable {
                try {
                    if (state->Canceled.load()) {
                        DownloadDetail::CompleteFileDownload(
                            httpClient, state,
                            DownloadDetail::FileResult{std::unexpect,
                                                       DownloadDetail::MakeFailure(std::errc::operation_canceled,
                                                                                   "The operation was canceled.")});
                        return;
                    }
                    std::error_code statusError;
                    if (std::filesystem::is_directory(state->Target, statusError)) {
                        DownloadDetail::CompleteFileDownload(
                            httpClient, state,
                            DownloadDetail::FileResult{
                                std::unexpect,
                                DownloadDetail::MakeFailure(std::make_error_code(std::errc::is_a_directory),
                                                            "The destination path is a directory.")});
                        return;
                    }

                    // Download into a sibling temporary file and rename it over the target only on success, so a
                    // failed download never leaves a truncated target behind.
                    state->TempPath = state->Target;
                    state->TempPath += ".partial-" + CreateClientRequestId();
                    errno = 0;
                    std::ofstream file(state->TempPath, std::ios::binary | std::ios::trunc);
                    if (!file) {
                        DownloadDetail::CompleteFileDownload(
                            httpClient, state,
                            DownloadDetail::FileResult{
                                std::unexpect, DownloadDetail::MakeFailure(DownloadDetail::LastOpenError(),
                                                                           "Failed to open destination file.")});
                        return;
                    }
                    if (state->Canceled.load()) {
                        file.close();
                        DownloadDetail::AbandonFileDownload(
                            httpClient, state,
                            DownloadDetail::MakeFailure(std::errc::operation_canceled, "The operation was canceled."));
                        return;
                    }

                    // The chunk requests are started from the I/O executor, like any other download. If the post
                    // throws, the discarded task takes only the stream; the state still lets us clean up.
                    boost::asio::post(httpClient.get_executor(), [&httpClient, state, options = std::move(options),
                                                                  operationOptions = std::move(operationOptions),
                                                                  file = std::move(file), requestOptions,
                                                                  start]() mutable {
                        try {
                            std::lock_guard lock(state->SignalMutex);
                            DownloadDetail::StartDownload(
                                httpClient, std::move(options), std::move(operationOptions), FileSink{std::move(file)},
                                DownloadDetail::FileFinalizer<THttp, State>{&httpClient, state}, requestOptions, start);
                            // An abort that arrived before the inner download installed its handler was only recorded.
                            if (state->Canceled.load()) {
                                state->Signal.emit(boost::asio::cancellation_type::terminal);
                            }
                        } catch (...) {
                            DownloadDetail::AbandonFileDownloadFromIo(httpClient, state,
                                                                      DownloadDetail::CurrentFailure());
                        }
                    });
                } catch (...) {
                    DownloadDetail::AbandonFileDownload(httpClient, state, DownloadDetail::CurrentFailure());
                }
            });
    } catch (...) {
        // Nothing has started and no file exists, so only the completion remains to be delivered.
        DownloadDetail::AbandonFileDownload(httpClient, state, DownloadDetail::CurrentFailure());
    }
}
} // namespace AVEVA::AzureClient::Private
