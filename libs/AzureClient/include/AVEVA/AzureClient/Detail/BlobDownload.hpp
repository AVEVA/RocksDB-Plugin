// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "AVEVA/AzureClient/BlobOperationOptions.hpp"
#include "AVEVA/AzureClient/BlobStorageError.hpp"
#include "AVEVA/AzureClient/BlobStorageErrorCode.hpp"
#include "AVEVA/AzureClient/Models/BlobModels.hpp"
#include "AVEVA/AzureClient/Response.hpp"
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/DownloadSinks.hpp>
#include <AVEVA/AzureClient/Detail/ProtocolConstants.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/Detail/AggregateDeadline.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <boost/system/system_error.hpp>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <gsl/util>
#include <ios>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Private {
namespace DownloadDetail {
/// <summary>
/// Runs a blocking task off the I/O thread: on the caller's scheduler, else the HTTP client's, and resumes the caller
/// on its own executor afterwards. A task still queued when the caller is cancelled is skipped and the await throws
/// operation_aborted; a task that has started always runs to completion and the await then returns normally.
/// </summary>
/// <typeparam name="THttp">The HTTP client type, which may provide a download writer.</typeparam>
/// <typeparam name="TTask">The callable type of the task.</typeparam>
/// <param name="httpClient">The HTTP client whose download writer runs the task when no scheduler is given.</param>
/// <param name="scheduler">The caller's scheduler, preferred over the HTTP client's writer when set.</param>
/// <param name="task">The blocking work to run.</param>
/// <exception cref="std::system_error">
/// Thrown with std::errc::operation_not_supported for a transport with neither a scheduler nor a download writer.
/// </exception>
/// <exception cref="boost::system::system_error">Thrown with operation_aborted when canceled before the task
/// started.</exception>
/// <remarks>Whatever the task throws is rethrown.</remarks>
template <class THttp, class TTask>
boost::asio::awaitable<void> RunOnWriter(THttp& httpClient, const std::optional<DownloadWriteScheduler>& scheduler,
                                         TTask task) {
    // The writer thread cannot read this coroutine's cancellation state, so a handler mirrors it into a flag. Ran is
    // only touched by the task's own hop and read after that hop completes, so it needs no atomicity.
    struct HopState {
        std::atomic<Cancellation> Canceled{Cancellation::Running};
        TaskProgress Progress = TaskProgress::Queued;
    };
    const auto hop = std::make_shared<HopState>();
    auto cancellation = co_await boost::asio::this_coro::cancellation_state;
    struct SlotGuard {
        boost::asio::cancellation_slot Slot;
        ~SlotGuard() {
            if (Slot.is_connected()) {
                Slot.clear();
            }
        }
    } guard{cancellation.slot()};
    const auto mirror = [hop](boost::asio::cancellation_type_t) { hop->Canceled = Cancellation::Canceled; };
    if (guard.Slot.is_connected()) {
        guard.Slot.emplace<decltype(mirror)>(mirror);
    }

    const auto body = [&task, hop]() -> boost::asio::awaitable<void> {
        // A cancellation that arrives while the task is queued must keep it from running: it may publish a file.
        if (hop->Canceled == Cancellation::Canceled) {
            throw boost::system::system_error(boost::asio::error::operation_aborted);
        }
        hop->Progress = TaskProgress::Started;
        task();
        co_return;
    };
    const auto run = [&](auto executor) -> boost::asio::awaitable<void> {
        try {
            // The hop gets its own slot so it cannot replace the handler above; a started task is not interruptible.
            boost::asio::cancellation_signal unused;
            co_await boost::asio::co_spawn(
                executor, body, boost::asio::bind_cancellation_slot(unused.slot(), boost::asio::use_awaitable));
        } catch (const boost::system::system_error& e) {
            if (e.code() != boost::asio::error::operation_aborted || hop->Progress == TaskProgress::Queued) {
                throw;
            }
        }
    };
    if (scheduler) {
        co_await run(*scheduler);
    } else if constexpr (requires {
                             boost::asio::co_spawn(httpClient.GetDownloadWriter(), body, boost::asio::use_awaitable);
                         }) {
        co_await run(httpClient.GetDownloadWriter());
    } else {
        // A detached thread could outlive the client, so refuse; callers map the throw to the download's error.
        throw std::system_error(std::make_error_code(std::errc::operation_not_supported),
                                "The HTTP client has no download writer and no write scheduler was supplied.");
    }
}
/// <summary>The default parallel download chunk size, shared with <see cref="BlobTransferLimits"/>.</summary>
inline constexpr std::size_t DefaultDownloadChunkSize = BlobTransferLimits::DefaultDownloadChunkSize;
/// <summary>The headroom added to a request's response body limit, shared with
/// <see cref="BlobTransferLimits"/>.</summary>
inline constexpr std::uint64_t ResponseBodySlack = BlobTransferLimits::ResponseBodySlack;
/// <summary>The cap on buffered download bytes, shared with <see cref="BlobTransferLimits"/>.</summary>
inline constexpr std::uint64_t MaxBufferedDownloadBytes = BlobTransferLimits::MaxBufferedDownloadBytes;

/// <summary>
/// Range-end arithmetic must never wrap: a wrapped end would make the download look complete or place data at the
/// wrong offset.
/// </summary>
/// <param name="lhs">The first addend.</param>
/// <param name="rhs">The second addend.</param>
/// <returns>The sum, or nothing when it does not fit in 64 bits.</returns>
[[nodiscard]] constexpr std::optional<std::uint64_t> CheckedAdd(std::uint64_t lhs, std::uint64_t rhs) noexcept {
    if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs) {
        return std::nullopt;
    }
    return lhs + rhs;
}

/// <summary>Builds a client-side failure from a standard error condition.</summary>
/// <param name="error">The condition to report.</param>
/// <param name="message">Detail for the failure; may be empty.</param>
/// <returns>The failure carrying the matching error code.</returns>
[[nodiscard]] BlobStorageError MakeFailure(std::errc error, std::string message = {});

/// <summary>
/// Preserves the real error category so operational failures (disk full, permission denied)
/// are not reported as caller input errors.
/// </summary>
/// <param name="error">The error code to report unchanged.</param>
/// <param name="message">Detail for the failure.</param>
/// <returns>The failure carrying <paramref name="error"/>.</returns>
[[nodiscard]] BlobStorageError MakeFailure(std::error_code error, std::string message);

/// <summary>errno is the only cause a failed stream open exposes.</summary>
/// <returns>The error code for the current errno, or an I/O error when errno is zero.</returns>
[[nodiscard]] std::error_code LastOpenError() noexcept;

/// <summary>Builds the failure for a response the service sent that cannot be a valid answer to the request.</summary>
/// <param name="message">Detail for the failure.</param>
/// <returns>The failure carrying the InvalidResponse error code.</returns>
[[nodiscard]] BlobStorageError MakeInvalidResponse(std::string message);

/// <summary>
/// A sink may throw anything; an exception must never unwind into the event loop, so it counts as a failed write.
/// </summary>
/// <param name="sink">The sink to write to.</param>
/// <param name="data">The chunk to write.</param>
/// <returns>The sink's outcome, or Failed when it threw.</returns>
[[nodiscard]] inline WriteOutcome TryWrite(DownloadSinkLike auto& sink, std::string data) noexcept {
    try {
        return sink.Write(std::move(data));
    } catch (...) {
        return WriteOutcome::Failed;
    }
}

/// <summary>Classifies the outcome of a request as a failure.</summary>
/// <param name="error">The transport error of the request, if any.</param>
/// <param name="response">The response received, consulted for the service's error status.</param>
/// <returns>The failure to report, or nothing when the request succeeded.</returns>
[[nodiscard]] std::optional<BlobStorageError> ToFailure(std::error_code error, const HttpResponse& response);

/// <summary>Moves the body out of a response, leaving its headers in place.</summary>
/// <param name="response">The response whose body is taken.</param>
/// <returns>The body.</returns>
[[nodiscard]] std::string TakeBody(HttpResponse& response);

/// <summary>What a finished download reports.</summary>
struct DownloadSummary {
    HttpResponse FirstResponse; // headers of the first response; its body has been consumed
    Models::BlobProperties Properties;
    std::uint64_t BytesWritten = 0;
    std::optional<Models::BlobByteRange> ContentRange;
};

/// <summary>The summary of a download, or the failure that ended it.</summary>
using SummaryResult = std::expected<DownloadSummary, BlobStorageError>;

/// <summary>A download's result together with the sink it wrote to, which the caller takes back.</summary>
/// <typeparam name="TSink">The sink type the download wrote to.</typeparam>
template <DownloadSinkLike TSink> struct DownloadOutcome {
    SummaryResult Result;
    TSink Sink;
};

/// <summary>Thrown by a chunk worker once the download's failure is recorded, so the parallel group cancels its
/// siblings.</summary>
struct StopWorkers {};

/// <summary>Builds the failure reported when the caller cancels a download.</summary>
/// <returns>An operation_canceled failure.</returns>
[[nodiscard]] inline BlobStorageError CanceledFailure() {
    return MakeFailure(std::errc::operation_canceled, "The operation was canceled.");
}

/// <summary>
/// One download (to a stream, file, or memory). The first ranged GET (run by the owning coroutine) learns the blob
/// size and ETag; the rest is fetched in ChunkSize ranges by up to Concurrency worker coroutines. Each worker claims
/// the
/// next range, fetches it, waits until every earlier range has been written, then writes its own, so data reaches the
/// sink strictly in order and at most Concurrency chunks are buffered. Later chunks carry If-Match: &lt;first ETag&gt;
/// so a
/// concurrent overwrite fails instead of tearing.
///
/// The workers run on one strand, so their shared state needs no lock. They are awaited as a parallel group: the first
/// failure (or a cancellation of the awaiting coroutine) cancels the others, and the group completes only after all of
/// them have, so no worker outlives the owning coroutine that holds this object.
/// </summary>
/// <typeparam name="THttp">The HTTP client type the requests are sent through.</typeparam>
/// <typeparam name="TCred">The credential type the blob target authorizes with.</typeparam>
/// <typeparam name="TSink">The sink type the data is written to.</typeparam>
template <HttpClientLike THttp, class TCred, DownloadSinkLike TSink> class DownloadEngine final {
  public:
    /// <summary>Prepares a download; the HTTP client and target are borrowed and must outlive the engine.</summary>
    /// <param name="httpClient">The HTTP client to send the requests through.</param>
    /// <param name="target">The blob to download.</param>
    /// <param name="options">The range, conditions, chunking and limits of the download.</param>
    /// <param name="requestOptions">The options every request starts from; their total deadline covers the whole
    /// download.</param>
    /// <param name="sink">The destination the data is written to.</param>
    /// <param name="start">When the download's total deadline budget began.</param>
    DownloadEngine(THttp& httpClient, const BasicBlobTarget<TCred>& target, DownloadToOptions options,
                   AzureRequestOptions requestOptions, TSink sink, std::chrono::steady_clock::time_point start)
        : m_httpClient(httpClient), m_target(target), m_options(std::move(options)),
          m_requestOptions(std::move(requestOptions)), m_deadline(m_requestOptions, start), m_sink(std::move(sink)),
          m_chunkSize(m_options.ChunkSize == 0U ? DefaultDownloadChunkSize : m_options.ChunkSize),
          m_concurrency(std::min(std::max<std::size_t>(1U, m_options.Concurrency),
                                 std::max<std::size_t>(1U, MaxBufferedDownloadBytes / m_chunkSize))) {}

    /// <summary>Runs the download to completion; failures are returned in the outcome rather than thrown.</summary>
    /// <returns>The download's result together with the sink, which is handed back to the caller.</returns>
    boost::asio::awaitable<DownloadOutcome<TSink>> Run() {
        const auto executor = co_await boost::asio::this_coro::executor;

        try {
            co_await RunFirstRequest();
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
        }

        if (!m_failure.has_value() && m_ranging == Ranging::Ranged && m_next < *m_end) {
            try {
                co_await RunWorkers(executor);
            } catch (...) {
                Fail(MakeFailureFromCurrentException());
            }
        }
        co_return DownloadOutcome<TSink>{.Result = Finish(), .Sink = std::move(m_sink)};
    }

  private:
    /// <summary>
    /// The purpose of a GET, which decides whether it carries a Range header. <c>Probe</c> is the first ranged GET,
    /// which learns the blob's size and ETag; <c>Full</c> is the un-ranged GET that follows a probe that found the
    /// blob empty; <c>Chunk</c> is a ranged GET for one of the remaining chunks.
    /// </summary>
    enum class RequestKind : std::uint8_t { Probe, Full, Chunk };

    /// <summary>Records the first failure; later ones are consequences (cancelled siblings) and are dropped.</summary>
    /// <param name="error">The failure to record.</param>
    void Fail(BlobStorageError error) {
        if (!m_failure.has_value()) {
            m_failure = std::move(error);
        }
        if (m_turn.has_value()) {
            m_turn->cancel();
        }
    }

    /// <summary>
    /// Validates the requested range, then runs the probe and, for an empty blob, the un-ranged GET. Failures are
    /// recorded rather than thrown.
    /// </summary>
    boost::asio::awaitable<void> RunFirstRequest() {
        m_begin = m_options.Range.has_value() ? m_options.Range->Offset : 0U;
        if (m_options.Range.has_value() && m_options.Range->Length.has_value()) {
            const std::uint64_t length = *m_options.Range->Length;
            if (length == 0U || length > std::numeric_limits<std::uint64_t>::max() - m_begin) {
                Fail(MakeFailure(std::errc::invalid_argument,
                                 "Download range length must be non-zero and must not overflow."));
                co_return;
            }
            m_end = m_begin + length;
        }

        const std::uint64_t probeLength =
            m_end.has_value() ? std::min<std::uint64_t>(m_chunkSize, *m_end - m_begin) : m_chunkSize;
        auto probe = co_await Fetch(RequestKind::Probe, m_begin, probeLength, m_options.Conditions);
        if (!probe) {
            co_return;
        }
        if (co_await OnProbe(std::move(*probe), probeLength) == FullFetch::Needed) {
            if (auto full = co_await Fetch(RequestKind::Full, m_begin, 0U, m_options.Conditions)) {
                co_await OnFull(std::move(*full));
            }
        }
    }

    /// <summary>
    /// Sends one GET and returns its transport outcome. Empty means the request could not even be built (or the budget
    /// is spent) and the failure is recorded.
    /// </summary>
    /// <param name="kind">The purpose of the request; a full fetch carries no Range header.</param>
    /// <param name="offset">The first byte of the range.</param>
    /// <param name="length">The length of the range; the response body limit is sized from it.</param>
    /// <param name="conditions">The conditions the request is made conditional on.</param>
    /// <returns>The transport outcome, or nothing when no request was sent.</returns>
    boost::asio::awaitable<std::optional<HttpResult>> Fetch(RequestKind kind, std::uint64_t offset,
                                                            std::uint64_t length,
                                                            const Models::BlobRequestConditions& conditions) {
        AzureRequestOptions requestOptions = m_requestOptions;
        // Every request gets only what is left of the whole download's budget, and none starts once it is spent.
        if (m_deadline.Apply(requestOptions) == Admission::Refused) {
            Fail(MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
            co_return std::nullopt;
        }
        HttpRequest request;
        try {
            request = BuildBlobRequest(m_target, HttpMethod::Get);
            if (kind != RequestKind::Full) {
                AddHeader(request, RangeHeaderName,
                          BuildRangeHeaderValue(Models::BlobByteRange{.Offset = offset, .Length = length}));
                const std::uint64_t limit = length > std::numeric_limits<std::uint64_t>::max() - ResponseBodySlack
                                                ? std::numeric_limits<std::uint64_t>::max()
                                                : length + ResponseBodySlack;
                requestOptions.SetResponseBodyLimit(
                    std::max(requestOptions.GetResponseBodyLimit(), AVEVA::ByteCount{limit}));
            }
            ApplyBlobRequestConditions(request, conditions);
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
            co_return std::nullopt;
        }
        co_return co_await SendAuthorizedRequest(m_httpClient, m_target, std::move(request), std::move(requestOptions));
    }

    /// <summary>Separates a transport outcome into the error and the response, so both can be inspected
    /// uniformly.</summary>
    /// <param name="sent">The transport outcome.</param>
    /// <returns>The error (empty on success) and the response (empty on a transport error).</returns>
    static std::pair<std::error_code, HttpResponse> Split(HttpResult sent) {
        if (sent) {
            return {std::error_code{}, std::move(*sent)};
        }
        return {sent.error(), HttpResponse{}};
    }

    /// <summary>Takes the blob's properties and ETag from the first response; failures are recorded.</summary>
    /// <param name="response">The first successful response.</param>
    /// <returns>Invalid when the headers could not be parsed.</returns>
    [[nodiscard]] Validity CaptureFirstResponse(HttpResponse& response) {
        try {
            m_summary.Properties = ParseBlobProperties(response);
        } catch (const std::exception&) {
            Fail(MakeInvalidResponse("Failed to parse the download response headers."));
            return Validity::Invalid;
        }
        m_etag = std::string{FindHeaderValue(response, ETagHeaderName)};
        return Validity::Valid;
    }

    /// <summary>
    /// Checks the probe response, learns the blob's extent, and writes the first chunk. Failures are recorded.
    /// </summary>
    /// <param name="sent">The transport outcome of the probe.</param>
    /// <param name="probeLength">The length that was asked for.</param>
    /// <returns>FullFetch::Needed when the blob turned out to be empty and one un-ranged GET must follow.</returns>
    boost::asio::awaitable<FullFetch> OnProbe(HttpResult sent, std::uint64_t probeLength) {
        auto [error, response] = Split(std::move(sent));
        // A 0-byte blob rejects any range with 416; fall back to one un-ranged GET. That is only
        // equivalent when the request starts at byte 0 and has no length limit.
        const auto unboundedFromStart =
            !m_options.Range.has_value() || (m_begin == 0U && !m_options.Range->Length.has_value());
        if (!error && response.GetStatus() == HttpStatus::RangeNotSatisfiable && unboundedFromStart) {
            co_return FullFetch::Needed;
        }
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            co_return FullFetch::NotNeeded;
        }
        if (CaptureFirstResponse(response) == Validity::Invalid) {
            co_return FullFetch::NotNeeded;
        }

        if (response.GetStatus() != HttpStatus::PartialContent) {
            // A 200 means the Range header was ignored and the body is the whole blob from byte 0.
            if (m_begin != 0U) {
                Fail(MakeInvalidResponse("The service ignored the requested range."));
                co_return FullFetch::NotNeeded;
            }
            std::string body = TakeBody(response);
            if (m_end.has_value() && body.size() > *m_end) {
                body.resize(gsl::narrow_cast<std::size_t>(*m_end));
            }
            m_end = body.size();
            m_summary.FirstResponse = std::move(response);
            if (ExceedsMaxSize(*m_end) == SizeLimit::Exceeded) {
                co_return FullFetch::NotNeeded;
            }
            static_cast<void>(co_await WriteBody(std::move(body)));
            co_return FullFetch::NotNeeded;
        }

        // A 206 without a valid Content-Range cannot be placed or validated; refuse it rather than guess.
        const std::string_view rangeHeader = FindHeaderValue(response, "Content-Range");
        const auto parsedRange = rangeHeader.empty() ? std::nullopt : ParseContentRange(rangeHeader);
        if (!parsedRange.has_value() || parsedRange->Satisfaction == RangeSatisfaction::Unsatisfied ||
            parsedRange->Start != m_begin || parsedRange->End < parsedRange->Start) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            co_return FullFetch::NotNeeded;
        }
        const std::optional<std::uint64_t> total = parsedRange->Total;
        m_total = total;

        std::string body = TakeBody(response);
        if (body.empty() || parsedRange->End - parsedRange->Start != body.size() - 1U) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            co_return FullFetch::NotNeeded;
        }
        if (total.has_value()) {
            m_end = m_end.has_value() ? std::min(*m_end, *total) : *total;
        } else if (body.size() < probeLength) {
            m_end = CheckedAdd(m_begin, body.size());
            if (!m_end.has_value()) {
                Fail(MakeInvalidResponse("The download response range overflows."));
                co_return FullFetch::NotNeeded;
            }
        } else if (!m_end.has_value()) {
            Fail(MakeInvalidResponse("The download response does not report the blob size."));
            co_return FullFetch::NotNeeded;
        }

        if (*m_end < m_begin || body.size() != std::min<std::uint64_t>(probeLength, *m_end - m_begin)) {
            Fail(MakeInvalidResponse("The download response has an unexpected length."));
            co_return FullFetch::NotNeeded;
        }

        m_ranging = Ranging::Ranged;
        m_summary.FirstResponse = std::move(response);
        if (ExceedsMaxSize(*m_end - m_begin) == SizeLimit::Exceeded) {
            co_return FullFetch::NotNeeded;
        }
        try {
            m_sink.Reserve(*m_end - m_begin);
        } catch (const std::exception&) {
            Fail(MakeFailure(std::errc::not_enough_memory));
            co_return FullFetch::NotNeeded;
        } catch (...) {
            Fail(MakeFailure(std::errc::io_error, "The download sink failed to reserve space."));
            co_return FullFetch::NotNeeded;
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
            co_return FullFetch::NotNeeded;
        }
        // Without a validator the remaining chunks would be unconditional and could mix blob versions.
        if (*next < *m_end && m_chunkConditions.IfMatch.empty()) {
            Fail(MakeInvalidResponse("The download response has no ETag to keep later chunks on one blob version."));
            co_return FullFetch::NotNeeded;
        }
        m_next = *next;
        static_cast<void>(co_await WriteBody(std::move(body)));
        co_return FullFetch::NotNeeded;
    }

    /// <summary>Checks the un-ranged response for an empty blob and writes its body. Failures are recorded.</summary>
    /// <param name="sent">The transport outcome of the un-ranged GET.</param>
    boost::asio::awaitable<void> OnFull(HttpResult sent) {
        auto [error, response] = Split(std::move(sent));
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            co_return;
        }
        if (CaptureFirstResponse(response) == Validity::Invalid) {
            co_return;
        }
        std::string body = TakeBody(response);
        m_end = CheckedAdd(m_begin, body.size());
        if (!m_end.has_value()) {
            Fail(MakeInvalidResponse("The download response range overflows."));
            co_return;
        }
        m_summary.FirstResponse = std::move(response);
        if (ExceedsMaxSize(body.size()) == SizeLimit::Exceeded) {
            co_return;
        }
        static_cast<void>(co_await WriteBody(std::move(body)));
    }

    /// <summary>Fails the download when <paramref name="size"/> is over the caller's limit.</summary>
    /// <param name="size">The number of bytes the download would write.</param>
    /// <returns>Exceeded when the limit was passed, in which case the failure is recorded.</returns>
    [[nodiscard]] SizeLimit ExceedsMaxSize(std::uint64_t size) {
        if (!m_options.MaxSize.has_value() || size <= *m_options.MaxSize) {
            return SizeLimit::Within;
        }
        Fail(MakeFailure(std::errc::file_too_large, "The download is larger than the configured maximum size."));
        return SizeLimit::Exceeded;
    }

    /// <summary>Checks a chunk response against the range that was asked for; Invalid means the failure is
    /// recorded.</summary>
    /// <param name="sent">The transport outcome of the chunk request.</param>
    /// <param name="offset">The first byte that was asked for.</param>
    /// <param name="length">The number of bytes that were asked for.</param>
    /// <param name="body">Receives the response body.</param>
    /// <returns>Valid when the response is exactly the requested range.</returns>
    [[nodiscard]] Validity ValidateChunk(HttpResult sent, std::uint64_t offset, std::uint64_t length,
                                         std::string& body) {
        auto [error, response] = Split(std::move(sent));
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            return Validity::Invalid;
        }
        body = TakeBody(response);
        if (response.GetStatus() != HttpStatus::PartialContent || body.size() != length) {
            Fail(MakeInvalidResponse("The download response has an unexpected length."));
            return Validity::Invalid;
        }
        // A server that answers a different range would otherwise be written at the wrong offset.
        const std::string_view header = FindHeaderValue(response, "Content-Range");
        if (header.empty()) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            return Validity::Invalid;
        }
        const auto parsed = ParseContentRange(header);
        if (!parsed.has_value() || parsed->Satisfaction == RangeSatisfaction::Unsatisfied || parsed->Start != offset ||
            parsed->End < parsed->Start || body.empty() || parsed->End - parsed->Start != body.size() - 1U ||
            (m_total.has_value() && parsed->Total.has_value() && *parsed->Total != *m_total)) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            return Validity::Invalid;
        }
        return Validity::Valid;
    }

    /// <summary>
    /// Sinks that block on the disk are written from the writer scheduler (see RunOnWriter) so a slow write never
    /// stalls the I/O thread that is also servicing every other connection. Only the worker whose turn it is writes, so
    /// the sink is never used concurrently and receives data in offset order.
    /// </summary>
    /// <param name="data">The next chunk, in offset order.</param>
    /// <returns>Failed when the write failed, in which case the failure is recorded.</returns>
    boost::asio::awaitable<WriteOutcome> WriteBody(std::string data) {
        const std::size_t size = data.size();
        WriteOutcome outcome = WriteOutcome::Failed;
        if constexpr (TSink::Mode == WriteMode::Inline) {
            outcome = TryWrite(m_sink, std::move(data));
        } else {
            try {
                co_await RunOnWriter(m_httpClient, m_options.WriteScheduler,
                                     [&] { outcome = TryWrite(m_sink, std::move(data)); });
            } catch (...) {
                Fail(MakeFailureFromCurrentException());
                co_return WriteOutcome::Failed;
            }
        }
        if (outcome == WriteOutcome::Failed) {
            Fail(MakeFailure(std::errc::io_error, "Failed to write the downloaded data."));
            co_return WriteOutcome::Failed;
        }
        m_written += size;
        co_return WriteOutcome::Written;
    }

    /// <summary>
    /// Claims, fetches and writes chunks until none is left or the download has failed.
    /// </summary>
    /// <exception cref="StopWorkers">Thrown once a failure has been recorded, to cancel the sibling
    /// workers.</exception>
    boost::asio::awaitable<void> Worker() {
        try {
            for (;;) {
                if (m_failure.has_value()) {
                    throw StopWorkers{};
                }
                if (m_next >= *m_end) {
                    co_return;
                }
                const std::uint64_t length = std::min<std::uint64_t>(m_chunkSize, *m_end - m_next);
                const std::uint64_t offset = m_next;
                m_next += length;

                auto sent = co_await Fetch(RequestKind::Chunk, offset, length, m_chunkConditions);
                if (!sent) {
                    throw StopWorkers{};
                }
                std::string body;
                if (ValidateChunk(std::move(*sent), offset, length, body) == Validity::Invalid) {
                    throw StopWorkers{};
                }

                // Hold the chunk until every earlier one is written. A notification wakes the wait; so does a
                // cancellation, which the next await then rethrows as operation_aborted.
                while (!m_failure.has_value() && m_begin + m_written != offset) {
                    co_await m_turn->async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
                }
                if (m_failure.has_value() || co_await WriteBody(std::move(body)) == WriteOutcome::Failed) {
                    throw StopWorkers{};
                }
                m_turn->cancel();
            }
        } catch (const StopWorkers&) {
            throw;
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
            throw StopWorkers{};
        }
    }

    /// <summary>Runs the chunk workers as a parallel group and waits for all of them.</summary>
    /// <param name="executor">The executor the workers' strand is built on.</param>
    boost::asio::awaitable<void> RunWorkers(const boost::asio::any_io_executor& executor) {
        const auto strand = boost::asio::make_strand(executor);
        m_turn.emplace(strand);
        // Only ever woken by cancel(), never by expiry.
        m_turn->expires_at(std::chrono::steady_clock::time_point::max());

        const std::uint64_t remaining = *m_end - m_next;
        const std::uint64_t chunks = remaining / m_chunkSize + (remaining % m_chunkSize != 0U ? 1U : 0U);
        const std::size_t workers = gsl::narrow_cast<std::size_t>(std::min<std::uint64_t>(m_concurrency, chunks));

        using Op = decltype(boost::asio::co_spawn(strand, Worker(), boost::asio::deferred));
        std::vector<Op> operations;
        operations.reserve(workers);
        for (std::size_t index = 0; index < workers; ++index) {
            operations.push_back(boost::asio::co_spawn(strand, Worker(), boost::asio::deferred));
        }
        // Completes only after every worker has, so none can touch this object once the owner returns.
        co_await boost::asio::experimental::make_parallel_group(std::move(operations))
            .async_wait(boost::asio::experimental::wait_for_one_error(), boost::asio::use_awaitable);
        m_turn.reset();
    }

    /// <summary>Checks that everything arrived in time and builds the outcome.</summary>
    /// <returns>The recorded failure if there is one, otherwise the summary of what was written.</returns>
    [[nodiscard]] SummaryResult Finish() {
        if (!m_failure.has_value() && (!m_end.has_value() || *m_end < m_begin || m_written != *m_end - m_begin)) {
            Fail(MakeInvalidResponse("The download ended before all data was received."));
        }
        // The last write can outlast the budget after the final response arrived in time.
        if (!m_failure.has_value() && m_deadline.State() == DeadlineState::Expired) {
            Fail(MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
        }
        if (m_failure.has_value()) {
            return std::unexpected(std::move(*m_failure));
        }
        m_summary.BytesWritten = m_written;
        m_summary.Properties.ContentLength = m_written;
        if (m_ranging == Ranging::Ranged) {
            m_summary.ContentRange = Models::BlobByteRange{.Offset = m_begin, .Length = m_written};
        }
        return std::move(m_summary);
    }

    THttp& m_httpClient;
    const BasicBlobTarget<TCred>& m_target;
    DownloadToOptions m_options;
    AzureRequestOptions m_requestOptions;
    AggregateDeadline m_deadline;
    TSink m_sink;
    std::size_t m_chunkSize;
    std::size_t m_concurrency;

    // Workers share everything below; they run on one strand.
    std::optional<boost::asio::steady_timer> m_turn;
    Models::BlobRequestConditions m_chunkConditions;
    std::string m_etag;
    std::uint64_t m_begin = 0;
    std::optional<std::uint64_t> m_end;
    std::optional<std::uint64_t> m_total;
    std::uint64_t m_next = 0;
    std::uint64_t m_written = 0;
    Ranging m_ranging = Ranging::Unranged;
    std::optional<BlobStorageError> m_failure;
    DownloadSummary m_summary;
};

/// <summary>Converts a finished download's summary into the response returned to the caller.</summary>
/// <param name="summary">The summary of the download.</param>
/// <returns>The response carrying the result and the first HTTP response.</returns>
[[nodiscard]] Response<Models::DownloadBlobToResult> ToDownloadToResponse(DownloadSummary summary);

/// <summary>
/// The engine's state lives in this coroutine's frame, which is why the arguments are taken by value.
/// </summary>
/// <typeparam name="THttp">The HTTP client type the requests are sent through.</typeparam>
/// <typeparam name="TCred">The credential type the blob target authorizes with.</typeparam>
/// <typeparam name="TSink">The sink type the data is written to.</typeparam>
/// <param name="httpClient">The HTTP client to send the requests through.</param>
/// <param name="target">The blob to download.</param>
/// <param name="options">The range, conditions, chunking and limits of the download.</param>
/// <param name="sink">The destination the data is written to.</param>
/// <param name="requestOptions">The options every request starts from; their total deadline covers the whole
/// download.</param>
/// <param name="start">When the total deadline budget began; defaults to now.</param>
/// <returns>The download's result together with the sink.</returns>
template <HttpClientLike THttp, class TCred, DownloadSinkLike TSink>
boost::asio::awaitable<DownloadOutcome<TSink>>
RunDownload(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> target, DownloadToOptions options,
            TSink sink, AzureRequestOptions requestOptions,
            std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now()) {
    DownloadEngine<THttp, TCred, TSink> engine(httpClient, *target, std::move(options), std::move(requestOptions),
                                               std::move(sink), start);
    co_return co_await engine.Run();
}

/// <summary>Closes the temporary file and, on success, renames it over <paramref name="target"/>; any failure removes
/// the temporary file.</summary>
/// <param name="file">The stream of the temporary file.</param>
/// <param name="tempPath">The temporary file the data was written to.</param>
/// <param name="target">The destination to publish the file to.</param>
/// <param name="result">The result of the download.</param>
/// <returns>The response on success, otherwise the download's failure or the one that closing or renaming
/// caused.</returns>
[[nodiscard]] std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>
FinalizeFileDownload(std::ofstream& file, const std::filesystem::path& tempPath, const std::filesystem::path& target,
                     SummaryResult result);

/// <summary>The response of a download to a file, or the failure that ended it.</summary>
using FileResult = std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>;

/// <summary>Converts the exception being handled into a failure.</summary>
/// <returns>The failure for the current exception, or an out-of-memory failure when even that could not be
/// built.</returns>
[[nodiscard]] inline BlobStorageError CurrentFailure() {
    try {
        return MakeFailureFromCurrentException();
    } catch (...) {
        return MakeFailure(std::errc::not_enough_memory);
    }
}

/// <summary>Removes the temporary file (if any) and reports <paramref name="error"/>. Blocks on the disk, so it runs on
/// a writer thread.</summary>
/// <param name="tempPath">The temporary file to remove; may be empty.</param>
/// <param name="error">The failure to report.</param>
/// <returns>A result holding <paramref name="error"/>.</returns>
inline FileResult AbandonFileDownload(const std::filesystem::path& tempPath, BlobStorageError error) noexcept {
    if (!tempPath.empty()) {
        std::error_code ignored;
        std::filesystem::remove(tempPath, ignored);
    }
    return FileResult{std::unexpect, std::move(error)};
}
} // namespace DownloadDetail

/// <summary>Downloads the blob into memory. See <see cref="DownloadBlobToSpanAsync"/> for the transfer
/// behavior.</summary>
/// <typeparam name="THttp">The HTTP client type the requests are sent through.</typeparam>
/// <typeparam name="TCred">The credential type the blob target authorizes with.</typeparam>
/// <param name="httpClient">The HTTP client to send the requests through.</param>
/// <param name="options">The blob to download.</param>
/// <param name="operationOptions">The range, conditions, chunking and limits of the download.</param>
/// <param name="requestOptions">The options every request starts from.</param>
/// <returns>The content and properties, or the failure; failures are returned rather than thrown.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DownloadBlobResult>, BlobStorageError>>
DownloadBlobAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                  DownloadBlobOptions operationOptions, AzureRequestOptions requestOptions) {
    auto outcome = co_await DownloadDetail::RunDownload(httpClient, std::move(options),
                                                        ToDownloadToOptions(std::move(operationOptions)), StringSink{},
                                                        std::move(requestOptions));
    if (!outcome.Result.has_value()) {
        co_return std::unexpected(std::move(outcome.Result).error());
    }
    Models::DownloadBlobResult value;
    value.Content = outcome.Sink.Take();
    value.Properties = std::move(outcome.Result->Properties);
    value.ContentRange = outcome.Result->ContentRange;
    co_return Response<Models::DownloadBlobResult>{std::move(value), std::move(outcome.Result->FirstResponse),
                                                   std::nullopt};
}

/// <summary>
/// Downloads the blob into a caller-owned buffer, in parallel ranged chunks written in offset order.
/// </summary>
/// <typeparam name="THttp">The HTTP client type the requests are sent through.</typeparam>
/// <typeparam name="TCred">The credential type the blob target authorizes with.</typeparam>
/// <param name="httpClient">The HTTP client to send the requests through.</param>
/// <param name="options">The blob to download.</param>
/// <param name="operationOptions">The range, conditions, chunking and limits of the download.</param>
/// <param name="destination">The buffer to copy into; a download larger than it fails.</param>
/// <param name="requestOptions">The options every request starts from.</param>
/// <returns>The bytes written and properties, or the failure; failures are returned rather than thrown.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>>
DownloadBlobToSpanAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                        DownloadToOptions operationOptions, std::span<char> destination,
                        AzureRequestOptions requestOptions) {
    auto outcome = co_await DownloadDetail::RunDownload(httpClient, std::move(options), std::move(operationOptions),
                                                        SpanSink{destination}, std::move(requestOptions));
    if (!outcome.Result.has_value()) {
        co_return std::unexpected(std::move(outcome.Result).error());
    }
    co_return DownloadDetail::ToDownloadToResponse(std::move(*outcome.Result));
}

/// <summary>
/// Every blocking filesystem call (directory check, open, close, rename, remove) runs on the write scheduler (see
/// RunOnWriter), never on the thread that runs the coroutine. The download goes to a sibling temporary file that is
/// renamed over <paramref name="path"/> only on success, so a failed download never leaves a truncated target behind. A
/// cancellation that arrives before the rename removes the temporary file; the rename is the commit point.
/// </summary>
/// <typeparam name="THttp">The HTTP client type the requests are sent through.</typeparam>
/// <typeparam name="TCred">The credential type the blob target authorizes with.</typeparam>
/// <param name="httpClient">The HTTP client to send the requests through.</param>
/// <param name="options">The blob to download.</param>
/// <param name="operationOptions">The range, conditions, chunking, limits and write scheduler of the download.</param>
/// <param name="path">The file to publish the download to.</param>
/// <param name="requestOptions">The options every request starts from; their total deadline also covers the file
/// work.</param>
/// <returns>The bytes written and properties, or the failure; failures are returned rather than thrown.</returns>
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>>
DownloadBlobToFileAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                        DownloadToOptions operationOptions, std::filesystem::path path,
                        AzureRequestOptions requestOptions) {
    using namespace DownloadDetail;
    // The budget covers the hand-offs to the writer and back, so it starts here rather than at the first GET.
    const auto start = std::chrono::steady_clock::now();
    const AggregateDeadline deadline(requestOptions, start);
    const auto scheduler = operationOptions.WriteScheduler;

    std::filesystem::path tempPath;
    std::ofstream file;
    std::optional<BlobStorageError> prepareFailure;
    try {
        co_await RunOnWriter(httpClient, scheduler, [&] {
            std::error_code statusError;
            if (std::filesystem::is_directory(path, statusError)) {
                prepareFailure = MakeFailure(std::make_error_code(std::errc::is_a_directory),
                                             "The destination path is a directory.");
                return;
            }
            tempPath = path;
            tempPath += ".partial-" + CreateClientRequestId();
            errno = 0;
            file.open(tempPath, std::ios::binary | std::ios::trunc);
            if (!file) {
                prepareFailure = MakeFailure(LastOpenError(), "Failed to open destination file.");
                tempPath.clear();
            }
        });
    } catch (...) {
        prepareFailure = CurrentFailure();
    }
    if (prepareFailure.has_value()) {
        co_return std::unexpected(std::move(*prepareFailure));
    }

    std::optional<DownloadOutcome<FileSink>> outcome;
    try {
        outcome.emplace(co_await RunDownload(httpClient, std::move(options), std::move(operationOptions),
                                             FileSink{std::move(file)}, std::move(requestOptions), start));
    } catch (const boost::system::system_error& e) {
        // Cancelled before the download started; the temporary file exists but nothing was written.
        if (e.code() != boost::asio::error::operation_aborted) {
            throw;
        }
        co_return AbandonFileDownload(tempPath, CanceledFailure());
    }

    FileResult finished = std::unexpected(BlobStorageError{});
    const auto finalize = [&] {
        try {
            // The commit point: once the rename starts the destination is published and a cancellation is too late.
            if (outcome->Result.has_value() && deadline.State() == DeadlineState::Expired) {
                outcome->Sink.File().close();
                finished = AbandonFileDownload(
                    tempPath, MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
            } else {
                finished = FinalizeFileDownload(outcome->Sink.File(), tempPath, path, std::move(outcome->Result));
            }
        } catch (...) {
            finished = AbandonFileDownload(tempPath, CurrentFailure());
        }
    };
    try {
        co_await RunOnWriter(httpClient, scheduler, finalize);
    } catch (const boost::system::system_error& e) {
        if (e.code() == boost::asio::error::operation_aborted && outcome->Result.has_value()) {
            // The hop never started, so the rename did not happen and the cancellation still wins.
            outcome->Sink.File().close();
            finished = AbandonFileDownload(tempPath, CanceledFailure());
        } else {
            finalize();
        }
    } catch (...) {
        // The hop failed too, but leaking the partial file would be worse than blocking this thread once.
        finalize();
    }
    co_return finished;
}
} // namespace AVEVA::AzureClient::Private
