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
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
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
// Runs a blocking task off the I/O thread: on the caller's scheduler, else the HTTP client's, and resumes the caller
// on its own executor afterwards. Throws std::errc::operation_not_supported for a transport with neither, and
// rethrows whatever the task throws.
template <class THttp, class TTask>
boost::asio::awaitable<void> RunOnWriter(THttp& httpClient, const std::optional<DownloadWriteScheduler>& scheduler,
                                         TTask task) {
    const auto body = [&task]() -> boost::asio::awaitable<void> {
        task();
        co_return;
    };
    if (scheduler) {
        co_await boost::asio::co_spawn(*scheduler, body, boost::asio::use_awaitable);
    } else if constexpr (requires {
                             boost::asio::co_spawn(httpClient.GetDownloadWriter(), body, boost::asio::use_awaitable);
                         }) {
        co_await boost::asio::co_spawn(httpClient.GetDownloadWriter(), body, boost::asio::use_awaitable);
    } else {
        // A detached thread could outlive the client, so refuse; callers map the throw to the download's error.
        throw std::system_error(std::make_error_code(std::errc::operation_not_supported),
                                "The HTTP client has no download writer and no write scheduler was supplied.");
    }
}

inline constexpr std::size_t DefaultDownloadChunkSize = BlobTransferLimits::DefaultDownloadChunkSize;
inline constexpr std::uint64_t ResponseBodySlack = BlobTransferLimits::ResponseBodySlack;
inline constexpr std::uint64_t MaxBufferedDownloadBytes = BlobTransferLimits::MaxBufferedDownloadBytes;

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

// A sink may throw anything; an exception must never unwind into the event loop, so it counts as a failed write.
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

template <DownloadSinkLike TSink> struct DownloadOutcome {
    SummaryResult Result;
    TSink Sink;
};

// Thrown by a chunk worker once the download's failure is recorded, so the parallel group cancels its siblings.
struct StopWorkers {};

[[nodiscard]] inline BlobStorageError CanceledFailure() {
    return MakeFailure(std::errc::operation_canceled, "The operation was canceled.");
}

// One download (to a stream, file, or memory). The first ranged GET (run by the owning coroutine) learns the blob
// size and ETag; the rest is fetched in ChunkSize ranges by up to Concurrency worker coroutines. Each worker claims the
// next range, fetches it, waits until every earlier range has been written, then writes its own, so data reaches the
// sink strictly in order and at most Concurrency chunks are buffered. Later chunks carry If-Match: <first ETag> so a
// concurrent overwrite fails instead of tearing.
//
// The workers run on one strand, so their shared state needs no lock. They are awaited as a parallel group: the first
// failure (or a cancellation of the awaiting coroutine) cancels the others, and the group completes only after all of
// them have, so no worker outlives the owning coroutine that holds this object.
template <HttpClientLike THttp, class TCred, DownloadSinkLike TSink> class DownloadEngine final {
  public:
    DownloadEngine(THttp& httpClient, const BasicBlobTarget<TCred>& target, DownloadToOptions options,
                   AzureRequestOptions requestOptions, TSink sink, std::chrono::steady_clock::time_point start)
        : m_httpClient(httpClient), m_target(target), m_options(std::move(options)),
          m_requestOptions(std::move(requestOptions)), m_deadline(m_requestOptions, start), m_sink(std::move(sink)),
          m_chunkSize(m_options.ChunkSize == 0U ? DefaultDownloadChunkSize : m_options.ChunkSize),
          m_concurrency(std::min(std::max<std::size_t>(1U, m_options.Concurrency),
                                 std::max<std::size_t>(1U, MaxBufferedDownloadBytes / m_chunkSize))) {}

    boost::asio::awaitable<DownloadOutcome<TSink>> Run() {
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        const auto cancellation = co_await boost::asio::this_coro::cancellation_state;
        const auto executor = co_await boost::asio::this_coro::executor;

        try {
            co_await RunFirstRequest();
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
        }

        if (!m_failure.has_value() && m_ranged && m_next < *m_end) {
            try {
                co_await RunWorkers(executor);
            } catch (...) {
                Fail(MakeFailureFromCurrentException());
            }
        }
        if (!m_failure.has_value() && IsAbortingCancellation(cancellation.cancelled())) {
            Fail(CanceledFailure());
        }
        co_return DownloadOutcome<TSink>{.Result = Finish(), .Sink = std::move(m_sink)};
    }

  private:
    enum class RequestKind : std::uint8_t { Probe, Full, Chunk };

    // Records the first failure; later ones are consequences (cancelled siblings) and are dropped.
    void Fail(BlobStorageError error) {
        if (!m_failure.has_value()) {
            m_failure = std::move(error);
        }
        if (m_turn.has_value()) {
            m_turn->cancel();
        }
    }

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
        HttpResult sent;
        if (!co_await Fetch(RequestKind::Probe, m_begin, probeLength, m_options.Conditions, sent)) {
            co_return;
        }
        if (co_await OnProbe(std::move(sent), probeLength)) {
            if (!co_await Fetch(RequestKind::Full, m_begin, 0U, m_options.Conditions, sent)) {
                co_return;
            }
            co_await OnFull(std::move(sent));
        }
    }

    // Sends one GET. False means the request could not even be built (or the budget is spent) and the failure is
    // recorded; otherwise `out` holds the transport outcome.
    boost::asio::awaitable<bool> Fetch(RequestKind kind, std::uint64_t offset, std::uint64_t length,
                                       const Models::BlobRequestConditions& conditions, HttpResult& out) {
        AzureRequestOptions requestOptions = m_requestOptions;
        // Every request gets only what is left of the whole download's budget, and none starts once it is spent.
        if (!m_deadline.Apply(requestOptions)) {
            Fail(MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
            co_return false;
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
                requestOptions.SetResponseBodyLimit(std::max(requestOptions.GetResponseBodyLimit(), limit));
            }
            ApplyBlobRequestConditions(request, conditions);
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
            co_return false;
        }
        out = co_await SendAuthorizedRequest(m_httpClient, m_target, std::move(request), std::move(requestOptions));
        co_return true;
    }

    static std::pair<std::error_code, HttpResponse> Split(HttpResult sent) {
        if (sent) {
            return {std::error_code{}, std::move(*sent)};
        }
        return {sent.error(), HttpResponse{}};
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

    // True when the blob turned out to be empty and one un-ranged GET must follow.
    boost::asio::awaitable<bool> OnProbe(HttpResult sent, std::uint64_t probeLength) {
        auto [error, response] = Split(std::move(sent));
        // A 0-byte blob rejects any range with 416; fall back to one un-ranged GET. That is only
        // equivalent when the request starts at byte 0 and has no length limit.
        const bool unboundedFromStart =
            !m_options.Range.has_value() || (m_begin == 0U && !m_options.Range->Length.has_value());
        if (!error && response.GetStatus() == HttpStatusRangeNotSatisfiable && unboundedFromStart) {
            co_return true;
        }
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            co_return false;
        }
        if (!CaptureFirstResponse(response)) {
            co_return false;
        }

        if (response.GetStatus() != HttpStatusPartialContent) {
            // A 200 means the Range header was ignored and the body is the whole blob from byte 0.
            if (m_begin != 0U) {
                Fail(MakeInvalidResponse("The service ignored the requested range."));
                co_return false;
            }
            std::string body = TakeBody(response);
            if (m_end.has_value() && body.size() > *m_end) {
                body.resize(gsl::narrow_cast<std::size_t>(*m_end));
            }
            m_end = body.size();
            m_summary.FirstResponse = std::move(response);
            if (ExceedsMaxSize(*m_end)) {
                co_return false;
            }
            co_await WriteBody(std::move(body));
            co_return false;
        }

        // A 206 without a valid Content-Range cannot be placed or validated; refuse it rather than guess.
        const std::string_view rangeHeader = FindHeaderValue(response, "Content-Range");
        const auto parsedRange = rangeHeader.empty() ? std::nullopt : ParseContentRange(rangeHeader);
        if (!parsedRange.has_value() || parsedRange->Unsatisfied || parsedRange->Start != m_begin ||
            parsedRange->End < parsedRange->Start) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            co_return false;
        }
        const std::optional<std::uint64_t> total = parsedRange->Total;
        m_total = total;

        std::string body = TakeBody(response);
        if (body.empty() || parsedRange->End - parsedRange->Start != body.size() - 1U) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            co_return false;
        }
        if (total.has_value()) {
            m_end = m_end.has_value() ? std::min(*m_end, *total) : *total;
        } else if (body.size() < probeLength) {
            m_end = CheckedAdd(m_begin, body.size());
            if (!m_end.has_value()) {
                Fail(MakeInvalidResponse("The download response range overflows."));
                co_return false;
            }
        } else if (!m_end.has_value()) {
            Fail(MakeInvalidResponse("The download response does not report the blob size."));
            co_return false;
        }

        if (*m_end < m_begin || body.size() != std::min<std::uint64_t>(probeLength, *m_end - m_begin)) {
            Fail(MakeInvalidResponse("The download response has an unexpected length."));
            co_return false;
        }

        m_ranged = true;
        m_summary.FirstResponse = std::move(response);
        if (ExceedsMaxSize(*m_end - m_begin)) {
            co_return false;
        }
        try {
            m_sink.Reserve(*m_end - m_begin);
        } catch (const std::exception&) {
            Fail(MakeFailure(std::errc::not_enough_memory));
            co_return false;
        } catch (...) {
            Fail(MakeFailure(std::errc::io_error, "The download sink failed to reserve space."));
            co_return false;
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
            co_return false;
        }
        // Without a validator the remaining chunks would be unconditional and could mix blob versions.
        if (*next < *m_end && m_chunkConditions.IfMatch.empty()) {
            Fail(MakeInvalidResponse("The download response has no ETag to keep later chunks on one blob version."));
            co_return false;
        }
        m_next = *next;
        co_await WriteBody(std::move(body));
        co_return false;
    }

    boost::asio::awaitable<void> OnFull(HttpResult sent) {
        auto [error, response] = Split(std::move(sent));
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            co_return;
        }
        if (!CaptureFirstResponse(response)) {
            co_return;
        }
        std::string body = TakeBody(response);
        m_end = CheckedAdd(m_begin, body.size());
        if (!m_end.has_value()) {
            Fail(MakeInvalidResponse("The download response range overflows."));
            co_return;
        }
        m_summary.FirstResponse = std::move(response);
        if (ExceedsMaxSize(body.size())) {
            co_return;
        }
        co_await WriteBody(std::move(body));
    }

    // Fails the download when `size` is over the caller's limit; true means it failed.
    [[nodiscard]] bool ExceedsMaxSize(std::uint64_t size) {
        if (!m_options.MaxSize.has_value() || size <= *m_options.MaxSize) {
            return false;
        }
        Fail(MakeFailure(std::errc::file_too_large, "The download is larger than the configured maximum size."));
        return true;
    }

    // Checks a chunk response against the range that was asked for; false means the failure is recorded.
    [[nodiscard]] bool ValidateChunk(HttpResult sent, std::uint64_t offset, std::uint64_t length, std::string& body) {
        auto [error, response] = Split(std::move(sent));
        if (auto failure = ToFailure(error, response); failure.has_value()) {
            Fail(std::move(*failure));
            return false;
        }
        body = TakeBody(response);
        if (response.GetStatus() != HttpStatusPartialContent || body.size() != length) {
            Fail(MakeInvalidResponse("The download response has an unexpected length."));
            return false;
        }
        // A server that answers a different range would otherwise be written at the wrong offset.
        const std::string_view header = FindHeaderValue(response, "Content-Range");
        if (header.empty()) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            return false;
        }
        const auto parsed = ParseContentRange(header);
        if (!parsed.has_value() || parsed->Unsatisfied || parsed->Start != offset || parsed->End < parsed->Start ||
            body.empty() || parsed->End - parsed->Start != body.size() - 1U ||
            (m_total.has_value() && parsed->Total.has_value() && *parsed->Total != *m_total)) {
            Fail(MakeInvalidResponse("The download response has an unexpected Content-Range."));
            return false;
        }
        return true;
    }

    // Sinks that block on the disk are written from the writer scheduler (see RunOnWriter) so a slow write never
    // stalls the I/O thread that is also servicing every other connection. Only the worker whose turn it is writes, so
    // the sink is never used concurrently and receives data in offset order.
    boost::asio::awaitable<bool> WriteBody(std::string data) {
        const std::size_t size = data.size();
        bool ok = false;
        if constexpr (TSink::WritesInline) {
            ok = TryWrite(m_sink, std::move(data));
        } else {
            try {
                co_await RunOnWriter(m_httpClient, m_options.WriteScheduler,
                                     [&] { ok = TryWrite(m_sink, std::move(data)); });
            } catch (...) {
                Fail(MakeFailureFromCurrentException());
                co_return false;
            }
        }
        if (!ok) {
            Fail(MakeFailure(std::errc::io_error, "Failed to write the downloaded data."));
            co_return false;
        }
        m_written += size;
        co_return true;
    }

    boost::asio::awaitable<void> Worker() {
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        const auto cancellation = co_await boost::asio::this_coro::cancellation_state;
        const auto stopped = [&] {
            if (m_failure.has_value()) {
                return true;
            }
            if (IsAbortingCancellation(cancellation.cancelled())) {
                Fail(CanceledFailure());
                return true;
            }
            return false;
        };
        try {
            for (;;) {
                if (stopped()) {
                    throw StopWorkers{};
                }
                if (m_next >= *m_end) {
                    co_return;
                }
                const std::uint64_t length = std::min<std::uint64_t>(m_chunkSize, *m_end - m_next);
                const std::uint64_t offset = m_next;
                m_next += length;

                HttpResult sent;
                if (!co_await Fetch(RequestKind::Chunk, offset, length, m_chunkConditions, sent)) {
                    throw StopWorkers{};
                }
                std::string body;
                if (!ValidateChunk(std::move(sent), offset, length, body)) {
                    throw StopWorkers{};
                }

                // Hold the chunk until every earlier one is written; a notification (or a cancellation) wakes the wait.
                while (!stopped() && m_begin + m_written != offset) {
                    co_await m_turn->async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
                }
                if (stopped() || !co_await WriteBody(std::move(body))) {
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

    [[nodiscard]] SummaryResult Finish() {
        if (!m_failure.has_value() && (!m_end.has_value() || *m_end < m_begin || m_written != *m_end - m_begin)) {
            Fail(MakeInvalidResponse("The download ended before all data was received."));
        }
        // The last write can outlast the budget after the final response arrived in time.
        if (!m_failure.has_value() && m_deadline.Expired()) {
            Fail(MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
        }
        if (m_failure.has_value()) {
            return std::unexpected(std::move(*m_failure));
        }
        m_summary.BytesWritten = m_written;
        m_summary.Properties.ContentLength = m_written;
        if (m_ranged) {
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
    bool m_ranged = false;
    std::optional<BlobStorageError> m_failure;
    DownloadSummary m_summary;
};

[[nodiscard]] Response<Models::DownloadBlobToResult> ToDownloadToResponse(DownloadSummary summary);

// The engine's state lives in this coroutine's frame, which is why the arguments are taken by value.
template <HttpClientLike THttp, class TCred, DownloadSinkLike TSink>
boost::asio::awaitable<DownloadOutcome<TSink>>
RunDownload(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> target, DownloadToOptions options,
            TSink sink, AzureRequestOptions requestOptions,
            std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now()) {
    DownloadEngine<THttp, TCred, TSink> engine(httpClient, *target, std::move(options), std::move(requestOptions),
                                               std::move(sink), start);
    co_return co_await engine.Run();
}

// Closes the temporary file and, on success, renames it over `target`; any failure removes the temporary file.
[[nodiscard]] std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>
FinalizeFileDownload(std::ofstream& file, const std::filesystem::path& tempPath, const std::filesystem::path& target,
                     SummaryResult result);

using FileResult = std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>;

[[nodiscard]] inline BlobStorageError CurrentFailure() {
    try {
        return MakeFailureFromCurrentException();
    } catch (...) {
        return MakeFailure(std::errc::not_enough_memory);
    }
}

// Removes the temporary file (if any) and reports `error`. Blocks on the disk, so it runs on a writer thread.
inline FileResult AbandonFileDownload(const std::filesystem::path& tempPath, BlobStorageError error) noexcept {
    if (!tempPath.empty()) {
        std::error_code ignored;
        std::filesystem::remove(tempPath, ignored);
    }
    return FileResult{std::unexpect, std::move(error)};
}
} // namespace DownloadDetail

// Downloads the blob into memory. See DownloadBlobToSpanAsync for the transfer behavior.
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

// Every blocking filesystem call (directory check, open, close, rename, remove) runs on the write scheduler (see
// RunOnWriter), never on the thread that runs the coroutine. The download goes to a sibling temporary file that is
// renamed over `path` only on success, so a failed download never leaves a truncated target behind. A cancellation
// that arrives before the rename removes the temporary file; the rename is the commit point.
template <HttpClientLike THttp, class TCred>
boost::asio::awaitable<std::expected<Response<Models::DownloadBlobToResult>, BlobStorageError>>
DownloadBlobToFileAsync(THttp& httpClient, std::shared_ptr<const BasicBlobTarget<TCred>> options,
                        DownloadToOptions operationOptions, std::filesystem::path path,
                        AzureRequestOptions requestOptions) {
    using namespace DownloadDetail;
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    const auto cancellation = co_await boost::asio::this_coro::cancellation_state;
    // The budget covers the hand-offs to the writer and back, so it starts here rather than at the first GET.
    const auto start = std::chrono::steady_clock::now();
    const AggregateDeadline deadline(requestOptions, start);
    const auto scheduler = operationOptions.WriteScheduler;
    const auto canceled = [&] { return IsAbortingCancellation(cancellation.cancelled()); };

    std::filesystem::path tempPath;
    std::ofstream file;
    std::optional<BlobStorageError> prepareFailure;
    if (canceled()) {
        co_return std::unexpected(CanceledFailure());
    }
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
    if (canceled()) {
        file.close();
        co_return AbandonFileDownload(tempPath, CanceledFailure());
    }

    auto outcome = co_await RunDownload(httpClient, std::move(options), std::move(operationOptions),
                                        FileSink{std::move(file)}, std::move(requestOptions), start);

    FileResult finished = std::unexpected(BlobStorageError{});
    const auto finalize = [&] {
        try {
            const bool succeeded = outcome.Result.has_value();
            // The commit point: once the rename starts the destination is published and a cancellation is too late.
            if (succeeded && canceled()) {
                outcome.Sink.File().close();
                finished = AbandonFileDownload(tempPath, CanceledFailure());
            } else if (succeeded && deadline.Expired()) {
                outcome.Sink.File().close();
                finished = AbandonFileDownload(
                    tempPath, MakeFailure(std::errc::timed_out, "The download exceeded its total deadline."));
            } else {
                finished = FinalizeFileDownload(outcome.Sink.File(), tempPath, path, std::move(outcome.Result));
            }
        } catch (...) {
            finished = AbandonFileDownload(tempPath, CurrentFailure());
        }
    };
    try {
        co_await RunOnWriter(httpClient, scheduler, finalize);
    } catch (...) {
        // The hop failed too, but leaking the partial file would be worse than blocking this thread once.
        finalize();
    }
    co_return finished;
}
} // namespace AVEVA::AzureClient::Private
