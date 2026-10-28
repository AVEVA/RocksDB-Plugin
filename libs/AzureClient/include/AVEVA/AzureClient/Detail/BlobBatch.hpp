// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/AggregateDeadline.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <expected>
#include <gsl/util>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Private {
// Limits of the Blob Batch API.
inline constexpr std::size_t MaxBatchSubrequests = 256;
inline constexpr std::size_t MaxBlobNameLength = 1024;

// Appends one multipart/mixed part carrying `subrequest` (method, URL and headers, no body) to `body`.
void AppendBatchDeletePart(std::string& body, std::string_view boundary, std::size_t contentId,
                           const HttpRequest& subrequest);
void AppendBatchClosingBoundary(std::string& body, std::string_view boundary);

// Parses a Blob Batch response whose parts answer, in Content-ID order, the delete of each of `names`.
// A 404 BlobNotFound part is reported as AlreadyAbsent rather than as an error. Throws std::invalid_argument when the
// response is not a well-formed multipart/mixed answer to every sub-request.
[[nodiscard]] std::vector<Models::DeleteBlobItemResult> ParseBatchDeleteResponse(const HttpResponse& response,
                                                                                 std::span<const std::string> names);

namespace BatchDetail {
// Thrown by a worker once the failure is recorded, so the parallel group cancels its siblings.
struct StopWorkers {};

// The state shared by the workers of one batch delete. Workers run on one strand, so none of it needs a lock; the
// owning coroutine outlives every worker because it awaits the whole group.
template <HttpClientLike THttp, TokenCredentialLike TCred> class BatchDeleteRun {
  public:
    BatchDeleteRun(THttp& httpClient, const BasicContainerTarget<TCred>& target, const std::vector<std::string>& names,
                   const AzureRequestOptions& requestOptions)
        : m_httpClient(httpClient), m_target(target), m_names(names), m_requestOptions(requestOptions),
          m_deadline(requestOptions), m_auth(MakeRequestAuth(*target.Connection, target.Credential != nullptr)) {
        m_items.resize(names.size());
    }

    [[nodiscard]] std::size_t ChunkCount() const noexcept {
        // Ceiling division without the addition that could wrap for sizes near SIZE_MAX.
        return (m_names.size() / MaxBatchSubrequests) + (m_names.size() % MaxBatchSubrequests != 0U ? 1U : 0U);
    }

    // Each worker claims the next unstarted chunk until none is left. The first failure stops every worker: the
    // others are cancelled by the group and report operation_canceled, which is ignored because the first one wins.
    boost::asio::awaitable<void> Worker() {
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        const auto cancellation = co_await boost::asio::this_coro::cancellation_state;
        try {
            for (;;) {
                if (m_failure) {
                    throw StopWorkers{};
                }
                if (IsAbortingCancellation(cancellation.cancelled())) {
                    Fail(MakeClientError(std::make_error_code(std::errc::operation_canceled),
                                         "The operation was canceled."));
                    throw StopWorkers{};
                }
                if (m_nextChunk >= ChunkCount()) {
                    co_return;
                }
                co_await RunChunk(m_nextChunk++);
            }
        } catch (const StopWorkers&) {
            throw;
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
            throw StopWorkers{};
        }
    }

    void Fail(BlobStorageError error) {
        if (!m_failure) {
            m_failure = std::move(error);
        }
    }

    [[nodiscard]] std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError> TakeResult() {
        if (m_failure) {
            return std::unexpected(std::move(*m_failure));
        }
        Models::DeleteBlobsResult result{.Items = std::move(m_items)};
        return Response<Models::DeleteBlobsResult>{std::move(result),
                                                   std::move(m_rawResponse).value_or(HttpResponse{})};
    }

    [[nodiscard]] bool Failed() const noexcept { return m_failure.has_value(); }

  private:
    void AuthorizeSubrequest(HttpRequest& subrequest, std::string_view token) const {
        switch (m_auth.Type) {
        case RequestAuth::Kind::SharedKey:
            AuthorizeRequest(*m_auth.Signer, subrequest);
            break;
        case RequestAuth::Kind::Token:
            AddHeader(subrequest, AuthorizationHeaderName, MakeBearerValue(token));
            break;
        case RequestAuth::Kind::None:
            // The SAS token is already part of the sub-request URL.
            break;
        }
    }

    // Builds the multipart body of one chunk. Called once per HTTP attempt: every sub-request carries its own date and
    // authorization, and those expire or are single-use, so they must be regenerated on a retry. The sub-request URLs
    // never change, so they are encoded on the first attempt and reused by every retry through `urls`.
    [[nodiscard]] std::string BuildChunkBody(std::size_t begin, std::size_t end, const std::string& boundary,
                                             std::string_view token, std::vector<std::string>& urls) const {
        if (urls.size() != end - begin) {
            urls.clear();
            urls.reserve(end - begin);
            for (std::size_t index = begin; index < end; ++index) {
                urls.push_back(
                    BuildBlobUrl(MakeBlobTarget(m_target.Connection, m_target.ContainerName, m_names[index])));
            }
        }
        // One date per body: sub-requests are built within the same instant, well inside the allowed clock skew.
        const std::string date = BuildDateHeaderValue(std::chrono::system_clock::now());
        constexpr std::size_t PerPartOverhead = 512U;
        std::size_t reserveSize = boundary.size() + PerPartOverhead;
        for (const auto& url : urls) {
            reserveSize += url.size() + PerPartOverhead;
        }
        std::string body;
        body.reserve(reserveSize);
        for (std::size_t index = begin; index < end; ++index) {
            HttpRequest subrequest;
            subrequest.SetMethod(HttpMethod::Delete);
            subrequest.SetUrl(urls[index - begin]);
            AddHeader(subrequest, XMsDateHeaderName, date);
            AuthorizeSubrequest(subrequest, token);
            AddHeader(subrequest, ContentLengthHeaderName, std::string_view{"0"});
            AppendBatchDeletePart(body, boundary, index - begin, subrequest);
        }
        AppendBatchClosingBoundary(body, boundary);
        return body;
    }

    // Sends one chunk and stores its results in the chunk's own slice of the result. A failure is recorded and stops
    // the workers.
    boost::asio::awaitable<void> RunChunk(std::size_t chunk) {
        const std::size_t begin = chunk * MaxBatchSubrequests;
        const std::size_t end = std::min(begin + MaxBatchSubrequests, m_names.size());

        HttpRequest request;
        std::string boundary;
        try {
            boundary = "batch_" + CreateClientRequestId();
            request = BuildContainerRequest(m_target, HttpMethod::Post, "comp=batch");
            AddHeader(request, ContentTypeHeaderName, "multipart/mixed; boundary=" + boundary);
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
            throw StopWorkers{};
        }

        AzureRequestOptions chunkOptions = m_requestOptions;
        // Each batch gets only what is left of the whole delete's budget, and none starts once it is spent.
        if (!m_deadline.Apply(chunkOptions)) {
            Fail(MakeClientError(std::make_error_code(std::errc::timed_out),
                                 "The batch delete exceeded its total deadline."));
            throw StopWorkers{};
        }

        // The bearer token for each attempt comes from the pipeline's rebuild hook, so a retry never replays stale
        // sub-request dates, signatures or tokens.
        std::vector<std::string> urls;
        HttpResult sent =
            co_await SendAuthorizedRequest(m_httpClient, m_target, std::move(request), std::move(chunkOptions),
                                           [&](HttpRequest& attempt, std::string_view token) {
                                               attempt.SetBody(BuildChunkBody(begin, end, boundary, token, urls));
                                           });

        const std::span<const std::string> names = std::span{m_names}.subspan(begin, end - begin);
        auto outcome = ParseOutcome<std::vector<Models::DeleteBlobItemResult>>(
            std::move(sent),
            [names](const HttpResponse& response) { return ParseBatchDeleteResponse(response, names); });
        if (!outcome) {
            Fail(std::move(outcome).error());
            throw StopWorkers{};
        }
        std::ranges::move(outcome->Value(), m_items.begin() + gsl::narrow_cast<std::ptrdiff_t>(begin));
        if (!m_rawResponse.has_value()) {
            m_rawResponse = std::move(*outcome).RawResponse();
        }
    }

    THttp& m_httpClient;
    const BasicContainerTarget<TCred>& m_target;
    const std::vector<std::string>& m_names;
    AzureRequestOptions m_requestOptions;
    AggregateDeadline m_deadline;
    RequestAuth m_auth;

    std::vector<Models::DeleteBlobItemResult> m_items;
    std::optional<BlobStorageError> m_failure;
    std::optional<HttpResponse> m_rawResponse;
    std::size_t m_nextChunk = 0;
};
} // namespace BatchDetail

// Deletes `names` from the container with Blob Batch requests. Invalid names fail the whole call before anything is
// sent; an empty list completes successfully with an empty result. The names are split into requests of at most
// MaxBatchSubrequests deletes, run by up to MaxConcurrentRequests workers. The first failure cancels the requests in
// flight, and the result is returned once every worker has finished. Cancelling the awaiting coroutine cancels all
// workers and fails with operation_canceled.
template <HttpClientLike THttp, TokenCredentialLike TCred>
boost::asio::awaitable<std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError>>
DeleteBlobsAsync(THttp& httpClient, std::shared_ptr<const BasicContainerTarget<TCred>> target,
                 std::vector<std::string> blobNames, DeleteBlobsOptions options, AzureRequestOptions requestOptions) {
    using Result = std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError>;
    if (blobNames.empty()) {
        co_return Result{Response<Models::DeleteBlobsResult>{}};
    }
    for (const std::string& name : blobNames) {
        if (name.empty() || name.size() > MaxBlobNameLength) {
            co_return InvalidArgument("Blob names must be between 1 and 1024 characters long.");
        }
    }

    co_await boost::asio::this_coro::throw_if_cancelled(false);
    const auto cancellation = co_await boost::asio::this_coro::cancellation_state;
    const auto executor = co_await boost::asio::this_coro::executor;

    BatchDetail::BatchDeleteRun<THttp, TCred> run(httpClient, *target, blobNames, requestOptions);
    const std::size_t workers = std::min(std::max<std::size_t>(1U, options.MaxConcurrentRequests), run.ChunkCount());
    const auto strand = boost::asio::make_strand(executor);
    using Op = decltype(boost::asio::co_spawn(strand, run.Worker(), boost::asio::deferred));
    std::vector<Op> operations;
    try {
        operations.reserve(workers);
        for (std::size_t index = 0; index < workers; ++index) {
            operations.push_back(boost::asio::co_spawn(strand, run.Worker(), boost::asio::deferred));
        }
        // Waits for every worker even after a failure, so nothing touches `run` once this coroutine returns.
        co_await boost::asio::experimental::make_parallel_group(std::move(operations))
            .async_wait(boost::asio::experimental::wait_for_one_error(), boost::asio::use_awaitable);
    } catch (...) {
        run.Fail(MakeFailureFromCurrentException());
    }
    if (!run.Failed() && IsAbortingCancellation(cancellation.cancelled())) {
        run.Fail(MakeClientError(std::make_error_code(std::errc::operation_canceled), "The operation was canceled."));
    }
    co_return run.TakeResult();
}
} // namespace AVEVA::AzureClient::Private
