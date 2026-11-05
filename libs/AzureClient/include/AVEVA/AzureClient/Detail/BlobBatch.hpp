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
#include <gsl/pointers>
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
/// <summary>The most sub-requests the Blob Batch API accepts in one request.</summary>
inline constexpr std::size_t MaxBatchSubrequests = 256;
/// <summary>The longest blob name, in characters, that the service accepts.</summary>
inline constexpr std::size_t MaxBlobNameLength = 1024;

/// <summary>Appends one multipart/mixed part carrying <paramref name="subrequest"/> (method, URL and headers, no body)
/// to <paramref name="body"/>.</summary>
/// <param name="body">The multipart body being built; left untouched when the sub-request is rejected.</param>
/// <param name="boundary">The multipart boundary of <paramref name="body"/>.</param>
/// <param name="contentId">The Content-ID of the part, which the response uses to answer it.</param>
/// <param name="subrequest">The sub-request to carry; its URL must be absolute and include a path.</param>
/// <exception cref="std::invalid_argument">
/// Thrown when the URL is not absolute, has no path, or contains CR or LF, or when a header is not valid on the wire.
/// </exception>
void AppendBatchDeletePart(std::string& body, std::string_view boundary, std::size_t contentId,
                           const HttpRequest& subrequest);
/// <summary>Terminates the multipart body once every part has been appended.</summary>
/// <param name="body">The multipart body being built.</param>
/// <param name="boundary">The multipart boundary of <paramref name="body"/>.</param>
void AppendBatchClosingBoundary(std::string& body, std::string_view boundary);

/// <summary>
/// Parses a Blob Batch response whose parts answer, in Content-ID order, the delete of each of
/// <paramref name="names"/>. A 404 BlobNotFound part is reported as AlreadyAbsent rather than as an error.
/// </summary>
/// <param name="response">The response to the batch request.</param>
/// <param name="names">The blob names that were deleted, in Content-ID order.</param>
/// <returns>One result per name, in the order of <paramref name="names"/>; or, when the service rejected the batch as
/// a whole (a single part without a Content-ID carrying an error status), that error mapped like any failed
/// response.</returns>
/// <exception cref="std::invalid_argument">
/// Thrown when the response is not a well-formed multipart/mixed answer to every sub-request.
/// </exception>
[[nodiscard]] std::expected<std::vector<Models::DeleteBlobItemResult>, BlobStorageError>
ParseBatchDeleteResponse(const HttpResponse& response, std::span<const std::string> names);

namespace BatchDetail {
/// <summary>Thrown by a worker once the failure is recorded, so the parallel group cancels its siblings.</summary>
struct StopWorkers {};

/// <summary>
/// The state shared by the workers of one batch delete. Workers run on one strand, so none of it needs a lock; the
/// owning coroutine outlives every worker because it awaits the whole group. The run owns the names and shares the
/// target and the HTTP client's owner, so nothing it uses depends on the client that started it.
/// </summary>
/// <typeparam name="THttp">The HTTP client type the batch requests are sent through.</typeparam>
/// <typeparam name="TCred">The credential type the container target authorizes with.</typeparam>
template <HttpClientLike THttp, TokenCredentialLike TCred> class BatchDeleteRun {
  public:
    /// <summary>Prepares the run.</summary>
    /// <param name="httpClient">The HTTP client to send the batch requests through; kept alive by
    /// <paramref name="httpOwner"/> when that is set, otherwise by the caller.</param>
    /// <param name="httpOwner">The shared owner of the HTTP client; null when it is only borrowed.</param>
    /// <param name="target">The container the blobs are deleted from.</param>
    /// <param name="names">The blob names to delete.</param>
    /// <param name="requestOptions">The options every batch request starts from; their total deadline is the budget of
    /// the whole run.</param>
    BatchDeleteRun(THttp& httpClient, std::shared_ptr<THttp> httpOwner,
                   gsl::not_null<std::shared_ptr<const BasicContainerTarget<TCred>>> target,
                   std::vector<std::string> names, AzureRequestOptions requestOptions)
        : m_httpClient(&httpClient), m_httpOwner(std::move(httpOwner)), m_target(std::move(target)),
          m_names(std::move(names)), m_requestOptions(std::move(requestOptions)), m_deadline(m_requestOptions),
          m_auth(MakeRequestAuth(*m_target->Connection, PresenceOf(m_target->Credential))) {
        m_items.resize(m_names.size());
    }

    /// <summary>Counts the batch requests needed to cover every name.</summary>
    /// <returns>The number of chunks of at most <see cref="MaxBatchSubrequests"/> names.</returns>
    [[nodiscard]] std::size_t ChunkCount() const noexcept {
        // Ceiling division without the addition that could wrap for sizes near SIZE_MAX.
        return (m_names.size() / MaxBatchSubrequests) + (m_names.size() % MaxBatchSubrequests != 0U ? 1U : 0U);
    }

    /// <summary>
    /// Each worker claims the next unstarted chunk until none is left. The first failure stops every worker: the
    /// others are cancelled by the group and report operation_canceled, which is ignored because the first one wins.
    /// </summary>
    /// <exception cref="StopWorkers">Thrown after a failure has been recorded, to cancel the sibling
    /// workers.</exception>
    boost::asio::awaitable<void> Worker() {
        try {
            for (;;) {
                if (m_failure) {
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

    /// <summary>Records a failure; only the first one is kept.</summary>
    /// <param name="error">The failure to record.</param>
    void Fail(BlobStorageError error) {
        if (!m_failure) {
            m_failure = std::move(error);
        }
    }

    /// <summary>Takes the outcome of the run once every worker has finished.</summary>
    /// <returns>The recorded failure if there is one, otherwise the per-blob results with the first response
    /// received.</returns>
    [[nodiscard]] std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError> TakeResult() {
        if (m_failure) {
            return std::unexpected(std::move(*m_failure));
        }
        Models::DeleteBlobsResult result{.Items = std::move(m_items)};
        return Response<Models::DeleteBlobsResult>{std::move(result),
                                                   std::move(m_rawResponse).value_or(HttpResponse{})};
    }

  private:
    /// <summary>Authorizes a sub-request the way the whole run is authorized.</summary>
    /// <param name="subrequest">The sub-request to authorize.</param>
    /// <param name="token">The bearer token of the current attempt; used only for token authorization.</param>
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

    /// <summary>
    /// Builds the multipart body of one chunk. Called once per HTTP attempt: every sub-request carries its own date and
    /// authorization, and those expire or are single-use, so they must be regenerated on a retry. The sub-request URLs
    /// never change, so they are encoded on the first attempt and reused by every retry through
    /// <paramref name="urls"/>.
    /// </summary>
    /// <param name="begin">The index of the chunk's first name.</param>
    /// <param name="end">The index one past the chunk's last name.</param>
    /// <param name="boundary">The multipart boundary of the body.</param>
    /// <param name="token">The bearer token of this attempt; used only for token authorization.</param>
    /// <param name="urls">The cache of encoded sub-request URLs; filled on the first call and reused
    /// afterwards.</param>
    /// <returns>The complete multipart body.</returns>
    /// <exception cref="std::invalid_argument">Thrown when a sub-request is     rejected by
    /// <see cref="AppendBatchDeletePart"/>.</exception>
    [[nodiscard]] std::string BuildChunkBody(std::size_t begin, std::size_t end, const std::string& boundary,
                                             std::string_view token, std::vector<std::string>& urls) const {
        if (urls.size() != end - begin) {
            urls.clear();
            urls.reserve(end - begin);
            for (std::size_t index = begin; index < end; ++index) {
                urls.push_back(BuildBlobUrl(MakeBlobTarget(
                    m_target->Connection, ContainerNameText{m_target->ContainerName}, BlobNameText{m_names[index]})));
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
            subrequest.SetUrl(AVEVA::HttpUrl{urls[index - begin]});
            AddHeader(subrequest, XMsDateHeaderName, date);
            AuthorizeSubrequest(subrequest, token);
            AddHeader(subrequest, ContentLengthHeaderName, std::string_view{"0"});
            AppendBatchDeletePart(body, boundary, index - begin, subrequest);
        }
        AppendBatchClosingBoundary(body, boundary);
        return body;
    }

    /// <summary>
    /// Sends one chunk and stores its results in the chunk's own slice of the result. A failure is recorded and stops
    /// the workers.
    /// </summary>
    /// <param name="chunk">The index of the chunk to send.</param>
    /// <exception cref="StopWorkers">Thrown after a failure has been recorded, to cancel the sibling
    /// workers.</exception>
    boost::asio::awaitable<void> RunChunk(std::size_t chunk) {
        const std::size_t begin = chunk * MaxBatchSubrequests;
        const std::size_t end = std::min(begin + MaxBatchSubrequests, m_names.size());

        HttpRequest request;
        std::string boundary;
        try {
            boundary = "batch_" + CreateClientRequestId();
            request = BuildContainerRequest(*m_target, HttpMethod::Post, "comp=batch");
            AddHeader(request, ContentTypeHeaderName, "multipart/mixed; boundary=" + boundary);
        } catch (...) {
            Fail(MakeFailureFromCurrentException());
            throw StopWorkers{};
        }

        AzureRequestOptions chunkOptions = m_requestOptions;
        // Each batch gets only what is left of the whole delete's budget, and none starts once it is spent.
        if (m_deadline.Apply(chunkOptions) == Admission::Refused) {
            Fail(MakeClientError(std::make_error_code(std::errc::timed_out),
                                 "The batch delete exceeded its total deadline."));
            throw StopWorkers{};
        }

        // The bearer token for each attempt comes from the pipeline's rebuild hook, so a retry never replays stale
        // sub-request dates, signatures or tokens.
        std::vector<std::string> urls;
        HttpResult sent = co_await SendAuthorizedRequest(
            *m_httpClient, *m_target, std::move(request), std::move(chunkOptions),
            [&](HttpRequest& attempt, std::string_view token) {
                attempt.SetBody(AVEVA::HttpBody{BuildChunkBody(begin, end, boundary, token, urls)});
            });

        const std::span<const std::string> names = std::span{m_names}.subspan(begin, end - begin);
        auto outcome = ParseOutcome<std::expected<std::vector<Models::DeleteBlobItemResult>, BlobStorageError>>(
            std::move(sent),
            [names](const HttpResponse& response) { return ParseBatchDeleteResponse(response, names); });
        if (!outcome) {
            Fail(std::move(outcome).error());
            throw StopWorkers{};
        }
        if (!outcome->Value().has_value()) {
            Fail(std::move(outcome->Value()).error());
            throw StopWorkers{};
        }
        std::ranges::move(*outcome->Value(), m_items.begin() + gsl::narrow_cast<std::ptrdiff_t>(begin));
        if (!m_rawResponse.has_value()) {
            m_rawResponse = std::move(*outcome).RawResponse();
        }
    }

    gsl::not_null<THttp*> m_httpClient;
    std::shared_ptr<THttp> m_httpOwner;
    gsl::not_null<std::shared_ptr<const BasicContainerTarget<TCred>>> m_target;
    std::vector<std::string> m_names;
    AzureRequestOptions m_requestOptions;
    AggregateDeadline m_deadline;
    RequestAuth m_auth;

    std::vector<Models::DeleteBlobItemResult> m_items;
    std::optional<BlobStorageError> m_failure;
    std::optional<HttpResponse> m_rawResponse;
    std::size_t m_nextChunk = 0;
};
} // namespace BatchDetail

/// <summary>
/// Deletes <paramref name="blobNames"/> from the container with Blob Batch requests. Invalid names fail the whole call
/// before anything is sent; an empty list completes successfully with an empty result. The names are split into
/// requests of at most <see cref="MaxBatchSubrequests"/> deletes, run by up to
/// <c>MaxConcurrentRequests</c> workers. The first failure cancels the requests in flight, and the result is returned
/// once every worker has finished. Cancelling the awaiting coroutine cancels all workers and fails with
/// operation_canceled.
/// </summary>
/// <typeparam name="THttp">The HTTP client type the batch requests are sent through.</typeparam>
/// <typeparam name="TCred">The credential type the container target authorizes with.</typeparam>
/// <param name="httpClient">The HTTP client to send the batch requests through.</param>
/// <param name="httpOwner">The shared owner of <paramref name="httpClient"/>, held for the whole call; null when the
/// caller keeps the HTTP client alive.</param>
/// <param name="target">The container the blobs are deleted from.</param>
/// <param name="blobNames">The names of the blobs to delete.</param>
/// <param name="options">The operation options; <c>MaxConcurrentRequests</c> bounds the parallel batch
/// requests.</param>
/// <param name="requestOptions">The options each batch request runs with; their total deadline covers the whole
/// call.</param>
/// <returns>The per-blob results, or the first failure; failures are returned rather than thrown.</returns>
template <HttpClientLike THttp, TokenCredentialLike TCred>
boost::asio::awaitable<std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError>>
DeleteBlobsAsync(THttp& httpClient, std::shared_ptr<THttp> httpOwner,
                 gsl::not_null<std::shared_ptr<const BasicContainerTarget<TCred>>> target,
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

    const auto executor = co_await boost::asio::this_coro::executor;

    BatchDetail::BatchDeleteRun<THttp, TCred> run(httpClient, std::move(httpOwner), std::move(target),
                                                  std::move(blobNames), std::move(requestOptions));
    const std::size_t workers = std::min(options.MaxConcurrentRequests.Value(), run.ChunkCount());
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
    co_return run.TakeResult();
}
} // namespace AVEVA::AzureClient::Private
