// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/AzureRequestOptions.hpp>
#include <AVEVA/AzureClient/BlobOperationOptions.hpp>
#include <AVEVA/AzureClient/BlobStorageError.hpp>
#include <AVEVA/AzureClient/BlobStorageErrorCode.hpp>
#include <AVEVA/AzureClient/Detail/AsyncInitiation.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/AzureClient/Detail/RequestPipeline.hpp>
#include <AVEVA/AzureClient/HttpClientLike.hpp>
#include <AVEVA/AzureClient/Models/BlobContainerModels.hpp>
#include <AVEVA/AzureClient/Response.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpMethod.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/post.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <expected>
#include <map>
#include <memory>
#include <mutex>
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

// Splits the names into Blob Batch requests of at most MaxBatchSubrequests deletes and runs them with a bounded
// number in flight. Chunk results are written to their own slice of the result, so concurrent completions never
// touch the same element; the first chunk-level failure stops new chunks from starting and is reported once the
// chunks already in flight have finished.
template <HttpClientLike THttp, TokenCredentialLike TCred, class TCompletion>
class BatchDeleteOperation final
    : public std::enable_shared_from_this<BatchDeleteOperation<THttp, TCred, TCompletion>> {
  public:
    BatchDeleteOperation(THttp& httpClient, std::shared_ptr<const BasicContainerTarget<TCred>> target,
                         std::vector<std::string> names, std::size_t maxConcurrentRequests, TCompletion completion,
                         AzureRequestOptions requestOptions)
        : m_httpClient(httpClient), m_target(std::move(target)), m_names(std::move(names)),
          m_maxConcurrent(std::max<std::size_t>(1U, maxConcurrentRequests)), m_completion(std::move(completion)),
          m_requestOptions(requestOptions), m_parentSlot(requestOptions.GetCancellationSlot()),
          m_auth(MakeRequestAuth(*m_target->Connection, m_target->Credential != nullptr)) {
        m_items.resize(m_names.size());
    }

    void Start() {
        if (m_parentSlot.is_connected()) {
            // The parent slot is assigned once here; every chunk gets its own signal so no chunk replaces the
            // handler of another, and a cancel reaches all of them.
            std::weak_ptr<BatchDeleteOperation> const weak = this->weak_from_this();
            THttp const* httpClient = &m_httpClient;
            m_parentSlot.assign([weak, httpClient](boost::asio::cancellation_type_t) {
                boost::asio::post(httpClient->get_executor(), [weak]() {
                    if (auto self = weak.lock()) {
                        self->OnCancel();
                    }
                });
            });
        }
        Pump();
    }

  private:
    [[nodiscard]] std::size_t ChunkCount() const noexcept {
        // Ceiling division without the addition that could wrap for sizes near SIZE_MAX.
        return (m_names.size() / MaxBatchSubrequests) + (m_names.size() % MaxBatchSubrequests != 0U ? 1U : 0U);
    }

    // Starts chunks while there is room and finishes once nothing is left in flight. Completions may arrive inline,
    // so re-entrant calls are expected; the lock makes each decision (start / finish / nothing) exactly once.
    void Pump() {
        const auto keepAlive = this->shared_from_this();
        for (;;) {
            std::optional<std::size_t> chunk;
            std::shared_ptr<boost::asio::cancellation_signal> signal;
            bool finish = false;
            {
                const std::scoped_lock lock(m_mutex);
                if (!m_failure && m_nextChunk < ChunkCount() && m_inFlight < m_maxConcurrent) {
                    chunk = m_nextChunk++;
                    ++m_inFlight;
                    signal = std::make_shared<boost::asio::cancellation_signal>();
                    m_signals.emplace(*chunk, signal);
                } else if (m_inFlight == 0 && !m_finished && (m_failure || m_nextChunk >= ChunkCount())) {
                    m_finished = true;
                    finish = true;
                }
            }
            if (chunk) {
                StartChunk(*chunk, signal);
                continue;
            }
            if (finish) {
                Finish();
            }
            return;
        }
    }

    // The bearer token for each attempt comes from the request pipeline's rebuild hook, so a retry never replays
    // stale sub-request dates, signatures or tokens.
    void StartChunk(std::size_t chunk, const std::shared_ptr<boost::asio::cancellation_signal>& signal) {
        // Slot assignment (inside SendChunk) and every emit on the signal are serialized by m_emitMutex because
        // cancellation_signal is not thread-safe. Recursive: a failure path may re-enter Pump on this thread.
        const std::scoped_lock emitLock(m_emitMutex);
        SendChunk(chunk, signal);
        // A cancel that raced the chunk start may have emitted before the pipeline attached its handler.
        if (m_cancelled.load(std::memory_order_acquire)) {
            signal->emit(boost::asio::cancellation_type::terminal);
        }
    }

    // Aborts every chunk in flight and stops new ones from starting. Each chunk reports operation_canceled through
    // its own completion, so the result is delivered once the in-flight chunks have wound down.
    void OnCancel() {
        std::vector<std::shared_ptr<boost::asio::cancellation_signal>> signals;
        {
            const std::scoped_lock lock(m_mutex);
            if (m_finished) {
                return;
            }
            m_cancelled.store(true, std::memory_order_release);
            if (!m_failure) {
                m_failure =
                    MakeClientError(std::make_error_code(std::errc::operation_canceled), "The operation was canceled.");
            }
            for (const auto& entry : m_signals) {
                signals.push_back(entry.second);
            }
        }
        {
            const std::scoped_lock emitLock(m_emitMutex);
            for (const auto& signal : signals) {
                signal->emit(boost::asio::cancellation_type::terminal);
            }
        }
        Pump();
    }

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
                    BuildBlobUrl(MakeBlobTarget(m_target->Connection, m_target->ContainerName, m_names.at(index))));
            }
        }
        std::string body;
        for (std::size_t index = begin; index < end; ++index) {
            HttpRequest subrequest;
            subrequest.SetMethod(HttpMethod::Delete);
            subrequest.SetUrl(urls[index - begin]);
            AddHeader(subrequest, XMsDateHeaderName, BuildDateHeaderValue(std::chrono::system_clock::now()));
            AuthorizeSubrequest(subrequest, token);
            AddHeader(subrequest, ContentLengthHeaderName, "0");
            AppendBatchDeletePart(body, boundary, index - begin, subrequest);
        }
        AppendBatchClosingBoundary(body, boundary);
        return body;
    }

    void SendChunk(std::size_t chunk, const std::shared_ptr<boost::asio::cancellation_signal>& signal) {
        const std::size_t begin = chunk * MaxBatchSubrequests;
        const std::size_t end = std::min(begin + MaxBatchSubrequests, m_names.size());

        HttpRequest request;
        std::string boundary;
        try {
            boundary = "batch_" + CreateClientRequestId();
            request = BuildContainerRequest(*m_target, HttpMethod::Post, "comp=batch");
            AddHeader(request, ContentTypeHeaderName, "multipart/mixed; boundary=" + boundary);
        } catch (...) {
            FailChunk(chunk, MakeFailureFromCurrentException());
            return;
        }

        AzureRequestOptions chunkOptions = m_requestOptions;
        chunkOptions.SetCancellationSlot(signal->slot());
        SendAuthorizedRequestAsync(
            m_httpClient, *m_target, std::move(request),
            [self = this->shared_from_this(), chunk, begin, end](std::error_code error, HttpResponse response) {
                self->OnChunkResponse(chunk, std::span{self->m_names}.subspan(begin, end - begin), error,
                                      std::move(response));
            },
            chunkOptions,
            [self = this->shared_from_this(), begin, end, boundary = std::move(boundary),
             urls = std::make_shared<std::vector<std::string>>()](HttpRequest& attempt, std::string_view token) {
                attempt.SetBody(self->BuildChunkBody(begin, end, boundary, token, *urls));
            });
    }
    void OnChunkResponse(std::size_t chunk, std::span<const std::string> names, std::error_code error,
                         HttpResponse response) {
        RequestFailure failure = DetermineBlobStorageFailure(error, response);
        std::vector<Models::DeleteBlobItemResult> items;
        if (!failure.Error) {
            try {
                items = ParseBatchDeleteResponse(response, names);
            } catch (const std::exception& exception) {
                failure = MakeInvalidResponseFailure(response, exception.what());
            }
        }

        if (failure.Error) {
            RecordFailure(chunk, ToDetails(std::move(failure)));
            return;
        }

        {
            const std::scoped_lock lock(m_mutex);
            std::ranges::move(items, m_items.begin() + static_cast<std::ptrdiff_t>(chunk * MaxBatchSubrequests));
            if (!m_rawResponse.has_value()) {
                m_rawResponse = std::move(response);
            }
            m_signals.erase(chunk);
            --m_inFlight;
        }
        Pump();
    }

    // Never completes inline: the failure may be found while still inside the initiating call.
    void FailChunk(std::size_t chunk, BlobStorageError error) {
        boost::asio::post(m_httpClient.get_executor(),
                          [self = this->shared_from_this(), chunk, error = std::move(error)]() mutable {
                              self->RecordFailure(chunk, std::move(error));
                          });
    }

    void RecordFailure(std::size_t chunk, BlobStorageError error) {
        {
            const std::scoped_lock lock(m_mutex);
            m_signals.erase(chunk);
            if (!m_failure) {
                m_failure = std::move(error);
            }
            --m_inFlight;
        }
        Pump();
    }

    void Finish() {
        // No other thread can touch the state any more: every chunk has completed and none will start.
        if (m_failure) {
            m_completion(std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError>{
                std::unexpected(std::move(*m_failure))});
            return;
        }
        Models::DeleteBlobsResult result{.Items = std::move(m_items)};
        m_completion(std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError>{
            Response<Models::DeleteBlobsResult>{std::move(result), m_rawResponse.value_or(HttpResponse{})}});
    }

    THttp& m_httpClient;
    std::shared_ptr<const BasicContainerTarget<TCred>> m_target;
    std::vector<std::string> m_names;
    std::size_t m_maxConcurrent;
    TCompletion m_completion;
    AzureRequestOptions m_requestOptions;
    boost::asio::cancellation_slot m_parentSlot;
    RequestAuth m_auth;

    std::mutex m_mutex;
    std::recursive_mutex m_emitMutex;
    std::vector<Models::DeleteBlobItemResult> m_items;
    std::optional<BlobStorageError> m_failure;
    std::optional<HttpResponse> m_rawResponse;
    std::size_t m_nextChunk = 0;
    std::size_t m_inFlight = 0;
    bool m_finished = false;
    std::atomic<bool> m_cancelled{false};
    std::map<std::size_t, std::shared_ptr<boost::asio::cancellation_signal>> m_signals;
};

// Deletes `names` from the container with Blob Batch requests. Invalid names fail the whole call before anything is
// sent; an empty list completes successfully with an empty result.
template <HttpClientLike THttp, TokenCredentialLike TCred, class TCompletion>
void DeleteBlobsAsync(THttp& httpClient, std::shared_ptr<const BasicContainerTarget<TCred>> target,
                      std::vector<std::string> names, const DeleteBlobsOptions& options, TCompletion completion,
                      AzureRequestOptions requestOptions) {
    if (names.empty()) {
        PostCompletion(httpClient, std::move(completion),
                       std::expected<Response<Models::DeleteBlobsResult>, BlobStorageError>{
                           Response<Models::DeleteBlobsResult>{}});
        return;
    }
    for (const std::string& name : names) {
        if (name.empty() || name.size() > MaxBlobNameLength) {
            PostCompletion(
                httpClient, std::move(completion),
                MakeError<Models::DeleteBlobsResult>(std::make_error_code(std::errc::invalid_argument),
                                                     "Blob names must be between 1 and 1024 characters long."));
            return;
        }
    }

    auto operation = std::make_shared<BatchDeleteOperation<THttp, TCred, TCompletion>>(
        httpClient, std::move(target), std::move(names), options.MaxConcurrentRequests, std::move(completion),
        requestOptions);
    operation->Start();
}
} // namespace AVEVA::AzureClient::Private
