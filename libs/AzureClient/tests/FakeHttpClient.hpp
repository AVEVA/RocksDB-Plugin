// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/ITokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include "../src/DownloadWriterPool.hpp"

#include <algorithm>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Tests
{
    class FakeHttpClient final : public IHttpClient
    {
      public:
        FakeHttpClient()
        {
            s_instances.push_back(this);
        }

        ~FakeHttpClient() override
        {
            auto it = std::ranges::find(s_instances, this);
            if (it != s_instances.end())
            {
                s_instances.erase(it);
            }
        }

        FakeHttpClient(const FakeHttpClient&) = delete;
        FakeHttpClient& operator=(const FakeHttpClient&) = delete;
        FakeHttpClient(FakeHttpClient&&) = delete;
        FakeHttpClient& operator=(FakeHttpClient&&) = delete;

        // Return a snapshot of live instances. Tests may call this to Poll all clients.
        static std::vector<FakeHttpClient*> Instances()
        {
            return s_instances;
        }

        struct RequestRecord
        {
            HttpRequest Request;
            HttpRequestOptions Options;

            // Body content captured at request-send time (see SendAsyncErased), so that it remains
            // valid for inspection even after Request's completion has run and, for the internal
            // zero-copy upload path (SetBodyView), the backing buffer may have been freed.
            std::string Body;
        };

        struct ScriptedResult
        {
            std::error_code Error;
            HttpResponse Response{200, {}, ""};
            bool Deferred = false;
            std::function<bool(const RequestRecord&)> MatchRequest;
        };

        // Backed by a real io_context (rather than boost::asio::system_executor) so that tests can
        // deterministically observe completions posted onto this executor -- e.g. by
        // Private::PostCompletion for synchronous early-exit validation failures (Task 8) -- via
        // Poll()/Run() instead of racing a background thread pool.
        executor_type get_executor() const override
        {
            return {m_context.get_executor()};
        }

        // Runs ready handlers on this client's executor without blocking; returns the number run.
        // Use after an ...Async call to observe a completion that Task 8's PostCompletion deferred.
        std::size_t Poll()
        {
            const std::size_t writesBefore = Private::StartedDownloadWrites().load();
            const std::size_t ran = PollRaw();
            SettleIfWritesStarted(writesBefore);
            return ran;
        }

        // Downloads write to their sink off the I/O thread; when a step started such a write this waits for it and
        // runs the completions it posts back, so a test observes the state a synchronous sink would have produced.
        void SettleIfWritesStarted(std::size_t writesBefore)
        {
            if (Private::StartedDownloadWrites().load() == writesBefore)
            {
                return;
            }
            for (;;)
            {
                while (Private::PendingDownloadWrites().load() > 0U)
                {
                    std::this_thread::yield();
                }
                const std::size_t startedNow = Private::StartedDownloadWrites().load();
                if (PollRaw() == 0U && Private::PendingDownloadWrites().load() == 0U &&
                    Private::StartedDownloadWrites().load() == startedNow)
                {
                    return;
                }
            }
        }

        std::size_t PollRaw()
        {
            // poll() leaves the context stopped once it runs out of work; restart so repeated
            // Poll() calls keep observing newly posted handlers.
            m_context.restart();
            return m_context.poll();
        }

        // Runs handlers (including timers such as retry backoff) until `done()` holds, blocking on the
        // io_context rather than sleeping. `timeout` is only a deadlock guard; returns false if it expires.
        [[nodiscard]] bool RunUntil(const std::function<bool()>& done,
            std::chrono::milliseconds timeout = std::chrono::seconds{10})
        {
            const auto deadline = std::chrono::steady_clock::now() + timeout;
            while (!done())
            {
                const auto remaining = deadline - std::chrono::steady_clock::now();
                if (remaining <= std::chrono::steady_clock::duration::zero())
                {
                    return false;
                }
                m_context.restart();
                m_context.run_one_for(remaining);
            }
            return true;
        }

        [[nodiscard]] bool WaitForRequest(std::size_t count,
            std::chrono::milliseconds timeout = std::chrono::seconds{10})
        {
            return RunUntil(
                [&]
            {
                return m_requests.size() >= count;
            },
                timeout);
        }

        void SendAsyncErased(HttpRequest request, CompletionHandler completion, HttpRequestOptions options) override
        {
            // Snapshot the body now, while any SetBodyView() backing buffer is still guaranteed
            // alive (i.e. before completion runs) -- mirrors how a real transport consumes the
            // bytes up front rather than retaining a live reference across the completion.
            std::string body = BodyAsString(request);
            m_requests.push_back(
                RequestRecord{.Request = std::move(request), .Options = options, .Body = std::move(body)});

            ScriptedResult scripted = DequeueResult(m_requests.back());
            if (scripted.Deferred)
            {
                const std::size_t requestIndex = m_requests.size() - 1U;
                m_pending.push_back(PendingCompletion{.Error = scripted.Error,
                    .Response = std::move(scripted.Response),
                    .Completion = std::move(completion),
                    .RequestIndex = requestIndex});

                // Honour the IHttpClient cancellation contract: if the caller attached a
                // cancellation slot to this request, forward cancellation signals into a
                // synchronous failure of the matching pending completion (if still pending),
                // mirroring how a real transport would abort an in-flight request.
                boost::asio::cancellation_slot slot = m_requests.at(requestIndex).Options.GetCancellationSlot();
                if (slot.is_connected())
                {
                    slot.assign([this, requestIndex](boost::asio::cancellation_type_t /*type*/)
                    {
                        (void)this->FailRequest(requestIndex, std::make_error_code(std::errc::operation_canceled));
                    });
                }
                return;
            }

            // Default to posting completions on the client's io_context to avoid re-entrant
            // synchronous completions that the real transport would not provide. Tests that need
            // inline completion can set CompleteInline() = true.
            if (m_completeInline)
            {
                const std::size_t writesBefore = Private::StartedDownloadWrites().load();
                completion(scripted.Error, std::move(scripted.Response));
                SettleIfWritesStarted(writesBefore);
            }
            else
            {
                auto exec = m_context.get_executor();
                boost::asio::post(exec,
                    [completion = std::move(completion),
                        error = scripted.Error,
                        response = std::move(scripted.Response)]() mutable
                {
                    completion(error, std::move(response));
                });
            }
        }

        void EnqueueResponse(HttpResponse response, std::error_code error = {}, bool deferred = false)
        {
            EnqueueResponse(std::move(response), error, deferred, {});
        }

        void EnqueueResponse(HttpResponse response,
            std::error_code error,
            bool deferred,
            std::function<bool(const RequestRecord&)> matchRequest)
        {
            m_scriptedResults.push_back(ScriptedResult{.Error = error,
                .Response = std::move(response),
                .Deferred = deferred,
                .MatchRequest = std::move(matchRequest)});
        }

        void EnqueueDeferredResponse(HttpResponse response, std::error_code error = {})
        {
            EnqueueResponse(std::move(response), error, true);
        }

        [[nodiscard]] bool CompleteNext()
        {
            if (m_pending.empty())
            {
                return false;
            }

            PendingCompletion pending = std::move(m_pending.front());
            m_pending.pop_front();
            // Complete immediately for test helpers: invoking the completion inline simplifies
            // test assertions that expect the callback to have been invoked after calling
            // CompleteNext/CompletePending. SendAsyncErased still posts completions when
            // CompleteInline is false, so this keeps the non-reentrant testing helper behavior.
            const std::size_t writesBefore = Private::StartedDownloadWrites().load();
            pending.Completion(pending.Error, std::move(pending.Response));
            SettleIfWritesStarted(writesBefore);
            return true;
        }

        [[nodiscard]] bool CompletePending(std::size_t index)
        {
            if (index >= m_pending.size())
            {
                return false;
            }

            PendingCompletion pending = std::move(m_pending.at(index));
            m_pending.erase(m_pending.begin() + static_cast<std::ptrdiff_t>(index));
            // Invoke inline for test helper immediacy.
            const std::size_t writesBefore = Private::StartedDownloadWrites().load();
            pending.Completion(pending.Error, std::move(pending.Response));
            SettleIfWritesStarted(writesBefore);
            return true;
        }

        [[nodiscard]] bool CompleteRequest(std::size_t requestIndex)
        {
            for (std::size_t index = 0; index < m_pending.size(); ++index)
            {
                if (m_pending.at(index).RequestIndex == requestIndex)
                {
                    return CompletePending(index);
                }
            }

            return false;
        }

        // Downloads write to their sink off the I/O thread; this waits for those writes and runs the completions
        // they post back, so a test observes the same state a synchronous sink would have produced.
        [[nodiscard]] bool FailPending(std::size_t index, std::error_code error, HttpResponse response = HttpResponse{})
        {
            if (index >= m_pending.size())
            {
                return false;
            }

            PendingCompletion pending = std::move(m_pending.at(index));
            m_pending.erase(m_pending.begin() + static_cast<std::ptrdiff_t>(index));
            // Invoke inline for test helper immediacy.
            const std::size_t writesBefore = Private::StartedDownloadWrites().load();
            pending.Completion(error, std::move(response));
            SettleIfWritesStarted(writesBefore);
            return true;
        }

        // Looks up a pending completion by its original RequestIndex (rather than its current
        // position in m_pending, which shifts as other requests complete) and fails it. Used to
        // forward a cancellation_slot signal to the specific request it was attached to.
        [[nodiscard]] bool FailRequest(std::size_t requestIndex,
            std::error_code error,
            HttpResponse response = HttpResponse{})
        {
            for (std::size_t index = 0; index < m_pending.size(); ++index)
            {
                if (m_pending.at(index).RequestIndex == requestIndex)
                {
                    return FailPending(index, error, std::move(response));
                }
            }

            return false;
        }

        [[nodiscard]] bool TimeoutPending(std::size_t index)
        {
            return FailPending(index, std::make_error_code(std::errc::timed_out));
        }

        [[nodiscard]] bool CancelPending(std::size_t index)
        {
            return FailPending(index, std::make_error_code(std::errc::operation_canceled));
        }

        void CompleteAll()
        {
            while (CompleteNext())
            {
            }
        }

        [[nodiscard]] std::size_t RequestCount() const noexcept
        {
            return m_requests.size();
        }

        [[nodiscard]] std::size_t PendingCount() const noexcept
        {
            return m_pending.size();
        }

        [[nodiscard]] bool NoRequestMade() const noexcept
        {
            return m_requests.empty();
        }

        [[nodiscard]] const std::vector<RequestRecord>& Requests() const noexcept
        {
            return m_requests;
        }

        [[nodiscard]] const RequestRecord& RequestAt(std::size_t index) const
        {
            return m_requests.at(index);
        }

        // Block IDs (percent-decoded) of every Put Block request seen so far, in send order.
        [[nodiscard]] std::vector<std::string> StagedBlockIds() const
        {
            std::vector<std::string> ids;
            for (const RequestRecord& record : m_requests)
            {
                const std::string& url = record.Request.GetUrl();
                const std::size_t pos = url.find("blockid=");
                if (pos == std::string::npos)
                {
                    continue;
                }
                std::string raw = url.substr(pos + 8U);
                raw = raw.substr(0, raw.find('&'));
                std::string decoded;
                for (std::size_t i = 0; i < raw.size(); ++i)
                {
                    if (raw.at(i) == '%' && i + 2U < raw.size())
                    {
                        decoded += static_cast<char>(std::stoi(raw.substr(i + 1U, 2U), nullptr, 16));
                        i += 2U;
                    }
                    else
                    {
                        decoded += raw.at(i);
                    }
                }
                ids.push_back(std::move(decoded));
            }
            return ids;
        }

        [[nodiscard]] const HttpRequest& LastRequest() const
        {
            assert(!m_requests.empty());
            return m_requests.back().Request;
        }

        [[nodiscard]] const HttpRequestOptions& LastRequestOptions() const
        {
            assert(!m_requests.empty());
            return m_requests.back().Options;
        }

        [[nodiscard]] static std::string FindHeaderValue(const HttpRequest& request, std::string_view headerName)
        {
            auto iequal = [](std::string_view a, std::string_view b) noexcept
            {
                if (a.size() != b.size())
                {
                    return false;
                }
                for (size_t i = 0; i < a.size(); ++i)
                {
                    auto ca = static_cast<unsigned char>(a.at(i));
                    auto cb = static_cast<unsigned char>(b.at(i));
                    if (ca >= 'A' && ca <= 'Z')
                    {
                        ca = static_cast<unsigned char>(ca - 'A' + 'a');
                    }
                    if (cb >= 'A' && cb <= 'Z')
                    {
                        cb = static_cast<unsigned char>(cb - 'A' + 'a');
                    }
                    if (ca != cb)
                    {
                        return false;
                    }
                }
                return true;
            };

            for (const auto& header : request.GetHeaders())
            {
                if (iequal(header.GetName(), headerName))
                {
                    return header.GetValue();
                }
            }

            return {};
        }

        // Materializes a request's body as a string regardless of whether it was set via SetBody()
        // (owned, GetBody() works directly) or SetBodyView() (non-owning; internal zero-copy upload
        // path -- GetBody() is empty by design, see HttpRequest.hpp). Must only be called while the
        // view's backing buffer is still alive -- i.e. before the request's completion handler has
        // run, per SetBodyView()'s lifetime contract.
        [[nodiscard]] static std::string BodyAsString(const HttpRequest& request)
        {
            if (!request.HasBodyView())
            {
                return request.GetBody();
            }

            const std::span<const std::byte> view = request.GetBodyView();
            return {reinterpret_cast<const char*>(view.data()), view.size()};
        }

        [[nodiscard]] HttpResponse& DefaultResponse() noexcept
        {
            return m_defaultResponse;
        }

        [[nodiscard]] std::error_code& DefaultError() noexcept
        {
            return m_defaultError;
        }

        [[nodiscard]] bool& DeferByDefault() noexcept
        {
            return m_deferByDefault;
        }

        // When true, completions are delivered inline (synchronous). Default false: completions are posted
        // to the internal io_context to better approximate a real transport that never invokes
        // completions on the initiating thread.
        [[nodiscard]] bool& CompleteInline() noexcept
        {
            return m_completeInline;
        }

      private:
        struct PendingCompletion
        {
            std::error_code Error;
            HttpResponse Response;
            CompletionHandler Completion;
            std::size_t RequestIndex = 0U;
        };

        [[nodiscard]] ScriptedResult DequeueResult(const RequestRecord& request)
        {
            if (m_scriptedResults.empty())
            {
                return ScriptedResult{.Error = m_defaultError,
                    .Response = m_defaultResponse,
                    .Deferred = m_deferByDefault};
            }

            for (auto it = m_scriptedResults.begin(); it != m_scriptedResults.end(); ++it)
            {
                if (!it->MatchRequest || it->MatchRequest(request))
                {
                    ScriptedResult scripted = std::move(*it);
                    m_scriptedResults.erase(it);
                    return scripted;
                }
            }

            return ScriptedResult{.Error = m_defaultError, .Response = m_defaultResponse, .Deferred = m_deferByDefault};
        }

        HttpResponse m_defaultResponse{201, {}, ""};
        std::error_code m_defaultError;
        bool m_deferByDefault = false;
        bool m_completeInline = false; // default to async (posted) completions per the IHttpClient contract (T26)

        // mutable: get_executor() is const (per IHttpClient), but io_context::get_executor() is not.
        mutable boost::asio::io_context m_context;
        std::deque<ScriptedResult> m_scriptedResults;
        std::deque<PendingCompletion> m_pending;
        std::vector<RequestRecord> m_requests;

        // Track live instances so test helpers can Poll() all clients automatically when needed.
        static inline std::vector<FakeHttpClient*> s_instances;
    };

    class ScriptedTokenCredential final : public ITokenCredential
    {
      public:
        struct Script
        {
            std::error_code Error;
            AccessToken Token;
            bool Deferred = false;
        };

        struct PendingRequest
        {
            std::vector<std::string> Scopes;
            std::error_code Error;
            AccessToken Token;
            GetTokenCompletionHandler Completion;
        };

        void EnqueueImmediate(std::error_code error, AccessToken token)
        {
            m_scripts.push_back(Script{.Error = error, .Token = std::move(token), .Deferred = false});
        }

        void EnqueueDeferred(std::error_code error, AccessToken token)
        {
            m_scripts.push_back(Script{.Error = error, .Token = std::move(token), .Deferred = true});
        }

        void GetTokenAsync(std::vector<std::string> scopes, GetTokenCompletionHandler completion) override
        {
            ++m_callCount;
            m_requestedScopes.push_back(scopes);

            Script script;
            if (!m_scripts.empty())
            {
                script = std::move(m_scripts.front());
                m_scripts.pop_front();
            }

            if (script.Deferred)
            {
                m_pending.push_back(PendingRequest{.Scopes = std::move(scopes),
                    .Error = script.Error,
                    .Token = std::move(script.Token),
                    .Completion = std::move(completion)});
                return;
            }

            completion(script.Error, std::move(script.Token));
        }

        [[nodiscard]] std::size_t PendingCount() const noexcept
        {
            return m_pending.size();
        }

        [[nodiscard]] bool CompleteNext()
        {
            return CompletePending(0U);
        }

        [[nodiscard]] bool CompletePending(std::size_t index)
        {
            if (index >= m_pending.size())
            {
                return false;
            }

            PendingRequest pending = std::move(m_pending.at(index));
            m_pending.erase(m_pending.begin() + static_cast<std::ptrdiff_t>(index));
            pending.Completion(pending.Error, std::move(pending.Token));
            return true;
        }

        [[nodiscard]] int CallCount() const noexcept
        {
            return m_callCount;
        }

        [[nodiscard]] const std::vector<std::vector<std::string>>& RequestedScopes() const noexcept
        {
            return m_requestedScopes;
        }

      private:
        int m_callCount = 0;
        std::vector<std::vector<std::string>> m_requestedScopes;
        std::deque<Script> m_scripts;
        std::deque<PendingRequest> m_pending;
    };
} // namespace AVEVA::AzureClient::Tests
