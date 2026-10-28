// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/Credentials.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/AzureClient/TokenCredential.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <algorithm>
#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/url/parse.hpp>

#include <cassert>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <expected>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

// Request-URL helpers and held-request bookkeeping shared by the test fakes (FakeHttpClient, FakeBlobServer and the
// loopback servers), so each does not carry its own copy.
namespace AVEVA::AzureClient::Tests {
// Decodes %XX escapes; a malformed or truncated escape is kept literally.
[[nodiscard]] inline std::string PercentDecode(std::string_view text) {
    std::string decoded;
    decoded.reserve(text.size());
    for (std::size_t i = 0; i < text.size(); ++i) {
        unsigned int value = 0;
        if (text[i] == '%' && i + 2 < text.size()) {
            const auto digits = text.data() + i + 1;
            const auto [end, ec] = std::from_chars(digits, digits + 2, value, 16);
            if (ec == std::errc{} && end == digits + 2) {
                decoded += static_cast<char>(value);
                i += 2;
                continue;
            }
        }
        decoded += text[i];
    }
    return decoded;
}

// The decoded path of a URL without its leading '/' and without the query ("container/blob").
[[nodiscard]] inline std::string UrlPath(const std::string& url) {
    const auto parsed = boost::urls::parse_uri_reference(url);
    if (!parsed) {
        return {};
    }
    std::string path = parsed->path();
    if (!path.empty()) {
        path.erase(0, 1);
    }
    return path;
}

// The decoded value of the first query parameter called `name`; empty if absent or valueless.
[[nodiscard]] inline std::string QueryValue(const std::string& url, std::string_view name) {
    const auto parsed = boost::urls::parse_uri_reference(url);
    if (!parsed) {
        return {};
    }
    const auto params = parsed->params();
    const auto it = params.find(name);
    return it == params.end() || !(*it).has_value ? std::string() : std::string((*it).value);
}

// Requests a fake holds until the test releases them, oldest first. An entry whose request has finished by other
// means (cancelled, timed out) stops counting as held. `Http` is anything with IsPending(requestIndex), and the
// caller serializes access (the fakes use their own mutex).
template <typename TMeta = std::monostate> class HeldRequests {
  public:
    struct Entry {
        std::size_t RequestIndex = 0;
        TMeta Meta{};
    };

    void Add(const std::size_t requestIndex, TMeta meta = {}) { m_entries.push_back({requestIndex, std::move(meta)}); }

    template <class Http> [[nodiscard]] std::size_t Count(const Http& http) {
        Prune(http);
        return m_entries.size();
    }

    // Removes and returns the entry at `position`, or nullopt if there is none.
    template <class Http> [[nodiscard]] std::optional<Entry> Take(const Http& http, const std::size_t position = 0) {
        Prune(http);
        if (position >= m_entries.size()) {
            return std::nullopt;
        }
        const auto it = m_entries.begin() + static_cast<std::ptrdiff_t>(position);
        Entry entry = std::move(*it);
        m_entries.erase(it);
        return entry;
    }

  private:
    template <class Http> void Prune(const Http& http) {
        std::erase_if(m_entries, [&](const Entry& entry) { return http.HasLeftPending(entry.RequestIndex); });
    }

    std::deque<Entry> m_entries;
};
} // namespace AVEVA::AzureClient::Tests

namespace AVEVA::AzureClient::Tests {

// A test-settable value that transport or pump threads read while the test thread may be changing it. Assignment and
// reads take a lock and reads return a copy, so a callback is never invoked under the lock and a copy stays valid.
template <class T> class Knob {
  public:
    Knob() = default;
    explicit Knob(T initial) : m_value(std::move(initial)) {}
    Knob(const Knob&) = delete;
    Knob& operator=(const Knob&) = delete;

    template <class U> Knob& operator=(U&& value) {
        const std::scoped_lock lock(m_mutex);
        m_value = std::forward<U>(value);
        return *this;
    }
    [[nodiscard]] T Get() const {
        const std::scoped_lock lock(m_mutex);
        return m_value;
    }

  private:
    mutable std::mutex m_mutex;
    T m_value{};
};
// Counts how often a download asked a FakeHttpClient for its writer executor (i.e. had no scheduler of its own).
// Downloads write on the client's own io_context, so Poll() settles them like any other completion.
struct TestDownloadWriters {
    std::atomic<std::size_t> Started{0U};
};

[[nodiscard]] inline TestDownloadWriters& DownloadWriters() {
    static TestDownloadWriters writers;
    return writers;
}

class FakeHttpClient final {
  public:
    using executor_type = boost::asio::io_context::executor_type;
    using CompletionHandler = std::move_only_function<void(std::error_code, HttpResponse)>;

    FakeHttpClient() : m_context(m_ownedContext) { RegisterInstance(); }

    // Runs on the caller's io_context, the way the production client is built; the caller then drives it.
    explicit FakeHttpClient(boost::asio::io_context& context) : m_context(context) { RegisterInstance(); }

    ~FakeHttpClient() {
        const std::scoped_lock lock(InstancesMutex());
        auto it = std::ranges::find(s_instances, this);
        if (it != s_instances.end()) {
            s_instances.erase(it);
        }
    }

    FakeHttpClient(const FakeHttpClient&) = delete;
    FakeHttpClient& operator=(const FakeHttpClient&) = delete;
    FakeHttpClient(FakeHttpClient&&) = delete;
    FakeHttpClient& operator=(FakeHttpClient&&) = delete;

    // The pipeline's retry timer. It always records the delay it was asked to wait. By default it then really
    // waits; while an InstantRetryTimers guard is alive it fires on the next poll, so a test can assert the computed
    // delay without sleeping.
    class RetryTimer {
      public:
        explicit RetryTimer(executor_type executor) : m_executor(executor), m_real(executor) {
            ConstructedCount().fetch_add(1, std::memory_order_relaxed);
        }
        ~RetryTimer() { cancel(); }
        RetryTimer(const RetryTimer&) = delete;
        RetryTimer& operator=(const RetryTimer&) = delete;

        void expires_after(std::chrono::nanoseconds delay) {
            {
                std::scoped_lock lock(DelayMutex());
                Delays().push_back(std::chrono::duration_cast<std::chrono::milliseconds>(delay));
            }
            m_real.expires_after(delay);
        }

        // Accepts any completion token, in particular as_tuple(use_awaitable); honours the awaiting coroutine's
        // cancellation slot in both modes.
        template <typename Token> auto async_wait(Token&& token) {
            return boost::asio::async_initiate<Token, void(boost::system::error_code)>(
                [this](auto handler) {
                    if (!InstantRetryFlag().load()) {
                        m_real.async_wait(std::move(handler));
                        return;
                    }
                    m_pending = std::make_shared<std::atomic<bool>>(false);
                    auto slot = boost::asio::get_associated_cancellation_slot(handler);
                    if (slot.is_connected()) {
                        slot.assign(
                            [cancelled = m_pending](boost::asio::cancellation_type_t) { cancelled->store(true); });
                    }
                    boost::asio::post(m_executor, [cancelled = m_pending, h = std::move(handler)]() mutable {
                        h(cancelled->load() ? boost::asio::error::operation_aborted : boost::system::error_code{});
                    });
                },
                token);
        }

        std::size_t cancel() {
            if (m_pending) {
                m_pending->store(true);
            }
            return m_real.cancel();
        }

      private:
        executor_type m_executor;
        boost::asio::steady_timer m_real;
        std::shared_ptr<std::atomic<bool>> m_pending;
    };
    using retry_timer_type = RetryTimer;

    // Makes retry timers fire immediately for as long as it lives.
    class InstantRetryTimers {
      public:
        InstantRetryTimers() { InstantRetryFlag().store(true); }
        ~InstantRetryTimers() { InstantRetryFlag().store(false); }
        InstantRetryTimers(const InstantRetryTimers&) = delete;
        InstantRetryTimers& operator=(const InstantRetryTimers&) = delete;
    };

    // Every retry delay requested by any pipeline, oldest first.
    static std::vector<std::chrono::milliseconds> RetryDelays() {
        std::scoped_lock lock(DelayMutex());
        return Delays();
    }
    static void ClearRetryDelays() {
        std::scoped_lock lock(DelayMutex());
        Delays().clear();
    }

    // Number of retry timers ever constructed by any pipeline; tests compare before/after deltas.
    static std::size_t RetryTimersConstructed() { return ConstructedCount().load(std::memory_order_relaxed); }

    // Clears the process-global knobs so one test cannot leak retry behaviour into the next (shuffle, repeat).
    static void ResetGlobalState() {
        InstantRetryFlag().store(false);
        ClearRetryDelays();
    }

    // Return a snapshot of live instances. Tests may call this to Poll all clients.
    static std::vector<FakeHttpClient*> Instances() {
        const std::scoped_lock lock(InstancesMutex());
        return s_instances;
    }

    struct RequestRecord {
        HttpRequest Request;
        HttpRequestOptions Options;

        // Body content captured at request-send time (see SendAsyncErased), so that it remains
        // valid for inspection even after Request's completion has run and, for the internal
        // zero-copy upload path (SetBodyView), the backing buffer may have been freed.
        std::string Body;

        // Position in Requests(), which is also the index CompleteRequest()/FailRequest() take.
        std::size_t Index = 0U;
    };

    struct ScriptedResult {
        std::error_code Error;
        HttpResponse Response{200, {}, ""};
        bool Deferred = false;
        std::function<bool(const RequestRecord&)> MatchRequest;
        // Runs just before a successful completion is delivered, so a deferred request applies its effect (e.g. a
        // page upload's bytes) only when the test releases it. It may amend the response (e.g. with the new ETag).
        // Not run when the request is failed instead.
        std::function<void(HttpResponse&)> OnComplete;
    };

    // Decides the result of a request before any scripted result is consulted; nullopt falls through to the
    // scripts. Called outside the client's lock, possibly from several threads at once.
    using Responder = std::function<std::optional<ScriptedResult>(const RequestRecord&)>;

    // Returns only once no thread is still inside the previous responder, so what it captured can be destroyed.
    // Must not be called from inside a responder.
    void SetResponder(Responder responder) {
        std::unique_lock lock(m_mutex);
        m_responder = std::move(responder);
        m_responderIdle.wait(lock, [&] { return m_respondersInFlight == 0U; });
    }

    // Backed by a real io_context (rather than boost::asio::system_executor) so that tests can
    // deterministically observe completions posted onto this executor -- e.g. by
    // Private::PostCompletion for synchronous early-exit validation failures (Task 8) -- via
    // Poll()/Run() instead of racing a background thread pool.
    executor_type get_executor() const noexcept { return m_context.get_executor(); }

    // Downloads write on this client's own io_context, so Poll() runs the writes together with every other
    // completion. The counter lets a test see whether a download fell back to the client's writer.
    [[nodiscard]] executor_type GetDownloadWriter() const noexcept {
        DownloadWriters().Started.fetch_add(1U);
        return m_context.get_executor();
    }

    // Records the request and completes it per the scripts. The awaiting coroutine's cancellation slot reaches a
    // deferred request, which then fails with operation_canceled, like a real transport aborting an in-flight request.
    boost::asio::awaitable<HttpResult> SendAsync(HttpRequest request, HttpRequestOptions options) {
        if (auto failure = TakeInitiationFailure()) {
            std::rethrow_exception(failure);
        }
        auto token = boost::asio::as_tuple(boost::asio::use_awaitable);
        boost::asio::cancellation_slot slot;
        auto [error, response] =
            co_await boost::asio::async_initiate<decltype(token), void(std::error_code, HttpResponse)>(
                [this, &slot](auto handler, HttpRequest req, HttpRequestOptions opts) {
                    slot = boost::asio::get_associated_cancellation_slot(handler);
                    SendAsyncErased(std::move(req), CompletionHandler{std::move(handler)}, std::move(opts), slot);
                },
                token, std::move(request), std::move(options));
        // The fake's handler must not outlive the request it belongs to.
        if (slot.is_connected()) {
            slot.clear();
        }
        if (error) {
            co_return std::unexpected(error);
        }
        co_return std::move(response);
    }

    // Makes the next SendAsync throw before any request is recorded (as an allocation failure would). Only that one
    // call is affected.
    void FailNextInitiation(std::exception_ptr failure) {
        std::scoped_lock lock(m_mutex);
        m_initiationFailure = std::move(failure);
    }

    // Runs ready handlers on this client's executor (coroutines, posted completions, instant retry timers) without
    // blocking; returns the number run.
    std::size_t Poll() { return PollRaw(); }

    // Runs `task` on the client's executor, the way a real transport completes: coroutines resumed from it then
    // continue on their own executor instead of the bare test thread. Inline when already on the executor (a handler
    // completing another request).
    template <class F> void RunOnExecutor(F&& task) {
        if (m_context.get_executor().running_in_this_thread()) {
            task();
            return;
        }
        boost::asio::post(m_context, [&task] { task(); });
        PollRaw();
    }

    std::size_t PollRaw() {
        std::scoped_lock lock(m_pollMutex);
        // poll() leaves the context stopped once it runs out of work; restart so repeated
        // Poll() calls keep observing newly posted handlers.
        m_context.restart();
        return m_context.poll();
    }

    // Runs handlers (including timers such as retry backoff) until `done()` holds, blocking on the
    // io_context rather than spinning. Each wait is capped so the poll lock is released and `done()` is
    // re-evaluated even when it turns true from another thread without a handler running here. `timeout` is only a
    // deadlock watchdog; returns false if it expires.
    [[nodiscard]] bool RunUntil(const std::function<bool()>& done,
                                std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        constexpr std::chrono::milliseconds maxWait{100};
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!done()) {
            const auto remaining = deadline - std::chrono::steady_clock::now();
            if (remaining <= std::chrono::steady_clock::duration::zero()) {
                return false;
            }
            std::scoped_lock lock(m_pollMutex);
            m_context.restart();
            m_context.run_one_for(std::min<std::chrono::steady_clock::duration>(remaining, maxWait));
        }
        return true;
    }

    [[nodiscard]] bool WaitForRequest(std::size_t count, std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        return RunUntil([&] { return RequestCount() >= count; }, timeout);
    }

    // Blocks on a condition variable (no polling) until `count` requests were sent, for tests where another thread
    // issues them. `timeout` is only a deadlock guard.
    [[nodiscard]] bool WaitForRequestCount(std::size_t count,
                                           std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        std::unique_lock lock(m_mutex);
        return m_requestCv.wait_for(lock, timeout, [&] { return m_requests.size() >= count; });
    }

    // `cancellation` is the awaiting coroutine's slot; it fails a deferred request when it fires.
    void SendAsyncErased(HttpRequest request, CompletionHandler completion, HttpRequestOptions options,
                         boost::asio::cancellation_slot cancellation = {}) {
        // Snapshot the body now, while any SetBodyView() backing buffer is still guaranteed
        // alive (i.e. before completion runs) -- mirrors how a real transport consumes the
        // bytes up front rather than retaining a live reference across the completion.
        std::string body = BodyAsString(request);

        // The retained record must not pin a zero-copy payload's owner for the rest of the test: a real transport
        // releases the body once the request finishes, and callers that pool their buffers rely on that to reuse them.
        if (request.HasBodyView() && request.GetBodyKeepAlive()) {
            std::shared_ptr<const void> keepAlive = request.GetBodyKeepAlive();
            request.SetBodyView(request.GetBodyView());
            completion = [inner = std::move(completion),
                          keepAlive = std::move(keepAlive)](std::error_code error, HttpResponse response) mutable {
                keepAlive.reset();
                inner(error, std::move(response));
            };
        }
        const RequestRecord* record = nullptr;
        std::size_t requestIndex = 0U;
        Responder responder;
        {
            std::scoped_lock lock(m_mutex);
            requestIndex = m_requests.size();
            m_requests.push_back(RequestRecord{
                .Request = std::move(request), .Options = options, .Body = std::move(body), .Index = requestIndex});
            // A deque never relocates its elements on push_back, so this stays valid for later requests.
            record = &m_requests.back();
            responder = m_responder;
            if (responder) {
                ++m_respondersInFlight;
            }
        }
        m_requestCv.notify_all();

        std::optional<ScriptedResult> custom;
        if (responder) {
            const ResponderCall call(*this);
            custom = responder(*record);
        }
        ScriptedResult scripted = custom.has_value() ? std::move(*custom) : DequeueResult(*record);
        if (scripted.Deferred) {
            {
                std::scoped_lock lock(m_mutex);
                m_pending.push_back(PendingCompletion{.Error = scripted.Error,
                                                      .Response = std::move(scripted.Response),
                                                      .Completion = std::move(completion),
                                                      .RequestIndex = requestIndex,
                                                      .OnComplete = std::move(scripted.OnComplete)});
                m_registeredDeferred.insert(requestIndex);
            }

            // Honour the HttpClient cancellation contract: when the awaiting coroutine is cancelled, fail the matching
            // pending request, mirroring how a real transport would abort an in-flight request.
            if (cancellation.is_connected()) {
                cancellation.assign([this, requestIndex](boost::asio::cancellation_type_t /*type*/) {
                    (void)this->FailRequest(requestIndex, std::make_error_code(std::errc::operation_canceled));
                });
            }
            return;
        }

        // Completions are always posted to the client's io_context: resuming the awaiting coroutine from inside its
        // own initiation would not be a legal asynchronous completion.
        auto exec = m_context.get_executor();
        boost::asio::post(exec, [completion = std::move(completion), error = scripted.Error,
                                 response = std::move(scripted.Response),
                                 onComplete = std::move(scripted.OnComplete)]() mutable {
            if (onComplete) {
                onComplete(response);
            }
            completion(error, std::move(response));
        });
    }

    void EnqueueResponse(HttpResponse response, std::error_code error = {}, bool deferred = false) {
        EnqueueResponse(std::move(response), error, deferred, {});
    }

    void EnqueueResponse(HttpResponse response, std::error_code error, bool deferred,
                         std::function<bool(const RequestRecord&)> matchRequest) {
        std::scoped_lock lock(m_mutex);
        m_scriptedResults.push_back(ScriptedResult{.Error = error,
                                                   .Response = std::move(response),
                                                   .Deferred = deferred,
                                                   .MatchRequest = std::move(matchRequest)});
    }

    void EnqueueDeferredResponse(HttpResponse response, std::error_code error = {}) {
        EnqueueResponse(std::move(response), error, true);
    }

    [[nodiscard]] bool CompleteNext() { return CompletePending(0U); }

    [[nodiscard]] bool CompletePending(std::size_t index) { return CompleteTaken(TakePending(index)); }

    // Templated only because PendingCompletion is declared further down.
    template <class TPending> [[nodiscard]] bool CompleteTaken(TPending pending) {
        if (!pending.has_value()) {
            return false;
        }

        // Completes on the client's executor right away, so a test that calls CompleteNext/CompletePending sees the
        // awaiting coroutine run up to its next suspension without a separate Poll().
        RunOnExecutor([&pending] {
            if (pending->OnComplete) {
                pending->OnComplete(pending->Response);
            }
            pending->Completion(pending->Error, std::move(pending->Response));
        });
        return true;
    }

    // True while the request is deferred and neither completed, failed nor cancelled.
    [[nodiscard]] bool IsPending(std::size_t requestIndex) const { return FindPending(requestIndex).has_value(); }

    // True once a deferred request was completed, failed or cancelled. A responder that holds a request records it
    // before SendAsyncErased registers it as pending, so "not pending yet" must not be mistaken for "not pending
    // any more": a hold list pruned in that window would lose the request for good.
    [[nodiscard]] bool HasLeftPending(std::size_t requestIndex) const {
        std::scoped_lock lock(m_mutex);
        return m_registeredDeferred.contains(requestIndex) && !FindPendingLocked(requestIndex).has_value();
    }

    [[nodiscard]] bool CompleteRequest(std::size_t requestIndex) {
        return CompleteTaken(TakePendingRequest(requestIndex));
    }

    [[nodiscard]] bool FailPending(std::size_t index, std::error_code error, HttpResponse response = HttpResponse{}) {
        return FailTaken(TakePending(index), error, std::move(response));
    }

    template <class TPending>
    [[nodiscard]] bool FailTaken(TPending pending, std::error_code error, HttpResponse response) {
        if (!pending.has_value()) {
            return false;
        }

        RunOnExecutor([&] { pending->Completion(error, std::move(response)); });
        return true;
    }

    // Looks up a pending completion by its original RequestIndex (rather than its current
    // position in m_pending, which shifts as other requests complete) and fails it. Used to
    // forward a cancellation_slot signal to the specific request it was attached to.
    [[nodiscard]] bool FailRequest(std::size_t requestIndex, std::error_code error,
                                   HttpResponse response = HttpResponse{}) {
        return FailTaken(TakePendingRequest(requestIndex), error, std::move(response));
    }

    [[nodiscard]] bool TimeoutPending(std::size_t index) {
        return FailPending(index, std::make_error_code(std::errc::timed_out));
    }

    [[nodiscard]] bool CancelPending(std::size_t index) {
        return FailPending(index, std::make_error_code(std::errc::operation_canceled));
    }

    void CompleteAll() {
        while (CompleteNext()) {
        }
    }

    [[nodiscard]] std::size_t RequestCount() const {
        std::scoped_lock lock(m_mutex);
        return m_requests.size();
    }

    [[nodiscard]] std::size_t PendingCount() const {
        std::scoped_lock lock(m_mutex);
        return m_pending.size();
    }

    [[nodiscard]] bool NoRequestMade() const { return RequestCount() == 0U; }

    // A copy of the records taken under the lock, so it can be inspected while other threads keep sending.
    [[nodiscard]] std::deque<RequestRecord> Requests() const {
        std::scoped_lock lock(m_mutex);
        return m_requests;
    }

    [[nodiscard]] const RequestRecord& RequestAt(std::size_t index) const {
        std::scoped_lock lock(m_mutex);
        return m_requests.at(index);
    }

    [[nodiscard]] const HttpRequest& LastRequest() const {
        std::scoped_lock lock(m_mutex);
        assert(!m_requests.empty());
        return m_requests.back().Request;
    }

    [[nodiscard]] const HttpRequestOptions& LastRequestOptions() const {
        std::scoped_lock lock(m_mutex);
        assert(!m_requests.empty());
        return m_requests.back().Options;
    }

    [[nodiscard]] static std::string FindHeaderValue(const HttpRequest& request, std::string_view headerName) {
        auto iequal = [](std::string_view a, std::string_view b) noexcept {
            if (a.size() != b.size()) {
                return false;
            }
            for (size_t i = 0; i < a.size(); ++i) {
                auto ca = static_cast<unsigned char>(a.at(i));
                auto cb = static_cast<unsigned char>(b.at(i));
                if (ca >= 'A' && ca <= 'Z') {
                    ca = static_cast<unsigned char>(ca - 'A' + 'a');
                }
                if (cb >= 'A' && cb <= 'Z') {
                    cb = static_cast<unsigned char>(cb - 'A' + 'a');
                }
                if (ca != cb) {
                    return false;
                }
            }
            return true;
        };

        for (const auto& header : request.GetHeaders()) {
            if (iequal(header.GetName(), headerName)) {
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
    [[nodiscard]] static std::string BodyAsString(const HttpRequest& request) {
        if (!request.HasBodyView()) {
            return request.GetBody();
        }

        const std::span<const std::byte> view = request.GetBodyView();
        return {reinterpret_cast<const char*>(view.data()), view.size()};
    }

    // These are assigned (`client.DeferByDefault() = true;`) by the test while the pump thread may be consulting them.
    [[nodiscard]] Knob<HttpResponse>& DefaultResponse() noexcept { return m_defaultResponse; }

    [[nodiscard]] Knob<std::error_code>& DefaultError() noexcept { return m_defaultError; }

    [[nodiscard]] Knob<bool>& DeferByDefault() noexcept { return m_deferByDefault; }

  private:
    struct PendingCompletion {
        std::error_code Error;
        HttpResponse Response;
        CompletionHandler Completion;
        std::size_t RequestIndex = 0U;
        std::function<void(HttpResponse&)> OnComplete;
    };

    // Removes a pending completion under the lock; the caller invokes it outside the lock so that a completion
    // may itself send requests.
    [[nodiscard]] std::optional<PendingCompletion> TakePending(std::size_t index) {
        std::scoped_lock lock(m_mutex);
        if (index >= m_pending.size()) {
            return std::nullopt;
        }

        PendingCompletion pending = std::move(m_pending.at(index));
        m_pending.erase(m_pending.begin() + static_cast<std::ptrdiff_t>(index));
        return pending;
    }

    // Finds and removes in one critical section: looking up the position first and taking it later lets another
    // thread shift the queue in between, so the wrong request would be completed.
    [[nodiscard]] std::optional<PendingCompletion> TakePendingRequest(std::size_t requestIndex) {
        std::scoped_lock lock(m_mutex);
        const auto it = std::ranges::find(m_pending, requestIndex, &PendingCompletion::RequestIndex);
        if (it == m_pending.end()) {
            return std::nullopt;
        }
        PendingCompletion pending = std::move(*it);
        m_pending.erase(it);
        return pending;
    }

    [[nodiscard]] std::optional<std::size_t> FindPending(std::size_t requestIndex) const {
        std::scoped_lock lock(m_mutex);
        return FindPendingLocked(requestIndex);
    }

    [[nodiscard]] std::optional<std::size_t> FindPendingLocked(std::size_t requestIndex) const {
        for (std::size_t index = 0; index < m_pending.size(); ++index) {
            if (m_pending.at(index).RequestIndex == requestIndex) {
                return index;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] ScriptedResult DefaultResult() const {
        return ScriptedResult{
            .Error = m_defaultError.Get(), .Response = m_defaultResponse.Get(), .Deferred = m_deferByDefault.Get()};
    }

    // Counts a responder call in progress so SetResponder() can wait for it.
    class ResponderCall {
      public:
        explicit ResponderCall(FakeHttpClient& owner) : m_owner(owner) {}
        ResponderCall(const ResponderCall&) = delete;
        ResponderCall& operator=(const ResponderCall&) = delete;
        ~ResponderCall() {
            const std::scoped_lock lock(m_owner.m_mutex);
            if (--m_owner.m_respondersInFlight == 0U) {
                m_owner.m_responderIdle.notify_all();
            }
        }

      private:
        FakeHttpClient& m_owner;
    };

    [[nodiscard]] ScriptedResult DequeueResult(const RequestRecord& request) {
        std::scoped_lock lock(m_mutex);
        if (m_scriptedResults.empty()) {
            return DefaultResult();
        }

        for (auto it = m_scriptedResults.begin(); it != m_scriptedResults.end(); ++it) {
            if (!it->MatchRequest || it->MatchRequest(request)) {
                ScriptedResult scripted = std::move(*it);
                m_scriptedResults.erase(it);
                return scripted;
            }
        }

        return DefaultResult();
    }

    Knob<HttpResponse> m_defaultResponse{HttpResponse{201, {}, ""}};
    Knob<std::error_code> m_defaultError;
    Knob<bool> m_deferByDefault;

    // Backs m_context unless the client is built on a caller-owned io_context.
    boost::asio::io_context m_ownedContext;
    boost::asio::io_context& m_context;
    // Guards the containers below; never held while a completion or responder runs.
    mutable std::mutex m_mutex;
    std::condition_variable m_requestCv;
    std::condition_variable m_responderIdle;
    std::size_t m_respondersInFlight = 0U;
    std::recursive_mutex m_pollMutex;
    Responder m_responder;
    std::exception_ptr m_initiationFailure;

    std::exception_ptr TakeInitiationFailure() {
        std::scoped_lock lock(m_mutex);
        return std::exchange(m_initiationFailure, nullptr);
    }
    std::deque<ScriptedResult> m_scriptedResults;
    std::deque<PendingCompletion> m_pending;
    std::deque<RequestRecord> m_requests;
    std::set<std::size_t> m_registeredDeferred;

    // Track live instances so test helpers can Poll() all clients automatically when needed.
    static inline std::vector<FakeHttpClient*> s_instances;

    static std::mutex& InstancesMutex() {
        static std::mutex mutex;
        return mutex;
    }

    void RegisterInstance() {
        const std::scoped_lock lock(InstancesMutex());
        s_instances.push_back(this);
    }
    static std::mutex& DelayMutex() {
        static std::mutex mutex;
        return mutex;
    }
    static std::vector<std::chrono::milliseconds>& Delays() {
        static std::vector<std::chrono::milliseconds> delays;
        return delays;
    }
    static std::atomic<std::size_t>& ConstructedCount() {
        static std::atomic<std::size_t> count{0};
        return count;
    }
    static std::atomic<bool>& InstantRetryFlag() {
        static std::atomic<bool> flag{false};
        return flag;
    }
};

// The clients under test run on the fake transport; these shadow the HttpClient-bound aliases of the library.
// Definitions are explicitly instantiated in FakeInstantiations.cpp.
using BlobClient = BasicBlobClient<FakeHttpClient>;
using PageBlobClient = BasicPageBlobClient<FakeHttpClient>;
using BlobContainerClient = BasicBlobContainerClient<FakeHttpClient>;
using BlobServiceClient = BasicBlobServiceClient<FakeHttpClient>;
using ClientSecretCredential = BasicClientSecretCredential<FakeHttpClient>;
using WorkloadIdentityCredential = BasicWorkloadIdentityCredential<FakeHttpClient>;
using ClientAssertionCredential = BasicClientAssertionCredential<FakeHttpClient>;
using ManagedIdentityCredential = BasicManagedIdentityCredential<FakeHttpClient>;
using AzurePipelinesCredential = BasicAzurePipelinesCredential<FakeHttpClient>;

// Returns a fixed token. The requested `scopes` are ignored.
class StaticTokenCredential {
  public:
    explicit StaticTokenCredential(std::string token, std::chrono::system_clock::time_point expiresOn =
                                                          std::chrono::system_clock::time_point::max())
        : m_token(std::move(token)), m_expiresOn(expiresOn) {}

    boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> /*scopes*/) {
        co_return AccessToken{.Token = m_token, .ExpiresOn = m_expiresOn};
    }

  private:
    std::string m_token;
    std::chrono::system_clock::time_point m_expiresOn;
};
class ScriptedTokenCredential final {
  public:
    // Matches the completion the deferred scripts are finished with.
    using TokenSignature = void(std::error_code, AccessToken);

    struct Script {
        std::error_code Error;
        AccessToken Token;
        bool Deferred = false;
        bool Throws = false;
    };

    struct PendingRequest {
        std::vector<std::string> Scopes;
        std::error_code Error;
        AccessToken Token;
        std::move_only_function<TokenSignature> Completion;
        std::size_t Id = 0U;
    };

    void EnqueueImmediate(std::error_code error, AccessToken token) {
        m_scripts.push_back(Script{.Error = error, .Token = std::move(token), .Deferred = false});
    }

    // The next token request throws from inside GetTokenAsync, as an allocation failure would.
    void EnqueueThrow() { m_scripts.push_back(Script{.Throws = true}); }

    // The next token request suspends until CompleteNext()/CompletePending() (or the awaiting coroutine is
    // cancelled, which completes it with operation_canceled). The awaiting coroutine resumes on its own executor, so
    // poll that executor after completing it.
    void EnqueueDeferred(std::error_code error, AccessToken token) {
        m_scripts.push_back(Script{.Error = error, .Token = std::move(token), .Deferred = true});
    }

    boost::asio::awaitable<TokenResult> GetTokenAsync(std::vector<std::string> scopes) {
        ++m_callCount;
        m_requestedScopes.push_back(scopes);

        Script script;
        if (!m_scripts.empty()) {
            script = std::move(m_scripts.front());
            m_scripts.pop_front();
        }

        if (script.Throws) {
            throw std::bad_alloc{};
        }

        if (!script.Deferred) {
            if (script.Error) {
                co_return std::unexpected(script.Error);
            }
            co_return std::move(script.Token);
        }

        auto token = boost::asio::as_tuple(boost::asio::use_awaitable);
        boost::asio::cancellation_slot slot;
        auto [error, accessToken] = co_await boost::asio::async_initiate<decltype(token), TokenSignature>(
            [this, &slot](auto handler, std::vector<std::string> scopeList, Script deferred) {
                slot = boost::asio::get_associated_cancellation_slot(handler);
                const std::size_t id = ++m_nextId;
                // Resuming an awaitable handler runs the coroutine in place, so hop to its executor first.
                auto executor = boost::asio::get_associated_executor(handler);
                m_pending.push_back(PendingRequest{
                    .Scopes = std::move(scopeList),
                    .Error = deferred.Error,
                    .Token = std::move(deferred.Token),
                    .Completion =
                        [executor, handler = std::move(handler)](std::error_code ec, AccessToken tok) mutable {
                            boost::asio::post(executor,
                                              [handler = std::move(handler), ec, tok = std::move(tok)]() mutable {
                                                  handler(ec, std::move(tok));
                                              });
                        },
                    .Id = id});
                if (slot.is_connected()) {
                    slot.assign([this, id](boost::asio::cancellation_type_t) { CancelRequest(id); });
                }
            },
            token, std::move(scopes), std::move(script));
        if (slot.is_connected()) {
            slot.clear();
        }
        if (error) {
            co_return std::unexpected(error);
        }
        co_return std::move(accessToken);
    }

    [[nodiscard]] std::size_t PendingCount() const noexcept { return m_pending.size(); }

    [[nodiscard]] bool CompleteNext() { return CompletePending(0U); }

    [[nodiscard]] bool CompletePending(std::size_t index) {
        if (index >= m_pending.size()) {
            return false;
        }

        PendingRequest pending = std::move(m_pending.at(index));
        m_pending.erase(m_pending.begin() + static_cast<std::ptrdiff_t>(index));
        pending.Completion(pending.Error, std::move(pending.Token));
        return true;
    }

    [[nodiscard]] int CallCount() const noexcept { return m_callCount; }

    [[nodiscard]] const std::vector<std::vector<std::string>>& RequestedScopes() const noexcept {
        return m_requestedScopes;
    }

  private:
    void CancelRequest(std::size_t id) {
        const auto it = std::ranges::find(m_pending, id, &PendingRequest::Id);
        if (it == m_pending.end()) {
            return;
        }
        PendingRequest pending = std::move(*it);
        m_pending.erase(it);
        pending.Completion(std::make_error_code(std::errc::operation_canceled), AccessToken{});
    }

    std::size_t m_nextId = 0U;
    int m_callCount = 0;
    std::vector<std::vector<std::string>> m_requestedScopes;
    std::deque<Script> m_scripts;
    std::deque<PendingRequest> m_pending;
};

namespace Detail {
class FakeHttpClientResetListener : public ::testing::EmptyTestEventListener {
    void OnTestStart(const ::testing::TestInfo&) override { FakeHttpClient::ResetGlobalState(); }
};

// One registration per test binary: the inline variable is initialised once however many files include the header.
inline const bool FakeHttpClientResetRegistered = [] {
    ::testing::UnitTest::GetInstance()->listeners().Append(new FakeHttpClientResetListener);
    return true;
}();
} // namespace Detail
} // namespace AVEVA::AzureClient::Tests
