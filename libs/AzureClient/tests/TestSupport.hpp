// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

// Single entry point for the AzureClient test helpers: client option fixtures, request/response assertions and the
// parallel-download scripting helpers. FakeHttpClient.hpp (which also carries the request-URL helpers) is included
// from here so tests need only this header.

#include "FakeHttpClient.hpp"

#include <AVEVA/AzureClient/BlobClientOptions.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/Detail/BlobRequestHelpers.hpp>
#include <AVEVA/HttpClient/HttpHeader.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <gtest/gtest.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <optional>
#include <random>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace AVEVA::AzureClient::Tests {
// Polls an asynchronous condition. The timeout is generous because a healthy run needs a fraction of it; it only
// bounds how long a failing run takes to report.
template <typename Predicate>
[[nodiscard]] bool WaitUntil(Predicate&& done, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    const auto limit = std::chrono::steady_clock::now() + timeout;
    while (!done()) {
        if (std::chrono::steady_clock::now() >= limit) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

// ---- Client option fixtures ----

inline constexpr std::string_view DefaultServiceEndpoint = "https://storageaccount.blob.core.windows.net/";

// Lets a test that serves requests from a loopback server redirect every fixture-built client to it.
inline std::string& ServiceEndpointOverride() {
    static std::string endpoint;
    return endpoint;
}

[[nodiscard]] inline std::string ResolveServiceEndpoint() {
    const std::string& configured = ServiceEndpointOverride();
    return configured.empty() ? std::string(DefaultServiceEndpoint) : configured;
}
inline constexpr std::string_view DefaultSasToken = "?sv=2025-01-05&sig=fakesig";
inline constexpr std::string_view DefaultApiVersion = "2023-11-03";
inline constexpr std::string_view DefaultLastModified = "Fri, 26 Jun 2015 18:59:17 GMT";
inline constexpr std::string_view DefaultETag = "\"0x8D1234\"";

[[nodiscard]] inline BlobClientOptions MakeBlobClientOptions(std::string containerName = "images",
                                                             std::string blobName = "photo.png") {
    BlobClientOptions options;
    options.ServiceEndpoint = ResolveServiceEndpoint();
    options.ContainerName = std::move(containerName);
    options.BlobName = std::move(blobName);
    options.SasToken = std::string(DefaultSasToken);
    options.ApiVersion = std::string(DefaultApiVersion);
    // Scripted single-response tests must not consume extra responses through automatic
    // retries; retry behavior is covered explicitly by RetryTests.
    options.Retry.MaxRetries = 0;
    return options;
}

[[nodiscard]] inline BlobContainerClientOptions MakeBlobContainerClientOptions(std::string containerName = "images") {
    BlobContainerClientOptions options;
    options.ServiceEndpoint = ResolveServiceEndpoint();
    options.ContainerName = std::move(containerName);
    options.SasToken = std::string(DefaultSasToken);
    options.ApiVersion = std::string(DefaultApiVersion);
    options.Retry.MaxRetries = 0;
    return options;
}

[[nodiscard]] inline BlobServiceClientOptions MakeBlobServiceClientOptions() {
    BlobServiceClientOptions options;
    options.ServiceEndpoint = ResolveServiceEndpoint();
    options.SasToken = std::string(DefaultSasToken);
    options.ApiVersion = std::string(DefaultApiVersion);
    options.Retry.MaxRetries = 0;
    return options;
}

[[nodiscard]] inline std::vector<HttpHeader>
MakeCanonicalSuccessHeaders(std::initializer_list<std::pair<std::string_view, std::string_view>> extraHeaders = {}) {
    std::vector<HttpHeader> headers{
        HttpHeader{"ETag", std::string(DefaultETag)},
        HttpHeader{"Last-Modified", std::string(DefaultLastModified)},
        HttpHeader{"x-ms-request-id", "request-123"},
    };
    for (const auto& [name, value] : extraHeaders) {
        bool replaced = false;
        for (auto& header : headers) {
            if (header.GetName() == name) {
                header = HttpHeader{std::string(name), std::string(value)};
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            headers.emplace_back(std::string(name), std::string(value));
        }
    }
    return headers;
}

[[nodiscard]] inline HttpResponse MakeAzureErrorResponse(unsigned int statusCode, std::string_view errorCode,
                                                         std::string_view message, std::string_view requestId) {
    std::vector<HttpHeader> headers;
    if (!errorCode.empty()) {
        headers.emplace_back("x-ms-error-code", std::string(errorCode));
    }
    if (!requestId.empty()) {
        headers.emplace_back("x-ms-request-id", std::string(requestId));
    }

    const std::string body =
        message.empty() ? std::string{} : "<Error><Message>" + std::string(message) + "</Message></Error>";
    return HttpResponse{statusCode, std::move(headers), body};
}

// ---- Running coroutines ----
//
// Client operations are awaitables, so a test starts one on an executor and drives that executor itself. A started
// operation only runs while its io_context is polled: Start*/Spawn poll once, so it has run up to its first
// suspension (e.g. a deferred request is pending) when they return. Completions are driven by FakeHttpClient::Poll()
// or by RunAwaitable.

namespace Detail {
template <class T> struct OperationState {
    std::optional<T> Value;
    std::exception_ptr Error;
    bool Done = false;
    boost::asio::cancellation_signal Signal;
};
template <> struct OperationState<void> {
    std::exception_ptr Error;
    bool Done = false;
    boost::asio::cancellation_signal Signal;
};
} // namespace Detail

// An awaitable started with StartAwaitable. Copies share the state, so it may be captured freely.
template <class T> class Operation {
  public:
    explicit Operation(std::shared_ptr<Detail::OperationState<T>> state) : m_state(std::move(state)) {}

    [[nodiscard]] bool Done() const noexcept { return m_state->Done; }

    // The value the awaitable returned. Throws what the coroutine threw, or std::logic_error while it is still
    // running (so gtest reports a failure instead of reading an empty optional).
    std::conditional_t<std::is_void_v<T>, void, T&> Result() const {
        Rethrow();
        if constexpr (!std::is_void_v<T>) {
            return *m_state->Value;
        }
    }

    // Emits cancellation on the coroutine's cancellation slot, as the owner of the awaiting coroutine would.
    void Cancel(boost::asio::cancellation_type type = boost::asio::cancellation_type::terminal) {
        m_state->Signal.emit(type);
    }

    [[nodiscard]] boost::asio::cancellation_signal& Signal() const noexcept { return m_state->Signal; }

  private:
    void Rethrow() const {
        if (!m_state->Done) {
            throw std::logic_error{"the awaitable has not completed (is a request still pending?)"};
        }
        if (m_state->Error) {
            std::rethrow_exception(m_state->Error);
        }
    }

    std::shared_ptr<Detail::OperationState<T>> m_state;
};

// Starts `awaitable` on `executor` with a cancellation slot bound to the returned Operation. Does not poll.
template <class T, class Executor>
[[nodiscard]] Operation<T> StartAwaitableOn(const Executor& executor, boost::asio::awaitable<T> awaitable) {
    auto state = std::make_shared<Detail::OperationState<T>>();
    boost::asio::co_spawn(
        executor, std::move(awaitable),
        boost::asio::bind_cancellation_slot(state->Signal.slot(), [state](std::exception_ptr error, auto&&... value) {
            state->Error = std::move(error);
            if constexpr (sizeof...(value) == 1U) {
                state->Value.emplace(std::forward<decltype(value)>(value)...);
            }
            state->Done = true;
        }));
    return Operation<T>{std::move(state)};
}

// Starts `awaitable` on the client's executor and polls it once.
template <class T>
[[nodiscard]] Operation<T> StartAwaitable(FakeHttpClient& httpClient, boost::asio::awaitable<T> awaitable) {
    auto operation = StartAwaitableOn(httpClient.get_executor(), std::move(awaitable));
    httpClient.Poll();
    return operation;
}

// Starts `awaitable` on `context` and polls it once.
template <class T>
[[nodiscard]] Operation<T> StartAwaitable(boost::asio::io_context& context, boost::asio::awaitable<T> awaitable) {
    auto operation = StartAwaitableOn(context.get_executor(), std::move(awaitable));
    context.restart();
    context.poll();
    return operation;
}

// Runs `awaitable` on the client's executor until it completes (real retry timers included) and returns its value.
// Throws instead of hanging when it does not complete within `timeout`, e.g. because a deferred request is never
// completed: use StartAwaitable for those.
template <class T>
T RunAwaitable(FakeHttpClient& httpClient, boost::asio::awaitable<T> awaitable,
               std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    auto operation = StartAwaitableOn(httpClient.get_executor(), std::move(awaitable));
    if (!httpClient.RunUntil([&] { return operation.Done(); }, timeout)) {
        throw std::runtime_error("the awaitable did not complete in time");
    }
    if constexpr (std::is_void_v<T>) {
        operation.Result();
    } else {
        return std::move(operation.Result());
    }
}

template <class T>
T RunAwaitable(boost::asio::io_context& context, boost::asio::awaitable<T> awaitable,
               std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    auto operation = StartAwaitableOn(context.get_executor(), std::move(awaitable));
    const auto limit = std::chrono::steady_clock::now() + timeout;
    while (!operation.Done()) {
        if (std::chrono::steady_clock::now() >= limit) {
            throw std::runtime_error("the awaitable did not complete in time");
        }
        context.restart();
        context.run_one_for(std::chrono::milliseconds(50));
    }
    if constexpr (std::is_void_v<T>) {
        operation.Result();
    } else {
        return std::move(operation.Result());
    }
}

// Starts `awaitable` and passes its value to `handler(T)` (`handler()` for void) when it completes; the handler
// never runs inside this call. An exception thrown by the coroutine is rethrown from the polling thread.
template <class T, class Executor, class Handler>
void SpawnOn(const Executor& executor, boost::asio::awaitable<T> awaitable, Handler handler) {
    boost::asio::co_spawn(executor, std::move(awaitable),
                          [handler = std::move(handler)](std::exception_ptr error, auto&&... value) mutable {
                              if (error) {
                                  std::rethrow_exception(error);
                              }
                              handler(std::forward<decltype(value)>(value)...);
                          });
}

template <class T, class Handler>
void Spawn(FakeHttpClient& httpClient, boost::asio::awaitable<T> awaitable, Handler handler) {
    SpawnOn(httpClient.get_executor(), std::move(awaitable), std::move(handler));
}

template <class T, class Handler>
void Spawn(boost::asio::io_context& context, boost::asio::awaitable<T> awaitable, Handler handler) {
    SpawnOn(context.get_executor(), std::move(awaitable), std::move(handler));
}

inline void ExpectOptionalBodyMatches(const HttpRequest& request, std::optional<std::string_view> expectedBody) {
    if (expectedBody.has_value()) {
        EXPECT_EQ(request.GetBody(), *expectedBody);
    }
}

inline void ExpectRequestContract(const HttpRequest& request, HttpMethod expectedMethod, std::string_view expectedUrl,
                                  std::optional<std::string_view> expectedBody = std::nullopt,
                                  std::string_view expectedApiVersion = DefaultApiVersion) {
    EXPECT_EQ(request.GetMethod(), expectedMethod);
    EXPECT_EQ(request.GetUrl(), expectedUrl);
    EXPECT_EQ(FakeHttpClient::FindHeaderValue(request, "x-ms-version"), expectedApiVersion);

    const std::string dateHeader = FakeHttpClient::FindHeaderValue(request, "x-ms-date");
    EXPECT_FALSE(dateHeader.empty());
    EXPECT_TRUE(Private::ParseHttpDateHeader(dateHeader).has_value());

    const std::string requestId = FakeHttpClient::FindHeaderValue(request, "x-ms-client-request-id");
    EXPECT_TRUE(std::regex_match(requestId,
                                 std::regex{"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"}));

    ExpectOptionalBodyMatches(request, expectedBody);
}

[[nodiscard]] inline AccessToken MakeToken(std::string token, std::chrono::seconds expiresIn) {
    return AccessToken{.Token = std::move(token), .ExpiresOn = std::chrono::system_clock::now() + expiresIn};
}

// ---- Parallel download scripting ----

[[nodiscard]] inline std::string RangeHeader(std::uint64_t offset, std::uint64_t length) {
    return "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1U);
}

// A 206 response for `body` at `offset` of a blob of `total` bytes.
[[nodiscard]] inline HttpResponse RangeResponse(std::uint64_t offset, std::string body, std::uint64_t total,
                                                std::string_view etag = DefaultETag) {
    const std::string contentRange = "bytes " + std::to_string(offset) + "-" +
                                     std::to_string(offset + body.size() - 1U) + "/" + std::to_string(total);
    const std::string contentLength = std::to_string(body.size());
    return HttpResponse{206,
                        MakeCanonicalSuccessHeaders(
                            {{"Content-Range", contentRange}, {"Content-Length", contentLength}, {"ETag", etag}}),
                        std::move(body)};
}

// Scripts `response` for the request whose Range header is `range`.
inline void EnqueueForRange(FakeHttpClient& httpClient, std::string range, HttpResponse response,
                            bool deferred = true) {
    httpClient.EnqueueResponse(std::move(response), {}, deferred,
                               [range = std::move(range)](const FakeHttpClient::RequestRecord& record) {
                                   return FakeHttpClient::FindHeaderValue(record.Request, "Range") == range;
                               });
}

// Scripts every chunk of `content` (ChunkSize `chunk`) as a deferred 206 response.
inline void EnqueueChunks(FakeHttpClient& httpClient, const std::string& content, std::size_t chunk,
                          std::string_view etag = DefaultETag) {
    for (std::size_t offset = 0; offset < content.size(); offset += chunk) {
        const std::string body = content.substr(offset, chunk);
        EnqueueForRange(httpClient, RangeHeader(offset, body.size()),
                        RangeResponse(offset, body, content.size(), etag));
    }
}

// Index of the recorded request whose Range header is `range`, or RequestCount() if none.
[[nodiscard]] inline std::size_t FindRequestByRange(const FakeHttpClient& httpClient, std::string_view range) {
    for (std::size_t index = 0; index < httpClient.RequestCount(); ++index) {
        if (FakeHttpClient::FindHeaderValue(httpClient.RequestAt(index).Request, "Range") == range) {
            return index;
        }
    }
    return httpClient.RequestCount();
}

// Uniquely named directory beneath the working directory, removed on destruction, so concurrent test processes
// never share (or delete) each other's files. create_directory() only reports success for a directory this call
// created, which makes the ownership claim race-free.
class ScopedTestDirectory {
  public:
    explicit ScopedTestDirectory(std::string_view prefix) {
        std::random_device entropy;
        for (;;) {
            const std::uint64_t suffix = (static_cast<std::uint64_t>(entropy()) << 32U) | entropy();
            auto candidate = std::filesystem::current_path() / (std::string{prefix} + "-" + std::to_string(suffix));
            if (std::filesystem::create_directory(candidate)) {
                m_path = std::move(candidate);
                return;
            }
        }
    }
    ScopedTestDirectory(const ScopedTestDirectory&) = delete;
    ScopedTestDirectory& operator=(const ScopedTestDirectory&) = delete;
    ~ScopedTestDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(m_path, ignored);
    }

    [[nodiscard]] const std::filesystem::path& Path() const noexcept { return m_path; }

  private:
    std::filesystem::path m_path;
};
} // namespace AVEVA::AzureClient::Tests
