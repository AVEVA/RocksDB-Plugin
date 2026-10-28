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

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <initializer_list>
#include <optional>
#include <random>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
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

// ---- Assertions and coroutine helpers ----

// Assertion that `future` is already satisfied. Completions are driven by Poll(), so there is nothing to wait
// for; a hung test is caught by the CTest timeout rather than a fixed sleep budget.
template <class T> [[nodiscard]] ::testing::AssertionResult ExpectReady(const std::future<T>& future) {
    if (future.wait_for(std::chrono::seconds{0}) == std::future_status::ready) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure() << "future was not ready after the client was polled";
}

// Drives the client once and returns the value of a use_future completion. Throws (which gtest reports as a
// failure) instead of blocking when the completion did not run.
template <class T> [[nodiscard]] T PollAndGet(FakeHttpClient& client, std::future<T>& future) {
    client.Poll();
    if (!ExpectReady(future)) {
        throw std::runtime_error("future was not ready after the client was polled");
    }
    return future.get();
}

// Awaits the deferred operation produced by `makeOperation()` and stores its result through
// `out`. This is a free coroutine taking a pointer rather than a capturing coroutine lambda, so
// the coroutine frame never outlives captured references (cppcoreguidelines-avoid-capturing-
// lambda-coroutines / cppcoreguidelines-avoid-reference-coroutine-parameters).
template <class Result, class MakeOperation>
boost::asio::awaitable<void> AwaitInto(std::optional<Result>* out, MakeOperation makeOperation) {
    *out = co_await makeOperation();
}

// Same idea, but only records whether the awaited result was engaged.
template <class MakeOperation> boost::asio::awaitable<void> AwaitHasValue(bool* out, MakeOperation makeOperation) {
    const auto result = co_await makeOperation();
    *out = result.has_value();
}

class CallbackExpectation {
  public:
    CallbackExpectation() = default;
    CallbackExpectation(const CallbackExpectation&) = delete;
    CallbackExpectation& operator=(const CallbackExpectation&) = delete;
    CallbackExpectation(CallbackExpectation&&) = delete;
    CallbackExpectation& operator=(CallbackExpectation&&) = delete;

    ~CallbackExpectation() { EXPECT_TRUE(m_invoked) << "Completion callback was not invoked."; }

    void MarkInvoked() { m_invoked = true; }

  private:
    bool m_invoked = false;
};

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
