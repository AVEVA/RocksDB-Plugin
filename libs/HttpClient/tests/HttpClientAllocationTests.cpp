// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"
#include "private/DiagnosticsSink.hpp"
#include "private/HttpClientImpl.hpp"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <boost/version.hpp>

#include <openssl/crypto.h>
#include <openssl/opensslv.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace AllocationFailure {
extern thread_local bool counting;
extern thread_local std::size_t allocationCount;
extern thread_local std::size_t allocationBytes;
extern thread_local int allocationsUntilFailure;
extern thread_local std::size_t failureMinimumSize;
} // namespace AllocationFailure

namespace {
namespace asio = boost::asio;

// Counts the allocations made by one Submit call, which includes limiter admission and starting the operation
// (a free slot starts the request synchronously). Network work happens later on the io_context and is excluded.
std::size_t AllocationsPerSubmit(std::size_t maxPerHost, bool withCancellationSlot) {
    asio::io_context context;
    AVEVA::HttpClientOptions options;
    options.SetMaxConnectionsPerHost(maxPerHost);
    AVEVA::Private::BasicHttpClientImpl<> impl(context, options);
    asio::cancellation_signal signal;
    AVEVA::HttpRequestOptions requestOptions;
    if (withCancellationSlot) {
        requestOptions.SetCancellationSlot(signal.slot());
    }
    AVEVA::HttpRequest request;
    request.SetMethod(AVEVA::HttpMethod::Get);
    request.SetUrl(HttpClientTests::ClosedLocalUrl());
    // Warm-up: the first request pays the one-time Asio service and resolver setup, which is not per-request cost.
    impl.Submit(request, requestOptions, AVEVA::Detail::MakeCompletion([](std::error_code, AVEVA::HttpResponse) {}));
    HttpClientTests::RunBounded(context);
    context.restart();

    auto completion = AVEVA::Detail::MakeCompletion([](std::error_code, AVEVA::HttpResponse) {});
    AllocationFailure::allocationCount = 0;
    AllocationFailure::counting = true;
    impl.Submit(std::move(request), std::move(requestOptions), std::move(completion));
    AllocationFailure::counting = false;
    const auto count = AllocationFailure::allocationCount;
    HttpClientTests::RunBounded(context);
    return count;
}

// Counts every allocation (client and loopback server share the thread) made by a second request that reuses the
// pooled keep-alive connection.
std::size_t AllocationsPerPooledRequest() {
    asio::io_context context;
    HttpClientTests::ScriptedServer server(
        context, [](int, const HttpClientTests::http::request<HttpClientTests::http::string_body>&) {
            return HttpClientTests::ServerAction{"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"};
        });
    AVEVA::HttpClientOptions options;
    AVEVA::Private::BasicHttpClientImpl<> impl(context, options);
    const auto url = server.Url();
    std::size_t count = 0;
    auto submit = [&](auto&& onDone) {
        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Get);
        request.SetUrl(url);
        impl.Submit(std::move(request), AVEVA::HttpRequestOptions{},
                    AVEVA::Detail::MakeCompletion(
                        [&, onDone](std::error_code error, AVEVA::HttpResponse) mutable { onDone(error); }));
    };
    submit([&](std::error_code firstError) {
        EXPECT_FALSE(firstError);
        AllocationFailure::allocationCount = 0;
        AllocationFailure::counting = true;
        submit([&](std::error_code secondError) {
            AllocationFailure::counting = false;
            count = AllocationFailure::allocationCount;
            EXPECT_FALSE(secondError);
            server.Stop();
        });
    });
    HttpClientTests::RunBounded(context);
    EXPECT_EQ(server.Accepted(), 1);
    return count;
}

// Not a timing test: it pins the allocation counts so a regression in per-request wrapping shows up.
TEST(HttpClientAllocations, SubmitAllocationsPerRequest) {
    const auto unlimited = AllocationsPerSubmit(0, false);
    const auto limited = AllocationsPerSubmit(4, false);
    const auto limitedCancellable = AllocationsPerSubmit(4, true);
    std::cout << "allocations per Submit: unlimited=" << unlimited << " limited=" << limited
              << " limited+cancellation=" << limitedCancellable << '\n';
    const auto pooled = AllocationsPerPooledRequest();
    std::cout << "allocations for a pooled keep-alive request: " << pooled << '\n';
    EXPECT_LE(unlimited, 90u);
    EXPECT_LE(limited, 120u);
    EXPECT_LE(limitedCancellable, 125u);
    EXPECT_LE(pooled, 210u);
}
// Serves loopback requests from its own thread. The allocation counters are thread_local, so everything the server
// allocates is excluded from a measurement taken on the client's thread.
class IndependentServer {
  public:
    explicit IndependentServer(std::string wire)
        : m_server(
              m_context,
              [wire = std::move(wire)](int, const HttpClientTests::http::request<HttpClientTests::http::string_body>&) {
                  return HttpClientTests::ServerAction{wire};
              }),
          m_work(asio::make_work_guard(m_context)), m_thread([this] { m_context.run(); }) {}

    IndependentServer(const IndependentServer&) = delete;
    IndependentServer& operator=(const IndependentServer&) = delete;

    ~IndependentServer() { Finish(); }

    std::string Url() const { return m_server.Url(); }
    // Stops and joins the server thread; the counters below are only valid afterwards. Idempotent.
    void Finish() {
        if (!m_thread.joinable()) {
            return;
        }
        asio::post(m_context, [this] { m_server.Stop(); });
        m_work.reset();
        m_thread.join();
    }
    int Accepted() const { return m_server.Accepted(); }
    int TotalRequests() const { return m_server.TotalRequests(); }

  private:
    asio::io_context m_context;
    HttpClientTests::ScriptedServer m_server;
    asio::executor_work_guard<asio::io_context::executor_type> m_work;
    std::thread m_thread;
};

struct PublicClientMeasurement {
    std::size_t requests = 0;
    std::size_t allocations = 0;
    std::size_t bytes = 0;
    int serverRequests = 0;
    int connections = 0;
    double requestsPerSecond = 0;
    double p50Micros = 0;
    double p99Micros = 0;
    double maxMicros = 0;
};

// Issues `requests` sequential GETs through the public HttpClient::SendAsync (so the final hand-off post is
// included) against a server on another thread. The warm-up request establishes the pooled keep-alive connection
// and pays the one-time Asio service setup, so the measured requests are the warm, pooled path.
class PublicClientDriver {
  public:
    PublicClientDriver(AVEVA::HttpClient& client, std::string url, std::size_t requests)
        : m_client(client), m_url(std::move(url)), m_remaining(requests) {
        m_latencies.reserve(requests);
    }

    void Next() {
        if (m_remaining == 0) {
            AllocationFailure::counting = false;
            m_end = std::chrono::steady_clock::now();
            return;
        }
        --m_remaining;
        AVEVA::HttpRequest request;
        request.SetMethod(AVEVA::HttpMethod::Get);
        request.SetUrl(m_url);
        m_started = std::chrono::steady_clock::now();
        HttpClientTests::SendWith(m_client, std::move(request), AVEVA::HttpRequestOptions{},
                                  [this](std::error_code error, AVEVA::HttpResponse response) {
                                      m_latencies.push_back(std::chrono::steady_clock::now() - m_started);
                                      if (error || response.GetStatus() != 200) {
                                          ++m_failures;
                                      }
                                      Next();
                                  });
    }

    void BeginMeasuring() {
        AllocationFailure::allocationCount = 0;
        AllocationFailure::allocationBytes = 0;
        AllocationFailure::counting = true;
        m_begin = std::chrono::steady_clock::now();
    }

    std::size_t Failures() const { return m_failures; }
    std::chrono::steady_clock::duration Elapsed() const { return m_end - m_begin; }
    std::vector<std::chrono::steady_clock::duration>& Latencies() { return m_latencies; }

  private:
    AVEVA::HttpClient& m_client;
    std::string m_url;
    std::size_t m_remaining;
    std::size_t m_failures = 0;
    std::chrono::steady_clock::time_point m_begin;
    std::chrono::steady_clock::time_point m_end;
    std::chrono::steady_clock::time_point m_started;
    std::vector<std::chrono::steady_clock::duration> m_latencies;
};

PublicClientMeasurement MeasurePublicClient(std::size_t bodyBytes, std::size_t requests) {
    const std::string body(bodyBytes, 'x');
    IndependentServer server("HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(bodyBytes) + "\r\n\r\n" + body);
    asio::io_context context;
    AVEVA::HttpClient client(context);

    PublicClientDriver warmUp(client, server.Url(), 1);
    warmUp.Next();
    HttpClientTests::RunBounded(context);
    context.restart();
    EXPECT_EQ(warmUp.Failures(), 0u);

    PublicClientDriver driver(client, server.Url(), requests);
    driver.BeginMeasuring();
    driver.Next();
    HttpClientTests::RunBounded(context);
    AllocationFailure::counting = false;
    EXPECT_EQ(driver.Failures(), 0u);

    PublicClientMeasurement result;
    result.requests = requests;
    result.allocations = AllocationFailure::allocationCount;
    result.bytes = AllocationFailure::allocationBytes;
    auto& latencies = driver.Latencies();
    std::ranges::sort(latencies);
    const auto percentile = [&](double fraction) {
        const auto index = std::min(latencies.size() - 1, static_cast<std::size_t>(fraction * latencies.size()));
        return std::chrono::duration<double, std::micro>(latencies[index]).count();
    };
    result.p50Micros = percentile(0.50);
    result.p99Micros = percentile(0.99);
    result.maxMicros = std::chrono::duration<double, std::micro>(latencies.back()).count();
    result.requestsPerSecond = static_cast<double>(requests) / std::chrono::duration<double>(driver.Elapsed()).count();
    server.Finish();
    result.serverRequests = server.TotalRequests();
    result.connections = server.Accepted();
    return result;
}

// Loopback CPU/allocation evidence only: it says nothing about Azure network latency. The server runs on its own
// thread, so the counted allocations belong to the public client (SendAsync adapter, pool, parser, final post).
TEST(HttpClientAllocations, PublicClientPooledRequestWithIndependentServer) {
    constexpr std::size_t requests = 200;
#ifdef NDEBUG
    constexpr const char* configuration = "Release";
#else
    constexpr const char* configuration = "Debug";
#endif
    std::cout << "BENCH http-client environment: configuration=" << configuration << " cplusplus="
#ifdef _MSVC_LANG
              << _MSVC_LANG
#else
              << __cplusplus
#endif
#ifdef _MSC_VER
              << " msvc=" << _MSC_VER
#endif
#ifdef __VERSION__
              << " compiler=\"" << __VERSION__ << '"'
#endif
              << " boost=" << BOOST_LIB_VERSION << " openssl=\"" << OpenSSL_version(OPENSSL_VERSION)
              << "\" server=independent-thread network=loopback state=warm-pooled\n";
    for (const std::size_t bodyBytes : {std::size_t{0}, std::size_t{16} * 1024}) {
        const auto m = MeasurePublicClient(bodyBytes, requests);
        std::cout << "BENCH http-client body_bytes=" << bodyBytes << " requests=" << m.requests
                  << " server_requests=" << m.serverRequests << " connections=" << m.connections
                  << " allocations_per_request=" << m.allocations / m.requests
                  << " bytes_per_request=" << m.bytes / m.requests << " requests_per_second=" << m.requestsPerSecond
                  << " p50_us=" << m.p50Micros << " p99_us=" << m.p99Micros << " max_us=" << m.maxMicros << '\n';
        EXPECT_EQ(m.serverRequests, static_cast<int>(requests) + 1);
        EXPECT_EQ(m.connections, 1);
        EXPECT_GT(m.allocations, 0u);
        EXPECT_LE(m.allocations / m.requests, 250u);
        EXPECT_LE(m.bytes / m.requests, bodyBytes * 2 + 16 * 1024);
    }
}
} // namespace

TEST(HttpClientDiagnosticsAllocation, DisabledSinkAllocatesNothingForTheEvent) {
    const std::shared_ptr<const AVEVA::Private::DiagnosticsSink> none;
    const std::string host(64, 'h');
    const std::string message(64, 'm');
    AllocationFailure::allocationCount = 0;
    AllocationFailure::counting = true;
    AVEVA::Private::DiagnosticsSink::Report(none, AVEVA::HttpDiagnosticKind::Timeout, AVEVA::HttpRequestPhase::Queued,
                                            host, "443", {}, message);
    AllocationFailure::counting = false;
    EXPECT_EQ(AllocationFailure::allocationCount, 0U);
}

TEST(HttpClientDiagnosticsAllocation, FailureBuildingTheEventIsSwallowed) {
    int calls = 0;
    AVEVA::HttpClientOptions options;
    options.SetDiagnosticsCallback([&](const AVEVA::HttpDiagnostic&) { ++calls; });
    const auto sink = AVEVA::Private::DiagnosticsSink::For(options);
    const std::string host(64, 'h');
    const std::string message(64, 'm');
    AllocationFailure::failureMinimumSize = 32;
    AllocationFailure::allocationsUntilFailure = 0;
    AVEVA::Private::DiagnosticsSink::Report(sink, AVEVA::HttpDiagnosticKind::Timeout, AVEVA::HttpRequestPhase::Queued,
                                            host, "443", {}, message);
    AllocationFailure::allocationsUntilFailure = -1;
    AllocationFailure::failureMinimumSize = 0;
    EXPECT_EQ(calls, 0);

    AVEVA::Private::DiagnosticsSink::Report(sink, AVEVA::HttpDiagnosticKind::Timeout, AVEVA::HttpRequestPhase::Queued,
                                            host, "443", {}, message);
    EXPECT_EQ(calls, 1);
}
