// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "HttpClientTestHelpers.hpp"
#include "private/HttpClientImpl.hpp"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <cstddef>
#include <iostream>
#include <string>
#include <system_error>

namespace AllocationFailure {
extern thread_local bool counting;
extern thread_local std::size_t allocationCount;
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
} // namespace
