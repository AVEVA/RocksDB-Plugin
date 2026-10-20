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
    auto completion = AVEVA::Detail::MakeCompletion([](std::error_code, AVEVA::HttpResponse) {});

    AllocationFailure::allocationCount = 0;
    AllocationFailure::counting = true;
    impl.Submit(std::move(request), std::move(requestOptions), std::move(completion));
    AllocationFailure::counting = false;
    const auto count = AllocationFailure::allocationCount;
    HttpClientTests::RunBounded(context);
    return count;
}

// Not a timing test: it pins the allocation counts so a regression in per-request wrapping shows up.
TEST(HttpClientAllocations, SubmitAllocationsPerRequest) {
    const auto unlimited = AllocationsPerSubmit(0, false);
    const auto limited = AllocationsPerSubmit(4, false);
    const auto limitedCancellable = AllocationsPerSubmit(4, true);
    std::cout << "allocations per Submit: unlimited=" << unlimited << " limited=" << limited
              << " limited+cancellation=" << limitedCancellable << '\n';
    // Before merging the limiter state into the submission state, a limited Submit made 121 allocations here.
    EXPECT_LE(limited, 119u);
}
} // namespace
