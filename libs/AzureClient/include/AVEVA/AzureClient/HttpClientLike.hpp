// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <concepts>
#include <system_error>
#include <utility>

namespace AVEVA::AzureClient {
namespace Private {
// The completion handler shape every HTTP client must accept; used only to check SendAsync.
struct ProbeHttpHandler {
    void operator()(std::error_code, HttpResponse) const {}
};
} // namespace Private

/// <summary>
/// What the Azure clients require of an HTTP client: an executor to complete on and a SendAsync that accepts a
/// completion handler invoked with <c>(std::error_code, HttpResponse)</c>. <c>AVEVA::HttpClient</c> models it;
/// tests supply scripted implementations.
/// </summary>
template <class T>
concept HttpClientLike = requires(T& client, const T& constClient, HttpRequest request, HttpRequestOptions options) {
    typename T::executor_type;
    { constClient.get_executor() } -> std::convertible_to<typename T::executor_type>;
    client.SendAsync(std::move(request), std::move(options), Private::ProbeHttpHandler{});
};
} // namespace AVEVA::AzureClient
