// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/awaitable.hpp>

#include <concepts>
#include <utility>

namespace AVEVA::AzureClient {
/// <summary>
/// What the Azure clients require of an HTTP client: an executor and a coroutine <c>SendAsync</c> that returns the
/// response or the transport error. <c>AVEVA::HttpClient</c> models it; tests supply scripted implementations.
/// </summary>
template <class T>
concept HttpClientLike = requires(T& client, const T& constClient, HttpRequest request, HttpRequestOptions options) {
    typename T::executor_type;
    { constClient.get_executor() } -> std::convertible_to<typename T::executor_type>;
    { client.SendAsync(std::move(request), std::move(options)) } -> std::same_as<boost::asio::awaitable<HttpResult>>;
};
} // namespace AVEVA::AzureClient
