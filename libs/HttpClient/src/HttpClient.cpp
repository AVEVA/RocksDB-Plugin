// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/HttpClientCore.hpp"

#include <memory>
#include <utility>

namespace AVEVA {
namespace {
/// <summary>
/// The actual request coroutine. Takes the core by value (a copy of the shared_ptr) so that the client owning it can
/// be destroyed before this coroutine ever starts running: awaitables begin suspended, so a coroutine that instead
/// captured `this` or a reference would read a destroyed client the first time it is resumed.
/// </summary>
boost::asio::awaitable<HttpResult> SendWith(std::shared_ptr<Private::HttpClientCore> core, HttpRequest request,
                                            HttpRequestOptions options) {
    co_return co_await core->Send(std::move(request), std::move(options));
}
} // namespace

class HttpClient::Impl {
  public:
    Impl(boost::asio::io_context& context, const HttpClientOptions& options)
        : core(std::make_shared<Private::HttpClientCore>(context, options)) {}

    std::shared_ptr<Private::HttpClientCore> core;
};

HttpClient::HttpClient(boost::asio::io_context& context, HttpClientOptions options)
    : m_context(&context), m_impl(std::make_unique<Impl>(context, options)) {}

HttpClient::~HttpClient() { m_impl->core->Shutdown(); }

std::unique_ptr<HttpClient> HttpClient::Create(boost::asio::io_context& context, HttpClientOptions options) {
    return std::make_unique<HttpClient>(context, std::move(options));
}

boost::asio::awaitable<HttpResult> HttpClient::SendAsync(HttpRequest request, HttpRequestOptions options) {
    // Copy the shared_ptr eagerly, before any coroutine is created, so the client can be destroyed while the request
    // is still running even though `co_spawn` does not resume this awaitable until later.
    return SendWith(m_impl->core, std::move(request), std::move(options));
}
} // namespace AVEVA