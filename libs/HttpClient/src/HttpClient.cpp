// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/HttpClientCore.hpp"

#include <memory>
#include <utility>

namespace AVEVA {
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
    // Held by the coroutine frame, so the client can be destroyed while the request is still running.
    const auto core = m_impl->core;
    co_return co_await core->Send(std::move(request), std::move(options));
}
} // namespace AVEVA