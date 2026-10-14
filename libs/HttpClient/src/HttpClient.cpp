// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/HttpClientImpl.hpp"

#include <memory>
#include <utility>

namespace AVEVA {
/// <summary>
/// The production client implementation: the shared one, with no fault injection.
/// </summary>
class HttpClient::Impl : public Private::BasicHttpClientImpl<Private::NoFault> {
  public:
    using BasicHttpClientImpl::BasicHttpClientImpl;
};

HttpClient::HttpClient(boost::asio::io_context& context, HttpClientOptions options)
    : m_context(&context), m_impl(std::make_unique<Impl>(context, options)) {}

HttpClient::~HttpClient() = default;

std::unique_ptr<HttpClient> HttpClient::Create(boost::asio::io_context& context, HttpClientOptions options) {
    return std::make_unique<HttpClient>(context, std::move(options));
}

void HttpClient::Submit(HttpRequest request, HttpRequestOptions options, Detail::CompletionPointer completion) {
    m_impl->Submit(std::move(request), std::move(options), std::move(completion));
}
} // namespace AVEVA
