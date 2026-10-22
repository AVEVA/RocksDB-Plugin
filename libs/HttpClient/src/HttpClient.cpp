// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/HttpClientImpl.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace AVEVA {
/// <summary>
/// The production client implementation: the shared one, with no fault injection.
/// </summary>
class HttpClient::Impl : public Private::BasicHttpClientImpl<Private::NoFault> {
  public:
    using BasicHttpClientImpl::BasicHttpClientImpl;

    std::mutex writerMutex;
    std::unique_ptr<boost::asio::thread_pool> writerPool;
};

HttpClient::HttpClient(boost::asio::io_context& context, HttpClientOptions options)
    : m_context(&context), m_impl(std::make_unique<Impl>(context, options)) {}

HttpClient::~HttpClient() {
    if (!m_impl->writerPool) {
        return;
    }
    if (m_impl->writerPool->get_executor().running_in_this_thread()) {
        // The last owner was released on a writer thread, which cannot join itself. A helper takes over the whole
        // implementation, joins the pool once the current task returns (queued writes still finish) and only then
        // tears the transport down, so no worker ever touches destroyed state.
        std::thread([impl = std::move(m_impl)]() mutable {
            impl->writerPool->join();
            impl.reset();
        }).detach();
        return;
    }
    // join() lets queued sink writes finish instead of abandoning them, which would strand their downloads.
    // It runs before m_impl is destroyed, so the pool is joined ahead of the transport teardown.
    m_impl->writerPool->join();
}
boost::asio::thread_pool::executor_type HttpClient::GetDownloadWriter() {
    const std::scoped_lock lock(m_impl->writerMutex);
    if (!m_impl->writerPool) {
        constexpr unsigned MaxWriterThreads = 4;
        const auto threads = std::clamp(std::thread::hardware_concurrency(), 1U, MaxWriterThreads);
        m_impl->writerPool = std::make_unique<boost::asio::thread_pool>(threads);
    }
    return m_impl->writerPool->get_executor();
}

std::unique_ptr<HttpClient> HttpClient::Create(boost::asio::io_context& context, HttpClientOptions options) {
    return std::make_unique<HttpClient>(context, std::move(options));
}

void HttpClient::Submit(HttpRequest request, HttpRequestOptions options, Detail::CompletionPointer completion) {
    m_impl->Submit(std::move(request), std::move(options), std::move(completion));
}
} // namespace AVEVA
