// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/HttpClientCore.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace AVEVA {
class HttpClient::Impl {
  public:
    Impl(boost::asio::io_context& context, const HttpClientOptions& options)
        : core(std::make_shared<Private::HttpClientCore>(context, options)) {}

    std::shared_ptr<Private::HttpClientCore> core;

    std::mutex writerMutex;
    std::unique_ptr<boost::asio::thread_pool> writerPool;
};

HttpClient::HttpClient(boost::asio::io_context& context, HttpClientOptions options)
    : m_context(&context), m_impl(std::make_unique<Impl>(context, options)) {}

HttpClient::~HttpClient() {
    if (!m_impl->writerPool) {
        m_impl->core->Shutdown();
        return;
    }
    if (m_impl->writerPool->get_executor().running_in_this_thread()) {
        // The last owner was released on a writer thread, which cannot join itself. The transport is torn down
        // here, synchronously, because the caller may destroy the io_context as soon as we return. Only the pool
        // goes to a helper thread, which joins it once the current task returns (queued writes still finish).
        m_impl->core->Shutdown();
        const auto diagnostics = m_impl->core->Diagnostics();
        // The pool travels as a raw pointer so a failed thread start cannot destroy it (a destructor join from one
        // of its own workers would deadlock); on failure it is deliberately leaked.
        auto* const rawPool = m_impl->writerPool.release();
        try {
            std::thread([rawPool] {
                const std::unique_ptr<boost::asio::thread_pool> pool(rawPool);
                pool->join();
            }).detach();
        } catch (...) {
            Private::DiagnosticsSink::Report(
                diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {}, {}, {},
                "The download writer pool could not be joined on a helper thread; it was leaked");
            rawPool->stop();
        }
        return;
    }
    // join() lets queued sink writes finish instead of abandoning them, which would strand their downloads.
    // It runs before the transport teardown so no worker touches destroyed state.
    m_impl->writerPool->join();
    m_impl->core->Shutdown();
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

boost::asio::awaitable<HttpResult> HttpClient::SendAsync(HttpRequest request, HttpRequestOptions options) {
    // Held by the coroutine frame, so the client can be destroyed while the request is still running.
    const auto core = m_impl->core;
    co_return co_await core->Send(std::move(request), std::move(options));
}
} // namespace AVEVA