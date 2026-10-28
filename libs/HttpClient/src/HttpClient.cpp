// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/HttpClient/HttpClient.hpp"

#include "private/HttpClientImpl.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <atomic>
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
        m_impl->Shutdown();
        return;
    }
    if (m_impl->writerPool->get_executor().running_in_this_thread()) {
        // The last owner was released on a writer thread, which cannot join itself. The transport is torn down
        // here, synchronously, because the caller may destroy the io_context as soon as we return. Only the pool
        // goes to a helper thread, which joins it once the current task returns (queued writes still finish).
        const auto diagnostics = m_impl->Shutdown();
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
    m_impl->Shutdown();
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

namespace {
/// <summary>
/// How many times the final post is attempted before the coroutine is resumed off its executor.
/// </summary>
constexpr int FinalPostAttempts = 3;

/// <summary>
/// Owns the coroutine's resumption handler, work guard and outcome until the handler has run, so that a failure to
/// schedule the final delivery cannot take the handler down with the discarded task.
/// </summary>
template <class Handler, class Work, class Slot> class FinalDelivery {
  public:
    FinalDelivery(Handler handler, Work work, Slot slot, bool ownsSlot)
        : m_handler(std::move(handler)), m_work(std::move(work)), m_slot(slot), m_ownsSlot(ownsSlot) {}

    void SetOutcome(std::error_code error, HttpResponse response) {
        m_error = error;
        m_response = std::move(response);
    }

    /// <summary>
    /// Runs the handler on the thread that calls it, once; later calls do nothing.
    /// </summary>
    void Deliver() { Run(m_ownsSlot); }

    /// <summary>
    /// As <c>Deliver</c>, for the last resort of an executor that keeps rejecting work. Off the handler's executor
    /// the adopted cancellation slot is left alone, since clearing it here would race with emissions serialized
    /// there.
    /// </summary>
    void DeliverInline() noexcept {
        try {
            Run(false);
        } catch (...) {
        }
    }

  private:
    void Run(bool clearSlot) {
        if (m_delivered.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        // Cleared on the handler's executor, so that it is serialized with emissions from the caller.
        if (clearSlot) {
            m_slot.clear();
        }
        std::move(m_handler)(m_error, std::move(m_response));
    }

    Handler m_handler;
    Work m_work;
    Slot m_slot;
    bool m_ownsSlot;
    std::error_code m_error;
    HttpResponse m_response;
    std::atomic<bool> m_delivered{false};
};
} // namespace

boost::asio::awaitable<HttpResult> HttpClient::SendAsync(HttpRequest request, HttpRequestOptions options) {
    using Signature = void(std::error_code, HttpResponse);
    auto token = boost::asio::as_tuple(boost::asio::use_awaitable);
    auto [error, response] = co_await boost::asio::async_initiate<decltype(token), Signature>(
        [this](auto handler, HttpRequest req, HttpRequestOptions opts) {
            auto executor = boost::asio::get_associated_executor(handler, get_executor());
            auto allocator = boost::asio::get_associated_allocator(handler);
            // Held for the whole operation so the coroutine's executor cannot run out of work (and let run()
            // return) before the completion is dispatched.
            auto work = boost::asio::make_work_guard(executor);
            auto cancellationSlot =
                boost::asio::get_associated_cancellation_slot(handler, boost::asio::cancellation_slot());
            const bool ownsSlot = cancellationSlot.is_connected() && !opts.GetCancellationSlot().is_connected();
            if (ownsSlot) {
                opts.SetCancellationSlot(cancellationSlot);
            }

            using Delivery = FinalDelivery<decltype(handler), decltype(work), decltype(cancellationSlot)>;
            Submit(std::move(req), std::move(opts),
                   Detail::MakeCompletion(
                       [executor, allocator,
                        state = std::allocate_shared<Delivery>(allocator, std::move(handler), std::move(work),
                                                               cancellationSlot, ownsSlot)](std::error_code ec,
                                                                                            HttpResponse res) mutable {
                           // Posted, not dispatched: an inline resumption would run on the connection's strand,
                           // which a pooled connection hands to the next queued request. The handler stays in
                           // `state` rather than in the posted task, so a post that cannot be scheduled does
                           // not take it down with the discarded task.
                           state->SetOutcome(ec, std::move(res));
                           // A failed post is usually a transient allocation failure, so retry it to keep the
                           // coroutine on its own executor.
                           for (int attempt = 0; attempt < FinalPostAttempts; ++attempt) {
                               try {
                                   boost::asio::post(executor, boost::asio::bind_allocator(
                                                                   allocator, [state]() { state->Deliver(); }));
                                   return;
                               } catch (...) {
                               }
                           }
                           // The executor keeps rejecting work. The result is still the caller's, so the
                           // coroutine resumes here, once, instead of never.
                           state->DeliverInline();
                       },
                       allocator));
        },
        token, std::move(request), std::move(options));
    if (error) {
        co_return std::unexpected(error);
    }
    co_return std::move(response);
}
} // namespace AVEVA
