#pragma once

#include <AVEVA/HttpClient/HttpClientError.hpp>
#include <AVEVA/HttpClient/HttpClientOptions.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <functional>
#include <memory>
#include <system_error>
#include <utility>

namespace boost
{
    namespace asio
    {
        class io_context;
    }
} // namespace boost

namespace AVEVA
{
    class IHttpClient
    {
      public:
        using CompletionHandler = std::move_only_function<void(std::error_code, HttpResponse)>;
        using executor_type = boost::asio::any_io_executor;

        IHttpClient() = default;
        IHttpClient(const IHttpClient&) = delete;
        IHttpClient& operator=(const IHttpClient&) = delete;
        IHttpClient(IHttpClient&&) = delete;
        IHttpClient& operator=(IHttpClient&&) = delete;
        virtual ~IHttpClient();

        static std::unique_ptr<IHttpClient> Create(boost::asio::io_context& context, HttpClientOptions options = {});

        // The executor associated with this client (used as the default executor for
        // completion tokens that do not carry their own associated executor).
        virtual executor_type get_executor() const = 0;

        // Type-erased send operation. Implementations must never invoke `completion`
        // synchronously/re-entrantly from within this call (see Task 8), and must honor
        // `options.GetCancellationSlot()` if set.
        virtual void SendAsyncErased(HttpRequest request, CompletionHandler completion, HttpRequestOptions options) = 0;

        // Legacy, non-completion-token overload preserved for source compatibility.
        void SendAsync(HttpRequest request, CompletionHandler completion, HttpRequestOptions options = {})
        {
            SendAsyncErased(std::move(request), std::move(completion), std::move(options));
        }

        // Completion-token-aware overload. Propagates the token's associated executor,
        // allocator, and cancellation slot (falling back to this client's executor, the
        // default allocator, and `options`'s existing cancellation slot respectively) so
        // that callers using `use_future`, `use_awaitable`, `bind_executor`, etc. get the
        // behavior they expect without this class needing to know about any of those
        // token types.
        template <class CompletionToken>
        auto SendAsync(HttpRequest request, HttpRequestOptions options, CompletionToken&& token)
        {
            using Signature = void(std::error_code, HttpResponse);
            return boost::asio::async_initiate<CompletionToken, Signature>(
                [this](auto&& handler, HttpRequest req, HttpRequestOptions opts)
                {
                    auto executor = boost::asio::get_associated_executor(handler, get_executor());
                    auto allocator = boost::asio::get_associated_allocator(handler);
                    auto cancellationSlot = boost::asio::get_associated_cancellation_slot(handler, boost::asio::cancellation_slot());
                    if (cancellationSlot.is_connected() && !opts.GetCancellationSlot().is_connected())
                    {
                        opts.SetCancellationSlot(cancellationSlot);
                    }

                    SendAsyncErased(std::move(req),
                        CompletionHandler(
                            [executor, allocator, handler = std::forward<decltype(handler)>(handler)](
                                std::error_code ec, HttpResponse response) mutable
                            {
                                auto work = boost::asio::make_work_guard(executor);
                                boost::asio::dispatch(executor,
                                    boost::asio::bind_allocator(allocator,
                                        [handler = std::move(handler), ec, response = std::move(response),
                                            work = std::move(work)]() mutable
                                        { std::move(handler)(ec, std::move(response)); }));
                            }),
                        std::move(opts));
                },
                token, std::move(request), std::move(options));
        }
    };
} // namespace AVEVA
