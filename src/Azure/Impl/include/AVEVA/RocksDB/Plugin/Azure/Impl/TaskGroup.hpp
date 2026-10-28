// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <utility>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Runs child coroutines concurrently, at most `limit` at a time, from inside a parent coroutine. It replaces
/// hand-managed futures: `Scope` does not return until every child has finished, so children may freely use anything
/// the parent's frame owns, and the first failure is rethrown only after the others have completed.
///
/// Children report failure by throwing. The group is not cancellable: waiting for the children is what keeps the
/// parent's buffers alive, so it must never be abandoned part way.
///
/// <code>
/// TaskGroup group(co_await boost::asio::this_coro::executor, 4);
/// co_await group.Scope([&](TaskGroup&amp; tasks) -> boost::asio::awaitable&lt;void&gt; {
///     for (auto&amp; item : items) {
///         co_await tasks.Spawn(Process(item)); // waits for a free slot when `limit` children are running
///     }
/// });
/// </code>
/// </summary>
class TaskGroup {
    using Channel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code)>;

    // Shared with the children: the last child may still be inside try_send when the parent resumes and leaves.
    struct State {
        State(const boost::asio::any_io_executor& executor, const size_t limit) : Done(executor, limit) {}

        Channel Done; // one message per finished child; capacity is the limit, so a child never waits to report
        std::mutex Mutex;
        std::exception_ptr Failure; // the first failure of any child

        void Fail(std::exception_ptr error) noexcept {
            const std::lock_guard lock(Mutex);
            if (!Failure) {
                Failure = std::move(error);
            }
        }

        [[nodiscard]] std::exception_ptr FirstFailure() {
            const std::lock_guard lock(Mutex);
            return Failure;
        }
    };

    boost::asio::any_io_executor m_executor;
    size_t m_limit;
    std::shared_ptr<State> m_state;
    size_t m_running = 0; // only touched by the parent coroutine

    template <typename Task> static boost::asio::awaitable<void> RunChild(std::shared_ptr<State> state, Task task) {
        try {
            co_await std::move(task);
        } catch (...) {
            state->Fail(std::current_exception());
        }
        state->Done.try_send(boost::system::error_code{});
    }

    boost::asio::awaitable<void> WaitForOne() {
        co_await m_state->Done.async_receive(boost::asio::use_awaitable);
        --m_running;
    }

  public:
    TaskGroup(boost::asio::any_io_executor executor, const size_t limit)
        : m_executor(std::move(executor)), m_limit(limit == 0 ? 1 : limit),
          m_state(std::make_shared<State>(m_executor, m_limit)) {}
    TaskGroup(const TaskGroup&) = delete;
    TaskGroup& operator=(const TaskGroup&) = delete;

    /// <summary>
    /// Starts `task` as a child. When `limit` children are already running, first waits for one to finish and
    /// rethrows the first child failure seen so far, so a failed batch stops starting new work.
    /// </summary>
    template <typename Task> boost::asio::awaitable<void> Spawn(Task task) {
        if (m_running >= m_limit) {
            co_await WaitForOne();
        }
        if (auto failure = m_state->FirstFailure()) {
            std::rethrow_exception(std::move(failure));
        }
        boost::asio::co_spawn(m_executor, RunChild(m_state, std::move(task)), boost::asio::detached);
        ++m_running;
    }

    /// <summary>
    /// Runs `body(*this)`, then waits for every child to finish whatever happened. Rethrows the body's failure, or
    /// else the first child failure.
    /// </summary>
    template <typename Body> boost::asio::awaitable<void> Scope(Body body) {
        std::exception_ptr failure;
        try {
            co_await body(*this);
        } catch (...) {
            failure = std::current_exception();
        }
        while (m_running > 0) {
            co_await WaitForOne();
        }
        if (!failure) {
            failure = m_state->FirstFailure();
        }
        if (failure) {
            std::rethrow_exception(std::move(failure));
        }
    }
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
