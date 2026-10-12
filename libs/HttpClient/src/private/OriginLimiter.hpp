// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace AVEVA::Private
{
    /// <summary>
    /// Caps in-flight requests per origin. Requests over the cap wait in FIFO order and start as earlier ones
    /// finish. A waiting request can still be cancelled, and <see cref="Close"/> fails whatever is left when the
    /// owner dies. All operations are thread-safe, and the callbacks it holds are always invoked outside the
    /// lock.
    /// </summary>
    class OriginLimiter : public std::enable_shared_from_this<OriginLimiter>
    {
      public:
        /// <summary>
        /// Begins a request that has been given a slot.
        /// </summary>
        using Start = std::move_only_function<void()>;

        /// <summary>
        /// Fails a request that never got a slot, with the reason it was dropped.
        /// </summary>
        using Abort = std::move_only_function<void(std::error_code)>;

        /// <summary>
        /// Constructs a limiter with a per-origin concurrency cap.
        /// </summary>
        /// <param name="maxPerOrigin">How many requests may run at once per origin.</param>
        explicit OriginLimiter(std::size_t maxPerOrigin) : m_max(maxPerOrigin)
        {
        }

        /// <summary>
        /// Issues an identifier for one queued request, for use with <see cref="Run"/> and
        /// <see cref="Cancel"/>.
        /// </summary>
        /// <returns>A ticket unique within this limiter.</returns>
        std::uint64_t NextTicket() noexcept
        {
            return ++m_nextTicket;
        }

        /// <summary>
        /// Runs the request now if the origin has capacity, otherwise queues it to be started by a later
        /// <see cref="Release"/> or failed through its abort.
        /// </summary>
        /// <param name="origin">The origin the request counts against.</param>
        /// <param name="ticket">Identifies the request while it waits (see <see cref="Cancel"/>).</param>
        /// <param name="start">Begins the request; invoked once, outside the lock.</param>
        /// <param name="abort">Fails the request if it never gets a slot.</param>
        void Run(const std::string& origin, std::uint64_t ticket, Start start, Abort abort)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            auto& state = m_origins[origin];
            if (state.active < m_max)
            {
                ++state.active;
                lock.unlock();
                start();
                return;
            }
            state.waiters.push_back(Waiter{ticket, std::move(start), std::move(abort)});
        }

        /// <summary>
        /// Hands the finished request's slot to the next waiter, or frees it. Must be called exactly once per
        /// started request.
        /// </summary>
        /// <param name="origin">The origin the finished request counted against.</param>
        void Release(const std::string& origin)
        {
            Start next;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                const auto it = m_origins.find(origin);
                if (it == m_origins.end())
                {
                    return;
                }
                if (it->second.waiters.empty())
                {
                    if (--it->second.active == 0)
                    {
                        m_origins.erase(it);
                    }
                    return;
                }
                next = std::move(it->second.waiters.front().start);
                it->second.waiters.pop_front();
            }
            next();
        }

        /// <summary>
        /// Removes a queued request and fails it with <c>operation_canceled</c>; a no-op if it already started
        /// or never existed.
        /// </summary>
        /// <param name="origin">The origin the request was queued under.</param>
        /// <param name="ticket">The ticket the request was queued with.</param>
        void Cancel(const std::string& origin, std::uint64_t ticket)
        {
            Abort abort;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                const auto it = m_origins.find(origin);
                if (it == m_origins.end())
                {
                    return;
                }
                auto& waiters = it->second.waiters;
                const auto waiter = std::ranges::find(waiters, ticket, &Waiter::ticket);
                if (waiter == waiters.end())
                {
                    return;
                }
                abort = std::move(waiter->abort);
                waiters.erase(waiter);
            }
            abort(std::make_error_code(std::errc::operation_canceled));
        }

        /// <summary>
        /// Fails every queued request with <c>operation_canceled</c>. Requests already started are left alone,
        /// and may still call <see cref="Release"/>.
        /// </summary>
        void Close()
        {
            std::vector<Abort> aborts;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                for (auto& [origin, state] : m_origins)
                {
                    for (auto& waiter : state.waiters)
                    {
                        aborts.push_back(std::move(waiter.abort));
                    }
                    state.waiters.clear();
                }
            }
            for (auto& abort : aborts)
            {
                abort(std::make_error_code(std::errc::operation_canceled));
            }
        }

      private:
        /// <summary>
        /// A request waiting for a slot, identified by its ticket so it can be cancelled before it starts.
        /// </summary>
        struct Waiter
        {
            std::uint64_t ticket;
            Start start;
            Abort abort;
        };

        /// <summary>
        /// One origin's running count and its FIFO queue of waiting requests.
        /// </summary>
        struct OriginState
        {
            std::size_t active = 0;
            std::deque<Waiter> waiters;
        };

        std::size_t m_max;
        std::mutex m_mutex;
        std::atomic<std::uint64_t> m_nextTicket{0};
        std::unordered_map<std::string, OriginState> m_origins;
    };
} // namespace AVEVA::Private
