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
    // Caps in-flight requests per origin. Requests over the cap wait in FIFO order and start as earlier ones
    // finish. A waiting request can still be cancelled, and Close() fails whatever is left when the client dies.
    class OriginLimiter : public std::enable_shared_from_this<OriginLimiter>
    {
      public:
        using Start = std::move_only_function<void()>;
        using Abort = std::move_only_function<void(std::error_code)>;

        explicit OriginLimiter(std::size_t maxPerOrigin) : m_max(maxPerOrigin)
        {
        }

        std::uint64_t NextTicket() noexcept
        {
            return ++m_nextTicket;
        }

        // Runs `start` now if the origin has capacity, otherwise queues it under `ticket` (see Cancel).
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

        // Hands the finished request's slot to the next waiter, or frees it.
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

        // Removes a queued request and fails it; a no-op if it already started.
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
        struct Waiter
        {
            std::uint64_t ticket;
            Start start;
            Abort abort;
        };

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
