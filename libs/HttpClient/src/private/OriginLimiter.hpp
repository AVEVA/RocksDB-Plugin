// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace AVEVA::Private {
/// <summary>
/// Caps in-flight requests per origin. Requests over the cap wait in FIFO order and start as earlier ones
/// finish. A waiting request can still be cancelled, and <see cref="Close"/> fails whatever is left when the
/// owner dies. All operations are thread-safe, and the callbacks it holds are always invoked outside the
/// lock.
/// </summary>
/// <typeparam name="Pending">What is queued for one request; movable. A concrete type chosen by the owner,
/// so waiting requests need no type erasure.</typeparam>
/// <typeparam name="Sink">Decides what happens to a request: <c>Start(Pending&amp;&amp;)</c> begins one that
/// has been given a slot, and <c>Abort(Pending&amp;&amp;, std::error_code)</c> fails one that never got a
/// slot. Copyable, and called outside the lock.</typeparam>
template <typename Pending, typename Sink> class OriginLimiter {
  public:
    /// <summary>
    /// Constructs a limiter with a per-origin concurrency cap.
    /// </summary>
    /// <param name="maxPerOrigin">How many requests may run at once per origin.</param>
    /// <param name="sink">Receives requests that start or are aborted.</param>
    OriginLimiter(std::size_t maxPerOrigin, Sink sink) : m_max(maxPerOrigin), m_sink(std::move(sink)) {}

    /// <summary>
    /// Issues an identifier for one queued request, for use with <see cref="Run"/> and
    /// <see cref="Cancel"/>.
    /// </summary>
    /// <returns>A ticket unique within this limiter.</returns>
    std::uint64_t NextTicket() noexcept { return ++m_nextTicket; }

    /// <summary>
    /// Runs the request now if the origin has capacity, otherwise queues it to be started by a later
    /// <see cref="Release"/> or failed through its abort.
    /// </summary>
    /// <param name="origin">The origin the request counts against.</param>
    /// <param name="ticket">Identifies the request while it waits (see <see cref="Cancel"/>).</param>
    /// <param name="pending">The request; started once, outside the lock, or aborted through the sink. It is
    /// consumed last, so <paramref name="origin"/> may safely refer to one of its fields.</param>
    void Run(const std::string& origin, std::uint64_t ticket, Pending&& pending) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_closed) {
            lock.unlock();
            m_sink.Abort(std::move(pending), std::make_error_code(std::errc::operation_canceled));
            return;
        }
        auto& state = m_origins[origin];
        if (state.active < m_max) {
            ++state.active;
            lock.unlock();
            m_sink.Start(std::move(pending));
            return;
        }
        state.waiters.push_back(Waiter{ticket, std::move(pending)});
    }

    /// <summary>
    /// Hands the finished request's slot to the next waiter, or frees it. Must be called exactly once per
    /// started request.
    /// </summary>
    /// <param name="origin">The origin the finished request counted against.</param>
    void Release(const std::string& origin) {
        std::optional<Pending> next;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_origins.find(origin);
            if (it == m_origins.end()) {
                return;
            }
            // After Close nothing may start: the owner may already be gone.
            if (m_closed) {
                if (it->second.active > 0) {
                    --it->second.active;
                }
                return;
            }
            if (it->second.waiters.empty()) {
                assert(it->second.active > 0);
                if (--it->second.active == 0) {
                    m_origins.erase(it);
                }
                return;
            }
            next.emplace(std::move(it->second.waiters.front().pending));
            it->second.waiters.pop_front();
        }
        m_sink.Start(std::move(*next));
    }

    /// <summary>
    /// Removes a queued request and fails it with <c>operation_canceled</c>; a no-op if it already started
    /// or never existed.
    /// </summary>
    /// <param name="origin">The origin the request was queued under.</param>
    /// <param name="ticket">The ticket the request was queued with.</param>
    void Cancel(const std::string& origin, std::uint64_t ticket) {
        std::optional<Pending> aborted;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed) {
                return;
            }
            const auto it = m_origins.find(origin);
            if (it == m_origins.end()) {
                return;
            }
            auto& waiters = it->second.waiters;
            const auto waiter = std::ranges::find(waiters, ticket, &Waiter::ticket);
            if (waiter == waiters.end()) {
                return;
            }
            aborted.emplace(std::move(waiter->pending));
            waiters.erase(waiter);
        }
        m_sink.Abort(std::move(*aborted), std::make_error_code(std::errc::operation_canceled));
    }

    /// <summary>
    /// Fails every queued request with <c>operation_canceled</c>. Requests already started are left alone,
    /// and may still call <see cref="Release"/>.
    /// </summary>
    void Close() {
        std::vector<Pending> aborts;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
            for (auto& [origin, state] : m_origins) {
                for (auto& waiter : state.waiters) {
                    aborts.push_back(std::move(waiter.pending));
                }
                state.waiters.clear();
            }
        }
        for (auto& pending : aborts) {
            m_sink.Abort(std::move(pending), std::make_error_code(std::errc::operation_canceled));
        }
    }

  private:
    /// <summary>
    /// A request waiting for a slot, identified by its ticket so it can be cancelled before it starts.
    /// </summary>
    struct Waiter {
        std::uint64_t ticket;
        Pending pending;
    };

    /// <summary>
    /// One origin's running count and its FIFO queue of waiting requests.
    /// </summary>
    struct OriginState {
        std::size_t active = 0;
        std::deque<Waiter> waiters;
    };

    std::size_t m_max;
    Sink m_sink;
    std::mutex m_mutex;
    bool m_closed = false;
    std::atomic<std::uint64_t> m_nextTicket{0};
    std::unordered_map<std::string, OriginState> m_origins;
};
} // namespace AVEVA::Private
