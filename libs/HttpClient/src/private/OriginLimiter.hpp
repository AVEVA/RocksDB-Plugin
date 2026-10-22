// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
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
template <typename Pending, typename Sink>
class OriginLimiter : public std::enable_shared_from_this<OriginLimiter<Pending, Sink>> {
  public:
    using Clock = std::chrono::steady_clock;

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
    /// Turns on deadline wake-ups: a queued request whose deadline passes is aborted with
    /// <paramref name="expiredError"/> at that moment instead of waiting for a slot. One timer serves the whole
    /// limiter, armed for the earliest queued deadline. The limiter must be owned by a <c>shared_ptr</c>.
    /// </summary>
    /// <param name="context">The io_context the timer runs on; must outlive the limiter.</param>
    /// <param name="expiredError">The error a request that timed out in the queue is aborted with.</param>
    void EnableDeadlines(boost::asio::io_context& context, std::error_code expiredError) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_timer.emplace(context);
        m_expiredError = expiredError;
    }

    /// <summary>
    /// Runs the request now if the origin has capacity, otherwise queues it to be started by a later
    /// <see cref="Release"/> or failed through its abort.
    /// </summary>
    /// <param name="origin">The origin the request counts against.</param>
    /// <param name="ticket">Identifies the request while it waits (see <see cref="Cancel"/>).</param>
    /// <param name="pending">The request; started once, outside the lock, or aborted through the sink. It is
    /// consumed last, so <paramref name="origin"/> may safely refer to one of its fields.</param>
    /// <param name="deadline">When a still-queued request is aborted with the error given to
    /// <see cref="EnableDeadlines"/>; ignored unless deadlines are enabled.</param>
    void Run(const std::string& origin, std::uint64_t ticket, Pending&& pending,
             Clock::time_point deadline = Clock::time_point::max()) {
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
        // Tickets are issued outside the lock, so two racing submitters can arrive out of order. Keeping the queue
        // sorted by ticket (an insertion that is almost always at the back) lets Cancel binary-search it.
        auto& waiters = state.waiters;
        const auto position = std::ranges::upper_bound(waiters, ticket, {}, &Waiter::ticket);
        // Indexed before the move below, which may empty the string `origin` refers to. If anything after the
        // index entry throws, the entry is dropped again so a request the caller is told failed cannot also
        // be woken or started later.
        const bool indexed = m_timer && deadline != Clock::time_point::max();
        if (indexed) {
            m_expiries.emplace(ExpiryKey{deadline, ticket}, origin);
        }
        try {
            if (indexed) {
                ArmTimerLocked();
            }
            waiters.insert(position, Waiter{ticket, deadline, std::move(pending)});
        } catch (...) {
            if (indexed) {
                m_expiries.erase(ExpiryKey{deadline, ticket});
            }
            throw;
        }
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
            auto& front = it->second.waiters.front();
            ForgetExpiryLocked(front.deadline, front.ticket);
            next.emplace(std::move(front.pending));
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
            const auto waiter = std::ranges::lower_bound(waiters, ticket, {}, &Waiter::ticket);
            if (waiter == waiters.end() || waiter->ticket != ticket) {
                return;
            }
            ForgetExpiryLocked(waiter->deadline, waiter->ticket);
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
            m_expiries.clear();
            ArmTimerLocked();
        }
        for (auto& pending : aborts) {
            m_sink.Abort(std::move(pending), std::make_error_code(std::errc::operation_canceled));
        }
    }

  private:
    using ExpiryKey = std::pair<Clock::time_point, std::uint64_t>;

    /// <summary>
    /// Drops a waiter that is leaving the queue from the deadline index and re-aims the timer.
    /// </summary>
    void ForgetExpiryLocked(Clock::time_point deadline, std::uint64_t ticket) {
        if (m_timer && m_expiries.erase(ExpiryKey{deadline, ticket}) != 0) {
            ArmTimerLocked();
        }
    }

    /// <summary>
    /// Points the single timer at the earliest queued deadline, or stops it when nothing is queued so an idle
    /// limiter never keeps the io_context running.
    /// </summary>
    void ArmTimerLocked() {
        if (!m_timer) {
            return;
        }
        if (m_expiries.empty()) {
            if (m_armedFor != Clock::time_point::max()) {
                m_timer->cancel();
                m_armedFor = Clock::time_point::max();
            }
            return;
        }
        const auto earliest = m_expiries.begin()->first.first;
        if (earliest == m_armedFor) {
            return;
        }
        m_armedFor = earliest;
        try {
            m_timer->expires_at(earliest);
            m_timer->async_wait(
                [weak = std::weak_ptr<OriginLimiter>(this->weak_from_this())](const boost::system::error_code& error) {
                    if (!error) {
                        if (auto self = weak.lock()) {
                            self->OnTimer();
                        }
                    }
                });
        } catch (...) {
            // Not armed after all; forgetting the target lets the next attempt try again.
            m_armedFor = Clock::time_point::max();
            throw;
        }
    }

    /// <summary>
    /// Aborts every queued request whose deadline has passed, then re-arms for the next one.
    /// </summary>
    void OnTimer() {
        std::vector<Pending> expired;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed) {
                return;
            }
            m_armedFor = Clock::time_point::max();
            const auto now = Clock::now();
            while (!m_expiries.empty() && m_expiries.begin()->first.first <= now) {
                const auto& [key, origin] = *m_expiries.begin();
                if (const auto it = m_origins.find(origin); it != m_origins.end()) {
                    auto& waiters = it->second.waiters;
                    const auto waiter = std::ranges::lower_bound(waiters, key.second, {}, &Waiter::ticket);
                    if (waiter != waiters.end() && waiter->ticket == key.second) {
                        expired.push_back(std::move(waiter->pending));
                        waiters.erase(waiter);
                    }
                }
                m_expiries.erase(m_expiries.begin());
            }
            ArmTimerLocked();
        }
        for (auto& pending : expired) {
            m_sink.Abort(std::move(pending), m_expiredError);
        }
    }

    /// <summary>
    /// A request waiting for a slot, identified by its ticket so it can be cancelled before it starts.
    /// </summary>
    struct Waiter {
        std::uint64_t ticket;
        Clock::time_point deadline;
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
    std::optional<boost::asio::steady_timer> m_timer;
    std::error_code m_expiredError;
    Clock::time_point m_armedFor = Clock::time_point::max();
    std::map<ExpiryKey, std::string> m_expiries; // Queued requests by (deadline, ticket), with their origin.
};
} // namespace AVEVA::Private
