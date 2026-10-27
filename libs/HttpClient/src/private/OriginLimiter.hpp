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
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>
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
/// <typeparam name="Timer">The deadline timer type; <c>steady_timer</c> unless a test substitutes one that can
/// fail.</typeparam>
template <typename Pending, typename Sink, typename Timer = boost::asio::steady_timer>
class OriginLimiter : public std::enable_shared_from_this<OriginLimiter<Pending, Sink, Timer>> {
  public:
    using Clock = std::chrono::steady_clock;

    struct OriginState;

    /// <summary>
    /// A request's origin, as resolved once by <see cref="Run"/>. Passing it to <see cref="Release"/> and
    /// <see cref="Cancel"/> skips re-hashing the origin string and needs no copy of it. It stays valid, though
    /// possibly detached from the limiter, for as long as it is held.
    /// </summary>
    using Handle = std::shared_ptr<OriginState>;

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
    /// <param name="pending">The request; started once, outside the lock, or aborted through the sink. A sink
    /// that also accepts <c>Start(Pending&amp;&amp;, const Handle&amp;)</c> is given the origin's handle.</param>
    /// <param name="deadline">When a still-queued request is aborted with the error given to
    /// <see cref="EnableDeadlines"/>; ignored unless deadlines are enabled.</param>
    /// <returns>The origin's handle, or null if the limiter was already closed.</returns>
    Handle Run(const std::string& origin, std::uint64_t ticket, Pending&& pending,
               Clock::time_point deadline = Clock::time_point::max()) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_closed) {
            lock.unlock();
            m_sink.Abort(std::move(pending), std::make_error_code(std::errc::operation_canceled));
            return nullptr;
        }
        Handle handle = FindOrCreateLocked(origin);
        auto& state = *handle;
        if (state.active < m_max) {
            ++state.active;
            lock.unlock();
            StartPending(std::move(pending), handle);
            return handle;
        }
        // Tickets are issued outside the lock, so two racing submitters can arrive out of order. The queue is
        // ordered by ticket (an insertion that is almost always at the back) and node-based, so Cancel and expiry
        // remove from the middle in logarithmic time without moving the other waiters.
        auto& waiters = state.waiters;
        // If anything after the index entry throws, the entry is dropped again so a request the caller is told
        // failed cannot also be woken or started later.
        const bool indexed = m_timer && deadline != Clock::time_point::max();
        if (indexed) {
            m_expiries.emplace(ExpiryKey{deadline, ticket}, &state);
        }
        try {
            if (indexed) {
                ArmTimerLocked();
            }
            waiters.emplace_hint(waiters.end(), std::piecewise_construct, std::forward_as_tuple(ticket),
                                 std::forward_as_tuple(deadline, std::move(pending)));
        } catch (...) {
            if (indexed) {
                m_expiries.erase(ExpiryKey{deadline, ticket});
            }
            throw;
        }
        return handle;
    }

    /// <summary>
    /// Hands the finished request's slot to the next waiter, or frees it. Must be called exactly once per
    /// started request.
    /// </summary>
    /// <param name="handle">The origin the finished request counted against, as returned by
    /// <see cref="Run"/>.</param>
    void Release(const Handle& handle) {
        if (!handle) {
            return;
        }
        std::optional<Pending> next;
        bool rearmFailed = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto& state = *handle;
            // After Close nothing may start: the owner may already be gone.
            if (m_closed) {
                if (state.active > 0) {
                    --state.active;
                }
                return;
            }
            if (state.waiters.empty()) {
                assert(state.active > 0);
                if (--state.active == 0) {
                    MakeDormantLocked(state);
                }
                return;
            }
            const auto front = state.waiters.begin();
            next.emplace(std::move(front->second.pending));
            m_expiries.erase(ExpiryKey{front->second.deadline, front->first});
            state.waiters.erase(front);
            // The hand-off is committed; a timer failure must not undo it.
            rearmFailed = RearmAfterCommitLocked();
        }
        const auto failure = rearmFailed ? FailUnarmedExpiries() : std::exception_ptr{};
        StartPending(std::move(*next), handle);
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    /// <summary>
    /// Releases a slot by origin name, for callers that did not keep the handle.
    /// </summary>
    /// <param name="origin">The origin the finished request counted against.</param>
    void Release(const std::string& origin) { Release(Lookup(origin)); }

    /// <summary>
    /// Removes a queued request and fails it with <c>operation_canceled</c>; a no-op if it already started
    /// or never existed.
    /// </summary>
    /// <param name="handle">The origin the request was queued under, as returned by <see cref="Run"/>.</param>
    /// <param name="ticket">The ticket the request was queued with.</param>
    void Cancel(const Handle& handle, std::uint64_t ticket) {
        if (!handle) {
            return;
        }
        std::optional<Pending> aborted;
        bool rearmFailed = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed) {
                return;
            }
            auto& waiters = handle->waiters;
            const auto waiter = waiters.find(ticket);
            if (waiter == waiters.end()) {
                return;
            }
            aborted.emplace(std::move(waiter->second.pending));
            m_expiries.erase(ExpiryKey{waiter->second.deadline, ticket});
            waiters.erase(waiter);
            rearmFailed = RearmAfterCommitLocked();
        }
        const auto failure = rearmFailed ? FailUnarmedExpiries() : std::exception_ptr{};
        m_sink.Abort(std::move(*aborted), std::make_error_code(std::errc::operation_canceled));
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    /// <summary>
    /// Cancels by origin name, for callers that did not keep the handle.
    /// </summary>
    /// <param name="origin">The origin the request was queued under.</param>
    /// <param name="ticket">The ticket the request was queued with.</param>
    void Cancel(const std::string& origin, std::uint64_t ticket) { Cancel(Lookup(origin), ticket); }

    /// <summary>
    /// How many times scheduling the deadline timer has failed after a waiter had already left the queue.
    /// </summary>
    std::uint64_t TimerFailures() {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_timerFailures;
    }

    /// <summary>
    /// Fails every queued request with <c>operation_canceled</c>. Requests already started are left alone,
    /// and may still call <see cref="Release"/>.
    /// </summary>
    void Close() {
        // Moving the whole map out is non-throwing, so no allocation can fail between taking the waiters and
        // aborting them. Origins that only had running requests need no entry: Release ignores unknown origins
        // once closed.
        decltype(m_origins) drained;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
            drained = std::move(m_origins);
            m_origins.clear();
            m_dormant = 0;
            m_expiries.clear();
            RearmQuietlyLocked();
        }
        const auto canceled = std::make_error_code(std::errc::operation_canceled);
        // A throwing completion must not abandon the remaining requests; the first failure is rethrown at the end.
        std::exception_ptr failure;
        for (auto& [origin, state] : drained) {
            for (auto& [ticket, waiter] : state->waiters) {
                try {
                    m_sink.Abort(std::move(waiter.pending), canceled);
                } catch (...) {
                    if (!failure) {
                        failure = std::current_exception();
                    }
                }
            }
        }
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

  private:
    using ExpiryKey = std::pair<Clock::time_point, std::uint64_t>;

    /// <summary>
    /// Re-aims the timer after the queue has already changed. The change is committed and the caller must
    /// still complete its request, so a scheduling failure cannot be thrown from here. It is retried once at
    /// once and counted.
    /// </summary>
    /// <returns>True if both attempts failed, so no timer covers the queued deadlines; the caller must then
    /// call <see cref="FailUnarmedExpiries"/> outside the lock.</returns>
    [[nodiscard]] bool RearmAfterCommitLocked() noexcept {
        for (int attempt = 0; attempt < 2; ++attempt) {
            try {
                ArmTimerLocked();
                return false;
            } catch (...) {
                ++m_timerFailures;
            }
        }
        return true;
    }

    /// <summary>
    /// Terminal recovery when no timer can be scheduled: every deadline-bearing waiter is failed with the
    /// expiry error now, because nothing else would wake it before a slot frees, which may be long after its
    /// deadline. Failing early beats missing the deadline silently. Never throws; the first exception from a
    /// completion is returned so the caller can rethrow it after finishing its own work.
    /// </summary>
    [[nodiscard]] std::exception_ptr FailUnarmedExpiries() noexcept {
        ReportTimerFailure();
        std::exception_ptr failure;
        for (;;) {
            std::optional<Pending> doomed;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_closed) {
                    break;
                }
                ExtractExpiredLocked(doomed, Clock::time_point::max());
                if (!doomed) {
                    break;
                }
            }
            try {
                m_sink.Abort(std::move(*doomed), m_expiredError);
            } catch (...) {
                if (!failure) {
                    failure = std::current_exception();
                }
            }
        }
        return failure;
    }

    /// <summary>
    /// Tells the owner, when its sink listens, that deadlines lost their timer, so production runs see it and
    /// not only the test-readable counter.
    /// </summary>
    void ReportTimerFailure() noexcept {
        if constexpr (requires { m_sink.TimerFailed(); }) {
            try {
                m_sink.TimerFailed();
            } catch (...) {
            }
        }
    }

    /// <summary>
    /// Removes the earliest waiter whose deadline is at or before <paramref name="limit"/> from the queue and
    /// the index, skipping index entries whose waiter is already gone.
    /// </summary>
    void ExtractExpiredLocked(std::optional<Pending>& expired, Clock::time_point limit) {
        while (!expired && !m_expiries.empty() && m_expiries.begin()->first.first <= limit) {
            const auto& [key, state] = *m_expiries.begin();
            auto& waiters = state->waiters;
            if (const auto waiter = waiters.find(key.second); waiter != waiters.end()) {
                expired.emplace(std::move(waiter->second.pending));
                waiters.erase(waiter);
            }
            m_expiries.erase(m_expiries.begin());
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
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed) {
                return;
            }
            m_armedFor = Clock::time_point::max();
        }
        const auto now = Clock::now();
        // One request is extracted per lock hold, so nothing allocates between taking a request out of the queue
        // and handing it to the sink; no batch container can fail halfway and strand extracted requests.
        for (;;) {
            std::optional<Pending> expired;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_closed) {
                    return;
                }
                ExtractExpiredLocked(expired, now);
                if (!expired) {
                    if (!RearmAfterCommitLocked()) {
                        return;
                    }
                }
            }
            if (!expired) {
                // No timer could be scheduled for the remaining waiters, and nothing else would wake them.
                if (const auto failure = FailUnarmedExpiries()) {
                    std::rethrow_exception(failure);
                }
                return;
            }
            try {
                m_sink.Abort(std::move(*expired), m_expiredError);
            } catch (...) {
                // Later deadlines must not be stranded by a throwing completion handler.
                bool rearmFailed = false;
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    rearmFailed = RearmAfterCommitLocked();
                }
                if (rearmFailed) {
                    static_cast<void>(FailUnarmedExpiries());
                }
                throw;
            }
        }
    }

    /// <summary>
    /// Re-aims the timer from a context that cannot report failure; if arming fails, the next queue activity
    /// retries it.
    /// </summary>
    void RearmQuietlyLocked() noexcept {
        try {
            ArmTimerLocked();
        } catch (...) {
        }
    }

    /// <summary>
    /// A request waiting for a slot; its ticket, the key it is queued under, lets it be cancelled before it
    /// starts.
    /// </summary>
    struct Waiter {
        Clock::time_point deadline;
        Pending pending;
    };

  public:
    /// <summary>
    /// One origin's running count and its FIFO queue of waiting requests. Idle ones stay in the table, up to a
    /// small cap, so a busy origin does not rebuild its state for every request.
    /// </summary>
    struct OriginState {
        std::string key;
        std::size_t active = 0;
        bool dormant = false;
        std::map<std::uint64_t, Waiter> waiters;
    };

  private:
    static constexpr std::size_t kMaxDormant = 64;

    Handle Lookup(const std::string& origin) {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_origins.find(origin);
        return it == m_origins.end() ? nullptr : it->second;
    }

    Handle FindOrCreateLocked(const std::string& origin) {
        if (const auto it = m_origins.find(origin); it != m_origins.end()) {
            if (it->second->dormant) {
                it->second->dormant = false;
                --m_dormant;
            }
            return it->second;
        }
        auto state = std::make_shared<OriginState>();
        state->key = origin;
        m_origins.emplace(origin, state);
        return state;
    }

    /// <summary>
    /// Parks an origin that has gone idle, or drops it when too many idle ones are already kept. Dropping is
    /// safe for holders of its handle: they only ever find it empty.
    /// </summary>
    void MakeDormantLocked(OriginState& state) noexcept {
        if (m_dormant < kMaxDormant) {
            state.dormant = true;
            ++m_dormant;
            return;
        }
        m_origins.erase(state.key);
    }

    void StartPending(Pending&& pending, const Handle& handle) {
        if constexpr (requires { m_sink.Start(std::move(pending), handle); }) {
            m_sink.Start(std::move(pending), handle);
        } else {
            m_sink.Start(std::move(pending));
        }
    }

    std::size_t m_max;
    Sink m_sink;
    std::mutex m_mutex;
    bool m_closed = false;
    std::atomic<std::uint64_t> m_nextTicket{0};
    std::unordered_map<std::string, Handle> m_origins;
    std::size_t m_dormant = 0;
    std::optional<Timer> m_timer;
    std::error_code m_expiredError;
    std::uint64_t m_timerFailures = 0;
    Clock::time_point m_armedFor = Clock::time_point::max();
    std::map<ExpiryKey, OriginState*> m_expiries; // Queued requests by (deadline, ticket), with their origin.
};
} // namespace AVEVA::Private
