// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "DiagnosticsSink.hpp"

#include "AVEVA/HttpClient/Units.hpp"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

namespace AVEVA::Private {
/// <summary>
/// Caps how many requests run against one origin at once. It is an async counting semaphore per origin: a request
/// that finds the origin full suspends in <see cref="Acquire"/> until a running request gives its slot back, so
/// queueing, cancellation and the request deadline all fall out of ordinary coroutine suspension.
/// </summary>
class OriginGate : public std::enable_shared_from_this<OriginGate> {
  public:
    /// <summary>
    /// A held slot. Returning it (by destroying it) hands the slot to the longest-waiting request for the origin,
    /// or frees it if nobody is waiting.
    /// </summary>
    class Slot {
      public:
        Slot() = default;
        Slot(Slot&& other) noexcept : m_gate(std::move(other.m_gate)), m_origin(std::move(other.m_origin)) {}
        Slot& operator=(Slot&& other) noexcept {
            if (this != &other) {
                Return();
                m_gate = std::move(other.m_gate);
                m_origin = std::move(other.m_origin);
            }
            return *this;
        }
        Slot(const Slot&) = delete;
        Slot& operator=(const Slot&) = delete;
        ~Slot() { Return(); }

      private:
        friend class OriginGate;
        Slot(std::shared_ptr<OriginGate> gate, std::string origin) noexcept
            : m_gate(std::move(gate)), m_origin(std::move(origin)) {}

        void Return() noexcept {
            if (m_gate) {
                std::exchange(m_gate, nullptr)->Release(m_origin);
            }
        }

        std::shared_ptr<OriginGate> m_gate;
        std::string m_origin;
    };

    /// <param name="executor">The executor waiting requests are resumed on.</param>
    /// <param name="limit">How many requests may hold a slot for one origin at a time; at least one.</param>
    /// <param name="diagnostics">Where a waiter that could not be woken is reported; null reports nothing.</param>
    OriginGate(boost::asio::any_io_executor executor, ConnectionCount limit,
               std::shared_ptr<const DiagnosticsSink> diagnostics)
        : m_executor(std::move(executor)), m_limit(std::max<std::size_t>(limit.Value(), 1)),
          m_diagnostics(std::move(diagnostics)) {}

    /// <summary>
    /// Waits for a slot on an origin. Waiting is cancellable; the time spent counts against whatever deadline the
    /// caller applies to the awaiting coroutine.
    /// </summary>
    /// <param name="origin">The "scheme://host:port" the slot counts against.</param>
    /// <returns>The slot, or <c>operation_canceled</c> if the gate closed. Throws <c>operation_aborted</c> if the
    /// wait was cancelled.</returns>
    boost::asio::awaitable<std::expected<Slot, std::error_code>> Acquire(std::string origin) {
        // Cancellation is handled below, after the queue entry and any handed-over slot are cleaned up. The flag
        // lives on the bottom-of-stack frame of the whole awaitable thread, not just this frame, so it must be
        // restored to whatever the caller had before every exit instead of simply turned back on here.
        const bool previous = co_await boost::asio::this_coro::throw_if_cancelled();
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        std::shared_ptr<Waiter> waiter;
        std::optional<std::expected<Slot, std::error_code>> immediate;
        {
            const std::scoped_lock lock(m_mutex);
            if (m_lifecycle == Lifecycle::Closed) {
                immediate.emplace(std::unexpected(std::make_error_code(std::errc::operation_canceled)));
            } else if (auto& state = m_origins[origin]; state.active < m_limit) {
                ++state.active;
                immediate.emplace(Slot(shared_from_this(), std::move(origin)));
            } else {
                waiter = std::make_shared<Waiter>(m_executor);
                state.queue.push_back(waiter);
            }
        }
        if (immediate) {
            co_await boost::asio::this_coro::throw_if_cancelled(previous);
            co_return std::move(*immediate);
        }

        const auto [error] = co_await waiter->signal.async_receive(boost::asio::as_tuple(boost::asio::use_awaitable));
        if (!error) {
            Slot slot(shared_from_this(), std::move(origin));
            co_await boost::asio::this_coro::throw_if_cancelled(previous);
            co_return std::move(slot);
        }

        // Cancelled or closed. A slot may still have been handed over in the meantime; if so it must not leak.
        Handover handover = Handover::Waiting;
        {
            const std::scoped_lock lock(m_mutex);
            handover = waiter->handover;
            if (handover == Handover::Waiting) {
                Forget(lock, origin, waiter);
            }
        }
        if (handover == Handover::HandedOver) {
            Release(origin);
        }
        if (error == boost::asio::experimental::error::channel_cancelled ||
            error == boost::asio::error::operation_aborted) {
            co_await boost::asio::this_coro::throw_if_cancelled(previous);
            throw boost::system::system_error(boost::asio::error::operation_aborted);
        }
        co_await boost::asio::this_coro::throw_if_cancelled(previous);
        co_return std::unexpected(std::make_error_code(std::errc::operation_canceled));
    }

    /// <summary>
    /// Fails every request still waiting and every later <see cref="Acquire"/>. Slots already held stay valid; giving
    /// one back after this finds no origin state and does nothing. The whole table is swapped out rather than copied
    /// so closing never allocates and so cannot fail.
    /// </summary>
    void Close() noexcept {
        OriginTable abandoned;
        {
            const std::scoped_lock lock(m_mutex);
            m_lifecycle = Lifecycle::Closed;
            abandoned.swap(m_origins);
        }
        for (const auto& [origin, state] : abandoned) {
            for (const auto& waiter : state.queue) {
                Wake(*waiter, std::make_error_code(std::errc::operation_canceled));
            }
        }
    }

  private:
    using Signal = boost::asio::experimental::concurrent_channel<void(boost::system::error_code)>;

    enum class Handover : std::uint8_t { Waiting, HandedOver };

    struct Waiter {
        explicit Waiter(const boost::asio::any_io_executor& executor) : signal(executor, 1) {}
        Signal signal;                         // Buffered, so waking a waiter never has to wait for it to be listening.
        Handover handover = Handover::Waiting; // Guarded by the gate's mutex.
    };

    struct OriginState {
        std::size_t active = 0;
        std::deque<std::shared_ptr<Waiter>> queue;
    };
    using OriginTable = std::unordered_map<std::string, OriginState>;

    void Forget(const std::scoped_lock<std::mutex>& /*heldMutex*/, const std::string& origin,
                const std::shared_ptr<Waiter>& waiter) {
        const auto it = m_origins.find(origin);
        if (it == m_origins.end()) {
            return;
        }
        std::erase(it->second.queue, waiter);
        if (it->second.active == 0 && it->second.queue.empty()) {
            m_origins.erase(it);
        }
    }

    void Release(const std::string& origin) noexcept {
        std::shared_ptr<Waiter> next;
        {
            const std::scoped_lock lock(m_mutex);
            const auto it = m_origins.find(origin);
            if (it == m_origins.end()) {
                return;
            }
            auto& state = it->second;
            if (state.queue.empty()) {
                if (--state.active == 0) {
                    m_origins.erase(it);
                }
                return;
            }
            // The slot passes straight to the next waiter, so `active` stays as it is.
            next = std::move(state.queue.front());
            state.queue.pop_front();
            next->handover = Handover::HandedOver;
        }
        Wake(*next, {});
    }

    /// <summary>
    /// Hands a waiter its outcome. If posting the wake-up fails (out of memory), closing the channel still wakes it,
    /// with an error; only if that fails too is it left to its own deadline. Either failure is reported.
    /// </summary>
    void Wake(Waiter& waiter, boost::system::error_code outcome) const noexcept {
        try {
            waiter.signal.try_send(outcome);
            return;
        } catch (const std::exception& failure) {
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {},
                                    {}, {}, failure.what());
        }
        try {
            waiter.signal.close();
        } catch (const std::exception& failure) {
            DiagnosticsSink::Report(m_diagnostics, HttpDiagnosticKind::SwallowedException, HttpRequestPhase::Queued, {},
                                    {}, {}, failure.what());
        }
    }

    boost::asio::any_io_executor m_executor;
    std::size_t m_limit;
    std::shared_ptr<const DiagnosticsSink> m_diagnostics;
    std::mutex m_mutex;
    enum class Lifecycle : std::uint8_t { Open, Closed };
    Lifecycle m_lifecycle = Lifecycle::Open;
    OriginTable m_origins;
};
} // namespace AVEVA::Private
