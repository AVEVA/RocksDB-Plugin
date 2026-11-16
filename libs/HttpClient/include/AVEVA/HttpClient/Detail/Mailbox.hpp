// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace AVEVA::Detail {
/// <summary>
/// A thread-safe queue of values with one awaiting receiver at a time. It is the stable-API replacement for
/// boost::asio::experimental::concurrent_channel: any thread may <see cref="Send"/>, and the receiver is resumed on
/// its own executor, so a sender never runs the receiver's code inline.
///
/// A receive completes with the next value, with operation_aborted if the receiver is cancelled (so a deadline or a
/// race against a timer ends the wait at once), or with shut_down once the mailbox is closed and drained. Values
/// sent while nobody is waiting are kept, so a send never needs the receiver to be listening yet.
/// </summary>
template <typename T> class Mailbox {
    using Handler = boost::asio::any_completion_handler<void(boost::system::error_code, T)>;

    struct Waiter {
        Handler Completion;
        boost::asio::cancellation_slot Slot;
        std::uint64_t Id = 0;
    };

    struct State {
        std::mutex Mutex;
        std::deque<T> Values;
        std::optional<Waiter> Pending;
        bool Closed = false;
        std::uint64_t NextId = 0;
    };

    // Holds the state weakly: the slot lives inside the state's waiter, so a strong reference would be a cycle.
    struct Cancel {
        std::weak_ptr<State> Target;
        boost::asio::any_io_executor Fallback;
        std::uint64_t Id;

        void operator()(boost::asio::cancellation_type_t) const {
            const auto state = Target.lock();
            if (!state) {
                return;
            }
            std::optional<Waiter> waiter;
            {
                const std::scoped_lock lock(state->Mutex);
                if (!state->Pending || state->Pending->Id != Id) {
                    return;
                }
                waiter = std::move(state->Pending);
                state->Pending.reset();
            }
            Deliver(std::move(*waiter), boost::asio::error::operation_aborted, T{}, Fallback);
        }
    };

    // Completes on the handler's own executor, never on the caller's thread. The slot is cleared there, before the
    // handler runs, so a later cancellation cannot reach a finished wait.
    static void Deliver(Waiter waiter, const boost::system::error_code error, T value,
                        const boost::asio::any_io_executor& fallback) {
        const auto executor = boost::asio::get_associated_executor(waiter.Completion, fallback);
        // The handler's own executor runs the completion; `fallback` only provides the hop off the caller's stack.
        boost::asio::post(fallback, boost::asio::bind_executor(executor, [waiter = std::move(waiter), error,
                                                                         value = std::move(value)]() mutable {
            waiter.Slot.clear();
            std::move(waiter.Completion)(error, std::move(value));
        }));
    }

    static void Initiate(const std::shared_ptr<State>& state, const boost::asio::any_io_executor& executor,
                         Handler handler) {
        auto slot = boost::asio::get_associated_cancellation_slot(handler);
        std::unique_lock lock(state->Mutex);
        if (state->Pending) {
            lock.unlock();
            Deliver({std::move(handler), {}, 0}, boost::asio::error::already_started, T{}, executor);
            return;
        }
        if (!state->Values.empty()) {
            T value = std::move(state->Values.front());
            state->Values.pop_front();
            lock.unlock();
            Deliver({std::move(handler), {}, 0}, {}, std::move(value), executor);
            return;
        }
        if (state->Closed) {
            lock.unlock();
            Deliver({std::move(handler), {}, 0}, boost::asio::error::shut_down, T{}, executor);
            return;
        }
        const auto id = ++state->NextId;
        state->Pending.emplace(Waiter{std::move(handler), slot, id});
        if (slot.is_connected()) {
            slot.template emplace<Cancel>(Cancel{state, executor, id});
        }
    }

    boost::asio::any_io_executor m_executor;
    std::shared_ptr<State> m_state = std::make_shared<State>();

  public:
    explicit Mailbox(boost::asio::any_io_executor executor) : m_executor(std::move(executor)) {}

    /// <summary>
    /// Queues a value, or hands it to the waiting receiver. Ignored once closed. Throws only if allocation fails,
    /// in which case the value was not delivered.
    /// </summary>
    void Send(T value) {
        std::unique_lock lock(m_state->Mutex);
        if (m_state->Closed) {
            return;
        }
        if (!m_state->Pending) {
            m_state->Values.push_back(std::move(value));
            return;
        }
        auto waiter = std::move(*m_state->Pending);
        m_state->Pending.reset();
        lock.unlock();
        Deliver(std::move(waiter), {}, std::move(value), m_executor);
    }

    /// <summary>
    /// Wakes the waiting receiver with shut_down (after any queued values are received) and rejects further sends.
    /// Throws only if allocation fails.
    /// </summary>
    void Close() {
        std::unique_lock lock(m_state->Mutex);
        m_state->Closed = true;
        if (!m_state->Pending) {
            return;
        }
        auto waiter = std::move(*m_state->Pending);
        m_state->Pending.reset();
        lock.unlock();
        Deliver(std::move(waiter), boost::asio::error::shut_down, T{}, m_executor);
    }

    /// <summary>
    /// Receives the next value. Only one receive may be outstanding; a second completes with already_started.
    /// </summary>
    template <boost::asio::completion_token_for<void(boost::system::error_code, T)> Token>
    auto AsyncReceive(Token&& token) {
        return boost::asio::async_initiate<Token, void(boost::system::error_code, T)>(
            [state = m_state, executor = m_executor](auto handler) { Initiate(state, executor, Handler(std::move(handler))); },
            token);
    }
};
} // namespace AVEVA::Detail
