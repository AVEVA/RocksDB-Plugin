// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include <atomic>
#include "Timeouts.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <utility>

namespace AVEVA::Private
{
    // Closes expired idle pooled sockets even when no request arrives. It sleeps until a pool gains an idle
    // connection, and goes back to sleep once both pools are empty. A dedicated thread is used instead of an
    // io_context timer so a pending timer cannot keep the caller's io_context::run() from returning.
    class IdleSweeper
    {
      public:
        using SweepFunction = std::function<std::size_t()>;

        IdleSweeper(SweepFunction plainSweep, SweepFunction tlsSweep, std::chrono::seconds idleTimeout)
            : m_plainSweep(std::move(plainSweep)), m_tlsSweep(std::move(tlsSweep)),
              m_interval(std::clamp(idleTimeout, std::chrono::seconds{1}, std::chrono::seconds{EffectivelyInfinite}))
        {
        }

        IdleSweeper(const IdleSweeper&) = delete;
        IdleSweeper& operator=(const IdleSweeper&) = delete;

        // Called when a pool gains an idle connection; schedules a sweep unless one is already pending.
        void Notify()
        {
            if (!m_scheduled.exchange(true))
            {
                SweepScheduler::Instance().Schedule(m_self, m_interval);
            }
        }

        // Runs on the scheduler thread. The flag is cleared before sweeping so a release that lands during the
        // sweep schedules itself instead of being lost.
        void RunSweep()
        {
            m_scheduled = false;
            if (m_plainSweep() + m_tlsSweep() > 0 && !m_scheduled.exchange(true))
            {
                SweepScheduler::Instance().Schedule(m_self, m_interval);
            }
        }

        // Must be called once the sweeper is owned by a shared_ptr.
        void BindSelf(const std::shared_ptr<IdleSweeper>& self)
        {
            m_self = self;
        }

      private:
        // One thread for the whole process, started on first use, that runs every client's sweeps in deadline
        // order. A per-client thread would cost a stack for each of many short-lived clients.
        class SweepScheduler
        {
          public:
            static SweepScheduler& Instance()
            {
                static SweepScheduler scheduler;
                return scheduler;
            }

            void Schedule(const std::weak_ptr<IdleSweeper>& sweeper, std::chrono::seconds delay)
            {
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    m_queue.emplace(Clock::now() + delay, sweeper);
                    if (!m_thread.joinable())
                    {
                        m_thread = std::jthread([this](std::stop_token stop)
                        {
                            Run(stop);
                        });
                    }
                }
                m_wake.notify_all();
            }

            ~SweepScheduler()
            {
                m_thread.request_stop();
                m_wake.notify_all();
            }

          private:
            using Clock = std::chrono::steady_clock;

            void Run(std::stop_token stop)
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                while (!stop.stop_requested())
                {
                    if (m_queue.empty())
                    {
                        m_wake.wait(lock, stop, [this] { return !m_queue.empty(); });
                        continue;
                    }
                    const auto due = m_queue.begin()->first;
                    if (Clock::now() < due)
                    {
                        m_wake.wait_until(lock, stop, due, [] { return false; });
                        continue;
                    }
                    auto sweeper = m_queue.begin()->second.lock();
                    m_queue.erase(m_queue.begin());
                    if (!sweeper)
                    {
                        continue;
                    }
                    lock.unlock();
                    try
                    {
                        sweeper->RunSweep();
                    }
                    catch (...)
                    {
                        // A failing sweep must not end the thread that serves every other client.
                    }
                    sweeper.reset();
                    lock.lock();
                }
            }

            std::mutex m_mutex;
            std::condition_variable_any m_wake;
            std::multimap<Clock::time_point, std::weak_ptr<IdleSweeper>> m_queue;
            std::jthread m_thread; // Declared last so every other member exists before the thread starts.
        };

        SweepFunction m_plainSweep;
        SweepFunction m_tlsSweep;
        std::chrono::seconds m_interval;
        std::atomic<bool> m_scheduled{false};
        std::weak_ptr<IdleSweeper> m_self;
    };
} // namespace AVEVA::Private
