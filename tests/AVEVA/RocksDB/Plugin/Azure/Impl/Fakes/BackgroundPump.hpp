// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "FakeHttpClient.hpp"

#include <boost/asio/post.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <thread>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// Runs the fake transport's io_context on a background thread, the way the host runs it in production, so the blocking
// calls of the plugin (which wait on the client's executor) can finish. Declare it after the fake and destroy
// everything that still needs the pump before it.
//
// The pump polls rather than blocking in run(): the fake's own Poll/RunUntil/Complete* helpers serialize on a lock
// that blocking in the context would hold while idle, which would stall every test-side completion.
class BackgroundPump {
  public:
    explicit BackgroundPump(AVEVA::AzureClient::Tests::FakeHttpClient& http)
        : m_thread([this, &http] {
              unsigned int idleRounds = 0;
              while (!m_stop.load()) {
                  if (http.PollRaw() != 0U) {
                      idleRounds = 0;
                  } else if (++idleRounds < 200U) {
                      std::this_thread::yield();
                  } else {
                      std::this_thread::sleep_for(std::chrono::microseconds(200));
                  }
              }
          }) {}
    BackgroundPump(const BackgroundPump&) = delete;
    BackgroundPump& operator=(const BackgroundPump&) = delete;
    ~BackgroundPump() {
        m_stop.store(true);
        m_thread.join();
    }

  private:
    std::atomic<bool> m_stop{false};
    std::thread m_thread;
};
// Returns once everything already queued on the fake's executor, and whatever it queued in turn, has run on the pump
// thread. A test calls it after starting an operation so that the request the operation sends is visible, the way it
// was when the transport completed inline. Does nothing on the pump thread itself.
inline void Settle(AVEVA::AzureClient::Tests::FakeHttpClient& http) {
    const auto executor = http.get_executor();
    if (executor.running_in_this_thread()) {
        return;
    }
    // A chain of continuations ends once a full round trip sends no new request.
    for (std::size_t previous = http.RequestCount() + 1; previous != http.RequestCount();) {
        previous = http.RequestCount();
        auto reached = std::make_shared<std::promise<void>>();
        auto future = reached->get_future();
        boost::asio::post(executor, [reached] { reached->set_value(); });
        if (future.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            return;
        }
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
