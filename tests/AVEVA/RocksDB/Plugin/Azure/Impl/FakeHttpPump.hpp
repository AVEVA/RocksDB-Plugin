// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "FakeHttpClient.hpp"

#include <chrono>
#include <stop_token>
#include <thread>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// Runs the fake transport's io_context on a helper thread. Plugin code blocks on futures, so completions the fake
// posts to its own io_context would otherwise never be delivered. The thread stops and joins on destruction.
inline std::jthread StartFakeHttpPump(AVEVA::AzureClient::Tests::FakeHttpClient& httpClient) {
    return std::jthread([&httpClient](const std::stop_token& stop) {
        while (!stop.stop_requested()) {
            httpClient.Poll();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
