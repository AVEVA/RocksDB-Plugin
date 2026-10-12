// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once

#include "FakeHttpClient.hpp"
#include "TestFixtures.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
// The plugin talks to a concrete AVEVA::HttpClient, so scripting its HTTP traffic means serving real HTTP. This runs
// the real client against a loopback server that hands every request it receives to a FakeHttpClient, which keeps
// its familiar script (EnqueueResponse, RequestAt, ...) and request log. Fixture-built clients are pointed at the
// loopback endpoint for the bridge's lifetime. One thread runs the shared io_context, so the plugin's blocking
// calls complete; a second one polls the fake so completions it posts to its own context are delivered.
class FakeHttpBridge {
  public:
    explicit FakeHttpBridge(AzureClient::Tests::FakeHttpClient& fake)
        : m_fake(fake), m_acceptor(m_context, tcp::endpoint{boost::asio::ip::address_v4::loopback(), 0}),
          m_http(m_context) {
        AzureClient::Tests::ServiceEndpointOverride() = Endpoint();
        boost::asio::co_spawn(m_context, Accept(), boost::asio::detached);
        m_contextThread = std::jthread([this](const std::stop_token& stop) {
            while (!stop.stop_requested()) {
                m_context.restart();
                m_context.run_one_for(std::chrono::milliseconds(20));
            }
        });
        m_pollThread = std::jthread([this](const std::stop_token& stop) {
            while (!stop.stop_requested()) {
                m_fake.Poll();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    ~FakeHttpBridge() {
        m_pollThread.request_stop();
        m_pollThread.join();
        m_contextThread.request_stop();
        m_contextThread.join();
        const std::scoped_lock lock(m_exchangesMutex);
        for (const auto& weak : m_exchanges) {
            if (const auto exchange = weak.lock()) {
                exchange->Signal.reset();
            }
        }
        AzureClient::Tests::ServiceEndpointOverride().clear();
    }

    FakeHttpBridge(const FakeHttpBridge&) = delete;
    FakeHttpBridge& operator=(const FakeHttpBridge&) = delete;
    FakeHttpBridge(FakeHttpBridge&&) = delete;
    FakeHttpBridge& operator=(FakeHttpBridge&&) = delete;

    [[nodiscard]] ::AVEVA::HttpClient& Client() noexcept { return m_http; }
    [[nodiscard]] boost::asio::io_context& Context() noexcept { return m_context; }

    [[nodiscard]] std::string Endpoint() const {
        return "http://127.0.0.1:" + std::to_string(m_acceptor.local_endpoint().port());
    }

  private:
    using tcp = boost::asio::ip::tcp;

    struct Exchange {
        explicit Exchange(boost::asio::io_context& context) : Signal(std::in_place, context) {
            Signal->expires_at(std::chrono::steady_clock::time_point::max());
        }
        // Reset on teardown: the fake can keep a deferred completion (and so this object) alive past the bridge.
        std::optional<boost::asio::steady_timer> Signal;
        std::optional<std::pair<std::error_code, HttpResponse>> Result;
    };

    [[nodiscard]] static std::string Lower(std::string text) {
        std::ranges::transform(text, text.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return text;
    }

    [[nodiscard]] static HttpMethod ParseMethod(std::string_view name) {
        if (name == "PUT") {
            return HttpMethod::Put;
        }
        if (name == "POST") {
            return HttpMethod::Post;
        }
        if (name == "DELETE") {
            return HttpMethod::Delete;
        }
        if (name == "HEAD") {
            return HttpMethod::Head;
        }
        return HttpMethod::Get;
    }

    [[nodiscard]] static std::string_view ReasonFor(unsigned status) {
        switch (status) {
        case 200:
            return "OK";
        case 201:
            return "Created";
        case 202:
            return "Accepted";
        case 206:
            return "Partial Content";
        case 404:
            return "Not Found";
        default:
            return "Status";
        }
    }

    boost::asio::awaitable<void> Accept() {
        for (;;) {
            auto [error, socket] = co_await m_acceptor.async_accept(boost::asio::as_tuple(boost::asio::use_awaitable));
            if (error) {
                co_return;
            }
            boost::asio::co_spawn(m_context, Serve(std::move(socket)), boost::asio::detached);
        }
    }

    // Parses the next request off the wire; empty when the peer closed the connection.
    boost::asio::awaitable<std::optional<HttpRequest>> ReadRequest(tcp::socket& socket, std::string& buffer) {
        const auto [readError, headSize] = co_await boost::asio::async_read_until(
            socket, boost::asio::dynamic_buffer(buffer), "\r\n\r\n", boost::asio::as_tuple(boost::asio::use_awaitable));
        if (readError) {
            co_return std::nullopt;
        }
        const std::string head = buffer.substr(0, headSize);
        buffer.erase(0, headSize);

        HttpRequest request;
        std::istringstream lines(head);
        std::string line;
        std::getline(lines, line);
        std::istringstream requestLine(line);
        std::string method;
        std::string target;
        requestLine >> method >> target;
        request.SetMethod(ParseMethod(method));
        request.SetUrl(Endpoint() + target);

        std::size_t bodySize = 0;
        while (std::getline(lines, line) && line != "\r" && !line.empty()) {
            const auto colon = line.find(':');
            if (colon == std::string::npos) {
                continue;
            }
            std::string name = line.substr(0, colon);
            std::string value = line.substr(colon + 1);
            value.erase(0, value.find_first_not_of(' '));
            while (!value.empty() && (value.back() == '\r' || value.back() == '\n')) {
                value.pop_back();
            }
            if (Lower(name) == "content-length") {
                bodySize = static_cast<std::size_t>(std::stoull(value));
            }
            request.AddHeader(HttpHeader(std::move(name), std::move(value)));
        }

        if (buffer.size() < bodySize) {
            const auto [bodyError, ignored] = co_await boost::asio::async_read(
                socket, boost::asio::dynamic_buffer(buffer), boost::asio::transfer_exactly(bodySize - buffer.size()),
                boost::asio::as_tuple(boost::asio::use_awaitable));
            if (bodyError) {
                co_return std::nullopt;
            }
        }
        request.SetBody(buffer.substr(0, bodySize));
        buffer.erase(0, bodySize);
        co_return request;
    }

    boost::asio::awaitable<void> Serve(tcp::socket socket) {
        std::string buffer;
        for (;;) {
            std::optional<HttpRequest> request = co_await ReadRequest(socket, buffer);
            if (!request) {
                co_return;
            }
            const bool isHead = request->GetMethod() == HttpMethod::Head;

            auto exchange = std::make_shared<Exchange>(m_context);
            {
                const std::scoped_lock lock(m_exchangesMutex);
                std::erase_if(m_exchanges, [](const auto& weak) { return weak.expired(); });
                m_exchanges.push_back(exchange);
            }
            auto completion = [this, exchange](std::error_code error, HttpResponse response) {
                boost::asio::post(m_context, [exchange, error, response = std::move(response)]() mutable {
                    exchange->Result.emplace(error, std::move(response));
                    if (exchange->Signal) {
                        exchange->Signal->cancel();
                    }
                });
            };
            m_fake.SendAsyncErased(std::move(*request), std::move(completion), HttpRequestOptions{});

            while (!exchange->Result) {
                co_await exchange->Signal->async_wait(boost::asio::as_tuple(boost::asio::use_awaitable));
            }
            auto& [error, response] = *exchange->Result;
            if (error) {
                // A transport failure scripted on the fake: drop the connection, as a real server fault would.
                co_return;
            }

            std::string wire = "HTTP/1.1 " + std::to_string(response.GetStatus()) + " " +
                               std::string(ReasonFor(response.GetStatus())) + "\r\n";
            for (const HttpHeader& header : response.GetHeaders()) {
                if (Lower(header.GetName()) != "content-length") {
                    wire += header.GetName() + ": " + header.GetValue() + "\r\n";
                }
            }
            wire += "Content-Length: " + std::to_string(response.GetBody().size()) + "\r\n\r\n";
            if (!isHead) {
                wire += response.GetBody();
            }
            const auto [writeError, written] = co_await boost::asio::async_write(
                socket, boost::asio::buffer(wire), boost::asio::as_tuple(boost::asio::use_awaitable));
            if (writeError) {
                co_return;
            }
        }
    }

    AzureClient::Tests::FakeHttpClient& m_fake;
    boost::asio::io_context m_context;
    tcp::acceptor m_acceptor;
    ::AVEVA::HttpClient m_http;
    std::mutex m_exchangesMutex;
    std::vector<std::weak_ptr<Exchange>> m_exchanges;
    std::jthread m_contextThread;
    std::jthread m_pollThread;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
