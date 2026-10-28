// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Compile-and-run check that consumers can use every client through the public headers alone (T01).
// This target deliberately has no include path into src/ or tests/: only <AVEVA/AzureClient/...>.
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobContainerClient.hpp"
#include "AVEVA/AzureClient/BlobServiceClient.hpp"
#include "AVEVA/AzureClient/PageBlobClient.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>
#include <AVEVA/HttpClient/HttpRequest.hpp>
#include <AVEVA/HttpClient/HttpRequestOptions.hpp>
#include <AVEVA/HttpClient/HttpResponse.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <future>
#include <string>
#include <string_view>
#include <utility>

namespace {
using namespace AVEVA::AzureClient;
using boost::asio::ip::tcp;

[[nodiscard]] std::size_t ContentLengthOf(std::string head) {
    std::ranges::transform(head, head.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    constexpr std::string_view Name = "content-length:";
    const auto at = head.find(Name);
    return at == std::string::npos ? 0U : static_cast<std::size_t>(std::stoull(head.substr(at + Name.size())));
}

// Serves one keep-alive connection: every request succeeds.
boost::asio::awaitable<void> Serve(tcp::socket socket, int* requests) {
    std::string buffer;
    for (;;) {
        const auto [readError, headSize] = co_await boost::asio::async_read_until(
            socket, boost::asio::dynamic_buffer(buffer), "\r\n\r\n", boost::asio::as_tuple(boost::asio::use_awaitable));
        if (readError) {
            co_return;
        }
        const std::string head = buffer.substr(0, headSize);
        buffer.erase(0, headSize);
        const std::size_t bodySize = ContentLengthOf(head);
        if (buffer.size() < bodySize) {
            const auto [bodyError, ignored] = co_await boost::asio::async_read(
                socket, boost::asio::dynamic_buffer(buffer), boost::asio::transfer_exactly(bodySize - buffer.size()),
                boost::asio::as_tuple(boost::asio::use_awaitable));
            if (bodyError) {
                co_return;
            }
        }
        buffer.erase(0, bodySize);

        const bool isList = head.contains("comp=list");
        const std::string body =
            isList ? R"(<?xml version="1.0" encoding="utf-8"?><EnumerationResults></EnumerationResults>)" : "";
        const std::string response = "HTTP/1.1 200 OK\r\nETag: \"etag\"\r\n"
                                     "Last-Modified: Sun, 06 Nov 1994 08:49:37 GMT\r\nx-ms-request-id: req\r\n"
                                     "Content-Length: " +
                                     std::to_string(body.size()) + "\r\n\r\n" + (head.starts_with("HEAD") ? "" : body);
        ++*requests;
        const auto [writeError, written] = co_await boost::asio::async_write(
            socket, boost::asio::buffer(response), boost::asio::as_tuple(boost::asio::use_awaitable));
        if (writeError) {
            co_return;
        }
    }
}

// This target has no include path into the HttpClient tests, so it cannot share ScriptedServer; the loopback server
// here is the only one it needs.
boost::asio::awaitable<void> Accept(tcp::acceptor* acceptor, int* requests) {
    for (;;) {
        auto [error, socket] = co_await acceptor->async_accept(boost::asio::as_tuple(boost::asio::use_awaitable));
        if (error) {
            co_return;
        }
        boost::asio::co_spawn(acceptor->get_executor(), Serve(std::move(socket), requests), boost::asio::detached);
    }
}

// A real HttpClient talking to a loopback server on an ephemeral port, all driven by one io_context.
class Loopback {
  public:
    Loopback() : m_acceptor(m_context, tcp::endpoint{boost::asio::ip::address_v4::loopback(), 0}) {
        boost::asio::co_spawn(m_context, Accept(&m_acceptor, &m_requests), boost::asio::detached);
    }

    [[nodiscard]] AVEVA::HttpClient& Http() noexcept { return m_http; }

    [[nodiscard]] std::string Endpoint() const {
        return "http://127.0.0.1:" + std::to_string(m_acceptor.local_endpoint().port());
    }

    // The acceptor always has a pending accept, so run() would never return. Instead each handler is run to
    // completion and `done` re-checked, so the loop ends as soon as the work finishes; the deadline only bounds a
    // hang and reports it as a failure rather than letting the following assertion fail without context.
    template <class TDone> void RunUntil(TDone done) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
        while (!done()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                ADD_FAILURE() << "loopback operation did not complete within 10s (" << m_requests
                              << " request(s) served)";
                return;
            }
            m_context.restart();
            m_context.run_one_for(std::chrono::milliseconds{50});
        }
    }

    [[nodiscard]] int Requests() const noexcept { return m_requests; }

  private:
    boost::asio::io_context m_context;
    tcp::acceptor m_acceptor;
    AVEVA::HttpClient m_http{m_context};
    int m_requests = 0;
};
constexpr const char* TestSasToken = "sv=2025-01-05&sig=fake";

[[nodiscard]] BlobClientOptions BlobOptions(const Loopback& loopback) {
    BlobClientOptions options;
    options.ServiceEndpoint = loopback.Endpoint();
    options.ContainerName = "container";
    options.BlobName = "blob";
    options.SasToken = TestSasToken;
    options.AllowInsecureHttp = true;
    return options;
}

// Free coroutine taking a pointer rather than a capturing coroutine lambda, so the coroutine
// frame never outlives captured references (cppcoreguidelines-avoid-capturing-lambda-coroutines).
template <class TResult> boost::asio::awaitable<void> AwaitHasValue(bool* out, bool* done, TResult result) {
    *out = (co_await std::move(result)).has_value();
    *done = true;
}

// Runs one operation of client by co_awaiting it in a coroutine on the client's executor.
template <class TClient, class TOperation>
void ExerciseOperation(Loopback& http, TClient& client, TOperation operation) {
    bool ok = false;
    bool done = false;
    boost::asio::co_spawn(client.get_executor(), AwaitHasValue(&ok, &done, operation(client)), boost::asio::detached);
    http.RunUntil([&] { return done; });
    EXPECT_TRUE(ok);
}
} // namespace

TEST(PublicHeaderSelfContainedTests, BlobClient) {
    Loopback http;
    BlobClient client{http.Http(), BlobOptions(http)};
    ExerciseOperation(http, client, [](BlobClient& c) { return c.GetPropertiesAsync(); });
    ExerciseOperation(http, client, [](BlobClient& c) { return c.DeleteAsync(); });
    EXPECT_EQ(http.Requests(), 2);
}

TEST(PublicHeaderSelfContainedTests, PageBlobClient) {
    Loopback http;
    PageBlobClient client{http.Http(), BlobOptions(http)};
    ExerciseOperation(http, client, [](PageBlobClient& c) { return c.DeleteAsync(); });
}

TEST(PublicHeaderSelfContainedTests, BlobContainerClient) {
    Loopback http;
    BlobContainerClientOptions options;
    options.ServiceEndpoint = http.Endpoint();
    options.ContainerName = "container";
    options.SasToken = TestSasToken;
    options.AllowInsecureHttp = true;
    BlobContainerClient client{http.Http(), std::move(options)};
    ExerciseOperation(http, client, [](BlobContainerClient& c) { return c.ListBlobsAsync(); });

    auto childClient = client.GetPageBlobClient("child");
    BlobClient& child = childClient;
    ExerciseOperation(http, child, [](BlobClient& c) { return c.ExistsAsync(); });
}
