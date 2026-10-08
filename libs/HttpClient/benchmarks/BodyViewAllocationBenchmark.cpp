// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Manual allocation benchmark for Task 12 (HttpRequest::SetBodyView).
//
// This is intentionally not built on a benchmarking framework: it exists purely to demonstrate,
// via a global operator new/delete override, that sending a large request body with SetBodyView() or
// SetBody() makes the client allocate nothing proportional to the payload size (an owned body is moved
// into the operation, not copied). Run the produced executable
// directly; it prints byte counts and exits non-zero if the expectation is violated.

#include "AVEVA/HttpClient/HttpClient.hpp"
#include "AVEVA/HttpClient/HttpRequest.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/write.hpp>

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <span>
#include <string>
#include <vector>

namespace
{
    std::atomic<std::size_t> g_bytesAllocated{0};
    std::atomic<bool> g_tracking{false};
} // namespace

void* operator new(std::size_t size)
{
    if (g_tracking.load(std::memory_order_relaxed))
    {
        g_bytesAllocated.fetch_add(size, std::memory_order_relaxed);
    }
    void* ptr = std::malloc(size == 0 ? 1 : size);
    if (ptr == nullptr)
    {
        throw std::bad_alloc();
    }
    return ptr;
}

void operator delete(void* ptr) noexcept
{
    std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept
{
    std::free(ptr);
}

namespace
{
    namespace asio = boost::asio;
    using Tcp = asio::ip::tcp;

    // Bytes allocated (as measured by the global operator new override above) while sending
    // `request` (with a body of `payloadSize` bytes, however it is represented) to a throwaway
    // local server. The server never reads the request at all -- it just grows its socket receive
    // buffer large enough to absorb the whole request without backpressure and replies
    // immediately -- so its own behavior can't allocate proportionally to the payload and
    // pollute the measurement. Only the client side is under test here.
    std::size_t MeasureAllocatedBytes(AVEVA::HttpRequest request, std::size_t payloadSize)
    {
        asio::io_context context;
        Tcp::acceptor acceptor(context, {asio::ip::make_address("127.0.0.1"), 0});
        Tcp::socket socket(context);
        const std::string canned = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

        acceptor.async_accept(socket, [&](boost::system::error_code error)
        {
            if (error)
            {
                return;
            }
            boost::system::error_code ignored;
            socket.set_option(asio::socket_base::receive_buffer_size(
                                   static_cast<int>(payloadSize * 2)),
                ignored);
            asio::async_write(socket, asio::buffer(canned), [](boost::system::error_code, std::size_t) {});
        });

        auto client = AVEVA::IHttpClient::Create(context);
        request.SetUrl("http://127.0.0.1:" + std::to_string(acceptor.local_endpoint().port()) + "/");

        g_bytesAllocated.store(0, std::memory_order_relaxed);
        g_tracking.store(true, std::memory_order_relaxed);

        client->SendAsync(std::move(request), [&](std::error_code, AVEVA::HttpResponse)
        {
            boost::system::error_code ignored;
            socket.shutdown(Tcp::socket::shutdown_both, ignored);
            socket.close(ignored);
            acceptor.close(ignored);
        });

        context.run();
        g_tracking.store(false, std::memory_order_relaxed);
        return g_bytesAllocated.load(std::memory_order_relaxed);
    }
} // namespace

int main()
{
    constexpr std::size_t payloadSize = 4u * 1024u * 1024u; // 4 MiB

    std::string ownedPayload(payloadSize, 'x');
    AVEVA::HttpRequest ownedRequest;
    ownedRequest.SetMethod(AVEVA::HttpMethod::Post);
    ownedRequest.SetBody(ownedPayload);
    const std::size_t bytesWithSetBody = MeasureAllocatedBytes(std::move(ownedRequest), payloadSize);

    std::vector<std::byte> viewPayload(payloadSize, std::byte{'x'});
    AVEVA::HttpRequest viewRequest;
    viewRequest.SetMethod(AVEVA::HttpMethod::Post);
    viewRequest.SetBodyView(viewPayload);
    const std::size_t bytesWithSetBodyView = MeasureAllocatedBytes(std::move(viewRequest), payloadSize);

    std::printf("SetBody:     %zu bytes allocated for a %zu byte payload\n", bytesWithSetBody, payloadSize);
    std::printf("SetBodyView: %zu bytes allocated for a %zu byte payload\n", bytesWithSetBodyView, payloadSize);

    // The client moves an owned body out of the request instead of copying it, so neither representation
    // may allocate anything proportional to the payload once the request has been handed over -- allow a
    // generous fixed slack for unrelated bookkeeping allocations (resolver/socket internals).
    constexpr std::size_t slack = 64u * 1024u;
    const bool ok = bytesWithSetBody < slack && bytesWithSetBodyView < slack;
    if (!ok)
    {
        std::fprintf(stderr, "FAILED: expected the client to send the body without a payload-sized allocation\n");
        return 1;
    }
    std::printf("OK: neither SetBody nor SetBodyView allocated a payload-sized buffer inside the client\n");
    return 0;
}
