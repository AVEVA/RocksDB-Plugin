// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Loopback measurement of the whole production request/read path: BlobClient -> request pipeline -> the real
// HttpClient -> a real TCP connection -> a page-blob service running on its own thread. It reports allocations, bytes,
// throughput and tail latency per request for warm, pooled connections. These are CPU/allocation numbers over
// loopback only; they say nothing about Azure network latency (see tools/db_bench for live runs, which must be
// configured explicitly). The binary replaces operator new, so it is a separate executable from the unit tests.
#include "AVEVA/AzureClient/BlobClient.hpp"
#include "AVEVA/AzureClient/BlobClientOptions.hpp"
#include "AVEVA/AzureClient/BlobOperationOptions.hpp"

#include <AVEVA/HttpClient/HttpClient.hpp>

#include <boost/asio/buffer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/version.hpp>

#include <gtest/gtest.h>

#include <openssl/crypto.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {
// Thread-local so the server threads' allocations never count toward the client's.
thread_local bool counting = false;
thread_local std::size_t allocationCount = 0;
thread_local std::size_t allocationBytes = 0;
} // namespace

void* operator new(std::size_t size) {
    if (counting) {
        ++allocationCount;
        allocationBytes += size;
    }
    if (void* memory = std::malloc(size != 0 ? size : 1)) {
        return memory;
    }
    throw std::bad_alloc();
}

void operator delete(void* memory) noexcept { std::free(memory); }

void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

namespace {
using boost::asio::ip::tcp;
namespace asio = boost::asio;

constexpr std::string_view Etag = "\"0x8DBENCH\"";

std::string Lower(std::string text) {
    std::ranges::transform(text, text.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return text;
}

// A page-blob service holding `blobSize` bytes, answering ranged GETs. One thread per connection, blocking I/O.
class BlobServer {
  public:
    explicit BlobServer(std::uint64_t blobSize)
        : m_blobSize(blobSize), m_acceptor(m_context, tcp::endpoint{asio::ip::address_v4::loopback(), 0}),
          m_acceptThread([this] { AcceptLoop(); }) {}

    BlobServer(const BlobServer&) = delete;
    BlobServer& operator=(const BlobServer&) = delete;

    ~BlobServer() { Stop(); }

    [[nodiscard]] std::string Endpoint() const {
        return "http://127.0.0.1:" + std::to_string(m_acceptor.local_endpoint().port());
    }
    [[nodiscard]] int Requests() const { return m_requests; }
    [[nodiscard]] int Connections() const { return m_connections; }

    void Stop() {
        if (m_stopping.exchange(true)) {
            return;
        }
        boost::system::error_code ignored;
        m_acceptor.close(ignored);
        m_acceptThread.join();
        const std::scoped_lock lock(m_mutex);
        for (auto& socket : m_sockets) {
            socket->shutdown(tcp::socket::shutdown_both, ignored);
            socket->close(ignored);
        }
        for (auto& worker : m_workers) {
            worker.join();
        }
        m_workers.clear();
    }

  private:
    void AcceptLoop() {
        for (;;) {
            auto socket = std::make_shared<tcp::socket>(m_context);
            boost::system::error_code error;
            m_acceptor.accept(*socket, error);
            if (error) {
                return;
            }
            const std::scoped_lock lock(m_mutex);
            if (m_stopping) {
                return;
            }
            ++m_connections;
            m_sockets.push_back(socket);
            m_workers.emplace_back([this, socket] { Serve(*socket); });
        }
    }

    void Serve(tcp::socket& socket) {
        std::string buffer;
        for (;;) {
            boost::system::error_code error;
            const auto headSize = asio::read_until(socket, asio::dynamic_buffer(buffer), "\r\n\r\n", error);
            if (error) {
                return;
            }
            const std::string head = Lower(buffer.substr(0, headSize));
            buffer.erase(0, headSize);

            std::uint64_t first = 0;
            std::uint64_t last = m_blobSize - 1;
            constexpr std::string_view RangePrefix = "range: bytes=";
            if (const auto at = head.find(RangePrefix); at != std::string::npos) {
                const char* cursor = head.c_str() + at + RangePrefix.size();
                char* end = nullptr;
                first = std::strtoull(cursor, &end, 10);
                last = std::min(last, std::strtoull(end + 1, nullptr, 10));
            }
            const std::string body(static_cast<std::size_t>(last - first + 1), 'z');
            const std::string response = "HTTP/1.1 206 Partial Content\r\nETag: " + std::string{Etag} +
                                         "\r\nLast-Modified: Sun, 06 Nov 1994 08:49:37 GMT\r\nx-ms-request-id: req\r\n"
                                         "Content-Range: bytes " +
                                         std::to_string(first) + '-' + std::to_string(last) + '/' +
                                         std::to_string(m_blobSize) +
                                         "\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
            ++m_requests;
            asio::write(socket, asio::buffer(response), error);
            if (error) {
                return;
            }
        }
    }

    std::uint64_t m_blobSize;
    asio::io_context m_context;
    tcp::acceptor m_acceptor;
    std::atomic<bool> m_stopping{false};
    std::atomic<int> m_requests{0};
    std::atomic<int> m_connections{0};
    std::mutex m_mutex;
    std::vector<std::shared_ptr<tcp::socket>> m_sockets;
    std::vector<std::thread> m_workers;
    std::thread m_acceptThread;
};

struct Measurement {
    std::size_t reads = 0;
    std::size_t failures = 0;
    std::size_t allocations = 0;
    std::size_t bytes = 0;
    double readsPerSecond = 0;
    double throughputMiBPerSecond = 0;
    double p50Micros = 0;
    double p99Micros = 0;
    double maxMicros = 0;
};

// Keeps `inFlight` ranged reads outstanding until `total` have completed. Every completion runs on the thread that
// drives the io_context, so the driver needs no locking.
class ReadDriver {
  public:
    ReadDriver(AVEVA::AzureClient::BlobClient& client, std::size_t total, std::size_t inFlight, std::size_t readSize)
        : m_client(client), m_total(total), m_inFlight(inFlight), m_readSize(readSize) {
        m_latencies.reserve(total);
        m_buffers.resize(inFlight, std::vector<char>(readSize));
        m_starts.resize(inFlight);
    }

    void Start() {
        m_begin = std::chrono::steady_clock::now();
        for (std::size_t slot = 0; slot < m_inFlight && m_issued < m_total; ++slot) {
            Issue(slot);
        }
    }

    void Issue(std::size_t slot) {
        ++m_issued;
        AVEVA::AzureClient::DownloadBlobOptions options;
        options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0, .Length = m_readSize};
        m_starts[slot] = std::chrono::steady_clock::now();
        m_client.DownloadToAsync(std::span<char>(m_buffers[slot]), std::move(options),
                                 [this, slot](auto result) { Completed(slot, result); });
    }

    template <class Result> void Completed(std::size_t slot, const Result& result) {
        m_latencies.push_back(std::chrono::steady_clock::now() - m_starts[slot]);
        if (!result.has_value() || result->Value().BytesWritten != m_readSize) {
            ++m_failures;
            if (m_firstFailure.empty()) {
                m_firstFailure = result.has_value() ? "short read: " + std::to_string(result->Value().BytesWritten)
                                                    : result.error().Message;
            }
        }
        if (m_issued < m_total) {
            Issue(slot);
        } else if (m_latencies.size() == m_total) {
            m_end = std::chrono::steady_clock::now();
            counting = false;
        }
    }

    [[nodiscard]] std::size_t Failures() const { return m_failures; }
    [[nodiscard]] const std::string& FirstFailure() const { return m_firstFailure; }
    [[nodiscard]] bool Finished() const { return m_latencies.size() == m_total; }

    [[nodiscard]] Measurement Summarize() {
        Measurement m;
        m.reads = m_total;
        m.failures = m_failures;
        std::ranges::sort(m_latencies);
        const auto micros = [&](double fraction) {
            const auto index =
                std::min(m_latencies.size() - 1, static_cast<std::size_t>(fraction * m_latencies.size()));
            return std::chrono::duration<double, std::micro>(m_latencies[index]).count();
        };
        m.p50Micros = micros(0.50);
        m.p99Micros = micros(0.99);
        m.maxMicros = std::chrono::duration<double, std::micro>(m_latencies.back()).count();
        const double seconds = std::chrono::duration<double>(m_end - m_begin).count();
        m.readsPerSecond = static_cast<double>(m_total) / seconds;
        m.throughputMiBPerSecond = static_cast<double>(m_total * m_readSize) / seconds / (1024.0 * 1024.0);
        return m;
    }

  private:
    AVEVA::AzureClient::BlobClient& m_client;
    std::size_t m_total;
    std::size_t m_inFlight;
    std::size_t m_readSize;
    std::size_t m_issued = 0;
    std::size_t m_failures = 0;
    std::string m_firstFailure;
    std::chrono::steady_clock::time_point m_begin;
    std::chrono::steady_clock::time_point m_end;
    std::vector<std::vector<char>> m_buffers;
    std::vector<std::chrono::steady_clock::time_point> m_starts;
    std::vector<std::chrono::steady_clock::duration> m_latencies;
};

struct RunResult {
    Measurement measurement;
    int serverRequests = 0;
    int connections = 0;
};

// Reads of `readSize` bytes with `inFlight` concurrent requests. One warm-up read per slot opens the connections and
// pays one-time service setup, so the measured reads use pooled keep-alive connections.
RunResult Measure(std::size_t readSize, std::size_t inFlight, std::size_t reads) {
    BlobServer server(readSize * 4);
    asio::io_context context;
    AVEVA::HttpClient http(context);
    AVEVA::AzureClient::BlobClientOptions options;
    options.ServiceEndpoint = server.Endpoint();
    options.ContainerName = "container";
    options.BlobName = "blob";
    options.SasToken = "sv=2025-01-05&sig=fake";
    options.AllowInsecureHttp = true;
    AVEVA::AzureClient::BlobClient client{http, options};

    const auto runUntilFinished = [&](ReadDriver& driver) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
        while (!driver.Finished() && std::chrono::steady_clock::now() < deadline) {
            context.restart();
            context.run_one_for(std::chrono::milliseconds{50});
        }
    };

    ReadDriver warmUp(client, inFlight, inFlight, readSize);
    warmUp.Start();
    runUntilFinished(warmUp);
    EXPECT_TRUE(warmUp.Finished());
    EXPECT_EQ(warmUp.Failures(), 0U) << warmUp.FirstFailure();

    ReadDriver driver(client, reads, inFlight, readSize);
    allocationCount = 0;
    allocationBytes = 0;
    counting = true;
    driver.Start();
    runUntilFinished(driver);
    counting = false;
    EXPECT_TRUE(driver.Finished());
    EXPECT_EQ(driver.Failures(), 0U) << driver.FirstFailure();

    RunResult result;
    result.measurement = driver.Summarize();
    result.measurement.allocations = allocationCount;
    result.measurement.bytes = allocationBytes;
    server.Stop();
    result.serverRequests = server.Requests();
    result.connections = server.Connections();
    return result;
}

void PrintEnvironment() {
#ifdef NDEBUG
    constexpr const char* configuration = "Release";
#else
    constexpr const char* configuration = "Debug";
#endif
    std::cout << "BENCH azure-client environment: configuration=" << configuration << " cplusplus="
#ifdef _MSVC_LANG
              << _MSVC_LANG
#else
              << __cplusplus
#endif
#ifdef _MSC_VER
              << " msvc=" << _MSC_VER
#endif
#ifdef __VERSION__
              << " compiler=\"" << __VERSION__ << '"'
#endif
              << " boost=" << BOOST_LIB_VERSION << " openssl=\"" << OpenSSL_version(OPENSSL_VERSION)
              << "\" server=independent-threads network=loopback state=warm-pooled\n";
}
} // namespace

// Loopback allocation/latency evidence for the end-to-end read path, one data point per read size and concurrency.
TEST(EndToEndAllocationBenchmark, PooledRangedReadsThroughTheProductionClientAndHttpStack) {
    PrintEnvironment();
    constexpr std::size_t reads = 200;
    struct Case {
        std::size_t readSize;
        std::size_t inFlight;
    };
    for (const Case c : {Case{4096, 1}, Case{64 * 1024, 1}, Case{64 * 1024, 8}}) {
        const auto run = Measure(c.readSize, c.inFlight, reads);
        const auto& m = run.measurement;
        std::cout << "BENCH azure-client read_bytes=" << c.readSize << " concurrency=" << c.inFlight
                  << " reads=" << m.reads << " server_requests=" << run.serverRequests
                  << " connections=" << run.connections << " allocations_per_read=" << m.allocations / m.reads
                  << " bytes_per_read=" << m.bytes / m.reads << " reads_per_second=" << m.readsPerSecond
                  << " mib_per_second=" << m.throughputMiBPerSecond << " p50_us=" << m.p50Micros
                  << " p99_us=" << m.p99Micros << " max_us=" << m.maxMicros << '\n';
        EXPECT_EQ(m.failures, 0U);
        EXPECT_EQ(run.serverRequests, static_cast<int>(reads + c.inFlight));
        // Under concurrency a new read can start before the previous one's connection returns to the pool, so the
        // connection count is reported as evidence rather than bounded; a single stream must reuse one connection.
        if (c.inFlight == 1) {
            EXPECT_EQ(run.connections, 1);
        }
        EXPECT_GT(m.allocations, 0U);
    }
}
