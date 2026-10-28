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
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>
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
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
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

// How the loopback service answers a request.
enum class ServerBehavior { Respond, Stall };

// A page-blob service holding `blobSize` bytes, answering ranged GETs. All socket I/O is asynchronous on a private
// io_context run by one thread, so shutdown is posted to that thread: no Asio object is touched from two threads and
// no thread is parked in a blocking call that needs a platform-specific wake-up.
class BlobServer {
  public:
    explicit BlobServer(std::uint64_t blobSize, ServerBehavior behavior = ServerBehavior::Respond)
        : m_blobSize(blobSize), m_behavior(behavior),
          m_acceptor(m_context, tcp::endpoint{asio::ip::address_v4::loopback(), 0}),
          m_port(m_acceptor.local_endpoint().port()), m_work(asio::make_work_guard(m_context)) {
        Accept();
        m_thread = std::thread([this] { m_context.run(); });
    }

    BlobServer(const BlobServer&) = delete;
    BlobServer& operator=(const BlobServer&) = delete;

    ~BlobServer() { Stop(); }

    [[nodiscard]] std::string Endpoint() const { return "http://127.0.0.1:" + std::to_string(m_port); }
    [[nodiscard]] unsigned short Port() const { return m_port; }
    [[nodiscard]] int Requests() const { return m_requests; }
    [[nodiscard]] int Connections() const { return m_connections; }

    // Idempotent. Closes the acceptor and every connection on the server thread, then lets the context drain.
    void Stop() {
        if (m_stopping.exchange(true)) {
            return;
        }
        asio::post(m_context, [this] {
            boost::system::error_code ignored;
            m_acceptor.close(ignored);
            for (auto& session : m_sessions) {
                session->socket.shutdown(tcp::socket::shutdown_both, ignored);
                session->socket.close(ignored);
            }
            m_sessions.clear();
            m_work.reset();
        });
        m_thread.join();
    }

  private:
    struct Session : std::enable_shared_from_this<Session> {
        explicit Session(tcp::socket connection) : socket(std::move(connection)) {}
        tcp::socket socket;
        std::string buffer;
        std::string response;
    };

    void Accept() {
        m_acceptor.async_accept([this](const boost::system::error_code& error, tcp::socket socket) {
            if (error) {
                return;
            }
            ++m_connections;
            auto session = std::make_shared<Session>(std::move(socket));
            m_sessions.push_back(session);
            Read(std::move(session));
            Accept();
        });
    }

    void Read(std::shared_ptr<Session> session) {
        auto& target = *session;
        asio::async_read_until(
            target.socket, asio::dynamic_buffer(target.buffer), "\r\n\r\n",
            [this, session = std::move(session)](const boost::system::error_code& error, std::size_t headSize) mutable {
                if (error) {
                    Forget(session);
                    return;
                }
                const std::string head = Lower(session->buffer.substr(0, headSize));
                session->buffer.erase(0, headSize);
                ++m_requests;
                if (m_behavior == ServerBehavior::Stall) {
                    return;
                }
                session->response = BuildResponse(head);
                auto& target = *session;
                asio::async_write(target.socket, asio::buffer(target.response),
                                  [this, session = std::move(session)](const boost::system::error_code& writeError,
                                                                       std::size_t) mutable {
                                      if (writeError) {
                                          Forget(session);
                                          return;
                                      }
                                      Read(std::move(session));
                                  });
            });
    }

    void Forget(const std::shared_ptr<Session>& session) { std::erase(m_sessions, session); }

    [[nodiscard]] std::string BuildResponse(const std::string& head) const {
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
        return "HTTP/1.1 206 Partial Content\r\nETag: " + std::string{Etag} +
               "\r\nLast-Modified: Sun, 06 Nov 1994 08:49:37 GMT\r\nx-ms-request-id: req\r\n"
               "Content-Range: bytes " +
               std::to_string(first) + '-' + std::to_string(last) + '/' + std::to_string(m_blobSize) +
               "\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
    }

    std::uint64_t m_blobSize;
    ServerBehavior m_behavior;
    asio::io_context m_context;
    tcp::acceptor m_acceptor;
    unsigned short m_port;
    asio::executor_work_guard<asio::io_context::executor_type> m_work;
    std::atomic<bool> m_stopping{false};
    std::atomic<int> m_requests{0};
    std::atomic<int> m_connections{0};
    std::vector<std::shared_ptr<Session>> m_sessions; // server thread only
    std::thread m_thread;
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

    // Stops issuing new reads so that the outstanding ones can be drained after a timeout.
    void Abandon() { m_issued = m_total; }

    [[nodiscard]] std::size_t Outstanding() const { return m_started - m_completed; }

    void Issue(std::size_t slot) {
        ++m_issued;
        ++m_started;
        AVEVA::AzureClient::DownloadBlobOptions options;
        options.Range = AVEVA::AzureClient::Models::BlobByteRange{.Offset = 0, .Length = m_readSize};
        m_starts[slot] = std::chrono::steady_clock::now();
        asio::co_spawn(m_client.get_executor(),
                       m_client.DownloadToAsync(std::span<char>(m_buffers[slot]), std::move(options)),
                       [this, slot](const std::exception_ptr& error, auto result) {
                           if (error) {
                               std::rethrow_exception(error);
                           }
                           Completed(slot, result);
                       });
    }

    template <class Result> void Completed(std::size_t slot, const Result& result) {
        ++m_completed;
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

    // Empty unless every read completed: statistics over a partial run would look like a success.
    [[nodiscard]] std::optional<Measurement> Summarize() {
        if (!Finished() || m_latencies.empty()) {
            return std::nullopt;
        }
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
    std::size_t m_started = 0;
    std::size_t m_completed = 0;
    std::size_t m_failures = 0;
    std::string m_firstFailure;
    std::chrono::steady_clock::time_point m_begin;
    std::chrono::steady_clock::time_point m_end;
    std::vector<std::vector<char>> m_buffers;
    std::vector<std::chrono::steady_clock::time_point> m_starts;
    std::vector<std::chrono::steady_clock::duration> m_latencies;
};

struct RunResult {
    std::optional<Measurement> measurement; // empty when the run did not finish within its bound
    std::size_t failures = 0;
    std::string firstFailure;
    bool timedOut = false;
    int serverRequests = 0;
    int connections = 0;
};

constexpr std::chrono::seconds DefaultBound{30};

// Reads of `readSize` bytes with `inFlight` concurrent requests. One warm-up read per slot opens the connections and
// pays one-time service setup, so the measured reads use pooled keep-alive connections. A run that exceeds `bound`
// is cancelled by stopping the server, drained, and reported as timed out without any statistics.
RunResult Measure(std::size_t readSize, std::size_t inFlight, std::size_t reads,
                  ServerBehavior behavior = ServerBehavior::Respond, std::chrono::milliseconds bound = DefaultBound) {
    BlobServer server(readSize * 4, behavior);
    asio::io_context context;
    AVEVA::HttpClient http(context);
    AVEVA::AzureClient::BlobClientOptions options;
    options.ServiceEndpoint = server.Endpoint();
    options.ContainerName = "container";
    options.BlobName = "blob";
    options.SasToken = "sv=2025-01-05&sig=fake";
    options.AllowInsecureHttp = true;
    if (behavior == ServerBehavior::Stall) {
        // Otherwise the pipeline would retry the closed connection with backoff and outlive the drain bound.
        options.Retry.MaxRetries = 0;
    }
    AVEVA::AzureClient::BlobClient client{http, options};

    const auto runUntil = [&](auto done, std::chrono::milliseconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (!done() && std::chrono::steady_clock::now() < deadline) {
            context.restart();
            context.run_one_for(std::chrono::milliseconds{50});
        }
        return done();
    };

    RunResult result;
    // Abandons the driver, closes the connections so outstanding reads fail, and waits for them to complete before
    // the driver they reference goes out of scope.
    const auto cancelAndDrain = [&](ReadDriver& driver) {
        counting = false;
        driver.Abandon();
        server.Stop();
        if (!runUntil([&] { return driver.Outstanding() == 0; }, std::chrono::seconds{10})) {
            ADD_FAILURE() << "outstanding reads did not drain after the server stopped";
        }
    };
    const auto record = [&](const ReadDriver& driver) {
        result.failures += driver.Failures();
        if (result.firstFailure.empty()) {
            result.firstFailure = driver.FirstFailure();
        }
    };

    ReadDriver warmUp(client, inFlight, inFlight, readSize);
    warmUp.Start();
    if (!runUntil([&] { return warmUp.Finished(); }, bound)) {
        result.timedOut = true;
        cancelAndDrain(warmUp);
        record(warmUp);
        result.serverRequests = server.Requests();
        result.connections = server.Connections();
        return result;
    }
    record(warmUp);

    ReadDriver driver(client, reads, inFlight, readSize);
    allocationCount = 0;
    allocationBytes = 0;
    counting = true;
    driver.Start();
    if (!runUntil([&] { return driver.Finished(); }, bound)) {
        result.timedOut = true;
        cancelAndDrain(driver);
    }
    counting = false;
    record(driver);

    result.measurement = driver.Summarize();
    if (result.measurement) {
        result.measurement->allocations = allocationCount;
        result.measurement->bytes = allocationBytes;
    }
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
        ASSERT_FALSE(run.timedOut) << "benchmark did not finish within " << DefaultBound.count() << "s";
        ASSERT_TRUE(run.measurement.has_value());
        const auto& m = *run.measurement;
        std::cout << "BENCH azure-client read_bytes=" << c.readSize << " concurrency=" << c.inFlight
                  << " reads=" << m.reads << " server_requests=" << run.serverRequests
                  << " connections=" << run.connections << " allocations_per_read=" << m.allocations / m.reads
                  << " bytes_per_read=" << m.bytes / m.reads << " reads_per_second=" << m.readsPerSecond
                  << " mib_per_second=" << m.throughputMiBPerSecond << " p50_us=" << m.p50Micros
                  << " p99_us=" << m.p99Micros << " max_us=" << m.maxMicros << '\n';
        EXPECT_EQ(run.failures, 0U) << run.firstFailure;
        EXPECT_EQ(run.serverRequests, static_cast<int>(reads + c.inFlight));
        // Under concurrency a new read can start before the previous one's connection returns to the pool, so the
        // connection count is reported as evidence rather than bounded; a single stream must reuse one connection.
        if (c.inFlight == 1) {
            EXPECT_EQ(run.connections, 1);
        }
        EXPECT_GT(m.allocations, 0U);
    }
}

// The server must stop promptly with no connection ever made.
TEST(EndToEndAllocationBenchmark, ServerStopsWithNoConnection) {
    BlobServer server(4096);
    server.Stop();
    EXPECT_EQ(server.Connections(), 0);
}

// Idle keep-alive connections that never send a request must not block shutdown.
TEST(EndToEndAllocationBenchmark, ServerStopsWithIdleKeepAliveConnections) {
    BlobServer server(4096);
    asio::io_context context;
    std::vector<tcp::socket> idle;
    for (int i = 0; i < 3; ++i) {
        idle.emplace_back(context);
        idle.back().connect(tcp::endpoint{asio::ip::address_v4::loopback(), server.Port()});
    }
    server.Stop();
}

// A stalled response must turn into a reported timeout, never into statistics, and must still shut down cleanly.
TEST(EndToEndAllocationBenchmark, StalledServerTimesOutWithoutStatistics) {
    const auto run = Measure(4096, 2, 10, ServerBehavior::Stall, std::chrono::milliseconds{300});
    EXPECT_TRUE(run.timedOut);
    EXPECT_FALSE(run.measurement.has_value());
    EXPECT_GE(run.serverRequests, 1);
}
