// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// Plugin-level measurement (T20): the real plugin file classes (BasicReadableFile with its MultiRead merge/split and
// prefetch slots, the download buffer pool, BasicWriteableFileImpl's write pipeline) over the library's real
// BlobClient request pipeline, talking to an in-process FakeBlobServer through the fake HTTP transport.
//
// What the numbers are, and are not:
//  - No TLS, no sockets, no HttpClient: this is the plugin and Azure-client CPU/allocation cost. HTTP-only and
//    BlobClient-over-TCP costs come from the other two benchmarks (see tools/db_bench); live Azure is never touched.
//  - The fake service runs inline on the calling thread, so its response-body copies are inside the counters. Absolute
//    counts are therefore upper bounds for the plugin; they are meant for before/after comparison on one build.
//  - "thread" counters are scalar operator new on the worker threads that issue the operations, summed. "process"
//    counters cover every thread (including the fake transport's background pump and any plugin worker) but not
//    aligned new, malloc called directly, or memory the OS maps for the stack and the C runtime.
//  - Peak/retained are growth of the process-wide live heap relative to the start of the window, measured with the
//    allocator's usable size. Retained is what is still live when the window ends, i.e. pooled buffers, caches and
//    prefetch results that the file object holds.
//  - Timings are reported, never asserted: this binary replaces operator new, and wall-clock thresholds are noise.
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/ReadableFile.hpp"

#include "FakeBlobEnvironment.hpp"

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/version.hpp>

#include <gtest/gtest.h>

#include <openssl/crypto.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <malloc.h>
#elif defined(__linux__)
#include <malloc.h>
#endif

namespace {
thread_local bool tlsCounting = false;
thread_local std::size_t tlsAllocations = 0;
thread_local std::size_t tlsBytes = 0;

std::atomic<bool> g_window{false};
std::atomic<std::size_t> g_processAllocations{0};
std::atomic<std::size_t> g_processBytes{0};
std::atomic<std::int64_t> g_liveBytes{0};
std::atomic<std::int64_t> g_peakLiveBytes{0};

std::size_t UsableSize(void* memory) {
#ifdef _WIN32
    return _msize(memory);
#elif defined(__linux__)
    return malloc_usable_size(memory);
#else
    (void)memory;
    return 0;
#endif
}

void RaisePeak(const std::int64_t live) {
    auto peak = g_peakLiveBytes.load(std::memory_order_relaxed);
    while (live > peak && !g_peakLiveBytes.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {
    }
}
} // namespace

void* operator new(std::size_t size) {
    void* memory = std::malloc(size != 0 ? size : 1);
    if (memory == nullptr) {
        throw std::bad_alloc();
    }
    if (tlsCounting) {
        ++tlsAllocations;
        tlsBytes += size;
    }
    if (g_window.load(std::memory_order_relaxed)) {
        g_processAllocations.fetch_add(1, std::memory_order_relaxed);
        g_processBytes.fetch_add(size, std::memory_order_relaxed);
        RaisePeak(g_liveBytes.fetch_add(static_cast<std::int64_t>(UsableSize(memory)), std::memory_order_relaxed) +
                  static_cast<std::int64_t>(UsableSize(memory)));
    }
    return memory;
}

void operator delete(void* memory) noexcept {
    if (memory != nullptr && g_window.load(std::memory_order_relaxed)) {
        g_liveBytes.fetch_sub(static_cast<std::int64_t>(UsableSize(memory)), std::memory_order_relaxed);
    }
    std::free(memory);
}

void operator delete(void* memory, std::size_t) noexcept { operator delete(memory); }

namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests {
namespace {
using Clock = std::chrono::steady_clock;

constexpr int64_t MiB = 1024 * 1024;
constexpr int64_t BlobBytes = 128 * MiB;
constexpr int64_t ReadBytes = 4 * MiB;

struct WorkerResult {
    std::vector<double> micros;
    std::size_t allocations = 0;
    std::size_t bytes = 0;
    std::size_t failures = 0;
};

struct Totals {
    std::size_t operations = 0;
    std::size_t failures = 0;
    std::size_t threadAllocations = 0;
    std::size_t threadBytes = 0;
    std::size_t processAllocations = 0;
    std::size_t processBytes = 0;
    std::int64_t peakBytes = 0;
    std::int64_t retainedBytes = 0;
    double seconds = 0;
    double p50Micros = 0;
    double p99Micros = 0;
    std::int64_t payloadBytes = 0;
};

// Runs `operation(thread, index)` (returning true on success) `perThread` times on each of `threads` workers started
// together. Latency storage is reserved before the window opens so it never shows up in the counters.
template <class Operation>
Totals RunWindow(const int threads, const int perThread, const int64_t payloadPerOperation, Operation operation) {
    std::vector<WorkerResult> results(static_cast<std::size_t>(threads));
    for (auto& result : results) {
        result.micros.reserve(static_cast<std::size_t>(perThread));
    }
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(threads));
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            auto& result = results[static_cast<std::size_t>(t)];
            ++ready;
            while (!go.load()) {
                std::this_thread::yield();
            }
            for (int i = 0; i < perThread; ++i) {
                tlsAllocations = 0;
                tlsBytes = 0;
                tlsCounting = true;
                const auto begin = Clock::now();
                const bool ok = operation(t, i);
                const auto end = Clock::now();
                tlsCounting = false;
                result.allocations += tlsAllocations;
                result.bytes += tlsBytes;
                result.failures += ok ? 0U : 1U;
                result.micros.push_back(std::chrono::duration<double, std::micro>(end - begin).count());
            }
        });
    }
    while (ready.load() != threads) {
        std::this_thread::yield();
    }
    g_processAllocations = 0;
    g_processBytes = 0;
    g_liveBytes = 0;
    g_peakLiveBytes = 0;
    const auto begin = Clock::now();
    g_window = true;
    go = true;
    for (auto& worker : workers) {
        worker.join();
    }
    const auto end = Clock::now();
    g_window = false;

    Totals totals;
    std::vector<double> all;
    all.reserve(static_cast<std::size_t>(threads) * static_cast<std::size_t>(perThread));
    for (const auto& result : results) {
        totals.threadAllocations += result.allocations;
        totals.threadBytes += result.bytes;
        totals.failures += result.failures;
        all.insert(all.end(), result.micros.begin(), result.micros.end());
    }
    std::ranges::sort(all);
    const auto at = [&](const double fraction) {
        return all[std::min(all.size() - 1, static_cast<std::size_t>(fraction * all.size()))];
    };
    totals.operations = all.size();
    totals.p50Micros = at(0.50);
    totals.p99Micros = at(0.99);
    totals.seconds = std::chrono::duration<double>(end - begin).count();
    totals.processAllocations = g_processAllocations;
    totals.processBytes = g_processBytes;
    totals.peakBytes = g_peakLiveBytes;
    totals.retainedBytes = g_liveBytes;
    totals.payloadBytes = payloadPerOperation * static_cast<int64_t>(all.size());
    return totals;
}

struct Requests {
    int Downloads = 0;
    int Uploads = 0;
    int Properties = 0;
};

Requests Snapshot(const FakeBlobServer& server) {
    return {static_cast<int>(server.Downloads.size()), static_cast<int>(server.Uploads.size()),
            static_cast<int>(server.PropertiesRequests)};
}

void Report(const char* workload, const int threads, const Totals& t, const Requests& before, const Requests& after) {
    const double n = static_cast<double>(t.operations);
    std::cout << "BENCH plugin workload=" << workload << " concurrency=" << threads << " ops=" << t.operations
              << " downloads=" << after.Downloads - before.Downloads << " uploads=" << after.Uploads - before.Uploads
              << " properties=" << after.Properties - before.Properties
              << " thread_allocs_per_op=" << static_cast<double>(t.threadAllocations) / n
              << " thread_bytes_per_op=" << static_cast<double>(t.threadBytes) / n
              << " process_allocs_per_op=" << static_cast<double>(t.processAllocations) / n
              << " process_bytes_per_op=" << static_cast<double>(t.processBytes) / n
              << " peak_live_bytes=" << t.peakBytes << " retained_bytes=" << t.retainedBytes
              << " mib_per_second=" << static_cast<double>(t.payloadBytes) / static_cast<double>(MiB) / t.seconds
              << " p50_us=" << t.p50Micros << " p99_us=" << t.p99Micros << '\n';
}

void PrintEnvironment() {
#ifdef NDEBUG
    constexpr const char* configuration = "Release";
#else
    constexpr const char* configuration = "Debug";
#endif
    std::cout << "BENCH plugin environment: configuration=" << configuration
#ifdef _MSC_VER
              << " msvc=" << _MSC_VER
#endif
#ifdef __VERSION__
              << " compiler=\"" << __VERSION__ << '"'
#endif
              << " boost=" << BOOST_LIB_VERSION << " openssl=\"" << OpenSSL_version(OPENSSL_VERSION)
              << "\" transport=in-process-fake-http(no TLS, no sockets) service=FakeBlobServer(inline, counted)"
              << " counters=operator-new-only(thread:issuing workers; process:all threads)\n";
}

class PluginPathBenchmark : public ::testing::Test {
  protected:
    FakeBlobEnvironment m_env;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

    void SetUp() override {
        // Per-operation debug logging would dominate the counters and the output.
        boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::warning);
    }

    void FillBlob() {
        m_env.Fill(BlobBytes);
        m_env.Blob->SetETag("etag");
    }

    std::shared_ptr<FakeReadableFileImpl> MakeImpl() {
        return std::make_shared<FakeReadableFileImpl>("bench.sst", m_env.Runtime, m_env.Client, nullptr, m_logger);
    }

    // The measured loop for a single read-style workload; each worker has its own scratch, allocated up front.
    // Returns the number of download requests the window sent to the service.
    template <class Operation>
    int RunReads(const char* workload, const int threads, const int perThread, const int64_t payload,
                 Operation operation) {
        PrintEnvironmentOnce();
        const auto before = Snapshot(*m_env.Blob);
        const auto totals = RunWindow(threads, perThread, payload, operation);
        const auto after = Snapshot(*m_env.Blob);
        Report(workload, threads, totals, before, after);
        EXPECT_EQ(totals.failures, 0U);
        EXPECT_GT(totals.threadAllocations, 0U);
        return after.Downloads - before.Downloads;
    }

    static void PrintEnvironmentOnce() {
        static const bool printed = (PrintEnvironment(), true);
        (void)printed;
    }
};

// 4 MiB random reads, single stream and under concurrency.
TEST_F(PluginPathBenchmark, RandomRead4MiB) {
    FillBlob();
    for (const int threads : {1, 4}) {
        FakeReadableFile file{MakeImpl()};
        std::vector<std::vector<char>> scratch(static_cast<std::size_t>(threads),
                                               std::vector<char>(static_cast<std::size_t>(ReadBytes)));
        RunReads("random-read-4MiB", threads, 16, ReadBytes, [&](const int t, const int i) {
            rocksdb::Slice result;
            const uint64_t offset = static_cast<uint64_t>((t * 16 + i) % 31) * ReadBytes + 123;
            const auto status = file.Read(offset, ReadBytes, rocksdb::IOOptions{}, &result,
                                          scratch[static_cast<std::size_t>(t)].data(), nullptr);
            return status.ok() && result.size() == static_cast<std::size_t>(ReadBytes);
        });
    }
}

// Sixteen 4 KiB requests with 1 KiB gaps: MultiRead merges them into one download and splits the bytes back out.
TEST_F(PluginPathBenchmark, MultiReadMergedRanges) {
    FillBlob();
    constexpr int requestCount = 16;
    constexpr uint64_t length = 4096;
    constexpr uint64_t stride = length + 1024;
    for (const int threads : {1, 4}) {
        FakeReadableFile file{MakeImpl()};
        std::vector<std::vector<char>> scratch(static_cast<std::size_t>(threads * requestCount),
                                               std::vector<char>(length));
        std::vector<rocksdb::FSReadRequest> requests(static_cast<std::size_t>(threads * requestCount));
        const int downloads =
            RunReads("multiread-merged-16x4KiB", threads, 32, requestCount * length, [&](const int t, const int i) {
                const std::span<rocksdb::FSReadRequest> batch(
                    requests.data() + static_cast<std::size_t>(t * requestCount), requestCount);
                const uint64_t base =
                    static_cast<uint64_t>(t * 32 + i) * (stride * requestCount) % (BlobBytes - stride * requestCount);
                for (int r = 0; r < requestCount; ++r) {
                    auto& request = batch[static_cast<std::size_t>(r)];
                    request.offset = base + static_cast<uint64_t>(r) * stride;
                    request.len = length;
                    request.scratch = scratch[static_cast<std::size_t>(t * requestCount + r)].data();
                }
                if (!file.MultiRead(batch.data(), batch.size(), rocksdb::IOOptions{}, nullptr).ok()) {
                    return false;
                }
                return std::ranges::all_of(
                    batch, [](const rocksdb::FSReadRequest& r) { return r.status.ok() && r.result.size() == length; });
            });
        EXPECT_EQ(downloads, threads * 32) << "one merged download per batch";
    }
}

// Reads whose range a completed prefetch already holds must not touch the service. A file keeps at most two prefetch
// slots, so each file object holds two prefetched regions and the window reads every one of them once.
TEST_F(PluginPathBenchmark, PrefetchHit4MiB) {
    FillBlob();
    constexpr int slotsPerFile = 2;
    constexpr int regions = 16;
    for (const int threads : {1, 4}) {
        std::vector<std::shared_ptr<FakeReadableFileImpl>> impls;
        std::vector<FakeReadableFile> files;
        for (int f = 0; f < regions / slotsPerFile; ++f) {
            impls.push_back(MakeImpl());
            files.emplace_back(impls.back());
            for (int s = 0; s < slotsPerFile; ++s) {
                const auto offset = static_cast<uint64_t>(f * slotsPerFile + s) * ReadBytes;
                ASSERT_TRUE(files.back().Prefetch(offset, ReadBytes, rocksdb::IOOptions{}, nullptr).ok());
            }
        }
        for (int r = 0; r < regions; ++r) {
            while (impls[static_cast<std::size_t>(r / slotsPerFile)]->HasPendingPrefetch(
                static_cast<int64_t>(r) * ReadBytes, 1)) {
                std::this_thread::yield();
            }
        }
        std::vector<std::vector<char>> scratch(static_cast<std::size_t>(threads),
                                               std::vector<char>(static_cast<std::size_t>(ReadBytes)));
        const int perThread = regions / threads;
        const int downloads =
            RunReads("prefetch-hit-4MiB", threads, perThread, ReadBytes, [&](const int t, const int i) {
                rocksdb::Slice result;
                const int region = t * perThread + i;
                const auto status = files[static_cast<std::size_t>(region / slotsPerFile)].Read(
                    static_cast<uint64_t>(region) * ReadBytes, ReadBytes, rocksdb::IOOptions{}, &result,
                    scratch[static_cast<std::size_t>(t)].data(), nullptr);
                return status.ok() && result.size() == static_cast<std::size_t>(ReadBytes);
            });
        EXPECT_EQ(downloads, 0) << "prefetch hits must not download";
    }
}
// Append 4 KiB then Sync, repeatedly: the small durable-write pattern of a WAL.
TEST_F(PluginPathBenchmark, SmallAppendSync4KiB) {
    m_env.Blob->SetCapacity(64 * MiB);
    FakeWriteableFileImpl file{"bench.log", m_env.Runtime, m_env.Client,
                               nullptr,     m_logger,      Configuration::PageBlob::DefaultBufferSize};
    const std::vector<char> record(4096, 'w');
    constexpr int operations = 64;
    PrintEnvironmentOnce();
    const auto before = Snapshot(*m_env.Blob);
    const auto totals = RunWindow(1, operations, static_cast<int64_t>(record.size()), [&](int, int) {
        file.Append(record);
        file.Sync();
        return true;
    });
    Report("small-append-sync-4KiB", 1, totals, before, Snapshot(*m_env.Blob));
    EXPECT_EQ(totals.failures, 0U);
    EXPECT_GT(m_env.Blob->Uploads.size(), 0U);
    EXPECT_EQ(file.GetFileSize(), operations * static_cast<int64_t>(record.size()));
    file.Close();
}
} // namespace
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl::Tests
