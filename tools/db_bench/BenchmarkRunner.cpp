// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// AVEVA db_bench wrapper: registers the Azure Page Blob filesystem plugin
// with the RocksDB ObjectLibrary and then hands control to the stock
// rocksdb::db_bench_tool. Configure the plugin via environment variables:
//
//   AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL  (storage account endpoint)
//   AVEVA_DB_BENCH_CONTAINER            (blob container name; used as dbName)
//   AVEVA_DB_BENCH_TENANT_ID            (Service Principal auth)
//   AVEVA_DB_BENCH_CLIENT_ID            (Service Principal auth)
//   AVEVA_DB_BENCH_CLIENT_SECRET        (Service Principal auth)
//
// Optional: AVEVA_DB_BENCH_IO_THREADS sets how many threads run the io_context (default: the hardware thread count,
// at least 2).
//
// All five variables are required; the tool exits with status 2 when one is missing.
//
// Pass RocksDB db_bench flags on the command line as usual. To route I/O
// through the plugin add:
//
//   --fs_uri=auto
//   --db=<account>+<container>/<db-subpath>
//
// `--fs_uri=auto` is replaced with the registered plugin name (Plugin::NameFor).
//
// See tools/db_bench/README.md for concrete invocation examples.

#include <AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/convenience.h>
#include <rocksdb/db_bench_tool.h>
#include <rocksdb/env.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

std::optional<std::string> Env(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return std::nullopt;
    }
    return std::string{value};
}

std::string Require(const char* name, const std::optional<std::string>& value) {
    if (!value) {
        std::cerr << "aveva_db_bench: missing required environment variable '" << name << "'\n";
        std::exit(2);
    }
    return *value;
}

// Runs the host io_context the plugin blocks on. The plugin is called from RocksDB threads, so the context must be run
// by dedicated threads that never call into RocksDB. The destructor stops the context and joins the threads, so they
// never observe a destroyed context.
class IoThreadPool {
  public:
    explicit IoThreadPool(const unsigned threadCount)
        : m_context(std::make_shared<boost::asio::io_context>()),
          m_workGuard(boost::asio::make_work_guard(*m_context)) {
        m_threads.reserve(threadCount);
        for (unsigned i = 0; i < threadCount; ++i) {
            m_threads.emplace_back([context = m_context] { context->run(); });
        }
    }
    IoThreadPool(const IoThreadPool&) = delete;
    IoThreadPool& operator=(const IoThreadPool&) = delete;
    IoThreadPool(IoThreadPool&&) = delete;
    IoThreadPool& operator=(IoThreadPool&&) = delete;
    ~IoThreadPool() {
        m_workGuard.reset();
        m_context->stop();
        m_threads.clear(); // jthread joins
    }

    [[nodiscard]] boost::asio::io_context& Context() noexcept { return *m_context; }

  private:
    std::shared_ptr<boost::asio::io_context> m_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_workGuard;
    std::vector<std::jthread> m_threads;
};

// Registers the plugin and runs db_bench, returning the process exit code. The io threads are scoped to this call, so
// they are joined (after the plugin objects are released) before main skips static destructors.
int Run(int argc, char** argv) {
    using AVEVA::RocksDB::Plugin::Azure::Plugin;

    using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

    const auto storageAccountUrl =
        Require("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL", Env("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL"));
    const auto container = Require("AVEVA_DB_BENCH_CONTAINER", Env("AVEVA_DB_BENCH_CONTAINER"));
    const auto tenantId = Require("AVEVA_DB_BENCH_TENANT_ID", Env("AVEVA_DB_BENCH_TENANT_ID"));
    const auto clientId = Require("AVEVA_DB_BENCH_CLIENT_ID", Env("AVEVA_DB_BENCH_CLIENT_ID"));
    const auto clientSecret = Require("AVEVA_DB_BENCH_CLIENT_SECRET", Env("AVEVA_DB_BENCH_CLIENT_SECRET"));

    namespace Models = AVEVA::RocksDB::Plugin::Azure::Models;
    const Models::AccountUrl accountUrl{storageAccountUrl};
    const Models::DbName dbName{container};
    const Plugin::StorageAccounts accounts = Plugin::AccountPair<ServicePrincipalStorageInfo>{
        ServicePrincipalStorageInfo{dbName, accountUrl, Models::ClientId{clientId}, Models::ClientSecret{clientSecret},
                                    Models::TenantId{tenantId}},
        std::nullopt};

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::warning);

    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

    // AVEVA_DB_BENCH_IO_THREADS overrides the io_context thread count, to test whether the shared pool limits throughput.
    unsigned ioThreadCount = std::max(2U, std::thread::hardware_concurrency());
    if (const auto requested = Env("AVEVA_DB_BENCH_IO_THREADS")) {
        const auto parsed = std::strtoul(requested->c_str(), nullptr, 10);
        if (parsed == 0 || parsed > 1024) {
            std::cerr << "aveva_db_bench: AVEVA_DB_BENCH_IO_THREADS must be between 1 and 1024\n";
            return 2;
        }
        ioThreadCount = static_cast<unsigned>(parsed);
    }
    std::cerr << "aveva_db_bench: running the io_context on " << ioThreadCount << " thread(s)\n";

    // Declared before the plugin objects so it is destroyed after them.
    IoThreadPool ioThreads{ioThreadCount};

    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;

    auto status = Plugin::Register(configOptions, &env, &guard, ioThreads.Context(),
                                   Plugin::RegisterOptions{.Accounts = accounts, .Log = logger});
    if (!status.ok()) {
        std::cerr << "aveva_db_bench: Plugin::Register failed: " << status.ToString() << "\n";
        return 3;
    }

    const auto fsUri = Plugin::NameFor(accounts);
    const auto dbPath = Plugin::UniquePrefix(accountUrl, dbName);

    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n";

    // Lets scripts pass `auto` instead of re-deriving the plugin name, in every form gflags accepts:
    // --fs_uri=auto, -fs_uri=auto, --fs_uri auto and -fs_uri auto.
    std::string fsUriValue = fsUri;
    std::string fsUriFlag = "--fs_uri=" + fsUri;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        if (arg == "--fs_uri=auto" || arg == "-fs_uri=auto") {
            argv[i] = fsUriFlag.data();
        } else if ((arg == "--fs_uri" || arg == "-fs_uri") && i + 1 < argc && std::string_view{argv[i + 1]} == "auto") {
            argv[++i] = fsUriValue.data();
        }
    }

    std::fflush(nullptr);
    std::cout.flush();
    const auto rc = rocksdb::db_bench_tool(argc, argv);
    // The registration refers to the io_context, which is destroyed when this function returns.
    guard.reset();
    Plugin::Unregister(accounts);
    return rc;
}

} // namespace

int main(int argc, char** argv) {
    const auto rc = Run(argc, argv);

    // Skip C++ static destructors: boost.log tears down thread-local storage
    // before the plugin's background threads exit, which triggers SIGABRT
    // (exit 134) after the benchmark has already finished.
    std::fflush(nullptr);
    std::cout.flush();
    std::_Exit(rc);
}