// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// AVEVA db_bench wrapper: registers the Azure Page Blob filesystem plugin
// with the RocksDB ObjectLibrary and then hands control to the stock
// rocksdb::db_bench_tool. Configure the plugin via environment variables:
//
//   AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL  (required to enable Azure backend)
//   AVEVA_DB_BENCH_CONTAINER            (blob container name; used as dbName)
//   AVEVA_DB_BENCH_TENANT_ID            (Service Principal auth)
//   AVEVA_DB_BENCH_CLIENT_ID            (Service Principal auth)
//   AVEVA_DB_BENCH_CLIENT_SECRET        (Service Principal auth)
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

#include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
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

} // namespace

int main(int argc, char** argv) {
    using AVEVA::RocksDB::Plugin::Azure::Plugin;
    using AVEVA::RocksDB::Plugin::Azure::Impl::StorageAccount;
    using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

    const auto storageAccountUrl =
        Require("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL", Env("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL"));
    const auto container = Require("AVEVA_DB_BENCH_CONTAINER", Env("AVEVA_DB_BENCH_CONTAINER"));
    const auto tenantId = Require("AVEVA_DB_BENCH_TENANT_ID", Env("AVEVA_DB_BENCH_TENANT_ID"));
    const auto clientId = Require("AVEVA_DB_BENCH_CLIENT_ID", Env("AVEVA_DB_BENCH_CLIENT_ID"));
    const auto clientSecret = Require("AVEVA_DB_BENCH_CLIENT_SECRET", Env("AVEVA_DB_BENCH_CLIENT_SECRET"));

    ServicePrincipalStorageInfo primary{
        container, storageAccountUrl, clientId, clientSecret, tenantId,
    };

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::warning);

    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;

    // The plugin blocks on this context from RocksDB threads, so it must be run by dedicated threads
    // that never call into RocksDB. It intentionally lives until process exit (see std::_Exit below).
    auto* ioContext = new boost::asio::io_context();
    auto workGuard =
        new boost::asio::executor_work_guard<boost::asio::io_context::executor_type>(ioContext->get_executor());
    (void)workGuard;
    std::vector<std::thread> ioThreads;
    for (int i = 0; i < 4; ++i) {
        ioThreads.emplace_back([ioContext] { ioContext->run(); });
        ioThreads.back().detach();
    }

    auto status = Plugin::Register(configOptions, &env, &guard, *ioContext, primary,
                                   /* backup */ std::nullopt, logger);
    if (!status.ok()) {
        std::cerr << "aveva_db_bench: Plugin::Register failed: " << status.ToString() << "\n";
        return 3;
    }

    const auto fsUri = Plugin::NameFor(primary);
    const auto dbPath = StorageAccount::UniquePrefix(storageAccountUrl, container);

    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n";

    // Lets scripts pass --fs_uri=auto instead of re-deriving the plugin name.
    std::string fsUriFlag = "--fs_uri=" + fsUri;
    for (int i = 1; i < argc; ++i) {
        if (std::string_view{argv[i]} == "--fs_uri=auto") {
            argv[i] = fsUriFlag.data();
        }
    }

    std::fflush(nullptr);
    std::cout.flush();
    const auto rc = rocksdb::db_bench_tool(argc, argv);

    // Skip C++ static destructors: boost.log tears down thread-local storage
    // before the plugin's background threads exit, which triggers SIGABRT
    // (exit 134) after the benchmark has already finished.
    std::fflush(nullptr);
    std::cout.flush();
    std::_Exit(rc);
}
