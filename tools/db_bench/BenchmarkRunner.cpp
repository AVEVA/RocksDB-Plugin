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
//   --fs_uri=azblobfs<container>
//   --db=<account>+<container>/<db-subpath>
//
// See tools/db_bench/README.md for concrete invocation examples.

#include "ScanThenMultiGet.hpp"

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <gflags/gflags.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db_bench_tool.h>
#include <rocksdb/env.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>

DEFINE_string(aveva_log_level, "info", "Plugin log level: trace|debug|info|warning|error|fatal");
DEFINE_uint64(aveva_scan_before_multiread, 0,
              "If > 0, scan up to this many records before each MultiGet. "
              "Requires --benchmarks=multireadrandom --multiread_batched=0.");

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

    // GraphDB uses 16 KiB blocks with LZ4
    // An empty result means the flag no longer exists upstream; fail rather than silently drop the default.
    for (const auto& [name, value] : {std::pair{"block_size", "16384"}, std::pair{"compression_type", "lz4"}}) {
        if (GFLAGS_NAMESPACE::SetCommandLineOptionWithMode(name, value, GFLAGS_NAMESPACE::SET_FLAGS_DEFAULT)
                .empty()) {
            std::cerr << "aveva_db_bench: could not set default for --" << name << "\n";
            return 2;
        }
    }

    // Parse wrapper flags
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/false);

    // Debug logging adds synchronous I/O overhead to write-heavy benchmarks.
    auto logLevel = boost::log::trivial::info;
    if (!boost::log::trivial::from_string(FLAGS_aveva_log_level.c_str(), FLAGS_aveva_log_level.size(), logLevel)) {
        std::cerr << "aveva_db_bench: --aveva_log_level must be one of trace|debug|info|warning|error|fatal, got '"
                  << FLAGS_aveva_log_level << "'\n";
        return 2;
    }
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= logLevel);
    const auto scanLength = static_cast<size_t>(FLAGS_aveva_scan_before_multiread);

    const auto storageAccountUrl =
        Require("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL", Env("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL"));
    const auto container = Require("AVEVA_DB_BENCH_CONTAINER", Env("AVEVA_DB_BENCH_CONTAINER"));
    const auto tenantId = Require("AVEVA_DB_BENCH_TENANT_ID", Env("AVEVA_DB_BENCH_TENANT_ID"));
    const auto clientId = Require("AVEVA_DB_BENCH_CLIENT_ID", Env("AVEVA_DB_BENCH_CLIENT_ID"));
    const auto clientSecret = Require("AVEVA_DB_BENCH_CLIENT_SECRET", Env("AVEVA_DB_BENCH_CLIENT_SECRET"));

    ServicePrincipalStorageInfo primary{
        container, storageAccountUrl, clientId, clientSecret, tenantId,
    };

    auto logger = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;

    auto status = Plugin::Register(configOptions, &env, &guard, primary,
                                   /* backup */ std::nullopt, logger);
    if (!status.ok()) {
        std::cerr << "aveva_db_bench: Plugin::Register failed: " << status.ToString() << "\n";
        return 3;
    }

    const auto fsUri = std::string{Plugin::Name} + container;
    const auto dbPath = StorageAccount::UniquePrefix(storageAccountUrl, container);

    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n";

    std::fflush(nullptr);
    std::cout.flush();
    int rc;
    if (scanLength > 0) {
        AVEVA::RocksDB::Plugin::Bench::ScanThenMultiGetHooks hooks(scanLength);
        rc = rocksdb::db_bench_tool(argc, argv, hooks);
        std::cout << "SCAN_THEN_MULTIGET queries=" << hooks.counters.queries.load()
                  << " scanned_records=" << hooks.counters.scannedRecords.load()
                  << " scanned_bytes=" << hooks.counters.scannedBytes.load()
                  << " fetched_keys=" << hooks.counters.fetchedKeys.load()
                  << " found_keys=" << hooks.counters.foundKeys.load() << "\n";
        if (rc == 0 && hooks.counters.queries == 0) {
            std::cerr << "aveva_db_bench: scan-before-MultiGet hook was not exercised; use normal Open and "
                         "--benchmarks=multireadrandom --multiread_batched=0\n";
            rc = 2;
        }
    } else {
        rc = rocksdb::db_bench_tool(argc, argv);
    }

    // Skip C++ static destructors: boost.log tears down thread-local storage
    // before the plugin's background threads exit, which triggers SIGABRT
    // (exit 134) after the benchmark has already finished.
    boost::log::core::get()->flush();
    std::fflush(nullptr);
    std::cout.flush();
    std::_Exit(rc);
}
