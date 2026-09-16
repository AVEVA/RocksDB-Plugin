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
//   --db=<storage-account-url>/<container>/<db-subpath>
//
// See tools/db_bench/README.md for concrete invocation examples.

#include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/convenience.h>
#include <rocksdb/db_bench_tool.h>
#include <rocksdb/env.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>

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

    auto storageAccountUrl = Env("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL");
    if (!storageAccountUrl) {
        std::cerr << "aveva_db_bench: AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL not set; "
                     "Azure plugin will NOT be registered. Falling through to stock db_bench.\n";
        return rocksdb::db_bench_tool(argc, argv);
    }

    const auto container    = Require("AVEVA_DB_BENCH_CONTAINER",     Env("AVEVA_DB_BENCH_CONTAINER"));
    const auto tenantId     = Require("AVEVA_DB_BENCH_TENANT_ID",     Env("AVEVA_DB_BENCH_TENANT_ID"));
    const auto clientId     = Require("AVEVA_DB_BENCH_CLIENT_ID",     Env("AVEVA_DB_BENCH_CLIENT_ID"));
    const auto clientSecret = Require("AVEVA_DB_BENCH_CLIENT_SECRET", Env("AVEVA_DB_BENCH_CLIENT_SECRET"));

    ServicePrincipalStorageInfo primary{
        container,
        *storageAccountUrl,
        clientId,
        clientSecret,
        tenantId,
    };

    auto logger = std::make_shared<
        boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

    rocksdb::ConfigOptions configOptions;
    rocksdb::Env* env = nullptr;
    std::shared_ptr<rocksdb::Env> guard;

    auto status = Plugin::Register(configOptions, &env, &guard, primary,
                                   /* backup */ std::nullopt, logger);
    if (!status.ok()) {
        std::cerr << "aveva_db_bench: Plugin::Register failed: " << status.ToString() << "\n";
        return 3;
    }

    const auto fsUri  = std::string{Plugin::Name} + container;
    const auto dbPath = StorageAccount::UniquePrefix(*storageAccountUrl, container);

    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n";

    return rocksdb::db_bench_tool(argc, argv);
}
