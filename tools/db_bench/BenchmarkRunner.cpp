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

#include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>

#include "ScanThenMultiGet.hpp"

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
#include <AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp>
#include <rocksdb/utilities/object_registry.h>
#endif

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/convenience.h>
#include <rocksdb/db_bench_tool.h>
#include <rocksdb/env.h>

#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
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

// Debug logging adds synchronous I/O overhead to write-heavy benchmarks.
bool ConfigureLogLevel(std::string* error) {
    namespace trivial = boost::log::trivial;
    trivial::severity_level level = trivial::info;
    if (const auto value = Env("AVEVA_DB_BENCH_LOG_LEVEL")) {
        if (!trivial::from_string(value->c_str(), value->size(), level)) {
            *error =
                "AVEVA_DB_BENCH_LOG_LEVEL must be one of trace|debug|info|warning|error|fatal, got '" + *value + "'";
            return false;
        }
    }
    boost::log::core::get()->set_filter(trivial::severity >= level);
    return true;
}

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
template <typename T> T EnvOr(const char* name, T defaultValue) {
    if (const auto value = Env(name)) {
        size_t consumed = 0;
        const auto parsed = std::stoull(*value, &consumed);
        if (value->find_first_not_of("0123456789") != std::string::npos || consumed != value->size() ||
            parsed > std::numeric_limits<T>::max()) {
            throw std::invalid_argument(std::string(name) + " requires an unsigned integer in range");
        }
        return static_cast<T>(parsed);
    }
    return defaultValue;
}

bool EnvFlagOr(const char* name, bool defaultValue) {
    if (const auto value = Env(name)) {
        if (*value == "0" || *value == "false") {
            return false;
        }
        if (*value == "1" || *value == "true") {
            return true;
        }
        throw std::invalid_argument(std::string(name) + " requires 0|1|false|true");
    }
    return defaultValue;
}

// ObjectLibrary matches the full URI, including the scheme separator.
constexpr const char* kCacheLibSecondaryCacheName = "aveva-cachelib-secondary-cache://";

// Registering is safe without cache settings; the factory reads them on demand.
void RegisterCacheLibSecondaryCache() {
    using namespace AVEVA::RocksDB::Plugin::Core;
    if (rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::SecondaryCache>(kCacheLibSecondaryCacheName)) {
        return;
    }
    rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::SecondaryCache>(
        kCacheLibSecondaryCacheName,
        [](const std::string&, std::unique_ptr<rocksdb::SecondaryCache>* guard,
           std::string* errmsg) -> rocksdb::SecondaryCache* {
            try {
                const auto cacheDir = Env("AVEVA_DB_BENCH_SECONDARY_CACHE_DIR");
                if (!cacheDir) {
                    throw std::invalid_argument("AVEVA_DB_BENCH_SECONDARY_CACHE_DIR must be set to use CacheLib");
                }
                SecondaryCacheOptions options;
                options.backend = SecondaryCacheBackend::CacheLib;
                options.cacheDir = *cacheDir;
                const auto capacityMB = EnvOr<size_t>("AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB",
                                                      SecondaryCacheOptions::kDefaultCapacity / (1024 * 1024));
                if (capacityMB > std::numeric_limits<size_t>::max() / (1024 * 1024)) {
                    throw std::invalid_argument("AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB is too large");
                }
                options.capacity = capacityMB * 1024 * 1024;
                options.logger = std::make_shared<SecondaryCacheOptions::Logger>();
                auto& cachelib = options.cachelib;
                cachelib.regionSizeBytes =
                    EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_REGION_SIZE_BYTES", cachelib.regionSizeBytes);
                cachelib.cleanRegions = EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_CLEAN_REGIONS", cachelib.cleanRegions);
                cachelib.blockSize = EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_BLOCK_SIZE", cachelib.blockSize);
                cachelib.readerThreads =
                    EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_READER_THREADS", cachelib.readerThreads);
                cachelib.writerThreads =
                    EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_WRITER_THREADS", cachelib.writerThreads);
                cachelib.requestOrderingShardsPower = EnvOr<uint32_t>(
                    "AVEVA_DB_BENCH_CACHELIB_REQUEST_ORDERING_SHARDS_POWER", cachelib.requestOrderingShardsPower);
                cachelib.maxParcelMemoryMB =
                    EnvOr<uint64_t>("AVEVA_DB_BENCH_CACHELIB_MAX_PARCEL_MEMORY_MB", cachelib.maxParcelMemoryMB);
                cachelib.dataChecksum = EnvFlagOr("AVEVA_DB_BENCH_CACHELIB_DATA_CHECKSUM", cachelib.dataChecksum);
                cachelib.logCountersOnShutdown = EnvFlagOr("AVEVA_DB_BENCH_CACHELIB_LOG_COUNTERS", true);
                if (const auto policy = Env("AVEVA_DB_BENCH_CACHELIB_EVICTION_POLICY")) {
                    if (*policy == "lru") {
                        cachelib.evictionPolicy = CacheLibSecondaryCacheOptions::EvictionPolicy::Lru;
                    } else if (*policy == "fifo") {
                        cachelib.evictionPolicy = CacheLibSecondaryCacheOptions::EvictionPolicy::Fifo;
                    } else {
                        throw std::invalid_argument("AVEVA_DB_BENCH_CACHELIB_EVICTION_POLICY must be fifo|lru");
                    }
                }
                if (Env("AVEVA_DB_BENCH_CACHELIB_ADMISSION_WRITE_RATE_BYTES_PER_SEC")) {
                    cachelib.admissionWriteRateBytesPerSec =
                        EnvOr<uint64_t>("AVEVA_DB_BENCH_CACHELIB_ADMISSION_WRITE_RATE_BYTES_PER_SEC", 0);
                }
                if (const auto level = Env("AVEVA_DB_BENCH_CACHELIB_LOG_LEVEL")) {
                    cachelib.cachelibLogLevel = *level;
                }
                if (const auto file = Env("AVEVA_DB_BENCH_CACHELIB_FILE_NAME")) {
                    cachelib.fileName = *file;
                }
                *guard = std::make_unique<CacheLibSecondaryCache>(options);
                return guard->get();
            } catch (const std::exception& error) {
                *errmsg = std::string("Failed to create CacheLib secondary cache: ") + error.what();
                return nullptr;
            }
        });
}
#endif

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

    if (std::string error; !ConfigureLogLevel(&error)) {
        std::cerr << "aveva_db_bench: " << error << "\n";
        return 2;
    }

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

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    RegisterCacheLibSecondaryCache();
#endif
    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n";
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    std::cerr << "  Optional secondary cache: --secondary_cache_uri=" << kCacheLibSecondaryCacheName
              << "\n  Set AVEVA_DB_BENCH_SECONDARY_CACHE_DIR; see tools/db_bench/README.md for tuning.\n";
#endif

    std::fflush(nullptr);
    std::cout.flush();
    int rc;
    if (const auto scanLength = Env("AVEVA_DB_BENCH_SCAN_BEFORE_MULTIREAD")) {
        size_t length = 0;
        try {
            size_t consumed = 0;
            const auto parsed = std::stoull(*scanLength, &consumed);
            if (scanLength->find_first_not_of("0123456789") != std::string::npos || consumed != scanLength->size() ||
                parsed > std::numeric_limits<size_t>::max()) {
                throw std::invalid_argument("expected a non-negative integer");
            }
            length = static_cast<size_t>(parsed);
        } catch (const std::exception& error) {
            std::cerr << "aveva_db_bench: invalid AVEVA_DB_BENCH_SCAN_BEFORE_MULTIREAD: " << error.what() << "\n";
            return 2;
        }
        if (length > 0) {
            AVEVA::RocksDB::Plugin::Bench::ScanThenMultiGetHooks hooks(length);
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
