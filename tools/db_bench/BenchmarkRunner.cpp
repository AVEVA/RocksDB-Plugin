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

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
#include <AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp>
#include <rocksdb/utilities/object_registry.h>
#endif

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
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

DEFINE_string(aveva_log_level, "info", "Plugin log level: trace|debug|info|warning|error|fatal");
DEFINE_uint64(aveva_scan_before_multiread, 0,
              "If > 0, scan up to this many records before each MultiGet. "
              "Requires --benchmarks=multireadrandom --multiread_batched=0.");

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
DEFINE_string(aveva_secondary_cache_dir, "", "Local SSD directory for the CacheLib cache file; required with "
                                             "--secondary_cache_uri=aveva-cachelib-secondary-cache://");
DEFINE_uint64(aveva_secondary_cache_capacity_mb, 512, "CacheLib secondary cache capacity in MiB");
DEFINE_uint32(aveva_cachelib_region_size_bytes, 4194304, "Navy region size; also the largest single entry");
DEFINE_uint32(aveva_cachelib_clean_regions, 1, "Clean regions kept ready for writes");
DEFINE_uint32(aveva_cachelib_block_size, 4096, "Navy device alignment (not the RocksDB SST block size)");
DEFINE_uint32(aveva_cachelib_reader_threads, 4, "Navy reader threads");
DEFINE_uint32(aveva_cachelib_writer_threads, 4, "Navy writer threads");
DEFINE_uint32(aveva_cachelib_request_ordering_shards_power, 16, "Log2 of the request-ordering slot count (1-20)");
DEFINE_uint64(aveva_cachelib_max_parcel_memory_mb, 32, "Upper bound for bytes of pending inserts, in MiB");
DEFINE_bool(aveva_cachelib_data_checksum, true, "Store and verify a checksum for each entry");
DEFINE_string(aveva_cachelib_eviction_policy, "fifo", "Region eviction policy: fifo|lru");
DEFINE_uint64(aveva_cachelib_admission_write_rate_bytes_per_sec, 0,
              "If > 0, bounds SSD write rate with Navy's dynamic random admission policy; 0 admits all inserts");
DEFINE_string(aveva_cachelib_log_level, "WARN", "folly log level for CacheLib internals");
DEFINE_string(aveva_cachelib_file_name, "navy.cache", "Cache file name inside the cache directory");
DEFINE_bool(aveva_cachelib_log_counters, true, "Log CACHELIB_COUNTER lines for Navy counters");
#endif

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

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
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
                if (FLAGS_aveva_secondary_cache_dir.empty()) {
                    throw std::invalid_argument("--aveva_secondary_cache_dir must be set to use CacheLib");
                }
                SecondaryCacheOptions options;
                options.backend = SecondaryCacheBackend::CacheLib;
                options.cacheDir = FLAGS_aveva_secondary_cache_dir;
                if (FLAGS_aveva_secondary_cache_capacity_mb > std::numeric_limits<size_t>::max() / (1024 * 1024)) {
                    throw std::invalid_argument("--aveva_secondary_cache_capacity_mb is too large");
                }
                options.capacity = static_cast<size_t>(FLAGS_aveva_secondary_cache_capacity_mb) * 1024 * 1024;
                options.logger = std::make_shared<SecondaryCacheOptions::Logger>();
                auto& cachelib = options.cachelib;
                cachelib.regionSizeBytes = FLAGS_aveva_cachelib_region_size_bytes;
                cachelib.cleanRegions = FLAGS_aveva_cachelib_clean_regions;
                cachelib.blockSize = FLAGS_aveva_cachelib_block_size;
                cachelib.readerThreads = FLAGS_aveva_cachelib_reader_threads;
                cachelib.writerThreads = FLAGS_aveva_cachelib_writer_threads;
                cachelib.requestOrderingShardsPower = FLAGS_aveva_cachelib_request_ordering_shards_power;
                cachelib.maxParcelMemoryMB = FLAGS_aveva_cachelib_max_parcel_memory_mb;
                cachelib.dataChecksum = FLAGS_aveva_cachelib_data_checksum;
                cachelib.logCountersOnShutdown = FLAGS_aveva_cachelib_log_counters;
                if (FLAGS_aveva_cachelib_eviction_policy == "lru") {
                    cachelib.evictionPolicy = CacheLibSecondaryCacheOptions::EvictionPolicy::Lru;
                } else if (FLAGS_aveva_cachelib_eviction_policy == "fifo") {
                    cachelib.evictionPolicy = CacheLibSecondaryCacheOptions::EvictionPolicy::Fifo;
                } else {
                    throw std::invalid_argument("--aveva_cachelib_eviction_policy must be fifo|lru");
                }
                if (FLAGS_aveva_cachelib_admission_write_rate_bytes_per_sec > 0) {
                    cachelib.admissionWriteRateBytesPerSec = FLAGS_aveva_cachelib_admission_write_rate_bytes_per_sec;
                }
                cachelib.cachelibLogLevel = FLAGS_aveva_cachelib_log_level;
                cachelib.fileName = FLAGS_aveva_cachelib_file_name;
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

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    RegisterCacheLibSecondaryCache();
#endif
    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n";
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    std::cerr << "  Optional secondary cache: --secondary_cache_uri=" << kCacheLibSecondaryCacheName
              << "\n  Set --aveva_secondary_cache_dir; see tools/db_bench/README.md for tuning.\n";
#endif

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
