// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

// AVEVA db_bench wrapper: registers the Azure Page Blob filesystem plugin
// and the AVEVA secondary cache backends with the RocksDB ObjectLibrary,
// then hands control to the stock rocksdb::db_bench_tool. Configure the
// Azure plugin via environment variables:
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
// To benchmark with an AVEVA secondary cache attached to the block cache,
// pass db_bench's own --secondary_cache_uri flag, selecting one of:
//
//   --secondary_cache_uri=aveva-file-based-secondary-cache://
//   --secondary_cache_uri=aveva-cachelib-secondary-cache://   (Linux builds with
//                                                              AVEVA_ROCKSDB_WITH_CACHELIB only)
//
// Both backends share:
//
//   AVEVA_DB_BENCH_SECONDARY_CACHE_DIR           (required to use either backend)
//   AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB   (optional, default 512)
//
// The CacheLib backend additionally reads (all optional, defaults shown):
//
//   AVEVA_DB_BENCH_CACHELIB_REGION_SIZE_BYTES          (4194304)
//   AVEVA_DB_BENCH_CACHELIB_CLEAN_REGIONS              (1)
//   AVEVA_DB_BENCH_CACHELIB_BLOCK_SIZE                 (4096)
//   AVEVA_DB_BENCH_CACHELIB_READER_THREADS             (4)
//   AVEVA_DB_BENCH_CACHELIB_WRITER_THREADS             (4)
//   AVEVA_DB_BENCH_CACHELIB_MAX_PARCEL_MEMORY_MB       (32)
//   AVEVA_DB_BENCH_CACHELIB_EVICTION_POLICY            (fifo|lru, default fifo)
//   AVEVA_DB_BENCH_CACHELIB_DATA_CHECKSUM              (0|1, default 1)
//   AVEVA_DB_BENCH_CACHELIB_ADMISSION_WRITE_RATE_BYTES_PER_SEC  (unset = admit everything)
//   AVEVA_DB_BENCH_CACHELIB_LOG_LEVEL                  (WARN)
//   AVEVA_DB_BENCH_CACHELIB_FILE_NAME                  (navy.cache)
//   AVEVA_DB_BENCH_CACHELIB_LOG_COUNTERS               (0|1, default 1: log "CACHELIB_COUNTER name=value"
//                                                       lines for every Navy counter on shutdown)
//
// See tools/db_bench/README.md for concrete invocation examples.

#include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>
#include <AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp>
#include <AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp>
#include <AVEVA/RocksDB/Plugin/Core/SecondaryCacheOptions.hpp>

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
#include <AVEVA/RocksDB/Plugin/Core/CacheLibSecondaryCache.hpp>
#endif

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/convenience.h>
#include <rocksdb/db_bench_tool.h>
#include <rocksdb/env.h>
#include <rocksdb/secondary_cache.h>
#include <rocksdb/utilities/object_registry.h>

#include <cstdlib>
#include <iostream>
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

// Parses an unsigned integral environment variable, or returns defaultValue
// if unset. Used for both the shared cache knobs and the CacheLib-specific
// ones (region size, thread counts, etc.).
template <typename T> T EnvOr(const char* name, T defaultValue) {
    if (const auto value = Env(name)) {
        return static_cast<T>(std::stoull(*value));
    }
    return defaultValue;
}

bool EnvFlagOr(const char* name, bool defaultValue) {
    if (const auto value = Env(name)) {
        return *value != "0" && *value != "false";
    }
    return defaultValue;
}

// Fields shared by every AVEVA secondary cache backend: the directory the
// cache owns on disk, its capacity, and a logger. Returns std::nullopt (and
// sets *errmsg) if the required directory environment variable is missing.
std::optional<AVEVA::RocksDB::Plugin::Core::SecondaryCacheOptions> CommonSecondaryCacheOptions(std::string* errmsg) {
    using AVEVA::RocksDB::Plugin::Core::SecondaryCacheOptions;

    const auto cacheDir = Env("AVEVA_DB_BENCH_SECONDARY_CACHE_DIR");
    if (!cacheDir) {
        *errmsg = "AVEVA_DB_BENCH_SECONDARY_CACHE_DIR must be set to use an AVEVA secondary cache backend";
        return std::nullopt;
    }

    SecondaryCacheOptions options;
    options.cacheDir = std::filesystem::path(*cacheDir);
    options.capacity = EnvOr<uint64_t>("AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB",
                                       SecondaryCacheOptions::kDefaultCapacity / (1024ULL * 1024ULL)) *
                       1024ULL * 1024ULL;
    options.logger =
        std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    return options;
}

// Name used both as the ObjectLibrary registration id and the
// --secondary_cache_uri value that selects this backend. Includes the
// "://" suffix because RocksDB's ObjectLibrary matches the registered id
// against the *entire* --secondary_cache_uri string - it doesn't strip the
// scheme separator before comparing.
constexpr const char* kFileBasedSecondaryCacheName = "aveva-file-based-secondary-cache://";
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
constexpr const char* kCacheLibSecondaryCacheName = "aveva-cachelib-secondary-cache://";
#endif

// Registers a factory for AVEVA's file-based secondary cache
// (FileBasedCompressedSecondaryCache) so db_bench's own --secondary_cache_uri
// flag can select it, the same way --fs_uri selects the Azure plugin. The
// factory reads its configuration from environment variables when db_bench
// actually instantiates the cache (i.e. only when --secondary_cache_uri
// requests it), so registering is always safe even if the cache is never used.
void RegisterFileBasedSecondaryCache() {
    using AVEVA::RocksDB::Plugin::Core::FileBasedCompressedSecondaryCache;
    using AVEVA::RocksDB::Plugin::Core::LocalFilesystem;

    if (rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::SecondaryCache>(kFileBasedSecondaryCacheName) !=
        nullptr) {
        return;
    }

    rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::SecondaryCache>(
        kFileBasedSecondaryCacheName,
        [](const std::string& /* uri */, std::unique_ptr<rocksdb::SecondaryCache>* guard,
           std::string* errmsg) -> rocksdb::SecondaryCache* {
            auto options = CommonSecondaryCacheOptions(errmsg);
            if (!options) {
                return nullptr;
            }

            // Ownership is transferred through *guard, the same way the Azure
            // filesystem factory in Plugin.cpp does it; the caller (RocksDB's
            // SecondaryCache::CreateFromString) moves the unique_ptr into the
            // shared_ptr it hands back to db_bench.
            *guard = std::make_unique<FileBasedCompressedSecondaryCache>(
                options->cacheDir, std::make_shared<LocalFilesystem>(), options->capacity, options->logger);
            return guard->get();
        });
}

#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
// Registers a factory for AVEVA's CacheLib (Navy) secondary cache backend,
// available only in builds with AVEVA_ROCKSDB_WITH_CACHELIB (Linux x64). The
// CacheLib-specific knobs (region size, thread counts, eviction policy, etc.)
// are read from environment variables, falling back to
// CacheLibSecondaryCacheOptions' own defaults when unset.
void RegisterCacheLibSecondaryCache() {
    using AVEVA::RocksDB::Plugin::Core::CacheLibSecondaryCache;
    using AVEVA::RocksDB::Plugin::Core::CacheLibSecondaryCacheOptions;

    if (rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::SecondaryCache>(kCacheLibSecondaryCacheName) !=
        nullptr) {
        return;
    }

    rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::SecondaryCache>(
        kCacheLibSecondaryCacheName,
        [](const std::string& /* uri */, std::unique_ptr<rocksdb::SecondaryCache>* guard,
           std::string* errmsg) -> rocksdb::SecondaryCache* {
            auto options = CommonSecondaryCacheOptions(errmsg);
            if (!options) {
                return nullptr;
            }

            CacheLibSecondaryCacheOptions cachelib;
            cachelib.regionSizeBytes =
                EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_REGION_SIZE_BYTES", cachelib.regionSizeBytes);
            cachelib.cleanRegions = EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_CLEAN_REGIONS", cachelib.cleanRegions);
            cachelib.blockSize = EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_BLOCK_SIZE", cachelib.blockSize);
            cachelib.readerThreads = EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_READER_THREADS", cachelib.readerThreads);
            cachelib.writerThreads = EnvOr<uint32_t>("AVEVA_DB_BENCH_CACHELIB_WRITER_THREADS", cachelib.writerThreads);
            cachelib.maxParcelMemoryMB =
                EnvOr<uint64_t>("AVEVA_DB_BENCH_CACHELIB_MAX_PARCEL_MEMORY_MB", cachelib.maxParcelMemoryMB);
            cachelib.dataChecksum = EnvFlagOr("AVEVA_DB_BENCH_CACHELIB_DATA_CHECKSUM", cachelib.dataChecksum);

            if (const auto policy = Env("AVEVA_DB_BENCH_CACHELIB_EVICTION_POLICY")) {
                if (*policy == "lru") {
                    cachelib.evictionPolicy = CacheLibSecondaryCacheOptions::EvictionPolicy::Lru;
                } else if (*policy == "fifo") {
                    cachelib.evictionPolicy = CacheLibSecondaryCacheOptions::EvictionPolicy::Fifo;
                } else {
                    *errmsg = "AVEVA_DB_BENCH_CACHELIB_EVICTION_POLICY must be 'fifo' or 'lru', got '" + *policy + "'";
                    return nullptr;
                }
            }

            if (const auto rate = Env("AVEVA_DB_BENCH_CACHELIB_ADMISSION_WRITE_RATE_BYTES_PER_SEC")) {
                cachelib.admissionWriteRateBytesPerSec = std::stoull(*rate);
            }

            if (const auto logLevel = Env("AVEVA_DB_BENCH_CACHELIB_LOG_LEVEL")) {
                cachelib.cachelibLogLevel = *logLevel;
            }

            if (const auto fileName = Env("AVEVA_DB_BENCH_CACHELIB_FILE_NAME")) {
                cachelib.fileName = *fileName;
            }

            // On by default for benchmarking: dumps Navy counters when db_bench closes the cache.
            cachelib.logCountersOnShutdown = EnvFlagOr("AVEVA_DB_BENCH_CACHELIB_LOG_COUNTERS", true);

            options->cachelib = cachelib;

            try {
                *guard = std::make_unique<CacheLibSecondaryCache>(*options);
            } catch (const std::exception& e) {
                *errmsg = std::string("Failed to create CacheLib secondary cache: ") + e.what();
                return nullptr;
            }
            return guard->get();
        });
}
#endif

} // namespace

int main(int argc, char** argv) {
    using AVEVA::RocksDB::Plugin::Azure::Plugin;
    using AVEVA::RocksDB::Plugin::Azure::Impl::StorageAccount;
    using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

    const auto storageAccountUrl = Require("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL", Env("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL"));
    const auto container    = Require("AVEVA_DB_BENCH_CONTAINER",     Env("AVEVA_DB_BENCH_CONTAINER"));
    const auto tenantId     = Require("AVEVA_DB_BENCH_TENANT_ID",     Env("AVEVA_DB_BENCH_TENANT_ID"));
    const auto clientId     = Require("AVEVA_DB_BENCH_CLIENT_ID",     Env("AVEVA_DB_BENCH_CLIENT_ID"));
    const auto clientSecret = Require("AVEVA_DB_BENCH_CLIENT_SECRET", Env("AVEVA_DB_BENCH_CLIENT_SECRET"));

    ServicePrincipalStorageInfo primary{
        container,
        storageAccountUrl,
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
    const auto dbPath = StorageAccount::UniquePrefix(storageAccountUrl, container);

    RegisterFileBasedSecondaryCache();
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
    RegisterCacheLibSecondaryCache();
#endif

    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n"
              << "  To also attach a secondary cache, add one of:\n"
              << "    --secondary_cache_uri=" << kFileBasedSecondaryCacheName << "\n"
#if defined(AVEVA_ROCKSDB_WITH_CACHELIB)
              << "    --secondary_cache_uri=" << kCacheLibSecondaryCacheName << "\n"
#endif
              << "  with AVEVA_DB_BENCH_SECONDARY_CACHE_DIR set (and optionally\n"
              << "  AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB and, for CacheLib,\n"
              << "  the AVEVA_DB_BENCH_CACHELIB_* knobs; see tools/db_bench/README.md).\n";

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
