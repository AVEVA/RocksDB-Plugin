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
// pass db_bench's own --secondary_cache_uri flag, e.g.:
//
//   --secondary_cache_uri=aveva-file-based-secondary-cache://   (legacy file-per-entry engine)
//   --secondary_cache_uri=aveva-region-secondary-cache://       (region/log-structured engine)
//
// configured via:
//
//   AVEVA_DB_BENCH_SECONDARY_CACHE_DIR           (required to use this backend)
//   AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB   (optional, default 512)
//
// See tools/db_bench/README.md for concrete invocation examples.

#include <AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp>
#include <AVEVA/RocksDB/Plugin/Azure/Plugin.hpp>
#include <AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp>
#include <AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp>

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

// Name used both as the ObjectLibrary registration id and the
// --secondary_cache_uri value that selects this backend.
constexpr const char* kFileBasedSecondaryCacheName = "aveva-file-based-secondary-cache://";

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
            const auto cacheDir = Env("AVEVA_DB_BENCH_SECONDARY_CACHE_DIR");
            if (!cacheDir) {
                *errmsg = "AVEVA_DB_BENCH_SECONDARY_CACHE_DIR must be set to use " +
                          std::string(kFileBasedSecondaryCacheName);
                return nullptr;
            }

            size_t capacity = FileBasedCompressedSecondaryCache::kDefaultCapacity;
            if (const auto capacityMb = Env("AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB")) {
                capacity = static_cast<size_t>(std::stoull(*capacityMb)) * 1024ULL * 1024ULL;
            }

            auto logger = std::make_shared<
                boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();

            // Ownership is transferred through *guard, the same way the Azure
            // filesystem factory in Plugin.cpp does it; the caller (RocksDB's
            // SecondaryCache::CreateFromString) moves the unique_ptr into the
            // shared_ptr it hands back to db_bench.
            *guard = std::make_unique<FileBasedCompressedSecondaryCache>(
                std::filesystem::path(*cacheDir), std::make_shared<LocalFilesystem>(), capacity, std::move(logger));
            return guard->get();
        });
}

template <typename T> T EnvOr(const char* name, T defaultValue) {
    if (const auto value = Env(name)) {
        return static_cast<T>(std::stoull(*value));
    }
    return defaultValue;
}

constexpr const char* kRegionSecondaryCacheName = "aveva-region-secondary-cache://";

// Registers the region/log-structured engine of FileBasedCompressedSecondaryCache
// under its own URI so it can be benchmarked side by side with the legacy
// file-per-entry engine. Knobs (all optional except the directory):
//
//   AVEVA_DB_BENCH_SECONDARY_CACHE_DIR, AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB
//   AVEVA_DB_BENCH_REGION_SIZE_MB          (default 64)
//   AVEVA_DB_BENCH_REGION_FLUSH_BLOCK_KB   (default 1024)
//   AVEVA_DB_BENCH_REGION_INDEX_SHARDS     (default 64)
//   AVEVA_DB_BENCH_REGION_ADMISSION        (admit-all|second-chance, default admit-all)
void RegisterRegionSecondaryCache() {
    using namespace AVEVA::RocksDB::Plugin::Core;

    if (rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::SecondaryCache>(kRegionSecondaryCacheName) !=
        nullptr) {
        return;
    }

    rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::SecondaryCache>(
        kRegionSecondaryCacheName,
        [](const std::string& /* uri */, std::unique_ptr<rocksdb::SecondaryCache>* guard,
           std::string* errmsg) -> rocksdb::SecondaryCache* {
            const auto cacheDir = Env("AVEVA_DB_BENCH_SECONDARY_CACHE_DIR");
            if (!cacheDir) {
                *errmsg = "AVEVA_DB_BENCH_SECONDARY_CACHE_DIR must be set to use " +
                          std::string(kRegionSecondaryCacheName);
                return nullptr;
            }

            FileBasedSecondaryCacheOptions options;
            options.engine = FileBasedSecondaryCacheEngine::kRegion;
            options.capacity = EnvOr<size_t>("AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB", options.capacity >> 20) << 20;
            options.regionSize = EnvOr<size_t>("AVEVA_DB_BENCH_REGION_SIZE_MB", options.regionSize >> 20) << 20;
            options.flushBlockSize =
                EnvOr<size_t>("AVEVA_DB_BENCH_REGION_FLUSH_BLOCK_KB", options.flushBlockSize >> 10) << 10;
            options.indexShards = EnvOr<uint32_t>("AVEVA_DB_BENCH_REGION_INDEX_SHARDS", options.indexShards);
            // Keep small benchmark regions valid: an entry must fit in a region minus its 1 MiB footer.
            if (options.regionSize > (1ULL << 20)) {
                options.maxEntrySize = std::min<size_t>(options.maxEntrySize, options.regionSize - (1ULL << 20));
            }
            if (const auto admission = Env("AVEVA_DB_BENCH_REGION_ADMISSION")) {
                if (*admission == "second-chance") {
                    options.admissionPolicy = FileBasedSecondaryCacheAdmissionPolicy::kSecondChance;
                } else if (*admission != "admit-all") {
                    *errmsg = "AVEVA_DB_BENCH_REGION_ADMISSION must be 'admit-all' or 'second-chance'";
                    return nullptr;
                }
            }

            auto logger = std::make_shared<
                boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
            try {
                *guard = std::make_unique<FileBasedCompressedSecondaryCache>(std::filesystem::path(*cacheDir),
                                                                             options, std::move(logger));
            } catch (const std::exception& e) {
                *errmsg = e.what();
                return nullptr;
            }
            return guard->get();
        });
}

} // namespace

int main(int argc, char** argv) {
    using AVEVA::RocksDB::Plugin::Azure::Plugin;
    using AVEVA::RocksDB::Plugin::Azure::Impl::StorageAccount;
    using AVEVA::RocksDB::Plugin::Azure::Models::ServicePrincipalStorageInfo;

    if (!Env("AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL")) {
        // Local-filesystem mode: skip Azure registration, keep the secondary cache URIs.
        RegisterFileBasedSecondaryCache();
        RegisterRegionSecondaryCache();
        const auto rc = rocksdb::db_bench_tool(argc, argv);
        std::fflush(nullptr);
        std::cout.flush();
        std::_Exit(rc);
    }

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
    RegisterRegionSecondaryCache();

    std::cerr << "aveva_db_bench: Azure plugin registered.\n"
              << "  Suggested flags:\n"
              << "    --fs_uri=" << fsUri << "\n"
              << "    --db=" << dbPath << "/<your-db-name>\n"
              << "  To also attach a secondary cache, add:\n"
              << "    --secondary_cache_uri=" << kFileBasedSecondaryCacheName << "\n"
              << "  with AVEVA_DB_BENCH_SECONDARY_CACHE_DIR set (and optionally\n"
              << "  AVEVA_DB_BENCH_SECONDARY_CACHE_CAPACITY_MB).\n";

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
