// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "PluginRegistry.hpp"
#include <rocksdb/file_system.h>

#include <boost/asio/io_context.hpp>
#include <boost/log/trivial.hpp>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace AVEVA::RocksDB::Plugin::Azure {
namespace {
using Registry::FileSystemFactory;
using Registry::LegacyPluginNameFor;
using Registry::OwnedIoContext;
using Registry::PluginNameFor;
using Logger = std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>;

/// <summary>
/// Everything one Register call carries, so the overloads translate their arguments once and RegisterImpl does not
/// repeat a long positional signature.
/// </summary>
template <class StorageInfo> struct RegisterOptions {
    boost::asio::io_context& IoContext;
    // Keeps whatever owns IoContext alive for as long as the factory or any filesystem exists.
    std::shared_ptr<void> Owner;
    StorageInfo Primary;
    std::optional<StorageInfo> Backup;
    Logger Log;
    int64_t DataFileBufferSize;
    int64_t DataFileInitialSize;
    std::optional<std::string_view> CachePath;
    size_t MaxCacheSize;
    int64_t MaxPrefetchBytes;
};

/// <summary>
/// Records the settings for the given storage accounts' plugin name (and the legacy name as an alias), then creates
/// the Env from it. Every filesystem created borrows `ioContext`; `owner`, when set, keeps whatever owns it alive
/// for as long as the factory or any filesystem exists.
/// </summary>
template <class StorageInfo>
rocksdb::Status RegisterImpl(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                             std::shared_ptr<rocksdb::Env>* guard, RegisterOptions<StorageInfo> options) {
    if (options.MaxPrefetchBytes < 0) {
        return rocksdb::Status::InvalidArgument("maxPrefetchBytes cannot be negative");
    }
    if (const auto problem = Impl::InvalidDataFileSizes(options.DataFileInitialSize, options.DataFileBufferSize);
        !problem.empty()) {
        return rocksdb::Status::InvalidArgument(std::string(problem));
    }
    if (options.MaxCacheSize > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        return rocksdb::Status::InvalidArgument("maxCacheSize cannot exceed INT64_MAX");
    }
    if (options.CachePath && options.MaxCacheSize == 0 && options.Log) {
        BOOST_LOG_SEV(*options.Log, boost::log::trivial::warning)
            << "cachePath is set but maxCacheSize is 0: nothing will ever be cached";
    }
    auto pluginName = PluginNameFor(options.Primary, options.Backup);
    auto legacyName = LegacyPluginNameFor(options.Primary, options.Backup);

    // Own a copy of the cache path: the factory may run long after the caller's string is gone.
    std::optional<std::string> ownedCachePath;
    if (options.CachePath) {
        ownedCachePath.emplace(*options.CachePath);
    }

    FileSystemFactory create = [&ioContext = options.IoContext, owner = std::move(options.Owner),
                                primary = std::move(options.Primary), backup = std::move(options.Backup),
                                logger = std::move(options.Log), dataFileBufferSize = options.DataFileBufferSize,
                                dataFileInitialSize = options.DataFileInitialSize,
                                cachePath = std::move(ownedCachePath), maxCacheSize = options.MaxCacheSize,
                                maxPrefetchBytes = options.MaxPrefetchBytes]() {
        auto impl = std::make_unique<Impl::BlobFilesystemImpl>(
            ioContext, primary, backup, dataFileInitialSize, dataFileBufferSize, logger,
            cachePath ? std::optional<std::string_view>(*cachePath) : std::nullopt, maxCacheSize);
        impl->SetMaxPrefetchBytes(maxPrefetchBytes);
        return std::unique_ptr<rocksdb::FileSystem>(
            new BlobFilesystem(rocksdb::FileSystem::Default(), std::move(impl), logger, owner));
    };

    for (const auto& name : {pluginName, legacyName}) {
        Registry::SetFactory(name, create);
    }

    return rocksdb::Env::CreateFromUri(configOptions, "", pluginName, env, guard);
}

/// <summary>
/// Shared body of every Register overload: they differ only in the credential type and in who owns the io_context.
/// </summary>
template <class StorageInfo>
rocksdb::Status RegisterWith(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                             std::shared_ptr<rocksdb::Env>* guard, boost::asio::io_context& ioContext,
                             std::shared_ptr<void> owner, StorageInfo primary, std::optional<StorageInfo> backup,
                             Logger logger, int64_t dataFileBufferSize, int64_t dataFileInitialSize,
                             std::optional<std::string_view> cachePath, size_t maxCacheSize, int64_t maxPrefetchBytes) {
    return RegisterImpl(configOptions, env, guard,
                        RegisterOptions<StorageInfo>{.IoContext = ioContext,
                                                     .Owner = std::move(owner),
                                                     .Primary = std::move(primary),
                                                     .Backup = std::move(backup),
                                                     .Log = std::move(logger),
                                                     .DataFileBufferSize = dataFileBufferSize,
                                                     .DataFileInitialSize = dataFileInitialSize,
                                                     .CachePath = cachePath,
                                                     .MaxCacheSize = maxCacheSize,
                                                     .MaxPrefetchBytes = maxPrefetchBytes});
}
} // namespace

std::string Plugin::NameFor(const Models::ServicePrincipalStorageInfo& primary,
                            const std::optional<Models::ServicePrincipalStorageInfo>& backup) {
    return PluginNameFor(primary, backup);
}

std::string Plugin::NameFor(const Models::ChainedCredentialInfo& primary,
                            const std::optional<Models::ChainedCredentialInfo>& backup) {
    return PluginNameFor(primary, backup);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
                 std::optional<Models::ServicePrincipalStorageInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize, int64_t maxPrefetchBytes) {
    return RegisterWith(configOptions, env, guard, ioContext, nullptr, std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize,
                        maxPrefetchBytes);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
                 std::optional<Models::ChainedCredentialInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize, int64_t maxPrefetchBytes) {
    return RegisterWith(configOptions, env, guard, ioContext, nullptr, std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize,
                        maxPrefetchBytes);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 std::shared_ptr<boost::asio::io_context> ioContext, Models::ServicePrincipalStorageInfo primary,
                 std::optional<Models::ServicePrincipalStorageInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize, int64_t maxPrefetchBytes) {
    if (!ioContext) {
        return rocksdb::Status::InvalidArgument("ioContext cannot be null");
    }
    auto& context = *ioContext;
    return RegisterWith(configOptions, env, guard, context, std::move(ioContext), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize,
                        maxPrefetchBytes);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 std::shared_ptr<boost::asio::io_context> ioContext, Models::ChainedCredentialInfo primary,
                 std::optional<Models::ChainedCredentialInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize, int64_t maxPrefetchBytes) {
    if (!ioContext) {
        return rocksdb::Status::InvalidArgument("ioContext cannot be null");
    }
    auto& context = *ioContext;
    return RegisterWith(configOptions, env, guard, context, std::move(ioContext), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize,
                        maxPrefetchBytes);
}

void Plugin::Unregister(const std::string& name) { Registry::ClearRegistration(name); }

void Plugin::Unregister(const Models::ServicePrincipalStorageInfo& primary,
                        const std::optional<Models::ServicePrincipalStorageInfo>& backup) {
    Registry::ClearRegistration(PluginNameFor(primary, backup));
    Registry::ClearRegistration(LegacyPluginNameFor(primary, backup));
}

void Plugin::Unregister(const Models::ChainedCredentialInfo& primary,
                        const std::optional<Models::ChainedCredentialInfo>& backup) {
    Registry::ClearRegistration(PluginNameFor(primary, backup));
    Registry::ClearRegistration(LegacyPluginNameFor(primary, backup));
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 Models::ServicePrincipalStorageInfo primary, std::optional<Models::ServicePrincipalStorageInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize, int64_t maxPrefetchBytes) {
    auto owner = OwnedIoContext::Acquire();
    auto& context = owner->Context();
    return RegisterWith(configOptions, env, guard, context, std::move(owner), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize,
                        maxPrefetchBytes);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 Models::ChainedCredentialInfo primary, std::optional<Models::ChainedCredentialInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize, int64_t maxPrefetchBytes) {
    auto owner = OwnedIoContext::Acquire();
    auto& context = owner->Context();
    return RegisterWith(configOptions, env, guard, context, std::move(owner), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize,
                        maxPrefetchBytes);
}
} // namespace AVEVA::RocksDB::Plugin::Azure