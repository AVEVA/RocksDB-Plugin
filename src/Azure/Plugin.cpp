// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"

#include <rocksdb/db.h>
#include <rocksdb/file_system.h>
#include <rocksdb/utilities/object_registry.h>

#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace AVEVA::RocksDB::Plugin::Azure {
namespace {
using FileSystemFactory = std::function<std::unique_ptr<rocksdb::FileSystem>()>;

/// <summary>
/// The settings of the latest registration for a plugin name. RocksDB keeps the first factory added under a name,
/// so the factory reads this state instead of capturing the settings; otherwise registering again (for example
/// with a new io_context after the previous one was destroyed) would keep using the old settings.
/// </summary>
struct Registration {
    std::mutex Mutex;
    FileSystemFactory Create;
};

/// <summary>
/// Returns the registration state for `pluginName`, adding the RocksDB factory that uses it on first call.
/// </summary>
std::shared_ptr<Registration> GetOrAddRegistration(const std::string& pluginName) {
    static std::mutex mutex;
    static std::unordered_map<std::string, std::shared_ptr<Registration>> registrations;

    std::scoped_lock lock(mutex);
    auto& registration = registrations[pluginName];
    if (!registration) {
        registration = std::make_shared<Registration>();
        rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::FileSystem>(
            pluginName,
            [registration](const std::string& /* uri */, std::unique_ptr<rocksdb::FileSystem>* f, std::string* errmsg) {
                FileSystemFactory create;
                {
                    std::scoped_lock registrationLock(registration->Mutex);
                    create = registration->Create;
                }

                if (!create) {
                    if (errmsg != nullptr) {
                        *errmsg = "Azure blob filesystem plugin has not been registered yet";
                    }
                    return static_cast<rocksdb::FileSystem*>(nullptr);
                }

                *f = create();
                return f->get();
            });
    }

    return registration;
}

/// <summary>
/// Records the settings for the given storage accounts' plugin name, then creates the Env from it. Every
/// filesystem created borrows `ioContext`, which is never owned by the plugin.
/// </summary>
template <class StorageInfo>
rocksdb::Status
RegisterImpl(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             boost::asio::io_context& ioContext, StorageInfo primary, std::optional<StorageInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
             size_t maxCacheSize) {
    auto pluginName = std::string(Plugin::Name) + primary.GetDbName();
    if (backup) {
        pluginName += backup->GetDbName();
    }

    // Own a copy of the cache path: the factory may run long after the caller's string is gone.
    std::optional<std::string> ownedCachePath;
    if (cachePath) {
        ownedCachePath.emplace(*cachePath);
    }

    FileSystemFactory create = [&ioContext, primary = std::move(primary), backup = std::move(backup),
                                logger = std::move(logger), dataFileBufferSize, dataFileInitialSize,
                                cachePath = std::move(ownedCachePath), maxCacheSize]() {
        auto impl = std::make_unique<Impl::BlobFilesystemImpl>(
            ioContext, primary, backup, dataFileInitialSize, dataFileBufferSize, logger,
            cachePath ? std::optional<std::string_view>(*cachePath) : std::nullopt, maxCacheSize);
        return std::unique_ptr<rocksdb::FileSystem>(
            new BlobFilesystem(rocksdb::FileSystem::Default(), std::move(impl), logger));
    };

    auto registration = GetOrAddRegistration(pluginName);
    {
        std::scoped_lock lock(registration->Mutex);
        registration->Create = std::move(create);
    }

    return rocksdb::Env::CreateFromUri(configOptions, "", pluginName, env, guard);
}
} // namespace

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
                 std::optional<Models::ServicePrincipalStorageInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize) {
    return RegisterImpl(configOptions, env, guard, ioContext, std::move(primary), std::move(backup), std::move(logger),
                        dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
                 std::optional<Models::ChainedCredentialInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize) {
    return RegisterImpl(configOptions, env, guard, ioContext, std::move(primary), std::move(backup), std::move(logger),
                        dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}
} // namespace AVEVA::RocksDB::Plugin::Azure