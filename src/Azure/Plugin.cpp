// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include <rocksdb/file_system.h>
#include <rocksdb/utilities/object_registry.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
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
/// The registration state per plugin name, shared by registration and unregistration.
/// </summary>
struct RegistrationTable {
    std::mutex Mutex;
    std::unordered_map<std::string, std::shared_ptr<Registration>> Registrations;
};

RegistrationTable& Table() {
    static RegistrationTable table;
    return table;
}

/// <summary>
/// Drops the factory settings (and with them the io_context reference or ownership) for `pluginName`. The RocksDB
/// factory itself stays in the ObjectLibrary, which cannot remove entries, and reports "not registered" until a
/// later Register call fills the state again.
/// </summary>
void ClearRegistration(const std::string& pluginName) {
    std::shared_ptr<Registration> registration;
    {
        std::scoped_lock lock(Table().Mutex);
        if (const auto it = Table().Registrations.find(pluginName); it != Table().Registrations.end()) {
            registration = it->second;
        }
    }
    if (registration) {
        // Destroy the old factory outside the lock: releasing an owned io_context may join its thread.
        FileSystemFactory released;
        std::scoped_lock lock(registration->Mutex);
        released = std::move(registration->Create);
        registration->Create = nullptr;
    }
}

/// <summary>
/// Returns the registration state for `pluginName`, adding the RocksDB factory that uses it on first call.
/// </summary>
std::shared_ptr<Registration> GetOrAddRegistration(const std::string& pluginName) {
    std::scoped_lock lock(Table().Mutex);
    auto& registration = Table().Registrations[pluginName];
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

                // RocksDB's C-style callers cannot handle exceptions escaping a factory.
                try {
                    *f = create();
                } catch (const std::exception& ex) {
                    if (errmsg != nullptr) {
                        *errmsg = std::string("Failed to create Azure blob filesystem: ") + ex.what();
                    }
                    return static_cast<rocksdb::FileSystem*>(nullptr);
                } catch (...) {
                    if (errmsg != nullptr) {
                        *errmsg = "Failed to create Azure blob filesystem: unknown error";
                    }
                    return static_cast<rocksdb::FileSystem*>(nullptr);
                }
                return f->get();
            });
    }

    return registration;
}

/// <summary>
/// Builds the RocksDB registry name for a primary/backup pair. Each part is length-prefixed and the whole is
/// hex-encoded, so distinct (account, database) pairs can never collide ("ab"+"c" vs "a"+"bc") and the result
/// contains only characters that are safe in a RocksDB object-registry identifier.
/// </summary>
template <class StorageInfo>
std::string PluginNameFor(const StorageInfo& primary, const std::optional<StorageInfo>& backup) {
    std::string canonical;
    const auto append = [&canonical](const std::string& part) {
        canonical += std::to_string(part.size());
        canonical += ':';
        canonical += part;
    };
    append(primary.GetStorageAccountUrl());
    append(primary.GetDbName());
    if (backup) {
        append(backup->GetStorageAccountUrl());
        append(backup->GetDbName());
    }

    static constexpr std::string_view digits = "0123456789abcdef";
    std::string name(Plugin::Name);
    name += '-';
    for (const char ch : canonical) {
        const auto c = static_cast<unsigned char>(ch);
        name += digits[c >> 4U];
        name += digits[c & 0x0FU];
    }
    return name;
}

/// <summary>
/// The name earlier versions registered: Name + primary database name + backup database name.
/// </summary>
template <class StorageInfo>
std::string LegacyPluginNameFor(const StorageInfo& primary, const std::optional<StorageInfo>& backup) {
    auto name = std::string(Plugin::Name) + primary.GetDbName();
    if (backup) {
        name += backup->GetDbName();
    }
    return name;
}

/// <summary>
/// An io_context with one worker thread, for callers of the deprecated Register overloads that have no io_context
/// of their own. Shared by every filesystem created that way and kept alive by them.
/// </summary>
class OwnedIoContext {
    boost::asio::io_context m_context;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_workGuard{
        boost::asio::make_work_guard(m_context)};
    std::thread m_thread;

  public:
    OwnedIoContext() : m_thread([this] { m_context.run(); }) {}
    ~OwnedIoContext() {
        m_workGuard.reset();
        m_thread.join();
    }
    OwnedIoContext(const OwnedIoContext&) = delete;
    OwnedIoContext& operator=(const OwnedIoContext&) = delete;

    [[nodiscard]] boost::asio::io_context& Context() { return m_context; }

    /// <summary>The process-wide instance; recreated when every previous user has released it.</summary>
    static std::shared_ptr<OwnedIoContext> Acquire() {
        static std::mutex mutex;
        static std::weak_ptr<OwnedIoContext> current;
        std::scoped_lock lock(mutex);
        auto shared = current.lock();
        if (!shared) {
            shared = std::make_shared<OwnedIoContext>();
            current = shared;
        }
        return shared;
    }
};

/// <summary>
/// Records the settings for the given storage accounts' plugin name (and the legacy name as an alias), then creates
/// the Env from it. Every filesystem created borrows `ioContext`; `owner`, when set, keeps whatever owns it alive
/// for as long as the factory or any filesystem exists.
/// </summary>
template <class StorageInfo>
rocksdb::Status
RegisterImpl(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             boost::asio::io_context& ioContext, std::shared_ptr<void> owner, StorageInfo primary,
             std::optional<StorageInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
             size_t maxCacheSize) {
    auto pluginName = PluginNameFor(primary, backup);
    auto legacyName = LegacyPluginNameFor(primary, backup);

    // Own a copy of the cache path: the factory may run long after the caller's string is gone.
    std::optional<std::string> ownedCachePath;
    if (cachePath) {
        ownedCachePath.emplace(*cachePath);
    }

    FileSystemFactory create = [&ioContext, owner = std::move(owner), primary = std::move(primary),
                                backup = std::move(backup), logger = std::move(logger), dataFileBufferSize,
                                dataFileInitialSize, cachePath = std::move(ownedCachePath), maxCacheSize]() {
        auto impl = std::make_unique<Impl::BlobFilesystemImpl>(
            ioContext, primary, backup, dataFileInitialSize, dataFileBufferSize, logger,
            cachePath ? std::optional<std::string_view>(*cachePath) : std::nullopt, maxCacheSize);
        return std::unique_ptr<rocksdb::FileSystem>(
            new BlobFilesystem(rocksdb::FileSystem::Default(), std::move(impl), logger, owner));
    };

    for (const auto& name : {pluginName, legacyName}) {
        auto registration = GetOrAddRegistration(name);
        std::scoped_lock lock(registration->Mutex);
        registration->Create = create;
    }

    return rocksdb::Env::CreateFromUri(configOptions, "", pluginName, env, guard);
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
                 size_t maxCacheSize) {
    return RegisterImpl(configOptions, env, guard, ioContext, nullptr, std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
                 std::optional<Models::ChainedCredentialInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize) {
    return RegisterImpl(configOptions, env, guard, ioContext, nullptr, std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 std::shared_ptr<boost::asio::io_context> ioContext, Models::ServicePrincipalStorageInfo primary,
                 std::optional<Models::ServicePrincipalStorageInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize) {
    if (!ioContext) {
        return rocksdb::Status::InvalidArgument("ioContext cannot be null");
    }
    auto& context = *ioContext;
    return RegisterImpl(configOptions, env, guard, context, std::move(ioContext), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 std::shared_ptr<boost::asio::io_context> ioContext, Models::ChainedCredentialInfo primary,
                 std::optional<Models::ChainedCredentialInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize) {
    if (!ioContext) {
        return rocksdb::Status::InvalidArgument("ioContext cannot be null");
    }
    auto& context = *ioContext;
    return RegisterImpl(configOptions, env, guard, context, std::move(ioContext), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}

void Plugin::Unregister(const std::string& name) { ClearRegistration(name); }

void Plugin::Unregister(const Models::ServicePrincipalStorageInfo& primary,
                        const std::optional<Models::ServicePrincipalStorageInfo>& backup) {
    ClearRegistration(PluginNameFor(primary, backup));
    ClearRegistration(LegacyPluginNameFor(primary, backup));
}

void Plugin::Unregister(const Models::ChainedCredentialInfo& primary,
                        const std::optional<Models::ChainedCredentialInfo>& backup) {
    ClearRegistration(PluginNameFor(primary, backup));
    ClearRegistration(LegacyPluginNameFor(primary, backup));
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 Models::ServicePrincipalStorageInfo primary, std::optional<Models::ServicePrincipalStorageInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize) {
    auto owner = OwnedIoContext::Acquire();
    auto& context = owner->Context();
    return RegisterImpl(configOptions, env, guard, context, std::move(owner), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}

rocksdb::Status
Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
                 Models::ChainedCredentialInfo primary, std::optional<Models::ChainedCredentialInfo> backup,
                 std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
                 int64_t dataFileBufferSize, int64_t dataFileInitialSize, std::optional<std::string_view> cachePath,
                 size_t maxCacheSize) {
    auto owner = OwnedIoContext::Acquire();
    auto& context = owner->Context();
    return RegisterImpl(configOptions, env, guard, context, std::move(owner), std::move(primary), std::move(backup),
                        std::move(logger), dataFileBufferSize, dataFileInitialSize, cachePath, maxCacheSize);
}
} // namespace AVEVA::RocksDB::Plugin::Azure