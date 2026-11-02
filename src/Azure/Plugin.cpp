// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "PluginRegistry.hpp"
#include <rocksdb/file_system.h>

#include <boost/asio/io_context.hpp>
#include <boost/log/trivial.hpp>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace AVEVA::RocksDB::Plugin::Azure {
namespace {
using Core::ByteCount;
using Registry::FileSystemFactory;
using Registry::LegacyPluginNameFor;
using Registry::PluginNameFor;

/// <summary>
/// Records the settings for the given storage accounts' plugin name (and the legacy name as an alias), then creates
/// the Env from it. Every filesystem created shares ownership of the io_context.
/// </summary>
template <class StorageInfo>
rocksdb::Status RegisterAccounts(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                 std::shared_ptr<rocksdb::Env>* guard, Plugin::RegisterOptions options,
                                 Plugin::AccountPair<StorageInfo> accounts) {
    // Every downstream error path dereferences the logger unconditionally, so a null one is replaced here, once.
    if (!options.Log) {
        options.Log = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    }
    if (options.Cache && options.Cache->MaxSize == ByteCount{}) {
        BOOST_LOG_SEV(*options.Log, boost::log::trivial::warning)
            << "cachePath is set but maxCacheSize is 0: nothing will ever be cached";
    }
    auto pluginName = PluginNameFor(accounts.Primary, accounts.Backup);
    auto legacyName = LegacyPluginNameFor(accounts.Primary, accounts.Backup);

    FileSystemFactory create = [options = std::move(options), accounts = std::move(accounts)]() {
        const auto cachePath = options.Cache ? std::optional<std::string_view>(options.Cache->Directory) : std::nullopt;
        const auto maxCacheSize = options.Cache ? options.Cache->MaxSize : ByteCount{};
        auto impl = std::make_unique<Impl::BlobFilesystemImpl>(*options.IoContext, accounts.Primary, accounts.Backup,
                                                               options.DataFileInitialSize, options.DataFileBufferSize,
                                                               options.Log, cachePath, maxCacheSize);
        impl->SetMaxPrefetchBytes(options.MaxPrefetchBytes);
        return std::unique_ptr<rocksdb::FileSystem>(
            new BlobFilesystem(rocksdb::FileSystem::Default(), std::move(impl), options.Log, options.IoContext));
    };

    for (const auto& name : {pluginName, legacyName}) {
        Registry::SetFactory(name, create, Registry::RegistrationOwner{pluginName});
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

std::string Plugin::UniquePrefix(const std::string& accountUrl, const std::string& container) {
    return Impl::StorageAccount::UniquePrefix(accountUrl, container);
}

rocksdb::Status Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                 std::shared_ptr<rocksdb::Env>* guard, RegisterOptions options) {
    if (!options.IoContext) {
        return rocksdb::Status::InvalidArgument("ioContext cannot be null");
    }
    if (options.MaxPrefetchBytes > Impl::MaxSignedBytes) {
        return rocksdb::Status::InvalidArgument("prefetch size cannot exceed INT64_MAX");
    }
    if (const auto problem = Impl::InvalidDataFileSizes(options.DataFileInitialSize, options.DataFileBufferSize);
        !problem.empty()) {
        return rocksdb::Status::InvalidArgument(std::string(problem));
    }
    if (options.Cache && options.Cache->MaxSize > Impl::MaxSignedBytes) {
        return rocksdb::Status::InvalidArgument("maxCacheSize cannot exceed INT64_MAX");
    }

    // Moving the accounts out leaves a valid (empty) variant behind; the rest of the options travel on.
    auto accounts = std::move(options.Accounts);
    return std::visit(
        [&](auto& pair) { return RegisterAccounts(configOptions, env, guard, std::move(options), std::move(pair)); },
        accounts);
}

void Plugin::Unregister(const std::string& name) { Registry::ClearRegistration(name); }

void Plugin::Unregister(const Models::ServicePrincipalStorageInfo& primary,
                        const std::optional<Models::ServicePrincipalStorageInfo>& backup) {
    const auto name = PluginNameFor(primary, backup);
    Registry::ClearRegistration(name);
    Registry::ClearRegistrationOwnedBy(LegacyPluginNameFor(primary, backup), Registry::RegistrationOwner{name});
}

void Plugin::Unregister(const Models::ChainedCredentialInfo& primary,
                        const std::optional<Models::ChainedCredentialInfo>& backup) {
    const auto name = PluginNameFor(primary, backup);
    Registry::ClearRegistration(name);
    Registry::ClearRegistrationOwnedBy(LegacyPluginNameFor(primary, backup), Registry::RegistrationOwner{name});
}
} // namespace AVEVA::RocksDB::Plugin::Azure
