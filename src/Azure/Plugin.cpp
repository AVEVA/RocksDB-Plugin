// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/HandleFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "PluginRegistry.hpp"
#include <rocksdb/file_system.h>

#include <boost/asio/io_context.hpp>
#include <boost/log/trivial.hpp>

#include <filesystem>
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
/// the Env from it. The factory and every filesystem it creates only refer to the host-owned io_context.
/// </summary>
template <class StorageInfo>
rocksdb::Status RegisterAccounts(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                 std::shared_ptr<rocksdb::Env>* guard, boost::asio::io_context& ioContext,
                                 Plugin::RegisterOptions options, Plugin::AccountPair<StorageInfo> accounts) {
    // Every downstream error path dereferences the logger unconditionally, so a null one is replaced here, once.
    if (!options.Log) {
        options.Log = std::make_shared<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>();
    }
    if (options.Cache && options.Cache->MaxSize.Bytes() == ByteCount{}) {
        BOOST_LOG_SEV(*options.Log, boost::log::trivial::warning)
            << "cachePath is set but maxCacheSize is 0: nothing will ever be cached";
    }
    auto pluginName = PluginNameFor(accounts.Primary, accounts.Backup);
    auto legacyName = LegacyPluginNameFor(accounts.Primary, accounts.Backup);

    // A pointer rather than a reference so the factory stays copyable; it is never null.
    FileSystemFactory create = [context = &ioContext, options = std::move(options), accounts = std::move(accounts)]() {
        const auto cachePath =
            options.Cache ? std::optional<std::filesystem::path>(options.Cache->Directory) : std::nullopt;
        const auto maxCacheSize = options.Cache ? options.Cache->MaxSize.Bytes() : ByteCount{};
        auto impl = std::make_unique<Impl::BlobFilesystemImpl>(*context, accounts.Primary, accounts.Backup,
                                                               options.DataFileInitialSize, options.DataFileBufferSize,
                                                               options.Log, cachePath, maxCacheSize);
        impl->SetMaxPrefetchBytes(options.MaxPrefetchBytes.Bytes());
        return std::unique_ptr<rocksdb::FileSystem>(
            Impl::HandleFactory::Create<BlobFilesystem>(rocksdb::FileSystem::Default(), std::move(impl), options.Log));
    };

    for (const auto& name : {pluginName, legacyName}) {
        Registry::SetFactory(name, create, Registry::RegistrationOwner{pluginName});
    }

    return rocksdb::Env::CreateFromUri(configOptions, "", pluginName, env, guard);
}
} // namespace

std::string Plugin::NameFor(const StorageAccounts& accounts) {
    return std::visit([](const auto& pair) { return PluginNameFor(pair.Primary, pair.Backup); }, accounts);
}

std::string Plugin::UniquePrefix(const Models::AccountUrl& accountUrl, const Models::DbName& dbName) {
    return Impl::StorageAccount::UniquePrefix(accountUrl, dbName);
}

rocksdb::Status Plugin::Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                 std::shared_ptr<rocksdb::Env>* guard, boost::asio::io_context& ioContext,
                                 RegisterOptions options) {
    if (const auto problem = Impl::InvalidDataFileSizes(options.DataFileInitialSize, options.DataFileBufferSize);
        !problem.empty()) {
        return rocksdb::Status::InvalidArgument(std::string(problem));
    }

    // Moving the accounts out leaves a valid (empty) variant behind; the rest of the options travel on.
    auto accounts = std::move(options.Accounts);
    return std::visit(
        [&](auto& pair) {
            return RegisterAccounts(configOptions, env, guard, ioContext, std::move(options), std::move(pair));
        },
        accounts);
}

// Clears the current name, and the legacy alias only while this registration still owns it.
void Plugin::Unregister(const StorageAccounts& accounts) {
    std::visit(
        [](const auto& pair) {
            const auto name = PluginNameFor(pair.Primary, pair.Backup);
            Registry::ClearRegistration(name);
            Registry::ClearRegistrationOwnedBy(LegacyPluginNameFor(pair.Primary, pair.Backup),
                                               Registry::RegistrationOwner{name});
        },
        accounts);
}
} // namespace AVEVA::RocksDB::Plugin::Azure
