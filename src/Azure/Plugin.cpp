// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileBasedCompressedSecondaryCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"

#include <rocksdb/db.h>
#include <rocksdb/file_system.h>
#include <rocksdb/utilities/object_registry.h>

namespace AVEVA::RocksDB::Plugin::Azure
{
    rocksdb::Status Plugin::Register(rocksdb::ConfigOptions& configOptions,
        rocksdb::Env** env,
        std::shared_ptr<rocksdb::Env>* guard,
        Models::ServicePrincipalStorageInfo primary,
        std::optional<Models::ServicePrincipalStorageInfo> backup,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        int64_t dataFileBufferSize,
        int64_t dataFileInitialSize,
        std::optional<std::string_view> cachePath,
        size_t maxCacheSize)
    {
        auto pluginName = std::string(Name) + primary.GetDbName();
        if (backup)
        {
            pluginName += backup->GetDbName();
        }

        if (cachePath && rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::SecondaryCache>(pluginName) == nullptr)
        {
            const auto cacheDir = std::filesystem::path(*cachePath);
            rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::SecondaryCache>(pluginName,
                [cacheDir, maxCacheSize, logger](const std::string& /* uri */, std::unique_ptr<rocksdb::SecondaryCache>* sc, std::string* /* errmsg */)
                {
                    sc->reset(new Core::FileBasedCompressedSecondaryCache(
                        cacheDir,
                        std::make_shared<Core::LocalFilesystem>(logger),
                        maxCacheSize,
                        Core::FileBasedCompressedSecondaryCache::kDefaultZstdLevel,
                        logger));
                    return sc->get();
                });
        }

        if (rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::FileSystem>(pluginName) == nullptr)
        {
            rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::FileSystem>(pluginName,
                [=, primary = std::move(primary), backup = std::move(backup), logger = std::move(logger)](const std::string& /* uri */, std::unique_ptr<rocksdb::FileSystem>* f, std::string* /* errmsg */)
                {
                    auto impl = std::make_unique<Impl::BlobFilesystemImpl>
                        (
                            primary,
                            backup,
                            dataFileInitialSize,
                            dataFileBufferSize,
                            logger
                        );

                    *f = std::unique_ptr<rocksdb::FileSystem>(new BlobFilesystem(rocksdb::FileSystem::Default(), std::move(impl), logger));
                    return f->get();
                });
        }

        return rocksdb::Env::CreateFromUri(configOptions, "", pluginName, env, guard);
    }

    rocksdb::Status Plugin::Register(rocksdb::ConfigOptions& configOptions,
        rocksdb::Env** env,
        std::shared_ptr<rocksdb::Env>* guard,
        Models::ChainedCredentialInfo primary,
        std::optional<Models::ChainedCredentialInfo> backup,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        int64_t dataFileBufferSize,
        int64_t dataFileInitialSize,
        std::optional<std::string_view> cachePath,
        size_t maxCacheSize)
    {
        auto pluginName = std::string(Name) + primary.GetDbName();
        if (backup)
        {
            pluginName += backup->GetDbName();
        }

        if (cachePath && rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::SecondaryCache>(pluginName) == nullptr)
        {
            const auto cacheDir = std::filesystem::path(*cachePath);
            rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::SecondaryCache>(pluginName,
                [cacheDir, maxCacheSize, logger](const std::string& /* uri */, std::unique_ptr<rocksdb::SecondaryCache>* sc, std::string* /* errmsg */)
                {
                    sc->reset(new Core::FileBasedCompressedSecondaryCache(
                        cacheDir,
                        std::make_shared<Core::LocalFilesystem>(logger),
                        maxCacheSize,
                        Core::FileBasedCompressedSecondaryCache::kDefaultZstdLevel,
                        logger));
                    return sc->get();
                });
        }

        if (rocksdb::ObjectLibrary::Default()->FindFactory<rocksdb::FileSystem>(pluginName) == nullptr)
        {
            rocksdb::ObjectLibrary::Default()->AddFactory<rocksdb::FileSystem>(pluginName,
                [=, primary = std::move(primary), backup = std::move(backup), logger = std::move(logger)](const std::string& /* uri */, std::unique_ptr<rocksdb::FileSystem>* f, std::string* /* errmsg */)
                {
                    auto impl = std::make_unique<Impl::BlobFilesystemImpl>
                        (
                            primary,
                            backup,
                            dataFileInitialSize,
                            dataFileBufferSize,
                            logger
                        );

                    *f = std::unique_ptr<rocksdb::FileSystem>(new BlobFilesystem(rocksdb::FileSystem::Default(), std::move(impl), std::move(logger)));
                    return f->get();
                });
        }

        return rocksdb::Env::CreateFromUri(configOptions, "", pluginName, env, guard);
    }
}
