// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Plugin.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/BlobFilesystem.hpp"

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
        std::shared_ptr<boost::log::sources::logger_mt> logger,
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
                            logger,
                            cachePath,
                            maxCacheSize
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
        std::shared_ptr<boost::log::sources::logger_mt> logger,
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
                            logger,
                            cachePath,
                            maxCacheSize
                        );

                    *f = std::unique_ptr<rocksdb::FileSystem>(new BlobFilesystem(rocksdb::FileSystem::Default(), std::move(impl), std::move(logger)));
                    return f->get();
                });
        }

        return rocksdb::Env::CreateFromUri(configOptions, "", pluginName, env, guard);
    }
}
