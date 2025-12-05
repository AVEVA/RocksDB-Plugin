// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"

#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>

#include <memory>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Azure
{
    struct Plugin
    {
        static const constexpr std::string_view Name = "azblobfs";
        static rocksdb::Status Register(rocksdb::ConfigOptions& configOptions,
            rocksdb::Env** env,
            std::shared_ptr<rocksdb::Env>* guard,
            Models::ServicePrincipalStorageInfo primary,
            std::optional<Models::ServicePrincipalStorageInfo> backup,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            int64_t dataFileBufferSize = Impl::Configuration::PageBlob::DefaultBufferSize,
            int64_t dataFileInitialSize = Impl::Configuration::PageBlob::DefaultSize,
            std::optional<std::string_view> cachePath = {},
            size_t maxCacheSize = Impl::Configuration::MaxCacheSize);
        static rocksdb::Status Register(rocksdb::ConfigOptions& configOptions,
            rocksdb::Env** env,
            std::shared_ptr<rocksdb::Env>* guard,
            Models::ChainedCredentialInfo primary,
            std::optional<Models::ChainedCredentialInfo> backup,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            int64_t dataFileBufferSize = Impl::Configuration::PageBlob::DefaultBufferSize,
            int64_t dataFileInitialSize = Impl::Configuration::PageBlob::DefaultSize,
            std::optional<std::string_view> cachePath = {},
            size_t maxCacheSize = Impl::Configuration::MaxCacheSize);
    };
}
