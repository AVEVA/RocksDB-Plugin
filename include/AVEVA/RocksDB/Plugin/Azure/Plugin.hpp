// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>

#include <memory>
#include <string_view>

namespace boost::asio {
class io_context;
} // namespace boost::asio

namespace AVEVA::RocksDB::Plugin::Azure {
struct Plugin {
    static const constexpr std::string_view Name = "azblobfs";

    /// <summary>
    /// Registers the Azure blob filesystem plugin with the RocksDB ObjectLibrary, performing all Azure I/O on
    /// `ioContext`. The plugin does not take ownership of `ioContext` and never runs, stops or destroys it.
    /// The caller must keep it alive and running on at least one thread for as long as any filesystem created
    /// by this registration exists, and must not use those filesystems from the threads running `ioContext`
    /// (filesystem calls block until their I/O completes on `ioContext`).
    /// Registering the same storage accounts again replaces the previous settings, including the io_context,
    /// for filesystems created afterwards.
    /// </summary>
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
             std::optional<Models::ServicePrincipalStorageInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Impl::Configuration::PageBlob::DefaultBufferSize,
             int64_t dataFileInitialSize = Impl::Configuration::PageBlob::DefaultSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0);

    /// <summary>
    /// Registers the Azure blob filesystem plugin with the RocksDB ObjectLibrary, performing all Azure I/O on
    /// `ioContext`. See the ServicePrincipalStorageInfo overload for the lifetime and threading requirements.
    /// </summary>
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
             std::optional<Models::ChainedCredentialInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Impl::Configuration::PageBlob::DefaultBufferSize,
             int64_t dataFileInitialSize = Impl::Configuration::PageBlob::DefaultSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
