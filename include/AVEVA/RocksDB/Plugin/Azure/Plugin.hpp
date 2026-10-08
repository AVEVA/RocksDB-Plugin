// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"

#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>

#include <memory>
#include <optional>
#include <string>
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
    ///
    /// WARNING: the registered factory holds a reference to `ioContext`. Destroying the context while it can
    /// still create a filesystem (i.e. before the next Register call or process exit) leaves that reference
    /// dangling; this cannot be detected. Token refreshes still in flight when a filesystem is destroyed keep
    /// that filesystem's HTTP client alive until they complete, so keep the context running until they drain.
    ///
    /// Preconditions for destroying a filesystem: `ioContext` must still be running (the destructor blocks until
    /// every async read has completed on it, logging a warning every 30 s while it waits), and the destructor
    /// must not run on a thread running `ioContext`. Write operations are refused once a lease renewal has
    /// failed or run past the lease.
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

    /// <summary>
    /// Returns the name under which Register publishes the filesystem for this primary/backup pair, e.g. for
    /// `--fs_uri` or Env::CreateFromUri. Only the account URLs and database names take part in the name.
    /// </summary>
    static std::string NameFor(const Models::ServicePrincipalStorageInfo& primary,
                               const std::optional<Models::ServicePrincipalStorageInfo>& backup = std::nullopt);
    static std::string NameFor(const Models::ChainedCredentialInfo& primary,
                               const std::optional<Models::ChainedCredentialInfo>& backup = std::nullopt);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
