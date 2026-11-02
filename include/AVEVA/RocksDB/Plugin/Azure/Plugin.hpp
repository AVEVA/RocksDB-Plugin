// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Defaults.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp" // main's Plugin.hpp provided it; consumers use PageBlob::Default*
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace boost::asio {
class io_context;
} // namespace boost::asio

namespace AVEVA::RocksDB::Plugin::Azure {
struct Plugin {
    static const constexpr std::string_view Name = "azblobfs";

    using Logger = std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>;

    /// <summary>The primary storage account and the optional backup account, both using the same credential
    /// kind.</summary>
    template <class StorageInfo> struct AccountPair {
        StorageInfo Primary;
        std::optional<StorageInfo> Backup;
    };
    using StorageAccounts =
        std::variant<AccountPair<Models::ServicePrincipalStorageInfo>, AccountPair<Models::ChainedCredentialInfo>>;

    /// <summary>A local file cache of blobs: the directory it lives in and the most it may hold.</summary>
    struct CacheOptions {
        std::string Directory;
        Core::ByteCount MaxSize;
    };

    /// <summary>Everything Register needs besides the RocksDB handles.</summary>
    struct RegisterOptions {
        /// <summary>Shared with every filesystem created from the registration; never null.</summary>
        std::shared_ptr<boost::asio::io_context> IoContext;
        StorageAccounts Accounts;
        /// <summary>A null logger is replaced by a default one.</summary>
        Logger Log;
        Core::ByteCount DataFileBufferSize = Defaults::DataFileBufferSize;
        Core::ByteCount DataFileInitialSize = Defaults::DataFileInitialSize;
        /// <summary>Absent: no cache is used.</summary>
        std::optional<CacheOptions> Cache;
        /// <summary>Caps the memory held by RocksDB prefetch buffers across all files of a filesystem; a prefetch that
        /// would exceed it is declined (reads then simply go to the blob). Zero disables prefetching.</summary>
        Core::ByteCount MaxPrefetchBytes = Defaults::MaxPrefetchBytes;
    };

    /// <summary>
    /// Registers the Azure blob filesystem plugin with the RocksDB ObjectLibrary, performing all Azure I/O on
    /// `options.IoContext`. The registration and every filesystem created from it share ownership of the context, so it
    /// can never be destroyed while the factory can still create a filesystem. The plugin never runs, stops or
    /// destroys it: the caller must keep it running on at least one thread for as long as any filesystem created by
    /// this registration exists, and must not use those filesystems from the threads running it (filesystem calls
    /// block until their I/O completes on the context).
    /// Registering the same storage accounts again replaces the previous settings, including the io_context, for
    /// filesystems created afterwards.
    ///
    /// Preconditions for destroying a filesystem: the io_context must still be running (the destructor blocks until
    /// every async read has completed on it, logging a warning every 30 s while it waits), and the destructor must not
    /// run on a thread running it. The same holds for the lease renewal loop: the io_context must keep running until
    /// every filesystem and database using it has been destroyed. As a last resort the destructor stops waiting after
    /// two minutes, logs an error and carries on, which can leave late completions touching freed state: that is a
    /// host bug, not a supported mode. Token refreshes still in flight when a filesystem is destroyed keep that
    /// filesystem's HTTP client alive until they complete. Write operations are refused once a lease renewal has
    /// failed or run past the lease.
    ///
    /// A cache holds at most `Cache->MaxSize` bytes (values above INT64_MAX are rejected with InvalidArgument). A cache
    /// with a MaxSize of zero can never hold a file, so it only adds a size lookup to every read: a warning is logged
    /// and nothing is cached.
    /// </summary>
    static rocksdb::Status Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                    std::shared_ptr<rocksdb::Env>* guard, RegisterOptions options);

    /// <summary>
    /// Removes the registration for this primary/backup pair (under both its current and legacy name), releasing
    /// the io_context it shares ownership of. Filesystems that already exist are unaffected; creating one afterwards
    /// fails with a "has not been registered" error until Register is called again.
    /// </summary>
    static void Unregister(const Models::ServicePrincipalStorageInfo& primary,
                           const std::optional<Models::ServicePrincipalStorageInfo>& backup = std::nullopt);
    static void Unregister(const Models::ChainedCredentialInfo& primary,
                           const std::optional<Models::ChainedCredentialInfo>& backup = std::nullopt);

    /// <summary>Removes the registration published under `name` (see NameFor). Unknown names are ignored.</summary>
    static void Unregister(const std::string& name);

    /// <summary>
    /// Returns the name under which Register publishes the filesystem for this primary/backup pair, e.g. for
    /// `--fs_uri` or Env::CreateFromUri. Only the account URLs and database names take part in the name.
    /// Register also publishes the filesystem under the name earlier versions used (`Name` followed by the primary
    /// and backup database names) so existing URIs keep working; when two registrations share those database names
    /// the latest one wins that legacy alias.
    /// </summary>
    static std::string NameFor(const Models::ServicePrincipalStorageInfo& primary,
                               const std::optional<Models::ServicePrincipalStorageInfo>& backup = std::nullopt);
    static std::string NameFor(const Models::ChainedCredentialInfo& primary,
                               const std::optional<Models::ChainedCredentialInfo>& backup = std::nullopt);

    /// <summary>
    /// Returns the database path prefix (`<account>+<container>`) to pass to rocksdb::DB::Open together with the
    /// filesystem registered for that storage account. `accountUrl` is e.g. "https://account.blob.core.windows.net".
    /// </summary>
    [[nodiscard]] static std::string UniquePrefix(const std::string& accountUrl, const std::string& container);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
