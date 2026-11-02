// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Defaults.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/Identifiers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/SignedByteCount.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Units.hpp"

#include <boost/log/trivial.hpp>
#include <rocksdb/env.h>

#include <filesystem>
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
        std::filesystem::path Directory;
        SignedByteCount MaxSize;
    };

    /// <summary>Everything Register needs besides the RocksDB handles.</summary>
    struct RegisterOptions {
        StorageAccounts Accounts;
        /// <summary>A null logger is replaced by a default one.</summary>
        Logger Log;
        Core::ByteCount DataFileBufferSize = Defaults::DataFileBufferSize;
        Core::ByteCount DataFileInitialSize = Defaults::DataFileInitialSize;
        /// <summary>Absent: no cache is used.</summary>
        std::optional<CacheOptions> Cache;
        /// <summary>Caps the memory held by RocksDB prefetch buffers across all files of a filesystem; a prefetch that
        /// would exceed it is declined (reads then simply go to the blob). Zero disables prefetching.</summary>
        SignedByteCount MaxPrefetchBytes = Defaults::MaxPrefetchBytes;
    };

    /// <summary>
    /// Registers the Azure blob filesystem plugin with the RocksDB ObjectLibrary, performing all Azure I/O on
    /// `ioContext`. The host owns the context; the plugin only refers to it and never runs, stops or destroys it. The
    /// host must keep it alive and running on at least one thread until it has called Unregister for these accounts
    /// and destroyed every filesystem created from the registration, and must not use those filesystems from the
    /// threads running it (filesystem calls block until their I/O completes on the context; such a call fails with
    /// InvalidArgument instead of deadlocking).
    /// Registering the same storage accounts again replaces the previous settings, including the io_context, for
    /// filesystems created afterwards.
    ///
    /// Preconditions for destroying a filesystem: the io_context must still be running (the destructor blocks until
    /// every async read has completed on it, logging a warning every 30 s while it waits), and the destructor must not
    /// run on a thread running it. The same holds for the lease renewal loop: the io_context must keep running until
    /// every filesystem and database using it has been destroyed. As a last resort the destructor stops waiting after
    /// two minutes and logs an error; the clients and caches then stay alive until the last read ends (forever if the
    /// io_context never runs it again). That is a host bug, not a supported mode. Token refreshes still in flight when
    /// a filesystem is destroyed keep that filesystem's HTTP client alive until they complete. Write operations are
    /// refused once a lease renewal has failed or run past the lease.
    ///
    /// A cache holds at most `Cache->MaxSize` bytes. A cache with a MaxSize of zero can never hold a file, so it only
    /// adds a size lookup to every read: a warning is logged and nothing is cached.
    /// </summary>
    static rocksdb::Status Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env,
                                    std::shared_ptr<rocksdb::Env>* guard, boost::asio::io_context& ioContext,
                                    RegisterOptions options);

    /// <summary>
    /// Removes the registration for these accounts (under both its current and legacy name), dropping its reference
    /// to the io_context. Filesystems that already exist are unaffected; creating one afterwards fails with a "has not
    /// been registered" error until Register is called again. Unknown accounts are ignored.
    /// </summary>
    static void Unregister(const StorageAccounts& accounts);

    /// <summary>
    /// Returns the name under which Register publishes the filesystem for these accounts, e.g. for
    /// `--fs_uri` or Env::CreateFromUri. Only the account URLs and database names take part in the name.
    /// Register also publishes the filesystem under the name earlier versions used (`Name` followed by the primary
    /// and backup database names) so existing URIs keep working; when two registrations share those database names
    /// the latest one wins that legacy alias.
    /// </summary>
    [[nodiscard]] static std::string NameFor(const StorageAccounts& accounts);

    /// <summary>
    /// Returns the database path prefix (`<account>+<container>`) to pass to rocksdb::DB::Open together with the
    /// filesystem registered for that storage account. `accountUrl` is e.g. "https://account.blob.core.windows.net".
    /// </summary>
    [[nodiscard]] static std::string UniquePrefix(const Models::AccountUrl& accountUrl, const Models::DbName& dbName);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
