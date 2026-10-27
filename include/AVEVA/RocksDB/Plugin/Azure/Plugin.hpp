// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Defaults.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp" // main's Plugin.hpp provided it; consumers use PageBlob::Default*
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
    /// WARNING: when `ioContext` is passed by reference the registered factory holds a reference to it. Destroying
    /// the context while it can still create a filesystem leaves that reference dangling; this cannot be detected.
    /// Call Unregister before destroying the context, or use the std::shared_ptr<io_context> overloads. Token
    /// refreshes still in flight when a filesystem is destroyed keep
    /// that filesystem's HTTP client alive until they complete, so keep the context running until they drain.
    ///
    /// Preconditions for destroying a filesystem: `ioContext` must still be running (the destructor blocks until
    /// every async read has completed on it, logging a warning every 30 s while it waits), and the destructor
    /// must not run on a thread running `ioContext`. The same holds for the lease renewal loop: `ioContext` must
    /// keep running until every filesystem and database using it has been destroyed. As a last resort the
    /// destructor stops waiting after two minutes, logs an error and carries on, which can leave late completions
    /// touching freed state: that is a host bug, not a supported mode. Write operations are refused once a lease
    /// renewal has failed or run past the lease.
    ///
    /// `maxPrefetchBytes` caps the memory held by RocksDB prefetch buffers across all files of a filesystem; a
    /// prefetch that would exceed it is declined (reads then simply go to the blob). Zero disables prefetching.
    ///
    /// cachePath enables a local file cache of blobs under that directory, holding at most maxCacheSize bytes
    /// (values above INT64_MAX are rejected with InvalidArgument). A cache with maxCacheSize of zero can never hold
    /// a file, so it only adds a size lookup to every read: a warning is logged and nothing is cached. Without
    /// cachePath no cache is used and maxCacheSize is ignored.
    /// </summary>
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
             std::optional<Models::ServicePrincipalStorageInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Defaults::DataFileBufferSize,
             int64_t dataFileInitialSize = Defaults::DataFileInitialSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0,
             int64_t maxPrefetchBytes = Defaults::MaxPrefetchBytes);

    /// <summary>
    /// Registers the Azure blob filesystem plugin with the RocksDB ObjectLibrary, performing all Azure I/O on
    /// `ioContext`. See the ServicePrincipalStorageInfo overload for the lifetime and threading requirements.
    /// </summary>
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
             std::optional<Models::ChainedCredentialInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Defaults::DataFileBufferSize,
             int64_t dataFileInitialSize = Defaults::DataFileInitialSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0,
             int64_t maxPrefetchBytes = Defaults::MaxPrefetchBytes);

    /// <summary>
    /// Like the io_context& overloads, but the registration and every filesystem created from it share ownership of
    /// `ioContext`, so it can never be destroyed while the factory can still create a filesystem. The caller still has
    /// to keep it running. Prefer this overload; it removes the dangling-reference hazard of the reference overloads.
    /// </summary>
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             std::shared_ptr<boost::asio::io_context> ioContext, Models::ServicePrincipalStorageInfo primary,
             std::optional<Models::ServicePrincipalStorageInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Defaults::DataFileBufferSize,
             int64_t dataFileInitialSize = Defaults::DataFileInitialSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0,
             int64_t maxPrefetchBytes = Defaults::MaxPrefetchBytes);

    /// <summary>See the shared_ptr&lt;io_context&gt; overload taking ServicePrincipalStorageInfo.</summary>
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             std::shared_ptr<boost::asio::io_context> ioContext, Models::ChainedCredentialInfo primary,
             std::optional<Models::ChainedCredentialInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Defaults::DataFileBufferSize,
             int64_t dataFileInitialSize = Defaults::DataFileInitialSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0,
             int64_t maxPrefetchBytes = Defaults::MaxPrefetchBytes);

    /// <summary>
    /// Removes the registration for this primary/backup pair (under both its current and legacy name), releasing
    /// the io_context reference or ownership it holds. Call it before destroying an io_context that was passed by
    /// reference. Filesystems that already exist are unaffected; creating one afterwards fails with a
    /// "has not been registered" error until Register is called again.
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

    /// <summary>
    /// Source-compatible overload from before the io_context was injected. The plugin creates its own io_context and
    /// one worker thread, shared by every filesystem created through the deprecated overloads and released when the
    /// last of them is destroyed. Prefer the overload that takes the host's io_context.
    /// </summary>
    [[deprecated("Pass the host io_context to Register; this overload runs its own io_context thread.")]]
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             Models::ServicePrincipalStorageInfo primary, std::optional<Models::ServicePrincipalStorageInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Defaults::DataFileBufferSize,
             int64_t dataFileInitialSize = Defaults::DataFileInitialSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0,
             int64_t maxPrefetchBytes = Defaults::MaxPrefetchBytes);

    /// <summary>See the deprecated ServicePrincipalStorageInfo overload without an io_context.</summary>
    [[deprecated("Pass the host io_context to Register; this overload runs its own io_context thread.")]]
    static rocksdb::Status
    Register(rocksdb::ConfigOptions& configOptions, rocksdb::Env** env, std::shared_ptr<rocksdb::Env>* guard,
             Models::ChainedCredentialInfo primary, std::optional<Models::ChainedCredentialInfo> backup,
             std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
             int64_t dataFileBufferSize = Defaults::DataFileBufferSize,
             int64_t dataFileInitialSize = Defaults::DataFileInitialSize,
             std::optional<std::string_view> cachePath = std::nullopt, size_t maxCacheSize = 0,
             int64_t maxPrefetchBytes = Defaults::MaxPrefetchBytes);
};
} // namespace AVEVA::RocksDB::Plugin::Azure
