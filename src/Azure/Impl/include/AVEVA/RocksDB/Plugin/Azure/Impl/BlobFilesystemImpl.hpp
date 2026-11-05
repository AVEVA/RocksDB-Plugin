// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobAttributes.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntimeTransport.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LeaseRenewalLoop.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SignedBytes.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCacheTestAccess.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/log/trivial.hpp>
#include <boost/scope/scope_exit.hpp>
#include <boost/scope/scope_fail.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <filesystem>
#include <format>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <source_location>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
// Boost.Asio pulls in <windows.h>, whose macros clash with RocksDB method names (mirrors rocksdb/env.h). These
// headers are internal (not installed), so the macros never reach consumers of the public headers.
#undef DeleteFile
#undef GetCurrentTime
#undef GetFreeSpace
#undef LoadLibrary
#endif
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>The largest count the signed 64-bit sizes of the blob client can represent.</summary>
inline constexpr Core::ByteCount MaxSignedBytes = SignedByteCount::Max;

/// <summary>
/// Returns why the data-file sizes cannot be used, or an empty view when they are valid. Page blobs are written in
/// 512-byte pages, so the buffer size must be a multiple of 512 of at least two pages. Both sizes must fit the signed
/// 64-bit sizes used below.
/// </summary>
[[nodiscard]] constexpr std::string_view InvalidDataFileSizes(const Core::ByteCount dataFileInitialSize,
                                                              const Core::ByteCount dataFileBufferSize) noexcept {
    if (dataFileBufferSize.Value() < 1024 || dataFileBufferSize.Value() % 512 != 0) {
        return "dataFileBufferSize must be a multiple of 512 and at least 1024 (two pages)";
    }
    if (dataFileBufferSize > MaxSignedBytes || dataFileInitialSize > MaxSignedBytes) {
        return "data file sizes cannot exceed INT64_MAX";
    }
    return {};
}

// Validates in a member initializer, so a bad value throws before the lease-renewal worker is started.
[[nodiscard]] inline Core::ByteCount RequireValidDataFileSizes(const Core::ByteCount dataFileInitialSize,
                                                               const Core::ByteCount dataFileBufferSize) {
    if (const auto problem = InvalidDataFileSizes(dataFileInitialSize, dataFileBufferSize); !problem.empty()) {
        throw std::invalid_argument(std::string(problem));
    }
    return dataFileInitialSize;
}

/// <summary>
/// Maps an HTTP client type to the file and directory types the filesystem hands out. The production client gets the
/// non-template classes (which can be forward declared by the public headers); any other client gets the templates.
/// </summary>
template <typename THttp> struct FilesystemTypes {
    using Blob = PageBlobClientOn<THttp>;
    using Readable = BasicReadableFileImpl<Blob>;
    using Sequential = BasicSequentialFileImpl<Blob>;
    using Writeable = BasicWriteableFileImpl<Blob>;
    using ReadWrite = BasicReadWriteFileImpl<Blob>;
    using Directory = DirectoryImpl;
    using Lock = BasicLockFileImpl<Blob>;
};
template <> struct FilesystemTypes<::AVEVA::HttpClient> {
    using Blob = PageBlobClient;
    using Readable = ReadableFileImpl;
    using Sequential = SequentialFileImpl;
    using Writeable = WriteableFileImpl;
    using ReadWrite = ReadWriteFileImpl;
    using Directory = DirectoryImpl;
    using Lock = LockFileImpl;
};

/// <summary>Whether any blob lives below a directory.</summary>
enum class DirectoryContents : std::uint8_t { Empty, NonEmpty };

/// <summary>Whether every file cache's background downloader went idle within the allowed time.</summary>
enum class CacheIdleness : std::uint8_t { TimedOut, Idle };

/// <summary>Whether the fatal "writes are fenced" message has been logged already (it is logged once).</summary>
enum class FenceLogState : std::uint8_t { NotLogged, Logged };

template <typename THttp> class BasicBlobFilesystemImpl {
    using Types = FilesystemTypes<THttp>;
    using ReadableFileType = typename Types::Readable;
    using SequentialFileType = typename Types::Sequential;
    using WriteableFileType = typename Types::Writeable;
    using ReadWriteFileType = typename Types::ReadWrite;
    using DirectoryType = typename Types::Directory;
    using LockFileType = typename Types::Lock;
    using AzureContainerClient = BasicAzureContainerClient<BlobContainerClientOn<THttp>>;

  private:
    // The container client shares the connection state and credential with the service client, so it is all we keep.
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    Core::ByteCount m_dataFileInitialSize;
    Core::ByteCount m_dataFileBufferSize;
    // How long LockFile waits on a lease held by another owner; the lease length when unset.
    std::optional<std::chrono::milliseconds> m_lockAcquireTimeout;
    // Holds the HTTP client (on the injected, host-owned io_context) and the write fence; the Azure clients below
    // share ownership of the HTTP client themselves.
    std::shared_ptr<BasicClientRuntime<THttp>> m_runtime;
    std::unordered_map<std::string, std::shared_ptr<BlobContainerClientOn<THttp>>, Core::StringHash, Core::StringEqual>
        m_clients;
    std::unordered_map<std::string, std::shared_ptr<Core::FileCache>, Core::StringHash, Core::StringEqual> m_fileCaches;
    // Async reads still running on the io_context; drained by the destructor before the runtime and caches above
    // are released (see AsyncReadTracker).
    std::shared_ptr<AsyncReadTracker> m_asyncReads;
    // Bytes held by per-file prefetch buffers across all open files; caps Prefetch memory.
    std::shared_ptr<std::atomic<int64_t>> m_prefetchBytes = std::make_shared<std::atomic<int64_t>>(0);
    // Cap on m_prefetchBytes; set before files are opened.
    Core::ByteCount m_maxPrefetchBytes = Defaults::MaxPrefetchBytes.Bytes();
    mutable std::atomic<FenceLogState> m_fencedLogged{FenceLogState::NotLogged};
    mutable std::mutex m_lockFilesMutex;
    boost::intrusive::list<LockFileType, boost::intrusive::constant_time_size<false>> m_locks;
    // Parallel to m_locks; lets the renewal loop keep locks alive while it renews outside m_lockFilesMutex.
    std::vector<std::weak_ptr<LockFileType>> m_renewableLocks;
    // Concrete callables for the lease renewal loop; defined in the .cpp where they can reach the private state.
    struct RenewableLocksSnapshot {
        BasicBlobFilesystemImpl* Owner;
        std::vector<std::shared_ptr<LockFileType>> operator()() const;
    };
    struct FenceWritesOnFatal {
        BasicBlobFilesystemImpl* Owner;
        void operator()() const;
    };
    using LeaseRenewal = BasicLeaseRenewalLoop<RenewableLocksSnapshot, FenceWritesOnFatal, LockFileType>;

    // Declared last: Stop()ped first thing in the destructor, before anything its callbacks use is released.
    std::shared_ptr<LeaseRenewal> m_leaseRenewal;

    // What the destructor hands to m_asyncReads when it gives up waiting for reads, so late completions never see
    // the runtime, clients or caches freed underneath them.
    struct SharedState {
        std::shared_ptr<BasicClientRuntime<THttp>> Runtime;
        std::unordered_map<std::string, std::shared_ptr<BlobContainerClientOn<THttp>>, Core::StringHash,
                           Core::StringEqual>
            Clients;
        std::unordered_map<std::string, std::shared_ptr<Core::FileCache>, Core::StringHash, Core::StringEqual>
            FileCaches;
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> Logger;
    };

  public:
    // Every io_context constructor takes the host-owned io_context that all Azure I/O runs on. The filesystem never
    // runs, stops or destroys it; the host must keep it alive and running for the filesystem's whole lifetime and must
    // not call into the filesystem from the threads running it.
    BasicBlobFilesystemImpl(
        boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
        std::optional<Models::ChainedCredentialInfo> backup, Core::ByteCount dataFileInitialSize,
        Core::ByteCount dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::filesystem::path> cachePath = {},
        Core::ByteCount maxCacheSize = Configuration::MaxCacheSize);
    BasicBlobFilesystemImpl(
        boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
        std::optional<Models::ServicePrincipalStorageInfo> backup, Core::ByteCount dataFileInitialSize,
        Core::ByteCount dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::filesystem::path> cachePath = {},
        Core::ByteCount maxCacheSize = Configuration::MaxCacheSize);

    // Builds the filesystem on an already-constructed runtime (and therefore any HTTP client). Used with shared-key
    // credentials. The tests pass a runtime that owns an in-memory fake and a short lock timeout, so that contention
    // does not take the production 20 seconds to give up.
    BasicBlobFilesystemImpl(
        std::shared_ptr<BasicClientRuntime<THttp>> runtime, const Models::DbName& name,
        const Models::AccountUrl& storageAccountUrl, const StorageAccountKey& storageAccountKey,
        Core::ByteCount dataFileInitialSize, Core::ByteCount dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::filesystem::path> cachePath = {}, Core::ByteCount maxCacheSize = Configuration::MaxCacheSize,
        std::optional<std::chrono::milliseconds> lockAcquireTimeout = {});

    // Stops lease renewal, then blocks until reads abandoned by RocksDB mid-flight have completed, so that their
    // completions never release the HTTP client or a file cache. Requires the host io_context to still be running.
    ~BasicBlobFilesystemImpl();
    BasicBlobFilesystemImpl(const BasicBlobFilesystemImpl&) = delete;
    BasicBlobFilesystemImpl& operator=(const BasicBlobFilesystemImpl&) = delete;
    BasicBlobFilesystemImpl(BasicBlobFilesystemImpl&&) = delete;
    BasicBlobFilesystemImpl& operator=(BasicBlobFilesystemImpl&&) = delete;

    [[nodiscard]] std::shared_ptr<ReadableFileType> CreateReadableFile(const std::string& filePath);
    // Caps the bytes held by prefetch buffers across all files opened afterwards; 0 declines every prefetch.
    void SetMaxPrefetchBytes(Core::ByteCount maxPrefetchBytes) { m_maxPrefetchBytes = maxPrefetchBytes; }
    [[nodiscard]] SequentialFileType CreateSequentialFile(const std::string& filePath);
    [[nodiscard]] WriteableFileType CreateWriteableFile(const std::string& filePath);
    [[nodiscard]] ReadWriteFileType CreateReadWriteFile(const std::string& filePath);
    [[nodiscard]] WriteableFileType ReopenWriteableFile(const std::string& filePath);
    LoggerImpl CreateLogger(const std::string& filePath,
                            std::chrono::seconds cooldown = Configuration::LogRateLimiterCooldown);
    std::shared_ptr<LockFileType> LockFile(const std::string& filePath);
    void UnlockFile(LockFileType& lock);
    DirectoryType CreateDirectory(const std::string& directoryPath);

    [[nodiscard]] AzureClient::Existence FileExists(const std::string& name);
    // Present when at least one blob lives below the directory; stops at the first one rather than listing every page.
    [[nodiscard]] DirectoryContents HasChildren(const std::string& directoryPath);
    // How many names one List Blobs page asks for; clamped to [1, Configuration::Transfer::MaxListPageSize].
    using ListPageSize = Core::StrongInteger<struct ListPageSizeTag, std::uint32_t>;
    // Lists every child, however many pages that takes; `pageSize` only changes how many requests it needs.
    std::vector<std::string> GetChildren(const std::string& directoryPath,
                                         ListPageSize pageSize = ListPageSize{
                                             static_cast<std::uint32_t>(Configuration::Transfer::MaxListPageSize)});
    std::vector<BlobAttributes> GetChildrenFileAttributes(const std::string& directoryPath);
    [[nodiscard]] AzureClient::PriorState DeleteFile(const std::string& filePath) const;
    // NonEmpty when something is left under the directory after the delete (a delete that raced with a create).
    [[nodiscard]] DirectoryContents DeleteDir(const std::string& directoryPath) const;
    [[nodiscard]] DirectoryContents DeleteDirIn(BlobContainerClientOn<THttp>& container, std::string_view prefix,
                                                std::string_view realPath) const;
    // Shrinks the file; throws std::invalid_argument when `size` exceeds the current size (extending is unsupported).
    void Truncate(const std::string& filePath, int64_t size) const;
    [[nodiscard]] int64_t GetFileSize(const std::string& filePath) const;
    [[nodiscard]] uint64_t GetFileModificationTime(const std::string& filePath) const;
    size_t GetLeaseClientCount();
    // Test-only: blocks until every file cache's background downloader is idle, or `timeout` (a guard against hangs)
    // elapses; reports TimedOut in that case.
    [[nodiscard]] CacheIdleness WaitForFileCachesIdleForTesting(std::chrono::milliseconds timeout) const;
    void RenameFile(const std::string& fromFilePath, const std::string& toFilePath) const;

  private:
    BasicBlobFilesystemImpl(
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger,
        std::shared_ptr<BasicClientRuntime<THttp>> runtime, Core::ByteCount dataFileInitialSize,
        Core::ByteCount dataFileBufferSize, std::optional<std::chrono::milliseconds> lockAcquireTimeout = {});
    void AddContainer(const BlobServiceClientOn<THttp>& serviceClient, const Models::AccountUrl& storageAccountUrl,
                      const Models::DbName& name, std::optional<std::filesystem::path> cachePath,
                      Core::ByteCount maxCacheSize);
    // Deletes the temporary blobs a rename left behind when it crashed or could not clean up after itself. Only safe
    // while holding the container's DB lock, since the lock holder is the only process that renames.
    void PurgeOrphanedRenameTemps(std::string_view prefix, std::string_view lockPath) const;
    template <class TInfo>
    void AddContainers(const TInfo& primary, const std::optional<TInfo>& backup,
                       std::optional<std::filesystem::path> cachePath, Core::ByteCount maxCacheSize);
    // Runs `fn` with the container registered for the account prefix. The reference is valid only for the call;
    // throws when the prefix is unknown.
    template <class TFn> decltype(auto) WithContainer(std::string_view prefix, TFn&& fn) const;
    // A path split into its storage-account prefix and blob name, with the blob client. The views point into the path
    // passed to ResolveBlob.
    struct ResolvedBlob {
        std::string_view Prefix;
        std::string_view RealPath;
        PageBlobClientOn<THttp> Blob;
    };
    [[nodiscard]] ResolvedBlob ResolveBlob(const std::string& path) const;
    void EnsureLiveness(std::source_location location = std::source_location::current()) const;

    // The steps of RenameFile, as coroutines so the whole rename is one blocking call. Each owns what it uses, so
    // nothing it holds can dangle across a suspension; the copy and cleanup steps borrow the clients of the rename that
    // awaits them, which stays suspended (and so alive) until they finish.
    using BlobClient = PageBlobClientOn<THttp>;
    // Every blob a rename may touch, named so they cannot be swapped. The temporary blob is used only when an
    // existing destination has to be replaced.
    struct RenameBlobs {
        BlobClient Source;
        BlobClient Destination;
        BlobClient Temporary;
        std::string SourceName;
        std::string DestinationName;
        std::string TemporaryName;
    };
    [[nodiscard]] boost::asio::awaitable<void> RenameBlobAsync(RenameBlobs blobs) const;
    // Server-side copy first; falls back to copying through this process when the service cannot authorize the source.
    [[nodiscard]] boost::asio::awaitable<void> CopyBlobPagesAsync(BlobOperations::Source<BlobClient> source,
                                                                  BlobOperations::Destination<BlobClient> target,
                                                                  Core::ByteCount size, std::string etag,
                                                                  std::string name) const;
    // Copies the content and logical size into `target`; returns its ETag after the final write.
    [[nodiscard]] boost::asio::awaitable<std::string> CopyIntoAsync(BlobOperations::Source<BlobClient> source,
                                                                    BlobOperations::Destination<BlobClient> target,
                                                                    Core::ByteCount size, std::string etag,
                                                                    std::string name) const;
    // Best-effort cleanup of a blob the rename wrote, which never masks the failure being unwound.
    [[nodiscard]] boost::asio::awaitable<void> DeleteQuietlyAsync(BlobOperations::Destination<BlobClient> blob,
                                                                  std::string name) const;
};
extern template class BasicBlobFilesystemImpl<::AVEVA::HttpClient>;

/// <summary>The production filesystem, over the real HTTP client. Non-template so it can be forward declared.</summary>
class BlobFilesystemImpl : public BasicBlobFilesystemImpl<::AVEVA::HttpClient> {
  public:
    using BasicBlobFilesystemImpl<::AVEVA::HttpClient>::BasicBlobFilesystemImpl;
};

namespace BlobFilesystemDetail {
/// <summary>What a listing callback wants next.</summary>
enum class Traversal : std::uint8_t { Continue, Stop };

/// <summary>
/// Lists every blob in the container whose name starts with `options.Prefix`, following continuation markers,
/// and invokes `onBlob` for each until it answers Stop.
/// </summary>
template <typename TContainer, typename TOnBlob>
    requires std::same_as<std::invoke_result_t<TOnBlob&, const AzureClient::Models::BlobItem&>, Traversal>
void ForEachBlobUntil(TContainer& container, AzureClient::ListBlobsOptions options, TOnBlob&& onBlob) {
    while (true) {
        auto page = Call(container, container.ListBlobsAsync(options));
        for (const auto& blob : page.Blobs) {
            if (onBlob(blob) == Traversal::Stop) {
                return;
            }
        }

        if (page.NextMarker.empty()) {
            break;
        }

        options.Marker = std::move(page.NextMarker);
    }
}

/// <summary>
/// Lists the blobs under `options.Prefix` one page at a time and invokes `onPage` with each page's blobs before
/// the next page is requested, so a caller can act on a page and let it go instead of holding the whole listing.
/// </summary>
template <typename TContainer, typename TOnPage>
void ForEachBlobPage(TContainer& container, AzureClient::ListBlobsOptions options, TOnPage&& onPage) {
    while (true) {
        auto page = Call(container, container.ListBlobsAsync(options));
        onPage(std::as_const(page.Blobs));

        if (page.NextMarker.empty()) {
            break;
        }

        options.Marker = std::move(page.NextMarker);
    }
}

template <typename TContainer, std::invocable<const AzureClient::Models::BlobItem&> TOnBlob>
void ForEachBlob(TContainer& container, AzureClient::ListBlobsOptions options, TOnBlob&& onBlob) {
    ForEachBlobUntil(container, std::move(options), [&onBlob](const AzureClient::Models::BlobItem& blob) {
        onBlob(blob);
        return Traversal::Continue;
    });
}

// RenameFile stages an overwrite in "<destination>.rename-<uuid>"; the uuid is 36 characters.
inline constexpr std::string_view RenameTempMarker = ".rename-";
inline constexpr size_t RenameTempUuidLength = 36;

// Matches boost::uuids::to_string output exactly: lowercase hex in 8-4-4-4-12 groups.
enum class UuidForm : std::uint8_t { Malformed, Canonical };

inline UuidForm ClassifyUuid(std::string_view text) {
    if (text.size() != RenameTempUuidLength) {
        return UuidForm::Malformed;
    }
    for (size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') {
                return UuidForm::Malformed;
            }
        } else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return UuidForm::Malformed;
        }
    }
    return UuidForm::Canonical;
}

/// <summary>What a blob name means to the filesystem; the temporary and lock blobs are not user files.</summary>
enum class BlobNameKind : std::uint8_t { Ordinary, RenameTemp, DbLock };

// A rename temporary is "<destination>.rename-<uuid>"; a database's lock file is named "LOCK" in its directory
// (possibly the container root).
inline BlobNameKind ClassifyBlobName(std::string_view blobName) {
    constexpr auto suffixLength = RenameTempMarker.size() + RenameTempUuidLength;
    if (blobName.size() > suffixLength) {
        const auto suffix = blobName.substr(blobName.size() - suffixLength);
        if (suffix.starts_with(RenameTempMarker) &&
            ClassifyUuid(suffix.substr(RenameTempMarker.size())) == UuidForm::Canonical) {
            return BlobNameKind::RenameTemp;
        }
    }
    constexpr std::string_view lock = "LOCK";
    if (blobName == lock || (blobName.ends_with(lock) && blobName[blobName.size() - lock.size() - 1] == '/')) {
        return BlobNameKind::DbLock;
    }
    return BlobNameKind::Ordinary;
}

// The directory holding a file, as a list prefix; empty when the file sits at the container root.
inline std::string DirectoryPrefixOfFile(std::string_view filePath) {
    const auto slash = filePath.rfind('/');
    return slash == std::string_view::npos ? std::string() : std::string(filePath.substr(0, slash + 1));
}

} // namespace BlobFilesystemDetail

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
    std::optional<Models::ChainedCredentialInfo> backup, Core::ByteCount dataFileInitialSize,
    Core::ByteCount dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::filesystem::path> cachePath, Core::ByteCount maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::make_shared<BasicClientRuntime<THttp>>(ioContext),
                              dataFileInitialSize, dataFileBufferSize) {
    AddContainers(primary, backup, cachePath, maxCacheSize);
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
    std::optional<Models::ServicePrincipalStorageInfo> backup, Core::ByteCount dataFileInitialSize,
    Core::ByteCount dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::filesystem::path> cachePath, Core::ByteCount maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::make_shared<BasicClientRuntime<THttp>>(ioContext),
                              dataFileInitialSize, dataFileBufferSize) {
    AddContainers(primary, backup, cachePath, maxCacheSize);
}

// Only the primary container gets a file cache; the backup is a fallback that is read through directly.
template <typename THttp>
template <class TInfo>
void BasicBlobFilesystemImpl<THttp>::AddContainers(const TInfo& primary, const std::optional<TInfo>& backup,
                                                   std::optional<std::filesystem::path> cachePath,
                                                   Core::ByteCount maxCacheSize) {
    const auto add = [&](const TInfo& info, std::optional<std::filesystem::path> path) {
        const auto accountUrl = info.GetStorageAccountUrl();
        const auto dbName = info.GetDbName();
        AddContainer(ClientFactory::CreateServiceClient(m_runtime, info), accountUrl, dbName, path, maxCacheSize);
    };
    add(primary, cachePath);
    if (backup) {
        add(*backup, std::nullopt);
    }
}

/// <summary>
/// Creates the container (if needed) for `name` in the storage account, registers the client under the account's
/// unique prefix and, when a cache path is given, sets up a file cache backed by that container.
/// </summary>
template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::AddContainer(const BlobServiceClientOn<THttp>& serviceClient,
                                                  const Models::AccountUrl& storageAccountUrl,
                                                  const Models::DbName& name,
                                                  std::optional<std::filesystem::path> cachePath,
                                                  Core::ByteCount maxCacheSize) {
    auto containerClient = BlobHelpers::GetContainerClient(serviceClient, name.Value());
    const auto uniquePrefix = StorageAccount::UniquePrefix(storageAccountUrl, name);
    if (cachePath) {
        m_fileCaches.emplace(uniquePrefix, std::make_shared<Core::FileCache>(
                                               *cachePath, maxCacheSize,
                                               std::make_shared<AzureContainerClient>(m_runtime, containerClient),
                                               std::make_shared<Core::LocalFilesystem>(m_logger), m_logger));
    }

    m_clients.emplace(uniquePrefix, std::move(containerClient));
}

// A rename that crashed (or could not delete its temporary blob) leaves a ".rename-<uuid>" blob behind. Nothing else
// refers to those. The lock holder is the only process that renames, so this runs only once the DB lock is held:
// purging at construction would let a second (read-only or standby) process delete the writer's in-flight temp.
// Failures only cost some storage, so they are logged rather than allowed to fail the lock.
template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::PurgeOrphanedRenameTemps(const std::string_view prefix,
                                                              const std::string_view lockPath) const {
    try {
        WithContainer(prefix, [&](auto& container) {
            std::vector<std::string> orphans;
            // Only this database's directory: temp blobs of other directories in the container may belong to renames
            // that are still in flight under their own lease.
            AzureClient::ListBlobsOptions options;
            options.Prefix = BlobFilesystemDetail::DirectoryPrefixOfFile(lockPath);
            options.MaxResults = static_cast<uint32_t>(Configuration::Transfer::MaxListPageSize);
            // A nested directory with its own LOCK blob is another database that may hold its lease and have a rename
            // in flight; holding this lock proves nothing about its temps, so that whole subtree is skipped.
            std::vector<std::string> foreignDatabases;
            std::vector<std::string> candidates;
            BlobFilesystemDetail::ForEachBlob(
                container, std::move(options), [&](const AzureClient::Models::BlobItem& blob) {
                    const auto kind = BlobFilesystemDetail::ClassifyBlobName(blob.Name);
                    if (kind == BlobFilesystemDetail::BlobNameKind::RenameTemp) {
                        candidates.push_back(blob.Name);
                    } else if (kind == BlobFilesystemDetail::BlobNameKind::DbLock && blob.Name != lockPath) {
                        foreignDatabases.push_back(BlobFilesystemDetail::DirectoryPrefixOfFile(blob.Name));
                    }
                });
            for (auto& candidate : candidates) {
                const auto foreign = std::ranges::any_of(
                    foreignDatabases, [&](const std::string& directory) { return candidate.starts_with(directory); });
                if (!foreign) {
                    orphans.push_back(std::move(candidate));
                }
            }
            if (orphans.empty()) {
                return;
            }
            AzureClient::DeleteBlobsOptions deleteOptions;
            deleteOptions.MaxConcurrentRequests =
                AzureClient::Concurrency{Configuration::Transfer::MaxConcurrentDeleteBatches};
            const auto result = BlobOperations::DeleteBatch(m_runtime, container, orphans, deleteOptions);
            for (const auto& item : result.Items) {
                if (item.Error) {
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                        << "Could not delete the orphaned rename temporary blob '" << item.Name
                        << "': " << item.Error->Message;
                } else {
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::info)
                        << "Deleted the orphaned rename temporary blob '" << item.Name << "'";
                }
            }
        });
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Could not clean up orphaned rename temporary blobs: " << e.what();
    } catch (...) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Could not clean up orphaned rename temporary blobs: unknown error";
    }
}
namespace BlobFilesystemDetail {
struct FileSizing {
    Core::ByteCount InitialSize;
    Core::ByteCount BufferSize;
};

// Only WAL and SST files get the configurable (larger) sizes; everything else uses the small defaults.
inline FileSizing SizingFor(const std::string& path, const Core::ByteCount dataInitialSize,
                            const Core::ByteCount dataBufferSize) {
    const auto fileType = Core::RocksDBHelpers::GetFileType(path);
    const auto isData =
        fileType == Core::RocksDBHelpers::FileClass::WAL || fileType == Core::RocksDBHelpers::FileClass::SST;
    return isData ? FileSizing{dataInitialSize, dataBufferSize}
                  : FileSizing{Defaults::DataFileInitialSize, Defaults::DataFileBufferSize};
}

// Returns the container's file cache, or nullptr when none is configured for that prefix.
template <class Map> std::shared_ptr<Core::FileCache> FindCache(const Map& caches, std::string_view prefix) {
    const auto cache = caches.find(prefix);
    return cache != caches.end() ? cache->second : nullptr;
}

// The blob-name prefix that selects the children of `realPath`: "realPath/" so that "foo" never matches a sibling
// such as "foobar". The empty path is the container root and has no prefix.
inline std::string DirectoryPrefix(std::string_view realPath) {
    std::string dirPrefix(realPath);
    if (!dirPrefix.empty() && !dirPrefix.ends_with('/')) {
        dirPrefix += '/';
    }
    return dirPrefix;
}

// The name of `blobName` relative to `dirPrefix`, or an empty view when it is not a child of it.
inline std::string_view ChildName(std::string_view blobName, std::string_view dirPrefix) {
    return blobName.size() > dirPrefix.size() && blobName.starts_with(dirPrefix) ? blobName.substr(dirPrefix.size())
                                                                                 : std::string_view{};
}

} // namespace BlobFilesystemDetail

template <typename THttp> int64_t BasicBlobFilesystemImpl<THttp>::GetFileSize(const std::string& filePath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    return WithContainer(prefix, [&](auto& container) -> int64_t {
        // A fresh cached size needs only the key; build the client only on a miss.
        if (const auto cached = m_runtime->MetadataCache().Size(container.BlobResourceUrl(realPath))) {
            return *cached;
        }
        auto client = container.GetPageBlobClient(std::string(realPath));
        return BlobOperations::GetCachedSize(m_runtime, client);
    });
}

template <typename THttp>
uint64_t BasicBlobFilesystemImpl<THttp>::GetFileModificationTime(const std::string& filePath) const {
    EnsureLiveness();

    auto blob = ResolveBlob(filePath);
    return static_cast<uint64_t>(BlobOperations::GetInfo(blob.Blob).LastModified.time_since_epoch().count());
}

template <typename THttp> size_t BasicBlobFilesystemImpl<THttp>::GetLeaseClientCount() {
    EnsureLiveness();

    std::scoped_lock _(m_lockFilesMutex);
    return m_locks.size();
}

template <typename THttp>
CacheIdleness
BasicBlobFilesystemImpl<THttp>::WaitForFileCachesIdleForTesting(const std::chrono::milliseconds timeout) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (const auto& [prefix, cache] : m_fileCaches) {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (Core::FileCacheTestAccess::WaitUntilIdle(
                *cache, (std::max)(remaining, std::chrono::milliseconds::zero())) == Core::IdleWaitResult::TimedOut) {
            return CacheIdleness::TimedOut;
        }
    }
    return CacheIdleness::Idle;
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger,
    std::shared_ptr<BasicClientRuntime<THttp>> runtime, Core::ByteCount dataFileInitialSize,
    Core::ByteCount dataFileBufferSize, std::optional<std::chrono::milliseconds> lockAcquireTimeout)
    : m_logger(std::move(logger)),
      m_dataFileInitialSize(RequireValidDataFileSizes(dataFileInitialSize, dataFileBufferSize)),
      m_dataFileBufferSize(dataFileBufferSize), m_lockAcquireTimeout(lockAcquireTimeout), m_runtime(std::move(runtime)),
      m_asyncReads(std::make_shared<AsyncReadTracker>(m_runtime->HttpClient().get_executor())),
      m_leaseRenewal{LeaseRenewal::Start(
          m_runtime->HttpClient().get_executor(), m_logger, RenewableLocksSnapshot{this}, FenceWritesOnFatal{this},
          RenewalInterval{std::chrono::duration_cast<std::chrono::milliseconds>(Configuration::RenewalDelay)})} {}

// Snapshot under the mutex but renew outside it, so LockFile/UnlockFile are not blocked behind network round trips
// and their retries. The shared_ptrs keep the locks alive meanwhile.
template <typename THttp>
std::vector<std::shared_ptr<typename BasicBlobFilesystemImpl<THttp>::LockFileType>>
BasicBlobFilesystemImpl<THttp>::RenewableLocksSnapshot::operator()() const {
    std::vector<std::shared_ptr<LockFileType>> locks;
    std::scoped_lock lock(Owner->m_lockFilesMutex);
    std::erase_if(Owner->m_renewableLocks, [](const auto& weak) { return weak.expired(); });
    for (const auto& weak : Owner->m_renewableLocks) {
        if (auto shared = weak.lock()) {
            locks.push_back(std::move(shared));
        }
    }
    return locks;
}

template <typename THttp> void BasicBlobFilesystemImpl<THttp>::FenceWritesOnFatal::operator()() const {
    Owner->m_runtime->Fence().Fence();
}

// Stops lease renewal and waits for in-flight async reads first: their completions use the clients and logger that
// are destroyed right after this body.
template <typename THttp> BasicBlobFilesystemImpl<THttp>::~BasicBlobFilesystemImpl() {
    // Reads can only end through completions on the client executor. On a single-threaded context this wait never
    // ends, so name the violated precondition up front instead of leaving an unexplained hang. Keep waiting, since a
    // multi-threaded context can still drain, and do not assert: the wait must stay survivable.
    if (m_asyncReads->InFlight() > 0 && m_runtime->HttpClient().get_executor().running_in_this_thread()) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
            << "The filesystem is being destroyed on a thread running the client io_context while async reads are "
               "in flight; it must be destroyed from a thread that does not run it. Waiting for the reads may hang";
    }
    m_leaseRenewal->Stop(Configuration::ShutdownWaitTimeout);
    if (const auto inFlight = m_asyncReads->InFlight(); inFlight > 0) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::info)
            << "Waiting for " << inFlight << " in-flight async read(s) before closing the filesystem";
    }
    // The host must keep the io_context running here: reads only end once their completions run on it. Warn
    // periodically so a stopped io_context shows up as a diagnosable hang rather than a silent one.
    const auto giveUpAt = std::chrono::steady_clock::now() + Configuration::ShutdownWaitTimeout;
    while (m_asyncReads->DrainFor(Configuration::AsyncReadDrainWarning) == DrainResult::TimedOut) {
        if (std::chrono::steady_clock::now() >= giveUpAt) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
                << "Giving up on " << m_asyncReads->InFlight() << " in-flight async read(s) after "
                << Configuration::ShutdownWaitTimeout.count()
                << " s; the io_context was not running them to completion. The clients and caches stay alive until "
                   "the last of them ends";
            m_asyncReads->RetainUntilIdle(std::make_shared<const SharedState>(
                SharedState{m_runtime, std::move(m_clients), std::move(m_fileCaches), m_logger}));
            break;
        }
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Still waiting for " << m_asyncReads->InFlight()
            << " in-flight async read(s); the io_context must keep running until the filesystem is destroyed";
    }
}

template <typename THttp>
template <class TFn>
decltype(auto) BasicBlobFilesystemImpl<THttp>::WithContainer(const std::string_view prefix, TFn&& fn) const {
    // TODO: Future work to determine health of this client. Seeing that repeated 503s/403s caused successive calls to
    // fail with the same error.
    const auto client = m_clients.find(prefix);
    if (client == m_clients.end()) {
        throw std::runtime_error("Client not found for '" + std::string(prefix) + "'");
    }
    return std::forward<TFn>(fn)(*client->second);
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::ResolvedBlob
BasicBlobFilesystemImpl<THttp>::ResolveBlob(const std::string& path) const {
    const auto [prefix, realPath] = StorageAccount::StripPrefix(path);
    return WithContainer(prefix, [&](auto& container) {
        return ResolvedBlob{prefix, realPath, container.GetPageBlobClient(std::string(realPath))};
    });
}

template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::EnsureLiveness(const std::source_location location) const {
    if (m_runtime->Fence().State() == WriteFenceState::Fenced) {
        // Fatal once; every later rejected call is an expected consequence, so keep it out of the error stream.
        const auto severity = m_fencedLogged.exchange(FenceLogState::Logged) == FenceLogState::Logged
                                  ? boost::log::trivial::severity_level::debug
                                  : boost::log::trivial::severity_level::fatal;
        BOOST_LOG_SEV(*m_logger, severity)
            << "Unable to ensure safe database access when attempting to call " << location.function_name();
        throw WritesFencedException("Unable to ensure safe database access");
    }
}
template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    std::shared_ptr<BasicClientRuntime<THttp>> runtime, const Models::DbName& name,
    const Models::AccountUrl& storageAccountUrl, const StorageAccountKey& storageAccountKey,
    Core::ByteCount dataFileInitialSize, Core::ByteCount dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::filesystem::path> cachePath, Core::ByteCount maxCacheSize,
    std::optional<std::chrono::milliseconds> lockAcquireTimeout)
    : BasicBlobFilesystemImpl(std::move(logger), std::move(runtime), dataFileInitialSize, dataFileBufferSize,
                              lockAcquireTimeout) {
    AddContainer(ClientFactory::CreateSharedKeyServiceClient(m_runtime, storageAccountUrl, storageAccountKey),
                 storageAccountUrl, name, cachePath, maxCacheSize);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// Deletes the blob and drops its cached copy so a later blob with the same name is never served stale bytes.
template <typename THttp>
AzureClient::PriorState BasicBlobFilesystemImpl<THttp>::DeleteFile(const std::string& filePath) const {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(filePath);

    // Dropped whether or not the delete succeeds: a response lost after the service applied it must not leave the
    // old bytes servable by name.
    const boost::scope::scope_exit dropCached{[&] {
        if (const auto cache = m_fileCaches.find(prefix); cache != m_fileCaches.end()) {
            cache->second->RemoveFile(realPath);
        }
    }};
    // A suppressed (not found) error means there was nothing to delete.
    return BlobOperations::Delete(m_runtime, client);
}

// Removes every blob under the directory prefix. Object storage has no real directories, so the blobs are listed
// and deleted in batches to keep the request count low.
template <typename THttp>
DirectoryContents BasicBlobFilesystemImpl<THttp>::DeleteDir(const std::string& directoryPath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    return WithContainer(prefix, [&](auto& container) { return DeleteDirIn(container, prefix, realPath); });
}

template <typename THttp>
DirectoryContents BasicBlobFilesystemImpl<THttp>::DeleteDirIn(BlobContainerClientOn<THttp>& container,
                                                              const std::string_view prefix,
                                                              const std::string_view realPath) const {
    const boost::scope::scope_exit invalidateSizes{[this] { m_runtime->MetadataCache().InvalidateAll(); }};

    AzureClient::ListBlobsOptions options;
    options.Prefix = BlobFilesystemDetail::DirectoryPrefix(realPath);

    // Each listing page is deleted before the next one is requested, so planning memory is bounded by one page
    // instead of the whole directory. Batched deletes still cut the request count by up to 256x. Every listed blob
    // is attempted and the first per-blob failure is thrown at the end, so callers never mistake a partial delete
    // for success. A chunk-level failure (or a listing failure) propagates immediately: pages already deleted stay
    // deleted and later pages are not attempted, where before a listing failure deleted nothing.
    AzureClient::DeleteBlobsOptions deleteOptions;
    deleteOptions.MaxConcurrentRequests = AzureClient::Concurrency{Configuration::Transfer::MaxConcurrentDeleteBatches};
    std::optional<AzureClient::BlobStorageError> firstFailure;

    BlobFilesystemDetail::ForEachBlobPage(
        container, options, [&](const std::vector<AzureClient::Models::BlobItem>& page) {
            if (page.empty()) {
                return;
            }
            std::vector<std::string> names;
            names.reserve(page.size());
            for (const auto& blob : page) {
                names.push_back(blob.Name);
            }

            // Dropped whether or not every delete succeeds: a blob recreated under the same name must never be
            // served the old bytes from the cache. Removing a name that is not cached is a no-op.
            const boost::scope::scope_exit dropCached{[&] {
                if (const auto cache = m_fileCaches.find(prefix); cache != m_fileCaches.end()) {
                    for (const auto& name : names) {
                        cache->second->RemoveFile(name);
                    }
                }
            }};

            const auto result = BlobOperations::DeleteBatch(m_runtime, container, names, deleteOptions);
            for (const auto& item : result.Items) {
                if (item.Error) {
                    BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                        << "Failed to delete blob '" << item.Name << "': " << item.Error->Message;
                    if (!firstFailure) {
                        firstFailure = item.Error;
                    }
                }
            }
        });
    if (firstFailure) {
        ThrowRequestFailed(*firstFailure);
    }

    // Only "is anything left" matters to callers (a delete that raced with a create), so one result is enough
    // instead of listing every page again.
    auto remaining = DirectoryContents::Empty;
    options.MaxResults = 1;
    BlobFilesystemDetail::ForEachBlobUntil(container, options, [&remaining](const AzureClient::Models::BlobItem&) {
        remaining = DirectoryContents::NonEmpty;
        return BlobFilesystemDetail::Traversal::Stop;
    });
    return remaining;
}

// Shrinks the file and drops its cached copy, which could otherwise serve bytes past the new end. Like
// WriteableFile::Truncate this only shrinks: asking for a larger size throws std::invalid_argument (surfaced to
// RocksDB as IOStatus::InvalidArgument) instead of reporting success for a call that would change nothing.
// Truncating to the current size is a no-op.
template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::Truncate(const std::string& filePath, int64_t size) const {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(filePath);

    const auto info = BlobOperations::GetInfo(client);
    if (size > info.Size) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Refusing to extend '" << realPath << "' from " << info.Size << " to " << size
            << " bytes: Truncate can only shrink a file";
        throw std::invalid_argument("Truncate can only shrink the file. Cannot expand from " +
                                    std::to_string(info.Size) + " to " + std::to_string(size));
    }
    if (info.Size == size) {
        return;
    }

    // Resize before recording the new logical size: if the resize fails the metadata still matches the blob.
    // The service only accepts page-aligned blob sizes; the logical size lives in the metadata set afterwards.
    // The cached copy is dropped whether or not this succeeds: a failure after the resize leaves the blob changed.
    boost::scope::scope_exit dropCached{[&] {
        if (const auto cache = m_fileCaches.find(prefix); cache != m_fileCaches.end()) {
            cache->second->RemoveFile(std::string(realPath));
        }
    }};
    const auto [partialPage, alignedSize] = BlobHelpers::RoundToEndOfNearestPage(size);
    BlobOperations::SetCapacity(m_runtime, client, CountFromSigned(alignedSize));
    {
        boost::scope::scope_fail warn{[&] {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                << "'" << realPath << "' was resized to " << alignedSize
                << " bytes but its logical size could not be set to " << size
                << "; its recorded size may exceed its capacity until the truncate is retried";
        }};
        BlobOperations::SetSize(m_runtime, client, CountFromSigned(size));
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename THttp>
std::shared_ptr<typename BasicBlobFilesystemImpl<THttp>::ReadableFileType>
BasicBlobFilesystemImpl<THttp>::CreateReadableFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);
    return std::make_shared<ReadableFileType>(realPath, m_runtime,
                                              std::make_shared<PageBlobClientOn<THttp>>(std::move(blob)),
                                              BlobFilesystemDetail::FindCache(m_fileCaches, prefix), m_logger,
                                              m_asyncReads, m_prefetchBytes, m_maxPrefetchBytes);
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::SequentialFileType
BasicBlobFilesystemImpl<THttp>::CreateSequentialFile(const std::string& filePath) {
    return SequentialFileType{CreateReadableFile(filePath)};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::WriteableFileType
BasicBlobFilesystemImpl<THttp>::CreateWriteableFile(const std::string& filePath) {
    EnsureLiveness();

    const auto [initialSize, bufferSize] =
        BlobFilesystemDetail::SizingFor(filePath, m_dataFileInitialSize, m_dataFileBufferSize);

    auto [prefix, realPath, blob] = ResolveBlob(filePath);

    // Creating a writeable file always provides a "new" file; the overwriting create gives exactly that, with a
    // known size and capacity, in a single request.
    // The replaced blob's cached bytes are stale, and nothing else would drop them if the file is never synced. Dropped
    // even if the create fails: the service may have applied it before the response was lost.
    auto cache = BlobFilesystemDetail::FindCache(m_fileCaches, prefix);
    {
        const boost::scope::scope_exit dropCached{[&] {
            if (cache) {
                cache->RemoveFile(realPath);
            }
        }};
        BlobOperations::CreateOrReplace(m_runtime, blob, initialSize);
    }
    return WriteableFileType{
        realPath,
        m_runtime,
        std::make_shared<PageBlobClientOn<THttp>>(std::move(blob)),
        std::move(cache),
        m_logger,
        bufferSize,
        typename WriteableFileType::BlobState{.Size = Core::ByteCount{0}, .Capacity = initialSize}};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::ReadWriteFileType
BasicBlobFilesystemImpl<THttp>::CreateReadWriteFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(m_runtime, blob, Defaults::DataFileInitialSize);

    return ReadWriteFileType{realPath, m_runtime, std::make_shared<PageBlobClientOn<THttp>>(std::move(blob)),
                             BlobFilesystemDetail::FindCache(m_fileCaches, prefix), m_logger};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::WriteableFileType
BasicBlobFilesystemImpl<THttp>::ReopenWriteableFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);
    const auto [initialSize, bufferSize] =
        BlobFilesystemDetail::SizingFor(filePath, m_dataFileInitialSize, m_dataFileBufferSize);

    // RocksDB expects reopening to create a missing file, as the POSIX Env does.
    const auto created = BlobOperations::CreateIfNotExists(m_runtime, blob, initialSize);

    auto client = std::make_shared<PageBlobClientOn<THttp>>(std::move(blob));
    auto cache = BlobFilesystemDetail::FindCache(m_fileCaches, prefix);
    if (created == BlobOperations::CreateOutcome::Created) {
        // A blob this call just created is empty with a known capacity, so there is nothing to look up.
        return WriteableFileType{
            realPath,
            m_runtime,
            std::move(client),
            std::move(cache),
            m_logger,
            bufferSize,
            typename WriteableFileType::BlobState{.Size = Core::ByteCount{0}, .Capacity = initialSize}};
    }
    return WriteableFileType{realPath, m_runtime, std::move(client), std::move(cache), m_logger, bufferSize};
}

template <typename THttp>
LoggerImpl BasicBlobFilesystemImpl<THttp>::CreateLogger(const std::string& filePath,
                                                        const std::chrono::seconds cooldown) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(m_runtime, blob, Defaults::DataFileInitialSize);

    auto impl = std::make_unique<FileLogSink<WriteableFileType>>(std::make_unique<WriteableFileType>(
        realPath, m_runtime, std::make_shared<PageBlobClientOn<THttp>>(std::move(blob)), nullptr, m_logger,
        Defaults::DataFileBufferSize));
    return LoggerImpl{
        std::move(impl),
        std::make_unique<LogRateLimiter>(
            std::vector<std::string>{"Stalling writes because we have", "Stopping writes because we have"}, cooldown)};
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename THttp>
std::shared_ptr<typename BasicBlobFilesystemImpl<THttp>::LockFileType>
BasicBlobFilesystemImpl<THttp>::LockFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(m_runtime, blob, Defaults::DataFileInitialSize);
    auto lockFile = std::make_shared<LockFileType>(
        m_runtime, std::make_unique<PageBlobClientOn<THttp>>(std::move(blob)), Configuration::LeaseLength, m_logger,
        std::string(realPath), m_lockAcquireTimeout);
    // Lock() can retry for the full lease length. The mutex must not be held across it, or the renewal thread
    // would stall and the leases this Env already holds would expire.
    if (lockFile->Lock() == Azure::LockFile::LockResult::Acquired) {
        // The lease is held from here on, so a failure while registering it must not leave it orphaned until it
        // expires: nobody else holds a reference that could release it.
        boost::scope::scope_fail releaseLease{[&] {
            try {
                {
                    std::scoped_lock _(m_lockFilesMutex);
                    lockFile->unlink();
                    std::erase_if(m_renewableLocks, [&lockFile](const std::weak_ptr<LockFileType>& weak) {
                        const auto shared = weak.lock();
                        return !shared || shared == lockFile;
                    });
                }
                lockFile->Unlock();
            } catch (const std::exception& ex) {
                BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
                    << "Could not release the lease on '" << realPath << "' after registering it failed: " << ex.what();
            } catch (...) {
                BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
                    << "Could not release the lease on '" << realPath << "' after registering it failed";
            }
        }};
        {
            std::scoped_lock _(m_lockFilesMutex);
            m_renewableLocks.push_back(lockFile);
            m_locks.push_back(*lockFile);
        }
        // Purged only once the lease is registered for renewal, since the listing can outlast the lease.
        PurgeOrphanedRenameTemps(prefix, realPath);
        return lockFile;
    } else {
        const auto message =
            std::format("The targeted storage location is locked: container '{}', path '{}'", prefix, realPath);
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning) << message;
        throw std::runtime_error(message);
    }
}

// Deliberately skips EnsureLiveness: once writes are fenced the lease may already be lost, and releasing whatever is
// still held is exactly what shutdown must still be able to do. Releasing is never a write to the data.
template <typename THttp> void BasicBlobFilesystemImpl<THttp>::UnlockFile(LockFileType& lock) {
    {
        std::scoped_lock _(m_lockFilesMutex);
        lock.unlink();
        std::erase_if(m_renewableLocks, [&lock](const std::weak_ptr<LockFileType>& weak) {
            const auto shared = weak.lock();
            return !shared || shared.get() == &lock;
        });
    }
    // Released outside the list mutex. Unlock marks the lock as releasing first, so a renewal already in flight that
    // the service rejects because the release won is treated as benign rather than fatal.
    lock.Unlock();
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::DirectoryType
BasicBlobFilesystemImpl<THttp>::CreateDirectory(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    // Directories are virtual, but an unknown prefix must still fail here rather than on first use.
    WithContainer(prefix, [](auto&) {});
    return DirectoryType{realPath};
}

template <typename THttp> AzureClient::Existence BasicBlobFilesystemImpl<THttp>::FileExists(const std::string& name) {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(name);

    if (Call(client, client.ExistsAsync()) == AzureClient::Existence::Present) {
        return AzureClient::Existence::Present;
    }

    // Fallback: check if this is a directory
    // NOTE: This doesn't map 100% to how a filesystem would work because you can have empty
    // directories in any respectable fs. This probably won't matter for our use case.
    return HasChildren(name) == DirectoryContents::NonEmpty ? AzureClient::Existence::Present
                                                            : AzureClient::Existence::Absent;
}

// Stops at the first child instead of walking every page, which would cost a request per blob. Matches only children
// below "realPath/" so that "foo" does not match a sibling such as "foobar"; rename temporaries are not children.
template <typename THttp>
DirectoryContents BasicBlobFilesystemImpl<THttp>::HasChildren(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions options;
    options.Prefix = dirPrefix;
    options.MaxResults = 1;
    auto contents = DirectoryContents::Empty;
    WithContainer(prefix, [&](auto& container) {
        BlobFilesystemDetail::ForEachBlobUntil(
            container, std::move(options), [&](const AzureClient::Models::BlobItem& blob) {
                const auto isChild = BlobFilesystemDetail::ClassifyBlobName(blob.Name) !=
                                         BlobFilesystemDetail::BlobNameKind::RenameTemp &&
                                     !BlobFilesystemDetail::ChildName(blob.Name, dirPrefix).empty();
                contents = isChild ? DirectoryContents::NonEmpty : DirectoryContents::Empty;
                return isChild ? BlobFilesystemDetail::Traversal::Stop : BlobFilesystemDetail::Traversal::Continue;
            });
    });
    return contents;
}

template <typename THttp>
std::vector<std::string> BasicBlobFilesystemImpl<THttp>::GetChildren(const std::string& directoryPath,
                                                                     const ListPageSize pageSize) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);

    std::vector<std::string> children;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions opts;
    opts.Prefix = dirPrefix;
    opts.MaxResults = std::clamp(pageSize.Value(), std::uint32_t{1},
                                 static_cast<std::uint32_t>(Configuration::Transfer::MaxListPageSize));

    // Process all pages of results
    WithContainer(prefix, [&](auto& container) {
        BlobFilesystemDetail::ForEachBlob(container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
            if (BlobFilesystemDetail::ClassifyBlobName(blob.Name) == BlobFilesystemDetail::BlobNameKind::RenameTemp) {
                return;
            }
            if (const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix); !childName.empty()) {
                children.emplace_back(childName);
            }
        });
    });

    return children;
}

template <typename THttp>
std::vector<BlobAttributes>
BasicBlobFilesystemImpl<THttp>::GetChildrenFileAttributes(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    std::vector<BlobAttributes> attributes;

    AzureClient::ListBlobsOptions opts;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    opts.Prefix = dirPrefix;
    opts.MaxResults = static_cast<uint32_t>(Configuration::Transfer::MaxListPageSize);
    // The file size lives in blob metadata, so list it instead of issuing a GetProperties per blob.
    opts.Metadata = AzureClient::MetadataListing::Include;

    // Process all pages of results
    WithContainer(prefix, [&](auto& container) {
        BlobFilesystemDetail::ForEachBlob(container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
            const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix);
            if (childName.empty() ||
                BlobFilesystemDetail::ClassifyBlobName(blob.Name) == BlobFilesystemDetail::BlobNameKind::RenameTemp) {
                return;
            }

            attributes.emplace_back(BlobHelpers::FileSizeFromProperties(blob.Properties, blob.Name),
                                    std::string(childName));
        });
    });

    return attributes;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Copies the blob to its new name and carries over the logical file size, then deletes the source. Both paths must
/// live in the same storage account and container. A file of at most Transfer::MaxInlineRenameSize is downloaded
/// once and uploaded to the destination (see below); a larger one is copied inside the storage service (or, when the
/// service cannot authorize the source, in page-aligned chunks through this process).
///
/// Overwriting a small destination empties it and then writes it, so a crash in that short window leaves an empty
/// destination while the source is intact (BlobFilesystem rejects an empty CURRENT explicitly) and the rename can be
/// retried. Temporary blobs left by a crashed large rename are removed when this process next acquires the DB lock.
///
/// For a large file the storage service has no atomic blob replace, so an existing destination is only overwritten
/// after the
/// content has been fully copied to a temporary blob: a failure while copying leaves the destination and the
/// source untouched. The final commit (copy from the temporary blob over the destination) is not atomic, but it is
/// ordered so a torn commit is never mistaken for data: the destination is re-created empty with a logical size of
/// 0 (one request that also drops the old content), its pages are copied, and the real logical size is written
/// last. A failure in between therefore leaves an empty destination (BlobFilesystem rejects an empty CURRENT
/// explicitly) while the source is still intact, so the rename can be retried.
/// A destination that does not exist is written directly and removed again on failure.
/// </summary>
template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::RenameFile(const std::string& fromFilePath, const std::string& toFilePath) const {
    EnsureLiveness();

    const auto [prefixAccountFrom, realPathFrom] = StorageAccount::StripPrefix(fromFilePath);
    const auto [prefixAccountTo, realPathTo] = StorageAccount::StripPrefix(toFilePath);
    if (prefixAccountFrom != prefixAccountTo) {
        throw std::runtime_error("Attempting to rename file into another storage account");
    }

    if (realPathFrom == realPathTo) {
        // Nothing to do - file is already at the destination
        return;
    }

    WithContainer(prefixAccountTo, [&](auto& container) {
        auto srcClient = container.GetPageBlobClient(std::string(realPathFrom));
        auto destClient = container.GetPageBlobClient(std::string(realPathTo));
        // Copied, because the clients move into the rename below.
        const std::string srcUrl = srcClient.ResourceUrl();
        const std::string destUrl = destClient.ResourceUrl();
        // Whatever happens below, a cached copy of either name may no longer match its blob: a failed commit can
        // leave the destination emptied or partly written, and a failed source delete leaves both names holding data.
        boost::scope::scope_exit dropCached{[&] {
            m_runtime->MetadataCache().Invalidate(srcUrl);
            m_runtime->MetadataCache().Invalidate(destUrl);
            if (const auto cache = m_fileCaches.find(prefixAccountTo); cache != m_fileCaches.end()) {
                cache->second->RemoveFile(std::string(realPathFrom));
                cache->second->RemoveFile(std::string(realPathTo));
            }
        }};

        // One blocking call for the whole rename: the steps below are coroutines, so the sequencing reads top to
        // bottom.
        auto tempName = std::string(realPathTo) + std::string(BlobFilesystemDetail::RenameTempMarker) +
                        boost::uuids::to_string(boost::uuids::random_generator()());
        auto tempClient = container.GetPageBlobClient(tempName);
        Block(container, RenameBlobAsync(RenameBlobs{.Source = std::move(srcClient),
                                                     .Destination = std::move(destClient),
                                                     .Temporary = std::move(tempClient),
                                                     .SourceName = std::string(realPathFrom),
                                                     .DestinationName = std::string(realPathTo),
                                                     .TemporaryName = std::move(tempName)}));
    });
}

template <typename THttp>
boost::asio::awaitable<void> BasicBlobFilesystemImpl<THttp>::RenameBlobAsync(RenameBlobs blobs) const {
    auto& srcClient = blobs.Source;
    auto& destClient = blobs.Destination;
    const std::string& realPathFrom = blobs.SourceName;
    const std::string& realPathTo = blobs.DestinationName;
    const auto [size, cap, etag, modified] = co_await BlobOperations::GetInfoAsync(srcClient);

    // The cleanup of a failed step awaits, which a catch handler cannot do, so each step records its failure, cleans
    // up, then rethrows.
    std::exception_ptr failure;

    if (size <= SignedBytes(Configuration::Transfer::MaxInlineRenameSize)) {
        // Small file: one download, then write the destination directly. Overwriting an existing destination still
        // goes through an emptied blob (there is no atomic replace), but that window is one upload wide instead of a
        // multi-request copy, and a failure leaves the source intact for a retry.
        constexpr auto PageSize = SignedPageSize;
        const auto paddedSize = static_cast<size_t>((size + PageSize - 1) / PageSize * PageSize);
        // The download overwrites [0, size); only the page-alignment tail needs zeroing.
        const auto data = std::make_unique_for_overwrite<char[]>(paddedSize);
        const std::span<char> buffer(data.get(), paddedSize);
        if (size > 0 && co_await BlobOperations::DownloadAsync(srcClient, buffer.first(static_cast<size_t>(size)),
                                                               Core::ByteOffset{}, CountFromSigned(size),
                                                               etag) != CountFromSigned(size)) {
            throw std::runtime_error("Unexpected end of blob while renaming '" + realPathFrom + "'");
        }
        std::ranges::fill(buffer.subspan(static_cast<size_t>(size)), '\0');

        const auto created =
            co_await BlobOperations::CreateIfNotExistsAsync(m_runtime, destClient, CountFromSigned(cap));
        if (created == BlobOperations::CreateOutcome::AlreadyExisted) {
            co_await BlobOperations::CreateOrReplaceAsync(m_runtime, destClient, CountFromSigned(cap));
        }
        try {
            if (paddedSize != 0) {
                co_await BlobOperations::UploadPagesAsync(m_runtime, destClient, std::span<const char>(buffer),
                                                          Core::ByteOffset{});
            }
            co_await BlobOperations::SetSizeAsync(m_runtime, destClient, CountFromSigned(size));
        } catch (...) {
            failure = std::current_exception();
        }
        // A destination that existed keeps whatever was written; only one created here is removed again.
        if (failure && created == BlobOperations::CreateOutcome::Created) {
            co_await DeleteQuietlyAsync(BlobOperations::Destination(destClient), realPathTo);
        }
    } else if (co_await BlobOperations::CreateIfNotExistsAsync(m_runtime, destClient, CountFromSigned(cap)) ==
               BlobOperations::CreateOutcome::Created) {
        // Nothing to lose at the destination, so write it in place and remove the partial blob on failure.
        try {
            co_await CopyIntoAsync(BlobOperations::Source(srcClient), BlobOperations::Destination(destClient),
                                   CountFromSigned(size), etag, realPathFrom);
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            co_await DeleteQuietlyAsync(BlobOperations::Destination(destClient), realPathTo);
        }
    } else {
        auto& tempClient = blobs.Temporary;
        co_await BlobOperations::CreateOrReplaceAsync(m_runtime, tempClient, CountFromSigned(cap));
        try {
            // Commit: replace the destination from the fully written temporary blob.
            // The ETag from the temporary blob's final write conditions the copy. The destination's logical size
            // stays 0 until the last step.
            const auto tempEtag =
                co_await CopyIntoAsync(BlobOperations::Source(srcClient), BlobOperations::Destination(tempClient),
                                       CountFromSigned(size), etag, realPathFrom);
            co_await BlobOperations::CreateOrReplaceAsync(m_runtime, destClient, CountFromSigned(cap));
            co_await CopyBlobPagesAsync(BlobOperations::Source(tempClient), BlobOperations::Destination(destClient),
                                        CountFromSigned(size), tempEtag, realPathFrom);
            co_await BlobOperations::SetSizeAsync(m_runtime, destClient, CountFromSigned(size));
        } catch (...) {
            failure = std::current_exception();
        }
        co_await DeleteQuietlyAsync(BlobOperations::Destination(tempClient), blobs.TemporaryName);
    }
    if (failure) {
        std::rethrow_exception(failure);
    }

    co_await BlobOperations::DeleteAsync(m_runtime, srcClient);
}

template <typename THttp>
boost::asio::awaitable<void> BasicBlobFilesystemImpl<THttp>::CopyBlobPagesAsync(
    const BlobOperations::Source<BlobClient> source, const BlobOperations::Destination<BlobClient> target,
    const Core::ByteCount size, const std::string etag, const std::string name) const {
    // Server-side copy comes first; it needs the source to be readable by the service with the client's credential.
    if (co_await BlobOperations::CopyPagesServerSideAsync(m_runtime, source, target, size, etag) ==
        BlobOperations::ServerSideCopy::Declined) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Server-side copy of " << name
            << " was refused (the source could not be authorized); copying through the client instead";
        co_await BlobOperations::CopyPagesAsync(m_runtime, source, target, size, etag);
    }
}

template <typename THttp>
boost::asio::awaitable<std::string>
BasicBlobFilesystemImpl<THttp>::CopyIntoAsync(const BlobOperations::Source<BlobClient> source,
                                              const BlobOperations::Destination<BlobClient> target,
                                              const Core::ByteCount size, std::string etag, std::string name) const {
    co_await CopyBlobPagesAsync(source, target, size, std::move(etag), std::move(name));
    co_return co_await BlobOperations::SetSizeAsync(m_runtime, target.Blob(), size);
}

template <typename THttp>
boost::asio::awaitable<void>
BasicBlobFilesystemImpl<THttp>::DeleteQuietlyAsync(const BlobOperations::Destination<BlobClient> blob,
                                                   const std::string name) const {
    try {
        co_await BlobOperations::DeleteAsync(m_runtime, blob.Blob());
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Could not delete '" << name << "' while rolling back a rename: " << e.what();
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
