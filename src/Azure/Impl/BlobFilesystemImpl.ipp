// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"

#include <boost/asio/use_future.hpp>
#include <boost/scope/scope_exit.hpp>
#include <boost/scope/scope_fail.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <expected>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace BlobFilesystemDetail {
/// <summary>
/// Lists every blob in the container whose name starts with `options.Prefix`, following continuation markers,
/// and invokes `onBlob` for each.
/// </summary>
template <typename TContainer, std::predicate<const AzureClient::Models::BlobItem&> TOnBlob>
void ForEachBlobUntil(TContainer& container, AzureClient::ListBlobsOptions options, TOnBlob&& onBlob) {
    while (true) {
        auto page = Call([&](auto token) { return container.ListBlobsAsync(options, token); });
        for (const auto& blob : page.Blobs) {
            if (onBlob(blob)) {
                return;
            }
        }

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
        return false;
    });
}

inline uint32_t ToListPageSize(int32_t sizeHint) {
    return static_cast<uint32_t>(std::clamp(sizeHint, 1, Configuration::Transfer::MaxListPageSize));
}

// RenameFile stages an overwrite in "<destination>.rename-<uuid>"; the uuid is 36 characters.
inline constexpr std::string_view RenameTempMarker = ".rename-";
inline constexpr size_t RenameTempUuidLength = 36;

inline bool IsRenameTempName(std::string_view blobName) {
    constexpr auto suffixLength = RenameTempMarker.size() + RenameTempUuidLength;
    return blobName.size() > suffixLength &&
           blobName.substr(blobName.size() - suffixLength, RenameTempMarker.size()) == RenameTempMarker;
}

} // namespace BlobFilesystemDetail

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
    const std::string& storageAccountKey, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::make_shared<BasicClientRuntime<THttp>>(ioContext),
                              dataFileInitialSize, dataFileBufferSize) {
    AddContainer(ClientFactory::CreateSharedKeyServiceClient(m_runtime, storageAccountUrl, storageAccountKey),
                 storageAccountUrl, name, cachePath, maxCacheSize);
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
    const std::string& servicePrincipalId, const std::string& servicePrincipalSecret, const std::string& tenantId,
    int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::make_shared<BasicClientRuntime<THttp>>(ioContext),
                              dataFileInitialSize, dataFileBufferSize) {
    AddContainer(ClientFactory::CreateClientSecretServiceClient(m_runtime, storageAccountUrl, tenantId,
                                                                servicePrincipalId, servicePrincipalSecret),
                 storageAccountUrl, name, cachePath, maxCacheSize);
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
    const std::string& tenantId, const std::string& clientId, const std::string& serviceConnectionId,
    const std::string& accessToken, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::make_shared<BasicClientRuntime<THttp>>(ioContext),
                              dataFileInitialSize, dataFileBufferSize) {
    AddContainer(ClientFactory::CreatePipelinesServiceClient(m_runtime, storageAccountUrl, tenantId, clientId,
                                                             serviceConnectionId, accessToken),
                 storageAccountUrl, name, cachePath, maxCacheSize);
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
    std::optional<Models::ChainedCredentialInfo> backup, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::make_shared<BasicClientRuntime<THttp>>(ioContext),
                              dataFileInitialSize, dataFileBufferSize) {
    AddContainers(primary, backup, cachePath, maxCacheSize);
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
    std::optional<Models::ServicePrincipalStorageInfo> backup, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::make_shared<BasicClientRuntime<THttp>>(ioContext),
                              dataFileInitialSize, dataFileBufferSize) {
    AddContainers(primary, backup, cachePath, maxCacheSize);
}

// Only the primary container gets a file cache; the backup is a fallback that is read through directly.
template <typename THttp>
template <class TInfo>
void BasicBlobFilesystemImpl<THttp>::AddContainers(const TInfo& primary, const std::optional<TInfo>& backup,
                                                   std::optional<std::string_view> cachePath, size_t maxCacheSize) {
    AddContainer(ClientFactory::CreateServiceClient(m_runtime, primary), primary.GetStorageAccountUrl(),
                 primary.GetDbName(), cachePath, maxCacheSize);
    if (backup) {
        AddContainer(ClientFactory::CreateServiceClient(m_runtime, *backup), backup->GetStorageAccountUrl(),
                     backup->GetDbName(), std::nullopt, maxCacheSize);
    }
}

/// <summary>
/// Creates the container (if needed) for `name` in the storage account, registers the client under the account's
/// unique prefix and, when a cache path is given, sets up a file cache backed by that container.
/// </summary>
template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::AddContainer(const BlobServiceClientOn<THttp>& serviceClient,
                                                  const std::string& storageAccountUrl, const std::string& name,
                                                  std::optional<std::string_view> cachePath, size_t maxCacheSize) {
    auto containerClient = BlobHelpers::GetContainerClient(serviceClient, name);
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
void BasicBlobFilesystemImpl<THttp>::PurgeOrphanedRenameTemps(const std::string_view prefix) const {
    try {
        const auto& container = GetContainer(prefix);
        std::vector<std::string> orphans;
        AzureClient::ListBlobsOptions options;
        options.MaxResults = static_cast<uint32_t>(Configuration::Transfer::MaxListPageSize);
        BlobFilesystemDetail::ForEachBlob(*container, std::move(options),
                                          [&](const AzureClient::Models::BlobItem& blob) {
                                              if (BlobFilesystemDetail::IsRenameTempName(blob.Name)) {
                                                  orphans.push_back(blob.Name);
                                              }
                                          });
        for (const auto& name : orphans) {
            auto blob = container->GetPageBlobClient(name);
            CallResponse([&](auto token) { return blob.DeleteIfExistsAsync(token); });
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::info)
                << "Deleted the orphaned rename temporary blob '" << name << "'";
        }
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Could not clean up orphaned rename temporary blobs: " << e.what();
    }
}

namespace BlobFilesystemDetail {
struct FileSizing {
    int64_t InitialSize;
    int64_t BufferSize;
};

// Only WAL and SST files get the configurable (larger) sizes; everything else uses the small defaults.
inline FileSizing SizingFor(const std::string& path, int64_t dataInitialSize, int64_t dataBufferSize) {
    const auto fileType = Core::RocksDBHelpers::GetFileType(path);
    const auto isData =
        fileType == Core::RocksDBHelpers::FileClass::WAL || fileType == Core::RocksDBHelpers::FileClass::SST;
    return isData ? FileSizing{dataInitialSize, dataBufferSize}
                  : FileSizing{Configuration::PageBlob::DefaultSize, Configuration::PageBlob::DefaultBufferSize};
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

    auto [prefix, realPath, client] = ResolveBlob(filePath);

    return BlobOperations::GetCachedSize(*m_runtime, client);
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
bool BasicBlobFilesystemImpl<THttp>::WaitForFileCachesIdleForTesting(const std::chrono::milliseconds timeout) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (const auto& [prefix, cache] : m_fileCaches) {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (!cache->WaitUntilIdle((std::max)(remaining, std::chrono::milliseconds::zero()))) {
            return false;
        }
    }
    return true;
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger,
    std::shared_ptr<BasicClientRuntime<THttp>> runtime, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::optional<std::chrono::milliseconds> lockAcquireTimeout)
    : m_logger(std::move(logger)),
      m_dataFileInitialSize(RequireValidDataFileSizes(dataFileInitialSize, dataFileBufferSize)),
      m_dataFileBufferSize(dataFileBufferSize), m_lockAcquireTimeout(lockAcquireTimeout), m_runtime(std::move(runtime)),
      m_asyncReads(std::make_shared<AsyncReadTracker>(m_runtime->HttpClient().get_executor())),
      m_leaseRenewal{LeaseRenewal::Start(
          m_runtime->HttpClient().get_executor(), m_logger, RenewableLocksSnapshot{this}, FenceWritesOnFatal{this},
          std::chrono::duration_cast<std::chrono::milliseconds>(Configuration::RenewalDelay))} {
    // Reads the live lock list on every write; cleared in the destructor because files keep the runtime alive.
    m_runtime->SetWriteDeadlineSource([this] { return EarliestRenewalTimeRemaining(); });
}

// The tightest renewal deadline among held locks. Mutations are rejected once it passes and their requests are
// capped to it, so nothing lands after another writer may have taken over an expiring lease.
template <typename THttp>
std::optional<std::chrono::steady_clock::duration>
BasicBlobFilesystemImpl<THttp>::EarliestRenewalTimeRemaining() const {
    std::optional<std::chrono::steady_clock::duration> earliest;
    std::scoped_lock lock(m_lockFilesMutex);
    for (const auto& weak : m_renewableLocks) {
        if (const auto shared = weak.lock()) {
            if (const auto remaining = shared->RenewalTimeRemaining();
                remaining && (!earliest || *remaining < *earliest)) {
                earliest = remaining;
            }
        }
    }
    return earliest;
}

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
    Owner->m_runtime->FenceWrites();
}

// Stops lease renewal and waits for in-flight async reads first: their completions use the clients and logger that
// are destroyed right after this body.
template <typename THttp> BasicBlobFilesystemImpl<THttp>::~BasicBlobFilesystemImpl() {
    m_runtime->SetWriteDeadlineSource({});
    m_leaseRenewal->Stop(Configuration::ShutdownWaitTimeout);
    if (const auto inFlight = m_asyncReads->InFlight(); inFlight > 0) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::info)
            << "Waiting for " << inFlight << " in-flight async read(s) before closing the filesystem";
    }
    // The host must keep the io_context running here: reads only end once their completions run on it. Warn
    // periodically so a stopped io_context shows up as a diagnosable hang rather than a silent one.
    const auto giveUpAt = std::chrono::steady_clock::now() + Configuration::ShutdownWaitTimeout;
    while (!m_asyncReads->DrainFor(Configuration::AsyncReadDrainWarning)) {
        if (std::chrono::steady_clock::now() >= giveUpAt) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::error)
                << "Giving up on " << m_asyncReads->InFlight() << " in-flight async read(s) after "
                << Configuration::ShutdownWaitTimeout.count()
                << " s; the io_context was not running them to completion. Late completions may touch freed state";
            break;
        }
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Still waiting for " << m_asyncReads->InFlight()
            << " in-flight async read(s); the io_context must keep running until the filesystem is destroyed";
    }
}

template <typename THttp>
const std::shared_ptr<BlobContainerClientOn<THttp>>&
BasicBlobFilesystemImpl<THttp>::GetContainer(const std::string_view prefix) const {
    // TODO: Future work to determine health of this client. Seeing that repeated 503s/403s caused successive calls to
    // fail with the same error.
    auto client = m_clients.find(prefix);
    if (client != m_clients.end()) {
        return client->second;
    } else {
        throw std::runtime_error("Client not found for '" + std::string(prefix) + "'");
    }
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::ResolvedBlob
BasicBlobFilesystemImpl<THttp>::ResolveBlob(const std::string& path) const {
    const auto [prefix, realPath] = StorageAccount::StripPrefix(path);
    return {prefix, realPath, GetContainer(prefix)->GetPageBlobClient(std::string(realPath))};
}

template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::EnsureLiveness(const std::source_location location) const {
    if (m_runtime->WritesFenced()) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::fatal)
            << "Unable to ensure safe database access when attempting to call " << location.function_name();
        throw WritesFencedException("Unable to ensure safe database access");
    }
}
template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    std::shared_ptr<BasicClientRuntime<THttp>> runtime, const std::string& name, const std::string& storageAccountUrl,
    const std::string& storageAccountKey, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize,
    std::optional<std::chrono::milliseconds> lockAcquireTimeout)
    : BasicBlobFilesystemImpl(std::move(logger), std::move(runtime), dataFileInitialSize, dataFileBufferSize,
                              lockAcquireTimeout) {
    AddContainer(ClientFactory::CreateSharedKeyServiceClient(m_runtime, storageAccountUrl, storageAccountKey),
                 storageAccountUrl, name, cachePath, maxCacheSize);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

#include "BlobFilesystemImpl.Delete.ipp"
#include "BlobFilesystemImpl.FileOpen.ipp"
#include "BlobFilesystemImpl.Locking.ipp"
#include "BlobFilesystemImpl.Paths.ipp"
#include "BlobFilesystemImpl.Rename.ipp"
