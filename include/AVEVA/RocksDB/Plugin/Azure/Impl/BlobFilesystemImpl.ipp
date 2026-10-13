// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobOperations.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlockOn.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientFactory.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"

#include <boost/asio/use_future.hpp>
#include <boost/scope/scope_fail.hpp>

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
        auto page = Call(container, [&](auto token) { return container.ListBlobsAsync(options, token); });
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

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::ReadableFileType
BasicBlobFilesystemImpl<THttp>::CreateReadableFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);
    return ReadableFileType{
        realPath, m_runtime,    std::move(blob), BlobFilesystemDetail::FindCache(m_fileCaches, prefix),
        m_logger, m_asyncReads, m_prefetchBytes};
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

    auto [prefix, realPath, clientPtr] = ResolveBlob(filePath);

    auto& client = *clientPtr;
    // Creating a writeable file always provides a "new" file; the overwriting create gives exactly that, with a
    // known size and capacity, in a single request.
    BlobOperations::CreateOrReplace(*m_runtime, client, initialSize);
    const int64_t capacity = initialSize;

    return WriteableFileType{realPath,
                             m_runtime,
                             std::move(clientPtr),
                             BlobFilesystemDetail::FindCache(m_fileCaches, prefix),
                             m_logger,
                             bufferSize,
                             typename WriteableFileType::BlobState{0, capacity}};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::ReadWriteFileType
BasicBlobFilesystemImpl<THttp>::CreateReadWriteFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, clientPtr] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(*m_runtime, *clientPtr, Configuration::PageBlob::DefaultSize);

    return ReadWriteFileType{realPath, m_runtime, std::move(clientPtr),
                             BlobFilesystemDetail::FindCache(m_fileCaches, prefix), m_logger};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::WriteableFileType
BasicBlobFilesystemImpl<THttp>::ReopenWriteableFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(filePath);
    const auto [initialSize, bufferSize] =
        BlobFilesystemDetail::SizingFor(filePath, m_dataFileInitialSize, m_dataFileBufferSize);

    return WriteableFileType{realPath,          m_runtime,
                             std::move(client), BlobFilesystemDetail::FindCache(m_fileCaches, prefix),
                             m_logger,          static_cast<int64_t>(bufferSize)};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::WriteableFileType
BasicBlobFilesystemImpl<THttp>::ReuseWritableFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, clientPtr] = ResolveBlob(filePath);
    const auto [initialSize, bufferSize] =
        BlobFilesystemDetail::SizingFor(filePath, m_dataFileInitialSize, m_dataFileBufferSize);

    auto& client = *clientPtr;
    BlobOperations::CreateOrReplace(*m_runtime, client, initialSize);

    return WriteableFileType{realPath,
                             m_runtime,
                             std::move(clientPtr),
                             BlobFilesystemDetail::FindCache(m_fileCaches, prefix),
                             m_logger,
                             static_cast<int64_t>(bufferSize),
                             typename WriteableFileType::BlobState{0, initialSize}};
}

template <typename THttp>
LoggerImpl BasicBlobFilesystemImpl<THttp>::CreateLogger(const std::string& filePath, const int logLevel,
                                                        const std::chrono::seconds cooldown)
    requires std::same_as<THttp, ::AVEVA::HttpClient>
{
    EnsureLiveness();

    auto [prefix, realPath, clientPtr] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(*m_runtime, *clientPtr, Configuration::PageBlob::DefaultSize);

    auto impl = std::make_unique<WriteableFileType>(realPath, m_runtime, std::move(clientPtr), nullptr, m_logger,
                                                    Configuration::PageBlob::DefaultSize);
    return LoggerImpl{
        std::move(impl), logLevel,
        std::make_unique<LogRateLimiter>(
            std::vector<std::string>{"Stalling writes because we have", "Stopping writes because we have"}, cooldown)};
}

template <typename THttp>
std::shared_ptr<typename BasicBlobFilesystemImpl<THttp>::LockFileType>
BasicBlobFilesystemImpl<THttp>::LockFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(*m_runtime, *client, Configuration::PageBlob::DefaultSize);
    auto lockFile = std::make_shared<LockFileType>(m_runtime, std::move(client), Configuration::LeaseLength, m_logger,
                                                   std::string(realPath));
    // Lock() can retry for the full lease length. The mutex must not be held across it, or the renewal thread
    // would stall and the leases this Env already holds would expire.
    if (lockFile->Lock()) {
        std::scoped_lock _(m_lockFilesMutex);
        m_locks.push_back(*lockFile);
        m_renewableLocks.push_back(lockFile);
        assert(lockFile->is_linked());
        return lockFile;
    } else {
        throw std::runtime_error("The targeted storage location is locked");
    }
}

template <typename THttp> void BasicBlobFilesystemImpl<THttp>::UnlockFile(LockFileType& lock) {
    EnsureLiveness();

    {
        std::scoped_lock _(m_lockFilesMutex);
        lock.unlink();
        std::erase_if(m_renewableLocks, [&lock](const std::weak_ptr<LockFileType>& weak) {
            const auto shared = weak.lock();
            return !shared || shared.get() == &lock;
        });
    }
    // Released outside the list mutex; LockFileType serializes this against an in-flight renewal.
    lock.Unlock();
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::DirectoryType
BasicBlobFilesystemImpl<THttp>::CreateDirectory(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    return DirectoryType{m_runtime, GetContainer(prefix), realPath};
}

template <typename THttp> bool BasicBlobFilesystemImpl<THttp>::FileExists(const std::string& name) {
    EnsureLiveness();

    auto [prefix, realPath, clientPtr] = ResolveBlob(name);

    auto& client = *clientPtr;
    auto exists = BlockOn(client, [&](auto token) { return client.ExistsAsync(token); });
    if (!exists.has_value()) {
        ThrowRequestFailed(exists.error());
    }
    if (exists->Value()) {
        return true;
    }

    // Fallback: check if this is a directory
    // NOTE: This doesn't map 100% to how a filesystem would work because you can have empty
    // directories in any respectable fs. This probably won't matter for our use case.
    // Stop at the first child instead of walking every page, which would cost a request per blob.
    // Match only children below "realPath/" so that "foo" does not match a sibling such as "foobar".
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions options;
    options.Prefix = dirPrefix;
    options.MaxResults = 1;
    bool found = false;
    BlobFilesystemDetail::ForEachBlobUntil(*GetContainer(prefix), std::move(options),
                                           [&](const AzureClient::Models::BlobItem& blob) {
                                               found = !BlobFilesystemDetail::ChildName(blob.Name, dirPrefix).empty();
                                               return found;
                                           });
    return found;
}

template <typename THttp>
std::vector<std::string> BasicBlobFilesystemImpl<THttp>::GetChildren(const std::string& directoryPath,
                                                                     int32_t sizeHint) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);

    std::vector<std::string> children;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions opts;
    opts.Prefix = dirPrefix;
    opts.MaxResults = BlobFilesystemDetail::ToListPageSize(sizeHint);

    // Process all pages of results
    BlobFilesystemDetail::ForEachBlob(*container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
        if (const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix); !childName.empty()) {
            children.emplace_back(childName);
        }
    });

    return children;
}

template <typename THttp>
std::vector<BlobAttributes>
BasicBlobFilesystemImpl<THttp>::GetChildrenFileAttributes(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);
    std::vector<BlobAttributes> attributes;

    AzureClient::ListBlobsOptions opts;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    opts.Prefix = dirPrefix;
    opts.MaxResults = static_cast<uint32_t>(Configuration::Transfer::MaxListPageSize);
    // The file size lives in blob metadata, so list it instead of issuing a GetProperties per blob.
    opts.IncludeMetadata = true;

    // Process all pages of results
    BlobFilesystemDetail::ForEachBlob(*container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
        const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix);
        if (childName.empty()) {
            return;
        }

        attributes.emplace_back(BlobHelpers::FileSizeFromProperties(blob.Properties, blob.Name),
                                std::string(childName));
    });

    return attributes;
}

template <typename THttp> bool BasicBlobFilesystemImpl<THttp>::DeleteFile(const std::string& filePath) const {
    EnsureLiveness();

    auto [prefix, realPath, clientPtr] = ResolveBlob(filePath);
    auto& client = *clientPtr;
    const auto res = CallResponse(client, [&](auto token) { return client.DeleteIfExistsAsync(token); });

    auto cache = m_fileCaches.find(prefix);
    if (cache != m_fileCaches.end()) {
        cache->second->RemoveFile(realPath);
    }

    // A suppressed (not found) error means there was nothing to delete.
    return !res.Error().has_value();
}

template <typename THttp> size_t BasicBlobFilesystemImpl<THttp>::DeleteDir(const std::string& directoryPath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);

    AzureClient::ListBlobsOptions options;
    options.Prefix = BlobFilesystemDetail::DirectoryPrefix(realPath);

    std::vector<std::string> blobs;
    BlobFilesystemDetail::ForEachBlob(
        *container, options, [&blobs](const AzureClient::Models::BlobItem& blob) { blobs.push_back(blob.Name); });

    // Delete with a bounded number of requests in flight. Every blob is attempted, then the first failure (if any)
    // is thrown so callers never mistake a partial delete for success.
    std::optional<AzureClient::BlobStorageError> firstFailure;
    // A sliding window: as soon as the oldest delete finishes, the next one starts, so one slow request does not
    // hold back a whole batch.
    struct PendingDelete {
        std::unique_ptr<typename BlobContainerClientOn<THttp>::BlobClientType> client;
        std::future<
            std::expected<AzureClient::Response<AzureClient::Models::DeleteBlobResult>, AzureClient::BlobStorageError>>
            result;
        size_t index;
    };
    std::deque<PendingDelete> window;
    const auto finishOldest = [&] {
        auto& oldest = window.front();
        auto result = BlockOn(oldest.client->get_executor(), std::move(oldest.result));
        if (!result.has_value()) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                << "Failed to delete blob '" << blobs[oldest.index] << "': " << result.error().Message;
            if (!firstFailure) {
                firstFailure = result.error();
            }
        }
        window.pop_front();
    };
    for (size_t i = 0; i < blobs.size(); i++) {
        if (window.size() >= Configuration::Transfer::MaxConcurrentDeletes) {
            finishOldest();
        }
        auto client = container->GetBlobClient(blobs[i]);
        auto result = client->DeleteIfExistsAsync(boost::asio::use_future);
        window.push_back(PendingDelete{std::move(client), std::move(result), i});
    }
    while (!window.empty()) {
        finishOldest();
    }

    if (firstFailure) {
        ThrowRequestFailed(*firstFailure);
    }

    // Only "is anything left" matters to callers (a delete that raced with a create), so one result is enough
    // instead of listing every page again.
    size_t remaining = 0;
    options.MaxResults = 1;
    BlobFilesystemDetail::ForEachBlobUntil(*container, options, [&remaining](const AzureClient::Models::BlobItem&) {
        remaining = 1;
        return true;
    });
    return remaining;
}

template <typename THttp>
void BasicBlobFilesystemImpl<THttp>::Truncate(const std::string& filePath, int64_t size) const {
    EnsureLiveness();

    auto [prefix, realPath, clientPtr] = ResolveBlob(filePath);

    auto& client = *clientPtr;
    const auto fileSize = BlobOperations::GetSize(client);
    if (fileSize > size) {
        BlobOperations::SetSize(*m_runtime, client, size);
        // The service only accepts page-aligned blob sizes; the logical size lives in the metadata set above.
        const auto [partialPage, alignedSize] = BlobHelpers::RoundToEndOfNearestPage(size);
        Call(client, [&](auto token) {
            return client.ResizeAsync(static_cast<uint64_t>(alignedSize), AzureClient::ResizePageBlobOptions{}, token);
        });
    }
}

template <typename THttp> int64_t BasicBlobFilesystemImpl<THttp>::GetFileSize(const std::string& filePath) const {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(filePath);

    return BlobOperations::GetSize(*client);
}

template <typename THttp>
uint64_t BasicBlobFilesystemImpl<THttp>::GetFileModificationTime(const std::string& filePath) const {
    EnsureLiveness();

    const auto blob = ResolveBlob(filePath);
    return static_cast<uint64_t>(BlobOperations::GetInfo(*blob.Blob).LastModified.time_since_epoch().count());
}

template <typename THttp> size_t BasicBlobFilesystemImpl<THttp>::GetLeaseClientCount() {
    EnsureLiveness();

    std::scoped_lock _(m_lockFilesMutex);
    return m_locks.size();
}

/// <summary>
/// Copies the blob's populated pages to its new name in page-aligned chunks, carries over the logical file size and
/// then deletes the source. Both paths must live in the same storage account and container.
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

    const auto& container = GetContainer(prefixAccountTo);

    const auto srcClientPtr = container->GetPageBlobClient(std::string(realPathFrom));
    const auto destClientPtr = container->GetPageBlobClient(std::string(realPathTo));
    auto& srcClient = *srcClientPtr;
    auto& destClient = *destClientPtr;

    const auto [size, cap, etag, modified] = BlobOperations::GetInfo(srcClient);
    // Destination replaced via the same overwriting create.
    BlobOperations::CreateOrReplace(*m_runtime, destClient, cap);

    BlobOperations::CopyPages(*m_runtime, srcClient, destClient, size, etag);
    BlobOperations::SetSize(*m_runtime, destClient, size);
    CallResponse(srcClient, [&](auto token) { return srcClient.DeleteIfExistsAsync(token); });
}

template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger,
    std::shared_ptr<BasicClientRuntime<THttp>> runtime, int64_t dataFileInitialSize, int64_t dataFileBufferSize)
    : m_logger(std::move(logger)), m_dataFileInitialSize(dataFileInitialSize), m_dataFileBufferSize(dataFileBufferSize),
      m_runtime(std::move(runtime)),
      m_asyncReads(std::make_shared<AsyncReadTracker>(m_runtime->HttpClient().get_executor())),
      m_leaseRenewal{LeaseRenewal::Start(
          m_runtime->HttpClient().get_executor(), m_logger, RenewableLocksSnapshot{this}, FenceWritesOnFatal{this},
          std::chrono::duration_cast<std::chrono::milliseconds>(Configuration::RenewalDelay))} {}

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

template <typename THttp> BasicBlobFilesystemImpl<THttp>::~BasicBlobFilesystemImpl() {
    m_leaseRenewal->Stop();
    if (const auto inFlight = m_asyncReads->InFlight(); inFlight > 0) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::info)
            << "Waiting for " << inFlight << " in-flight async read(s) before closing the filesystem";
    }
    // The host must keep the io_context running here: reads only end once their completions run on it. Warn
    // periodically so a stopped io_context shows up as a diagnosable hang rather than a silent one.
    while (!m_asyncReads->DrainFor(Configuration::AsyncReadDrainWarning)) {
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
        throw std::runtime_error("Unable to ensure safe database access");
    }
}
template <typename THttp>
BasicBlobFilesystemImpl<THttp>::BasicBlobFilesystemImpl(
    std::shared_ptr<BasicClientRuntime<THttp>> runtime, const std::string& name, const std::string& storageAccountUrl,
    const std::string& storageAccountKey, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BasicBlobFilesystemImpl(std::move(logger), std::move(runtime), dataFileInitialSize, dataFileBufferSize) {
    AddContainer(ClientFactory::CreateSharedKeyServiceClient(m_runtime, storageAccountUrl, storageAccountKey),
                 storageAccountUrl, name, cachePath, maxCacheSize);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
