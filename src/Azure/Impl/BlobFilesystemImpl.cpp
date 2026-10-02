// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/RequestFailedException.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"

#include <boost/asio/use_future.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <future>
#include <sstream>
#include <string>
#include <vector>

using boost::log::trivial::severity_level;

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
namespace {
// The service accepts at most 5000 results per List Blobs page.
const constexpr int32_t g_maxListPageSize = 5000;

// Number of delete requests kept in flight at once by DeleteDir.
const constexpr std::size_t g_maxConcurrentDeletes = 256;

// Largest range downloaded and uploaded per request when copying a blob in RenameFile.
const constexpr int64_t g_maxCopyChunkSize = static_cast<int64_t>(4) * 1024 * 1024;

/// <summary>
/// Lists every blob in the container whose name starts with `options.Prefix`, following continuation markers,
/// and invokes `onBlob` for each.
/// </summary>
void ForEachBlob(AzureClient::BlobContainerClient& container, AzureClient::ListBlobsOptions options,
                 const std::function<void(const AzureClient::Models::BlobItem&)>& onBlob) {
    while (true) {
        auto page = Unwrap(container.ListBlobsAsync(options, boost::asio::use_future).get());
        for (const auto& blob : page.Blobs) {
            onBlob(blob);
        }

        if (page.NextMarker.empty()) {
            break;
        }

        options.Marker = std::move(page.NextMarker);
    }
}

uint32_t ToListPageSize(int32_t sizeHint) { return static_cast<uint32_t>(std::clamp(sizeHint, 1, g_maxListPageSize)); }
} // namespace

BlobFilesystemImpl::BlobFilesystemImpl(
    const std::string& name, const std::string& storageAccountUrl, const std::string& storageAccountKey,
    int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize) {
    auto options = BlobHelpers::CreateServiceClientOptions(storageAccountUrl);
    options.SharedKey.AccountName = BlobHelpers::AccountNameFromUrl(storageAccountUrl);
    options.SharedKey.AccountKey = storageAccountKey;
    AddContainer(AzureClient::BlobServiceClient{m_runtime->HttpClient(), std::move(options)}, storageAccountUrl, name,
                 cachePath, maxCacheSize);
}

BlobFilesystemImpl::BlobFilesystemImpl(
    const std::string& name, const std::string& storageAccountUrl, const std::string& servicePrincipalId,
    const std::string& servicePrincipalSecret, const std::string& tenantId, int64_t dataFileInitialSize,
    int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize) {
    auto options = BlobHelpers::CreateServiceClientOptions(storageAccountUrl);
    options.TokenCredential =
        BlobHelpers::CreateClientSecretCredential(*m_runtime, tenantId, servicePrincipalId, servicePrincipalSecret);
    AddContainer(AzureClient::BlobServiceClient{m_runtime->HttpClient(), std::move(options)}, storageAccountUrl, name,
                 cachePath, maxCacheSize);
}

BlobFilesystemImpl::BlobFilesystemImpl(
    const std::string& name, const std::string& storageAccountUrl, const std::string& tenantId,
    const std::string& clientId, const std::string& serviceConnectionId, const std::string& accessToken,
    int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize) {
    auto options = BlobHelpers::CreateServiceClientOptions(storageAccountUrl);
    options.TokenCredential =
        BlobHelpers::CreatePipelinesCredential(*m_runtime, tenantId, clientId, serviceConnectionId, accessToken);
    AddContainer(AzureClient::BlobServiceClient{m_runtime->HttpClient(), std::move(options)}, storageAccountUrl, name,
                 cachePath, maxCacheSize);
}

BlobFilesystemImpl::BlobFilesystemImpl(
    Models::ChainedCredentialInfo primary, std::optional<Models::ChainedCredentialInfo> backup,
    int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize) {
    AddContainer(BlobHelpers::CreateServiceClient(*m_runtime, primary), primary.GetStorageAccountUrl(),
                 primary.GetDbName(), cachePath, maxCacheSize);

    if (backup) {
        AddContainer(BlobHelpers::CreateServiceClient(*m_runtime, *backup), backup->GetStorageAccountUrl(),
                     backup->GetDbName(), std::nullopt, maxCacheSize);
    }
}

BlobFilesystemImpl::BlobFilesystemImpl(
    Models::ServicePrincipalStorageInfo primary, std::optional<Models::ServicePrincipalStorageInfo> backup,
    int64_t dataFileInitialSize, int64_t dataFileBufferSize,
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
    std::optional<std::string_view> cachePath, size_t maxCacheSize)
    : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize) {
    AddContainer(BlobHelpers::CreateServiceClient(*m_runtime, primary), primary.GetStorageAccountUrl(),
                 primary.GetDbName(), cachePath, maxCacheSize);

    if (backup) {
        AddContainer(BlobHelpers::CreateServiceClient(*m_runtime, *backup), backup->GetStorageAccountUrl(),
                     backup->GetDbName(), std::nullopt, maxCacheSize);
    }
}

/// <summary>
/// Creates the container (if needed) for `name` in the storage account, registers the client under the account's
/// unique prefix and, when a cache path is given, sets up a file cache backed by that container.
/// </summary>
void BlobFilesystemImpl::AddContainer(AzureClient::BlobServiceClient serviceClient,
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

    m_clients.emplace(uniquePrefix, ServiceContainer{std::move(serviceClient), std::move(containerClient)});
}

ReadableFileImpl BlobFilesystemImpl::CreateReadableFile(const std::string& filePath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);
    auto blobClient = std::make_shared<PageBlob>(m_runtime, container->GetPageBlobClient(std::string(realPath)));
    auto cache = m_fileCaches.find(prefix);
    if (cache != m_fileCaches.end()) {
        return ReadableFileImpl{realPath, std::move(blobClient), cache->second, m_logger};
    } else {
        return ReadableFileImpl{realPath, std::move(blobClient), nullptr, m_logger};
    }
}

WriteableFileImpl BlobFilesystemImpl::CreateWriteableFile(const std::string& filePath) {
    EnsureLiveness();

    const auto fileType = Core::RocksDBHelpers::GetFileType(filePath);
    const auto isData =
        fileType == Core::RocksDBHelpers::FileClass::WAL || fileType == Core::RocksDBHelpers::FileClass::SST;
    const auto initialSize = isData ? m_dataFileInitialSize : Configuration::PageBlob::DefaultSize;
    const auto bufferSize = isData ? m_dataFileBufferSize : Configuration::PageBlob::DefaultBufferSize;

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);

    auto client = container->GetPageBlobClient(std::string(realPath));
    const auto created = BlobHelpers::CreateIfNotExists(client, initialSize);

    // Creating a writeable file is intended to always provide a "new" file.
    // If the file previously existed, efficiently truncate it so that for
    // all intents and purposes, it's a new file.
    if (!created) {
        BlobHelpers::SetFileSize(client, 0);
        if (BlobHelpers::GetBlobCapacity(client) > initialSize) {
            Unwrap(client
                       .ResizeAsync(static_cast<uint64_t>(initialSize), AzureClient::ResizePageBlobOptions{},
                                    boost::asio::use_future)
                       .get());
        }
    }

    auto cache = m_fileCaches.find(prefix);
    auto blobClient = std::make_unique<PageBlob>(m_runtime, std::move(client));
    if (cache != m_fileCaches.end()) {
        return WriteableFileImpl{realPath, std::move(blobClient), cache->second, m_logger, bufferSize};
    } else {
        return WriteableFileImpl{realPath, std::move(blobClient), nullptr, m_logger, bufferSize};
    }
}

ReadWriteFileImpl BlobFilesystemImpl::CreateReadWriteFile(const std::string& filePath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);

    auto client = container->GetPageBlobClient(std::string(realPath));
    BlobHelpers::CreateIfNotExists(client, Configuration::PageBlob::DefaultSize);

    auto blobClient = std::make_shared<PageBlob>(m_runtime, std::move(client));

    auto cache = m_fileCaches.find(prefix);
    if (cache != m_fileCaches.end()) {
        return ReadWriteFileImpl{realPath, std::move(blobClient), cache->second, m_logger};
    } else {
        return ReadWriteFileImpl{realPath, std::move(blobClient), nullptr, m_logger};
    }
}

WriteableFileImpl BlobFilesystemImpl::ReopenWriteableFile(const std::string& filePath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);
    const auto fileType = Core::RocksDBHelpers::GetFileType(filePath);
    const auto isData =
        fileType == Core::RocksDBHelpers::FileClass::WAL || fileType == Core::RocksDBHelpers::FileClass::SST;
    const auto bufferSize = isData ? m_dataFileBufferSize : Configuration::PageBlob::DefaultBufferSize;

    auto client = std::make_shared<PageBlob>(m_runtime, container->GetPageBlobClient(std::string(realPath)));
    auto cache = m_fileCaches.find(prefix);
    if (cache != m_fileCaches.end()) {
        return WriteableFileImpl{realPath, std::move(client), cache->second, m_logger,
                                 static_cast<int64_t>(bufferSize)};
    } else {
        return WriteableFileImpl{realPath, std::move(client), nullptr, m_logger, static_cast<int64_t>(bufferSize)};
    }
}

WriteableFileImpl BlobFilesystemImpl::ReuseWritableFile(const std::string& filePath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);
    const auto fileType = Core::RocksDBHelpers::GetFileType(filePath);
    const auto isData =
        fileType == Core::RocksDBHelpers::FileClass::WAL || fileType == Core::RocksDBHelpers::FileClass::SST;
    const auto initialSize = isData ? m_dataFileInitialSize : Configuration::PageBlob::DefaultSize;
    const auto bufferSize = isData ? m_dataFileBufferSize : Configuration::PageBlob::DefaultBufferSize;

    // TODO: figure out what the intent here is for now just delete and recreate
    auto client = container->GetPageBlobClient(std::string(realPath));
    UnwrapResponse(client.DeleteIfExistsAsync(boost::asio::use_future).get());
    BlobHelpers::CreateIfNotExists(client, initialSize);

    auto cache = m_fileCaches.find(prefix);
    auto blobClient = std::make_shared<PageBlob>(m_runtime, std::move(client));
    if (cache != m_fileCaches.end()) {
        return WriteableFileImpl{realPath, std::move(blobClient), cache->second, m_logger,
                                 static_cast<int64_t>(bufferSize)};
    } else {
        return WriteableFileImpl{realPath, std::move(blobClient), nullptr, m_logger, static_cast<int64_t>(bufferSize)};
    }
}

LoggerImpl BlobFilesystemImpl::CreateLogger(const std::string& filePath, const int logLevel,
                                            const std::chrono::seconds cooldown) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);

    auto client = container->GetPageBlobClient(std::string(realPath));
    BlobHelpers::CreateIfNotExists(client, Configuration::PageBlob::DefaultSize);

    auto blobClient = std::make_shared<PageBlob>(m_runtime, std::move(client));
    auto impl = std::make_unique<WriteableFileImpl>(realPath, std::move(blobClient), nullptr, m_logger,
                                                    Configuration::PageBlob::DefaultSize);
    return LoggerImpl{
        std::move(impl), logLevel,
        std::make_unique<LogRateLimiter>(
            std::vector<std::string>{"Stalling writes because we have", "Stopping writes because we have"}, cooldown)};
}

std::shared_ptr<LockFileImpl> BlobFilesystemImpl::LockFile(const std::string& filePath) {
    EnsureLiveness();

    std::scoped_lock _(m_lockFilesMutex);

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);

    auto client = std::make_unique<AzureClient::PageBlobClient>(container->GetPageBlobClient(std::string(realPath)));
    BlobHelpers::CreateIfNotExists(*client, Configuration::PageBlob::DefaultSize);
    auto lockFile = std::make_shared<LockFileImpl>(m_runtime, std::move(client), Configuration::LeaseLength, m_logger,
                                                   std::string(realPath));
    if (lockFile->Lock()) {
        m_locks.push_back(*lockFile);
        assert(lockFile->is_linked());
        return lockFile;
    } else {
        throw std::runtime_error("The targeted storage location is locked");
    }
}

void BlobFilesystemImpl::UnlockFile(LockFileImpl& lock) {
    EnsureLiveness();

    std::scoped_lock _(m_lockFilesMutex);
    lock.Unlock();
    lock.unlink();
}

DirectoryImpl BlobFilesystemImpl::CreateDirectory(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    return DirectoryImpl{m_runtime, GetContainer(prefix), realPath};
}

bool BlobFilesystemImpl::FileExists(const std::string& name) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(name);
    const auto& container = GetContainer(prefix);

    auto client = container->GetPageBlobClient(std::string(realPath));
    auto props = client.GetPropertiesAsync(boost::asio::use_future).get();
    if (props.has_value()) {
        return true;
    }

    if (props.error().StatusCode != HttpStatus::NotFound) {
        ThrowRequestFailed(props.error());
    }

    // Fallback: check if this is a directory
    // NOTE: This doesn't map 100% to how a filesystem would work because you can have empty
    // directories in any respectable fs. This probably won't matter for our use case.
    return !GetChildren(name, 1).empty();
}

std::vector<std::string> BlobFilesystemImpl::GetChildren(const std::string& directoryPath, int32_t sizeHint) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);

    std::vector<std::string> children;
    AzureClient::ListBlobsOptions opts;
    opts.Prefix = realPath;
    opts.MaxResults = ToListPageSize(sizeHint);
    auto extractChildName = [&realPath](const auto& blob) -> std::string {
        if (blob.Name.starts_with(realPath)) {
            auto index = realPath.length();
            if (index >= blob.Name.size()) {
                // Exact match (e.g. listing "LOCK" where a blob named "LOCK" exists).
                // There is no child entry to return.
                return {};
            }

            if (blob.Name[index] == '/') {
                index++;
            }

            return index < blob.Name.size() ? blob.Name.substr(index) : std::string{};
        }
        return {};
    };

    // Process all pages of results
    ForEachBlob(*container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
        if (auto childName = extractChildName(blob); !childName.empty()) {
            children.emplace_back(std::move(childName));
        }
    });

    return children;
}

std::vector<BlobAttributes> BlobFilesystemImpl::GetChildrenFileAttributes(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);
    std::vector<BlobAttributes> attributes;

    AzureClient::ListBlobsOptions opts;
    opts.Prefix = realPath;
    opts.MaxResults = static_cast<uint32_t>(g_maxListPageSize);

    // Process all pages of results
    ForEachBlob(*container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
        if (blob.Name.size() <= realPath.length()) {
            return;
        }

        auto index = realPath.length();
        if (blob.Name[index] == '/') {
            index++;
        }
        if (index >= blob.Name.size()) {
            return;
        }

        auto client = container->GetPageBlobClient(blob.Name);
        attributes.emplace_back(BlobHelpers::GetFileSize(client), blob.Name.substr(index));
    });

    return attributes;
}

bool BlobFilesystemImpl::DeleteFile(const std::string& filePath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);
    auto client = container->GetPageBlobClient(std::string(realPath));
    const auto res = UnwrapResponse(client.DeleteIfExistsAsync(boost::asio::use_future).get());

    auto cache = m_fileCaches.find(prefix);
    if (cache != m_fileCaches.end()) {
        cache->second->RemoveFile(realPath);
    }

    // A suppressed (not found) error means there was nothing to delete.
    return !res.Error().has_value();
}

size_t BlobFilesystemImpl::DeleteDir(const std::string& directoryPath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);

    AzureClient::ListBlobsOptions options;
    // "" represent the root directory, so we would want to delete everything in the blob container.
    // append "/" in other cases in the event that we have a file with the same name as a directory.
    options.Prefix = realPath == "" ? realPath : std::string(realPath) + "/";

    std::vector<std::string> blobs;
    ForEachBlob(*container, options,
                [&blobs](const AzureClient::Models::BlobItem& blob) { blobs.push_back(blob.Name); });

    // Delete with a bounded number of requests in flight. Individual failures are logged rather than thrown;
    // the listing below reports how many blobs remain.
    for (size_t i = 0; i < blobs.size(); i += g_maxConcurrentDeletes) {
        std::vector<AzureClient::PageBlobClient> clients;
        std::vector<std::future<
            std::expected<AzureClient::Response<AzureClient::Models::DeleteBlobResult>, AzureClient::BlobStorageError>>>
            deletes;
        const auto batchEnd = std::min(i + g_maxConcurrentDeletes, blobs.size());
        clients.reserve(batchEnd - i);
        deletes.reserve(batchEnd - i);
        for (size_t j = i; j < batchEnd; j++) {
            clients.push_back(container->GetPageBlobClient(blobs[j]));
        }

        for (auto& client : clients) {
            deletes.push_back(client.DeleteIfExistsAsync(boost::asio::use_future));
        }

        for (size_t j = 0; j < deletes.size(); j++) {
            auto result = deletes[j].get();
            if (!result.has_value()) {
                BOOST_LOG_SEV(*m_logger, severity_level::warning)
                    << "Failed to delete blob '" << blobs[i + j] << "': " << result.error().Message;
            }
        }
    }

    // Listing blobs to ensure everything is deleted.
    options.Marker.clear();
    const auto remaining = Unwrap(container->ListBlobsAsync(options, boost::asio::use_future).get());
    return remaining.Blobs.size();
}

void BlobFilesystemImpl::Truncate(const std::string& filePath, int64_t size) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);

    auto client = container->GetPageBlobClient(std::string(realPath));
    const auto fileSize = BlobHelpers::GetFileSize(client);
    if (fileSize > size) {
        BlobHelpers::SetFileSize(client, size);
        Unwrap(
            client
                .ResizeAsync(static_cast<uint64_t>(size), AzureClient::ResizePageBlobOptions{}, boost::asio::use_future)
                .get());
    }
}

int64_t BlobFilesystemImpl::GetFileSize(const std::string& filePath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);

    auto client = container->GetPageBlobClient(std::string(realPath));
    return BlobHelpers::GetFileSize(client);
}

uint64_t BlobFilesystemImpl::GetFileModificationTime(const std::string& filePath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
    const auto& container = GetContainer(prefix);

    auto client = container->GetPageBlobClient(std::string(realPath));
    const auto props = Unwrap(client.GetPropertiesAsync(boost::asio::use_future).get());
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(props.LastModified.time_since_epoch()).count());
}

size_t BlobFilesystemImpl::GetLeaseClientCount() {
    EnsureLiveness();

    std::scoped_lock _(m_lockFilesMutex);
    return m_locks.size();
}

/// <summary>
/// Copies the blob to its new name in page-aligned chunks, carries over the logical file size and then deletes
/// the source. Both paths must live in the same storage account and container.
/// </summary>
void BlobFilesystemImpl::RenameFile(const std::string& fromFilePath, const std::string& toFilePath) const {
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

    auto srcClient = container->GetPageBlobClient(std::string(realPathFrom));
    auto destClient = container->GetPageBlobClient(std::string(realPathTo));

    // TODO: Check if there is already a file with this name
    const auto size = BlobHelpers::GetFileSize(srcClient);
    const auto cap = BlobHelpers::GetBlobCapacity(srcClient);
    BlobHelpers::CreateIfNotExists(destClient, cap);

    int64_t uploadOffset = 0;
    while (uploadOffset < size) {
        const auto readSize = std::min(size - uploadOffset, g_maxCopyChunkSize);

        AzureClient::DownloadBlobOptions options;
        options.Range =
            AzureClient::Models::BlobByteRange{static_cast<uint64_t>(uploadOffset), static_cast<uint64_t>(readSize)};
        auto chunk = Unwrap(srcClient
                                .DownloadAsync(std::move(options), boost::asio::use_future,
                                               RequestOptionsForTransfer(srcClient.GetDefaultRequestOptions(),
                                                                         static_cast<uint64_t>(readSize)))
                                .get());
        auto& buffer = chunk.Content;
        const auto bytesRead = static_cast<int64_t>(buffer.size());
        if (bytesRead == 0) {
            throw std::runtime_error("Unexpected end of blob while renaming '" + std::string(realPathFrom) + "'");
        }

        // this must be aligned to page size so in some cases need dummy data
        const auto remaining = buffer.size() % Configuration::PageBlob::PageSize;
        if (remaining != 0) {
            buffer.resize(buffer.size() + (Configuration::PageBlob::PageSize - remaining), '\0');
        }

        Unwrap(destClient
                   .UploadPagesAsync(static_cast<uint64_t>(uploadOffset), std::as_bytes(std::span<const char>(buffer)),
                                     boost::asio::use_future,
                                     RequestOptionsForTransfer(destClient.GetDefaultRequestOptions(), buffer.size()))
                   .get());

        uploadOffset += bytesRead;
    }

    BlobHelpers::SetFileSize(destClient, size);
    UnwrapResponse(srcClient.DeleteIfExistsAsync(boost::asio::use_future).get());
}

BlobFilesystemImpl::BlobFilesystemImpl(
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger,
    int64_t dataFileInitialSize, int64_t dataFileBufferSize)
    : m_logger(std::move(logger)), m_dataFileInitialSize(dataFileInitialSize), m_dataFileBufferSize(dataFileBufferSize),
      m_runtime(std::make_shared<ClientRuntime>()),
      m_lockRenewalThread{[this](std::stop_token stopToken) { RenewLease(stopToken); }} {}

const std::shared_ptr<AzureClient::BlobContainerClient>&
BlobFilesystemImpl::GetContainer(const std::string_view prefix) const {
    // TODO: Future work to determine health of this client. Seeing that repeated 503s/403s caused successive calls to
    // fail with the same error.
    auto client = m_clients.find(prefix);
    if (client != m_clients.end()) {
        return client->second.ContainerClient;
    } else {
        std::stringstream ss;
        ss << "Client not found for '" << prefix << "'";
        throw std::runtime_error(ss.str());
    }
}

void BlobFilesystemImpl::RenewLease(std::stop_token stopToken) {
    BOOST_LOG_SEV(*m_logger, severity_level::info) << "Starting blob lease renewal thread";
    try {
        while (!stopToken.stop_requested()) {
            if (stopToken.stop_requested()) {
                break;
            }

            // Sleep before attempting renewal
            static const constexpr auto sleepInterval = std::chrono::milliseconds(100);
            static const constexpr auto maxSleepIterations = 50;
            static_assert(sleepInterval * maxSleepIterations == Configuration::RenewalDelay);
            for (int i = 0; i < maxSleepIterations && !stopToken.stop_requested(); ++i) {
                std::this_thread::sleep_for(sleepInterval);
            }

            if (stopToken.stop_requested()) {
                break;
            }

            {
                std::scoped_lock lock(m_lockFilesMutex);
                std::vector<const LockFileImpl*> needsRetry;
                needsRetry.reserve(m_locks.size());
                for (const auto& l : m_locks) {
                    needsRetry.push_back(&l);
                }

                // Attempt to renew all locks with retries
                BOOST_LOG_SEV(*m_logger, severity_level::debug)
                    << "Attempting to renew " << needsRetry.size() << " leases";
                int retries = 0;
                while (needsRetry.size() > 0 && retries < 5 && !stopToken.stop_requested()) {
                    std::erase_if(needsRetry, [this](const auto& client) -> bool {
                        try {
                            client->Renew();
                            return true;
                        } catch (const RequestFailedException& e) {
                            if (e.StatusCode == HttpStatus::Conflict) {
                                BOOST_LOG_SEV(*m_logger, severity_level::error)
                                    << "Failed to renew lease due to conflict, lease might be expired: " << e.what();
                                throw;
                            } else {
                                BOOST_LOG_SEV(*m_logger, severity_level::error)
                                    << "Failed to renew lease: " << e.what();
                            }
                        }

                        return false;
                    });

                    retries++;
                    if (needsRetry.size() > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, severity_level::fatal) << "Stopping renewal thread " << e.what();
        m_filesystemStopSource.request_stop();
    } catch (...) {
        BOOST_LOG_SEV(*m_logger, severity_level::fatal) << "Stopping renewal thread";
        m_filesystemStopSource.request_stop();
    }

    BOOST_LOG_SEV(*m_logger, severity_level::info) << "Exiting blob lease renewal thread";
}

void BlobFilesystemImpl::EnsureLiveness(const std::source_location location) const {
    if (m_filesystemStopSource.stop_requested()) {
        BOOST_LOG_SEV(*m_logger, severity_level::fatal)
            << "Unable to ensure safe database access when attempting to call " << location.function_name();
        throw std::runtime_error("Unable to ensure safe database access");
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
