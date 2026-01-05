// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Core/RocksDBHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/LocalFilesystem.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobFilesystemImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/StorageAccount.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobHelpers.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/PageBlob.hpp"

#include <azure/storage/blobs.hpp>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    BlobFilesystemImpl::BlobFilesystemImpl(const std::string& name,
        const std::string& storageAccountUrl,
        const std::string& storageAccountKey,
        int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath,
        size_t maxCacheSize,
        GraphDb::Storage::OpenMode openMode)
        : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize)
    {
        ::Azure::Storage::Blobs::BlobServiceClient serviceClient
        {
            storageAccountUrl,
            std::make_shared<::Azure::Storage::StorageSharedKeyCredential>(storageAccountUrl, storageAccountKey),
            BlobHelpers::CreateBlobClientOptions()
        };

        auto containerClient = BlobHelpers::GetContainerClient(serviceClient, name);
        const auto uniquePrefix = StorageAccount::UniquePrefix(storageAccountUrl, name);
        if (cachePath)
        {
            m_fileCaches.emplace(uniquePrefix,
                std::make_shared<Core::FileCache>(*cachePath,
                    maxCacheSize,
                    std::make_shared<AzureContainerClient>(containerClient),
                    std::make_shared<Core::LocalFilesystem>(m_logger),
                    m_logger));
        }

        m_clients.emplace(uniquePrefix, ServiceContainer{ std::move(serviceClient), std::move(containerClient) });
        m_openMode = openMode;
    }

    BlobFilesystemImpl::BlobFilesystemImpl(const std::string& name,
        const std::string& storageAccountUrl,
        const std::string& servicePrincipalId,
        const std::string& servicePrincipalSecret,
        const std::string& tenantId,
        int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath,
        size_t maxCacheSize,
        GraphDb::Storage::OpenMode openMode)
        : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize)
    {
        ::Azure::Storage::Blobs::BlobServiceClient serviceClient
        {
            storageAccountUrl,
            std::make_shared<::Azure::Identity::ClientSecretCredential>(tenantId, servicePrincipalId, servicePrincipalSecret, BlobHelpers::CreateClientSecretCredentialOptions()),
            BlobHelpers::CreateBlobClientOptions()
        };

        auto containerClient = BlobHelpers::GetContainerClient(serviceClient, name);
        const auto uniquePrefix = StorageAccount::UniquePrefix(storageAccountUrl, name);
        if (cachePath)
        {
            m_fileCaches.emplace(uniquePrefix,
                std::make_shared<Core::FileCache>(*cachePath,
                    maxCacheSize,
                    std::make_shared<AzureContainerClient>(containerClient),
                    std::make_shared<Core::LocalFilesystem>(m_logger),
                    m_logger));
        }

        m_clients.emplace(uniquePrefix, ServiceContainer{ std::move(serviceClient), std::move(containerClient) });
        m_openMode = openMode;
    }

    BlobFilesystemImpl::BlobFilesystemImpl(const std::string& name,
        const std::string& storageAccountUrl,
        const std::string& tenantId,
        const std::string& clientId,
        const std::string& serviceConnectionId,
        const std::string& accessToken,
        int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath,
        size_t maxCacheSize,
        GraphDb::Storage::OpenMode openMode)
        : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize)
    {
        ::Azure::Storage::Blobs::BlobServiceClient serviceClient
        {
            storageAccountUrl,
            std::make_shared<::Azure::Identity::AzurePipelinesCredential>(tenantId, clientId, serviceConnectionId, accessToken, BlobHelpers::CreatePipelinesCredentialOptions()),
            BlobHelpers::CreateBlobClientOptions()
        };

        auto containerClient = BlobHelpers::GetContainerClient(serviceClient, name);
        const auto uniquePrefix = StorageAccount::UniquePrefix(storageAccountUrl, name);
        if (cachePath)
        {
            m_fileCaches.emplace(uniquePrefix,
                std::make_shared<Core::FileCache>(*cachePath,
                    maxCacheSize,
                    std::make_shared<AzureContainerClient>(containerClient),
                    std::make_shared<Core::LocalFilesystem>(m_logger),
                    m_logger));
        }

        m_clients.emplace(uniquePrefix, ServiceContainer{ std::move(serviceClient), std::move(containerClient) });
        m_openMode = openMode;
    }

    BlobFilesystemImpl::BlobFilesystemImpl(Models::ChainedCredentialInfo primary,
        std::optional<Models::ChainedCredentialInfo> backup,
        int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath,
        size_t maxCacheSize,
        GraphDb::Storage::OpenMode openMode)
        : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize)
    {
        auto serviceClient = BlobHelpers::CreateServiceClient(primary);
        auto containerClient = BlobHelpers::GetContainerClient(serviceClient, primary.GetDbName());
        const auto uniquePrefix = StorageAccount::UniquePrefix(primary.GetStorageAccountUrl(), primary.GetDbName());
        if (cachePath)
        {
            m_fileCaches.emplace(uniquePrefix,
                std::make_shared<Core::FileCache>(*cachePath,
                    maxCacheSize,
                    std::make_shared<AzureContainerClient>(containerClient),
                    std::make_shared<Core::LocalFilesystem>(m_logger),
                    m_logger));
        }

        m_clients.emplace(uniquePrefix, ServiceContainer{ std::move(serviceClient), std::move(containerClient) });

        if (backup)
        {
            auto backupServiceClient = BlobHelpers::CreateServiceClient(*backup);
            auto backupContainerClient = BlobHelpers::GetContainerClient(backupServiceClient, backup->GetDbName());
            m_clients.emplace(StorageAccount::UniquePrefix(backup->GetStorageAccountUrl(), backup->GetDbName()),
                ServiceContainer
                {
                    std::move(backupServiceClient),
                    std::move(backupContainerClient)
                });
        }
        m_openMode = openMode;
    }

    BlobFilesystemImpl::BlobFilesystemImpl(Models::ServicePrincipalStorageInfo primary,
        std::optional<Models::ServicePrincipalStorageInfo> backup,
        int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath,
        size_t maxCacheSize,
        GraphDb::Storage::OpenMode openMode)
        : BlobFilesystemImpl(std::move(logger), dataFileInitialSize, dataFileBufferSize)
    {
        auto serviceClient = BlobHelpers::CreateServiceClient(primary);
        auto containerClient = BlobHelpers::GetContainerClient(serviceClient, primary.GetDbName());
        const auto uniquePrefix = StorageAccount::UniquePrefix(primary.GetStorageAccountUrl(), primary.GetDbName());
        if (cachePath)
        {
            m_fileCaches.emplace(uniquePrefix,
                std::make_shared<Core::FileCache>(*cachePath,
                    maxCacheSize,
                    std::make_shared<AzureContainerClient>(containerClient),
                    std::make_shared<Core::LocalFilesystem>(m_logger),
                    m_logger));
        }

        m_clients.emplace(uniquePrefix, ServiceContainer{ std::move(serviceClient), std::move(containerClient) });

        if (backup)
        {
            auto backupServiceClient = BlobHelpers::CreateServiceClient(*backup);
            auto backupContainerClient = BlobHelpers::GetContainerClient(backupServiceClient, backup->GetDbName());
            m_clients.emplace(StorageAccount::UniquePrefix(backup->GetStorageAccountUrl(), backup->GetDbName()),
                ServiceContainer
                {
                    std::move(backupServiceClient),
                    std::move(backupContainerClient)
                });
        }
        m_openMode = openMode;
    }

    ReadableFileImpl BlobFilesystemImpl::CreateReadableFile(const std::string& filePath)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);
        auto pageBlobClient = container.GetPageBlobClient(std::string(realPath));
        auto blobClient = std::make_shared<PageBlob>(std::move(pageBlobClient));
        auto cache = m_fileCaches.find(prefix);
        if (cache != m_fileCaches.end())
        {
            return ReadableFileImpl{ realPath, std::move(blobClient), cache->second , m_openMode };
        }
        else
        {
            return ReadableFileImpl{ realPath, std::move(blobClient), nullptr, m_openMode };
        }
    }

    WriteableFileImpl BlobFilesystemImpl::CreateWriteableFile(const std::string& filePath)
    {
        const auto fileType = Core::RocksDBHelpers::GetFileType(filePath);
        const auto isData = fileType == Core::RocksDBHelpers::FileClass::WAL ||
            fileType == Core::RocksDBHelpers::FileClass::SST;
        const auto initialSize =
            isData ? m_dataFileInitialSize : Configuration::PageBlob::DefaultSize;
        const auto bufferSize =
            isData ? m_dataFileBufferSize : Configuration::PageBlob::DefaultBufferSize;

        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);

        auto client = container.GetPageBlobClient(std::string(realPath));
        auto res = client.CreateIfNotExists(initialSize);

        // Creating a writeable file is intended to always provide a "new" file.
        // If the file previously existed, efficiently truncate it so that for
        // all intents and purposes, it's a new file.
        if (!res.Value.Created)
        {
            BlobHelpers::SetFileSize(client, 0);
            if (client.GetProperties().Value.BlobSize > initialSize)
            {
                client.Resize(initialSize);
            }
        }

        auto cache = m_fileCaches.find(prefix);
        auto blobClient = std::make_unique<PageBlob>(std::move(client));
        if (cache != m_fileCaches.end())
        {
            return WriteableFileImpl{ realPath, std::move(blobClient), cache->second, m_logger, bufferSize };
        }
        else
        {
            return WriteableFileImpl{ realPath, std::move(blobClient), nullptr, m_logger, bufferSize };
        }
    }

    ReadWriteFileImpl BlobFilesystemImpl::CreateReadWriteFile(const std::string& filePath)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);

        auto client = std::make_shared<::Azure::Storage::Blobs::PageBlobClient>(container.GetPageBlobClient(std::string(realPath)));
        auto response = client->CreateIfNotExists(Configuration::PageBlob::DefaultSize);

        auto blobClient = std::make_shared<PageBlob>(std::move(*client));

        auto cache = m_fileCaches.find(prefix);
        if (cache != m_fileCaches.end())
        {
            return ReadWriteFileImpl{ realPath, std::move(blobClient), cache->second, m_logger };
        }
        else
        {
            return ReadWriteFileImpl{ realPath, std::move(blobClient), nullptr, m_logger };
        }
    }

    WriteableFileImpl BlobFilesystemImpl::ReopenWriteableFile(const std::string& filePath)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);
        const auto fileType = Core::RocksDBHelpers::GetFileType(filePath);
        const auto isData = fileType == Core::RocksDBHelpers::FileClass::WAL ||
            fileType == Core::RocksDBHelpers::FileClass::SST;
        const auto bufferSize =
            isData ? m_dataFileBufferSize : Configuration::PageBlob::DefaultBufferSize;

        auto client = std::make_shared<PageBlob>(container.GetPageBlobClient(std::string(realPath)));
        auto cache = m_fileCaches.find(prefix);
        if (cache != m_fileCaches.end())
        {
            return WriteableFileImpl{ realPath, std::move(client), cache->second, m_logger, static_cast<int64_t>(bufferSize) };
        }
        else
        {
            return WriteableFileImpl{ realPath, std::move(client), nullptr, m_logger, static_cast<int64_t>(bufferSize) };
        }
    }

    WriteableFileImpl BlobFilesystemImpl::ReuseWritableFile(const std::string& filePath)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);
        const auto fileType = Core::RocksDBHelpers::GetFileType(filePath);
        const auto isData = fileType == Core::RocksDBHelpers::FileClass::WAL ||
            fileType == Core::RocksDBHelpers::FileClass::SST;
        const auto initialSize = isData ? m_dataFileInitialSize : Configuration::PageBlob::DefaultSize;
        const auto bufferSize = isData ? m_dataFileBufferSize : Configuration::PageBlob::DefaultBufferSize;

        // TODO: figure out what the intent here is for now just delete and recreate
        auto client = container.GetPageBlobClient(std::string(realPath));
        client.DeleteIfExists();
        client.CreateIfNotExists(initialSize);

        auto cache = m_fileCaches.find(prefix);
        auto blobClient = std::make_shared<PageBlob>(std::move(client));
        if (cache != m_fileCaches.end())
        {
            return WriteableFileImpl{ realPath, std::move(blobClient), cache->second, m_logger, static_cast<int64_t>(bufferSize) };
        }
        else
        {
            return WriteableFileImpl{ realPath, std::move(blobClient), nullptr, m_logger, static_cast<int64_t>(bufferSize) };
        }
    }

    LoggerImpl BlobFilesystemImpl::CreateLogger(const std::string& filePath, const int logLevel)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);

        auto client = container.GetPageBlobClient(std::string(realPath));
        client.CreateIfNotExists(Configuration::PageBlob::DefaultSize);

        auto blobClient = std::make_shared<PageBlob>(std::move(client));
        auto impl = std::make_unique<WriteableFileImpl>(realPath, std::move(blobClient), nullptr, m_logger, Configuration::PageBlob::DefaultSize);
        return LoggerImpl{ std::move(impl), logLevel };
    }

    std::shared_ptr<LockFileImpl> BlobFilesystemImpl::LockFile(const std::string& filePath)
    {
        std::scoped_lock _(m_lockFilesMutex);

        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);

        auto client = std::make_unique<::Azure::Storage::Blobs::PageBlobClient>(container.GetPageBlobClient(std::string(realPath)));
        client->CreateIfNotExists(Configuration::PageBlob::DefaultSize);
        auto lockFile = std::make_shared<LockFileImpl>(std::move(client));
        m_locks.push_back(lockFile);
        if (lockFile->Lock())
        {
            return lockFile;
        }
        else
        {
            throw std::runtime_error("The targeted storage location is locked");
        }
    }

    bool BlobFilesystemImpl::UnlockFile(const LockFileImpl& lock)
    {
        std::scoped_lock _(m_lockFilesMutex);
        const auto it = std::find_if(m_locks.cbegin(),
            m_locks.cend(),
            [&lock](const auto& f)
            {
                return f.get() == &lock;
            });

        if (it != m_locks.cend())
        {
            (*it)->Unlock();
            m_locks.erase(it);
            return true;
        }
        else
        {
            return false;
        }
    }

    DirectoryImpl BlobFilesystemImpl::CreateDirectory(const std::string& directoryPath)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
        return DirectoryImpl{ GetContainer(prefix), realPath };
    }

    bool BlobFilesystemImpl::FileExists(const std::string& name)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(name);
        const auto& container = GetContainer(prefix);

        try
        {
            auto client = container.GetPageBlobClient(std::string(realPath));
            auto props = client.GetProperties();
            return true;
        }
        catch (const ::Azure::Storage::StorageException& ex)
        {
            if (ex.StatusCode == ::Azure::Core::Http::HttpStatusCode::NotFound)
            {
                // Fallback: check if this is a directory
                // NOTE: This doesn't map 100% to how a filesystem would work because you can have empty
                // directories in any respectable fs. This probably won't matter for our use case.
                if (GetChildren(name, 1).size() > 0)
                {
                    return true;
                }

                return false;
            }
            else
            {
                throw;
            }
        }
    }

    std::vector<std::string> BlobFilesystemImpl::GetChildren(const std::string& directoryPath, int32_t sizeHint)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
        const auto& container = GetContainer(prefix);

        std::vector<std::string> children;
        ::Azure::Storage::Blobs::ListBlobsOptions opts;
        opts.Prefix = realPath;
        opts.PageSizeHint = sizeHint;
        auto extractChildName = [&realPath](const auto& blob) -> std::string
            {
                size_t startPos = blob.Name.find(realPath);
                if (startPos != std::string::npos)
                {
                    auto index = startPos + realPath.length();
                    assert(index < blob.Name.size());

                    if (blob.Name[index] == '/')
                    {
                        index++;
                    }

                    return blob.Name.substr(index);
                }
                return {};
            };

        // Process all pages of results
        auto blobs = container.ListBlobs(opts);
        do
        {
            for (const auto& blob : blobs.Blobs)
            {
                if (auto childName = extractChildName(blob); !childName.empty())
                {
                    children.emplace_back(std::move(childName));
                }
            }

            if (!blobs.NextPageToken.HasValue())
            {
                break;
            }

            opts.ContinuationToken = blobs.NextPageToken;
            blobs = container.ListBlobs(opts);
        } while (true);

        return children;
    }

    std::vector<BlobAttributes> BlobFilesystemImpl::GetChildrenFileAttributes(const std::string& directoryPath)
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
        const auto& container = GetContainer(prefix);
        std::vector<BlobAttributes> attributes;

        ::Azure::Storage::Blobs::ListBlobsOptions opts;
        opts.Prefix = realPath;
        opts.PageSizeHint = 10000;  // hopefully plenty for now

        // Process all pages of results
        auto blobs = container.ListBlobs(opts);
        do
        {
            for (const auto& blob : blobs.Blobs)
            {
                const auto client = container.GetPageBlobClient(blob.Name);
                attributes.emplace_back(BlobHelpers::GetFileSize(client), blob.Name.substr(realPath.length()));
            }

            if (!blobs.NextPageToken.HasValue())
            {
                break;
            }

            opts.ContinuationToken = blobs.NextPageToken;
            blobs = container.ListBlobs(opts);
        } while (true);

        return attributes;
    }

    bool BlobFilesystemImpl::DeleteFile(const std::string& filePath) const
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);
        const auto client = container.GetPageBlobClient(std::string(realPath));
        const auto res = client.DeleteIfExists();

        auto cache = m_fileCaches.find(prefix);
        if (cache != m_fileCaches.end())
        {
            cache->second->RemoveFile(realPath);
        }

        return res.Value.Deleted;
    }

    size_t BlobFilesystemImpl::DeleteDir(const std::string& directoryPath) const
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
        const auto& container = GetContainer(prefix);

        ::Azure::Storage::Blobs::ListBlobsOptions options;
        // "" represent the root directory, so we would want to delete everything in the blob container.
        // append "/" in other cases in the event that we have a file with the same name as a directory.
        options.Prefix = realPath == "" ? realPath : std::string(realPath) + "/";

        auto blobsInDirectory = container.ListBlobs(options);
        std::vector<std::string> blobs;
        for (const auto& blob : blobsInDirectory.Blobs)
        {
            blobs.push_back(blob.Name);
        }

        // ListBlobs by default returns a maximum of 5000 blobs. Next page of blobs can be obtained with the NextPageToken
        options.ContinuationToken = blobsInDirectory.NextPageToken;
        while (blobsInDirectory.NextPageToken.HasValue())
        {
            blobsInDirectory = container.ListBlobs(options);
            for (const auto& blob : blobsInDirectory.Blobs)
            {
                blobs.push_back(blob.Name);
            }
            options.ContinuationToken = blobsInDirectory.NextPageToken;
        }

        // According to Blob Batch API maximum number of subrequests in a batch is 256.
        // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id#remarks
        const int maxBatchSize = 256;
        for (size_t i = 0; i < blobs.size(); i += maxBatchSize)
        {
            auto batch = container.CreateBatch();
            for (size_t j = i; j < i + maxBatchSize && j < blobs.size(); j++)
            {
                batch.DeleteBlob(blobs[j]);
            }
            container.SubmitBatch(batch);
        }

        options.ContinuationToken = "";

        // Listing blobs to ensure everything is deleted.
        // From testing the response from SubmitBatch doesn't appear to give any other status aside from a 202 Accepted.
        blobsInDirectory = container.ListBlobs(options);
        return blobsInDirectory.Blobs.size();
    }

    void BlobFilesystemImpl::Truncate(const std::string& filePath, int64_t size) const
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);

        const auto client = container.GetPageBlobClient(std::string(realPath));
        const auto fileSize = BlobHelpers::GetFileSize(client);
        if (fileSize > size)
        {
            BlobHelpers::SetFileSize(client, size);
            client.Resize(size);
        }
    }

    int64_t BlobFilesystemImpl::GetFileSize(const std::string& filePath) const
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);

        const auto client = container.GetPageBlobClient(std::string(realPath));
        return BlobHelpers::GetFileSize(client);
    }

    uint64_t BlobFilesystemImpl::GetFileModificationTime(const std::string& filePath) const
    {
        const auto [prefix, realPath] = StorageAccount::StripPrefix(filePath);
        const auto& container = GetContainer(prefix);

        const auto client = container.GetPageBlobClient(std::string(realPath));
        const auto props = client.GetProperties();
        const auto& modifiedTime = props.Value.LastModified;
        return static_cast<uint64_t>(::Azure::Core::_internal::PosixTimeConverter::DateTimeToPosixTime(modifiedTime));
    }

    size_t BlobFilesystemImpl::GetLeaseClientCount()
    {
        std::scoped_lock _(m_lockFilesMutex);
        return m_locks.size();
    }

    void BlobFilesystemImpl::RenameFile(const std::string& fromFilePath, const std::string& toFilePath) const
    {
        const auto [prefixAccountFrom, realPathFrom] = StorageAccount::StripPrefix(fromFilePath);
        const auto [prefixAccountTo, realPathTo] = StorageAccount::StripPrefix(toFilePath);
        if (prefixAccountFrom != prefixAccountTo)
        {
            throw std::runtime_error("Attempting to rename file into another storage account");
        }

        if (realPathFrom == realPathTo)
        {
            // Nothing to do - file is already at the destination
            return;
        }

        const auto& container = GetContainer(prefixAccountTo);

        const auto srcClient = container.GetPageBlobClient(std::string(realPathFrom));
        const auto destClient = container.GetPageBlobClient(std::string(realPathTo));

        // TODO: Check if there is already a file with this name
        const auto size = BlobHelpers::GetFileSize(srcClient);
        const auto cap = BlobHelpers::GetBlobCapacity(srcClient);
        destClient.CreateIfNotExists(static_cast<int64_t>(cap));
        ::Azure::Storage::Blobs::DownloadBlobOptions opt;
        opt.Range.Emplace(0, size);
        const auto downloadResponse = srcClient.Download(opt);
        const auto& content = downloadResponse.Value;

        static const constexpr auto maxUploadSize = static_cast<int64_t>(4) * 1024 * 1024;
        if (size > maxUploadSize)
        {
            int64_t uploadOffset = 0;
            std::vector<uint8_t> buffer(static_cast<size_t>(maxUploadSize));

            while (uploadOffset != size)
            {
                assert(size > uploadOffset);
                const auto readSize = std::min(size - uploadOffset, maxUploadSize);
                const auto bytesRead = content.BodyStream->ReadToCount(buffer.data(), static_cast<size_t>(readSize));
                const auto bytesRemaining = bytesRead % Configuration::PageBlob::PageSize;

                if (bytesRemaining != 0)
                {
                    const auto padding = Configuration::PageBlob::PageSize - bytesRemaining;

                    const auto endOfRealData = bytesRead;
                    const auto endOfUpload = bytesRead + padding;
                    std::fill(buffer.data() + endOfRealData, buffer.data() + endOfUpload, static_cast<uint8_t>(0));

                    ::Azure::Core::IO::MemoryBodyStream sendStream(buffer.data(), endOfUpload);
                    destClient.UploadPages(uploadOffset, sendStream);
                }
                else
                {
                    ::Azure::Core::IO::MemoryBodyStream sendStream(buffer.data(), bytesRead);
                    destClient.UploadPages(uploadOffset, sendStream);
                }

                uploadOffset += static_cast<int64_t>(bytesRead);
            }
        }
        else
        {
            auto buffer = content.BodyStream->ReadToEnd();

            // this must be aligned to page size so in some cases need dummy data
            const auto remaining = buffer.size() % Configuration::PageBlob::PageSize;
            if (remaining != 0)
            {
                const auto padding = Configuration::PageBlob::PageSize - remaining;
                buffer.resize(buffer.size() + padding);

                const auto endOfRealData = buffer.size() - padding;
                std::fill(buffer.data() + endOfRealData, buffer.data() + buffer.size(), static_cast<uint8_t>(0));
            }

            ::Azure::Core::IO::MemoryBodyStream sendStream(buffer);
            destClient.UploadPages(0, sendStream);
        }

        BlobHelpers::SetFileSize(destClient, size);
        srcClient.DeleteIfExists();
    }

    BlobFilesystemImpl::BlobFilesystemImpl(std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger, int64_t dataFileInitialSize, int64_t dataFileBufferSize)
        : m_logger(std::move(logger)),
        m_dataFileInitialSize(dataFileInitialSize),
        m_dataFileBufferSize(dataFileBufferSize),
        m_lockRenewalThread{ [this](std::stop_token stopToken) { RenewLease(stopToken); } }
    {
    }

    const::Azure::Storage::Blobs::BlobContainerClient& BlobFilesystemImpl::GetContainer(const std::string_view prefix) const
    {
        // TODO: Future work to determine health of this client. Seeing that repeated 503s/403s caused successive calls to fail with the same error.
        auto client = m_clients.find(prefix);
        if (client != m_clients.end())
        {
            return client->second.ContainerClient;
        }
        else
        {
            std::stringstream ss;
            ss << "Client not found for '" << prefix << "'";
            throw std::runtime_error(ss.str());
        }
    }

    void BlobFilesystemImpl::RenewLease(std::stop_token stopToken)
    {
        while (!stopToken.stop_requested())
        {
            {
                std::scoped_lock lock(m_lockFilesMutex);
                if (stopToken.stop_requested())
                {
                    break;
                }

                std::vector<LockFileImpl*> needsRetry;
                needsRetry.reserve(m_locks.size());
                for (const auto& lockPtr : m_locks)
                {
                    needsRetry.push_back(lockPtr.get());
                }

                int retries = 0;
                while (needsRetry.size() > 0 && retries < 5 && !stopToken.stop_requested())
                {
                    std::erase_if(needsRetry, [](const auto* client) -> bool
                        {
                            try
                            {
                                client->Renew();
                                return true;
                            }
                            catch (...)
                            {
                                return false;
                            }
                        });

                    retries++;
                }
            }

            // Sleep AFTER doing work - use VERY small increments for testing
            // 10000 iterations * 1ms = 10 seconds total
            for (int i = 0; i < 10000 && !stopToken.stop_requested(); ++i)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }
}
