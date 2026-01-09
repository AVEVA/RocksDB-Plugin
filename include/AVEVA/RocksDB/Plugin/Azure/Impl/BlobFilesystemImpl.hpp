// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobAttributes.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"

#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <boost/log/trivial.hpp>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <optional>
namespace AVEVA::RocksDB::Plugin::Azure::Impl
{
    class BlobFilesystemImpl
    {
        struct ServiceContainer
        {
            ServiceContainer(::Azure::Storage::Blobs::BlobServiceClient service,
                ::Azure::Storage::Blobs::BlobContainerClient container)
                : ServiceClient(std::move(service)), ContainerClient(std::move(container))
            {
            }

            ::Azure::Storage::Blobs::BlobServiceClient ServiceClient;
            ::Azure::Storage::Blobs::BlobContainerClient ContainerClient;
        };

        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
        int64_t m_dataFileInitialSize;
        int64_t m_dataFileBufferSize;
        std::unordered_map<std::string, ServiceContainer, Core::StringHash, Core::StringEqual> m_clients;
        std::unordered_map<std::string, std::shared_ptr<Core::FileCache>, Core::StringHash, Core::StringEqual> m_fileCaches;
        std::mutex m_lockFilesMutex;
        std::vector<std::shared_ptr<LockFileImpl>> m_locks;
        std::jthread m_lockRenewalThread;

    public:
        BlobFilesystemImpl(const std::string& name,
            const std::string& storageAccountUrl,
            const std::string& storageAccountKey,
            int64_t dataFileInitialSize,
            int64_t dataFileBufferSize,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            std::optional<std::string_view> cachePath = {},
            size_t maxCacheSize = Configuration::MaxCacheSize);
        BlobFilesystemImpl(const std::string& name,
            const std::string& storageAccountUrl,
            const std::string& servicePrincipalId,
            const std::string& servicePrincipalSecret,
            const std::string& tenantId,
            int64_t dataFileInitialSize,
            int64_t dataFileBufferSize,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            std::optional<std::string_view> cachePath = {},
            size_t maxCacheSize = Configuration::MaxCacheSize);
        BlobFilesystemImpl(const std::string& name,
            const std::string& storageAccountUrl,
            const std::string& tenantId,
            const std::string& clientId,
            const std::string& serviceConnectionId,
            const std::string& accessToken,
            int64_t dataFileInitialSize,
            int64_t dataFileBufferSize,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            std::optional<std::string_view> cachePath = {},
            size_t maxCacheSize = Configuration::MaxCacheSize);
        BlobFilesystemImpl(Models::ChainedCredentialInfo primary,
            std::optional<Models::ChainedCredentialInfo> backup,
            int64_t dataFileInitialSize,
            int64_t dataFileBufferSize,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            std::optional<std::string_view> cachePath = {},
            size_t maxCacheSize = Configuration::MaxCacheSize);
        BlobFilesystemImpl(Models::ServicePrincipalStorageInfo primary,
            std::optional<Models::ServicePrincipalStorageInfo> backup,
            int64_t dataFileInitialSize,
            int64_t dataFileBufferSize,
            std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
            std::optional<std::string_view> cachePath = {},
            size_t maxCacheSize = Configuration::MaxCacheSize);

        [[nodiscard]] ReadableFileImpl CreateReadableFile(const std::string& filePath);
        [[nodiscard]] WriteableFileImpl CreateWriteableFile(const std::string& filePath);
        [[nodiscard]] ReadWriteFileImpl CreateReadWriteFile(const std::string& filePath);
        [[nodiscard]] WriteableFileImpl ReopenWriteableFile(const std::string& filePath);
        [[nodiscard]] WriteableFileImpl ReuseWritableFile(const std::string& filePath);
        LoggerImpl CreateLogger(const std::string& filePath, int logLevel);
        std::shared_ptr<LockFileImpl> LockFile(const std::string& filePath);
        bool UnlockFile(const LockFileImpl& lock);
        DirectoryImpl CreateDirectory(const std::string& directoryPath);

        [[nodiscard]] bool FileExists(const std::string& name);
        std::vector<std::string> GetChildren(const std::string& directoryPath, int32_t sizeHint = 10000 /* hopefully plenty for now */);
        std::vector<BlobAttributes> GetChildrenFileAttributes(const std::string& directoryPath);
        [[nodiscard]] bool DeleteFile(const std::string& filePath) const;
        [[nodiscard]] size_t DeleteDir(const std::string& directoryPath) const;
        void Truncate(const std::string& filePath, int64_t size) const;
        [[nodiscard]] int64_t GetFileSize(const std::string& filePath) const;
        [[nodiscard]] uint64_t GetFileModificationTime(const std::string& filePath) const;
        size_t GetLeaseClientCount();
        void RenameFile(const std::string& fromFilePath, const std::string& toFilePath) const;
    private:
        BlobFilesystemImpl(std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger, int64_t dataFileInitialSize = 0, int64_t dataFileBufferSize = 0);
        [[nodiscard]] const ::Azure::Storage::Blobs::BlobContainerClient& GetContainer(std::string_view prefix) const;
        void RenewLease(std::stop_token stopToken);
    };
}
