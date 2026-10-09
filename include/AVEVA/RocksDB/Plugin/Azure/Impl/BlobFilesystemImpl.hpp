// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/BlobAttributes.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LeaseRenewalLoop.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LockFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/LoggerImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadWriteFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ReadableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/SequentialFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Impl/WriteableFileImpl.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ChainedCredentialInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Azure/Models/ServicePrincipalStorageInfo.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"
#include "AVEVA/RocksDB/Plugin/Core/Util.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <boost/log/trivial.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <source_location>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
// Boost.Asio pulls in <windows.h>, whose macros clash with RocksDB method names (mirrors rocksdb/env.h).
#undef DeleteFile
#undef GetCurrentTime
#undef GetFreeSpace
#undef LoadLibrary
#endif
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class BlobFilesystemImpl {
    struct ServiceContainer {
        ServiceContainer(AzureClient::BlobServiceClient service,
                         std::shared_ptr<AzureClient::BlobContainerClient> container)
            : ServiceClient(std::move(service)), ContainerClient(std::move(container)) {}

        AzureClient::BlobServiceClient ServiceClient;
        std::shared_ptr<AzureClient::BlobContainerClient> ContainerClient;
    };

    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;
    int64_t m_dataFileInitialSize;
    int64_t m_dataFileBufferSize;
    // HTTP client (on the injected, host-owned io_context) used by every Azure client below; declared before them
    // so it outlives them during destruction.
    std::shared_ptr<ClientRuntime> m_runtime;
    std::unordered_map<std::string, ServiceContainer, Core::StringHash, Core::StringEqual> m_clients;
    std::unordered_map<std::string, std::shared_ptr<Core::FileCache>, Core::StringHash, Core::StringEqual> m_fileCaches;
    // Async reads still running on the io_context; drained by the destructor before the runtime and caches above
    // are released (see AsyncReadTracker).
    std::shared_ptr<AsyncReadTracker> m_asyncReads;
    // Bytes held by per-file prefetch buffers across all open files; caps Prefetch memory.
    std::shared_ptr<std::atomic<int64_t>> m_prefetchBytes = std::make_shared<std::atomic<int64_t>>(0);
    std::mutex m_lockFilesMutex;
    boost::intrusive::list<LockFileImpl, boost::intrusive::constant_time_size<false>> m_locks;
    // Parallel to m_locks; lets the renewal loop keep locks alive while it renews outside m_lockFilesMutex.
    std::vector<std::weak_ptr<LockFileImpl>> m_renewableLocks;
    std::stop_source m_filesystemStopSource;
    // Declared last: Stop()ped first thing in the destructor, before anything its callbacks use is released.
    std::shared_ptr<LeaseRenewalLoop> m_leaseRenewal;

  public:
    // Every constructor takes the host-owned io_context that all Azure I/O runs on. The filesystem never runs,
    // stops or destroys it; the host must keep it alive and running for the filesystem's whole lifetime and must
    // not call into the filesystem from the threads running it.
    BlobFilesystemImpl(
        boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
        const std::string& storageAccountKey, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BlobFilesystemImpl(
        boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
        const std::string& servicePrincipalId, const std::string& servicePrincipalSecret, const std::string& tenantId,
        int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BlobFilesystemImpl(
        boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
        const std::string& tenantId, const std::string& clientId, const std::string& serviceConnectionId,
        const std::string& accessToken, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BlobFilesystemImpl(
        boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
        std::optional<Models::ChainedCredentialInfo> backup, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BlobFilesystemImpl(
        boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
        std::optional<Models::ServicePrincipalStorageInfo> backup, int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);

    // Stops lease renewal, then blocks until reads abandoned by RocksDB mid-flight have completed, so that their completions never release
    // the HTTP client or a file cache. Requires the host io_context to still be running.
    ~BlobFilesystemImpl();
    BlobFilesystemImpl(const BlobFilesystemImpl&) = delete;
    BlobFilesystemImpl& operator=(const BlobFilesystemImpl&) = delete;
    BlobFilesystemImpl(BlobFilesystemImpl&&) = delete;
    BlobFilesystemImpl& operator=(BlobFilesystemImpl&&) = delete;

    [[nodiscard]] ReadableFileImpl CreateReadableFile(const std::string& filePath);
    [[nodiscard]] SequentialFileImpl CreateSequentialFile(const std::string& filePath);
    [[nodiscard]] WriteableFileImpl CreateWriteableFile(const std::string& filePath);
    [[nodiscard]] ReadWriteFileImpl CreateReadWriteFile(const std::string& filePath);
    [[nodiscard]] WriteableFileImpl ReopenWriteableFile(const std::string& filePath);
    [[nodiscard]] WriteableFileImpl ReuseWritableFile(const std::string& filePath);
    LoggerImpl CreateLogger(const std::string& filePath, int logLevel,
                            std::chrono::seconds cooldown = Configuration::LogRateLimiterCooldown);
    std::shared_ptr<LockFileImpl> LockFile(const std::string& filePath);
    void UnlockFile(LockFileImpl& lock);
    DirectoryImpl CreateDirectory(const std::string& directoryPath);

    [[nodiscard]] bool FileExists(const std::string& name);
    std::vector<std::string> GetChildren(const std::string& directoryPath,
                                         int32_t sizeHint = 10000 /* hopefully plenty for now */);
    std::vector<BlobAttributes> GetChildrenFileAttributes(const std::string& directoryPath);
    [[nodiscard]] bool DeleteFile(const std::string& filePath) const;
    [[nodiscard]] size_t DeleteDir(const std::string& directoryPath) const;
    void Truncate(const std::string& filePath, int64_t size) const;
    [[nodiscard]] int64_t GetFileSize(const std::string& filePath) const;
    [[nodiscard]] uint64_t GetFileModificationTime(const std::string& filePath) const;
    size_t GetLeaseClientCount();
    void RenameFile(const std::string& fromFilePath, const std::string& toFilePath) const;

  private:
    BlobFilesystemImpl(
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger,
        boost::asio::io_context& ioContext, int64_t dataFileInitialSize, int64_t dataFileBufferSize);
    void AddContainer(AzureClient::BlobServiceClient serviceClient, const std::string& storageAccountUrl,
                      const std::string& name, std::optional<std::string_view> cachePath, size_t maxCacheSize);
    [[nodiscard]] const std::shared_ptr<AzureClient::BlobContainerClient>& GetContainer(std::string_view prefix) const;
    void EnsureLiveness(std::source_location location = std::source_location::current()) const;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
