// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/AzureContainerClient.hpp"
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

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"
#include <boost/log/trivial.hpp>

#include <chrono>
#include <concepts>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <source_location>
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
/// <summary>
/// Returns why the data-file sizes cannot be used, or an empty view when they are valid. Page blobs are written in
/// 512-byte pages, so the buffer size must be a positive multiple of 512; the initial size only has to be
/// non-negative.
/// </summary>
[[nodiscard]] constexpr std::string_view InvalidDataFileSizes(int64_t dataFileInitialSize,
                                                              int64_t dataFileBufferSize) noexcept {
    if (dataFileBufferSize <= 0 || dataFileBufferSize % 512 != 0) {
        return "dataFileBufferSize must be a positive multiple of 512";
    }
    if (dataFileInitialSize < 0) {
        return "dataFileInitialSize cannot be negative";
    }
    return {};
}

// Validates in a member initializer, so a bad value throws before the lease-renewal worker is started.
[[nodiscard]] inline int64_t RequireValidDataFileSizes(int64_t dataFileInitialSize, int64_t dataFileBufferSize) {
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
    using Directory = BasicDirectoryImpl<BlobContainerClientOn<THttp>>;
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
    int64_t m_dataFileInitialSize;
    int64_t m_dataFileBufferSize;
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
    int64_t m_maxPrefetchBytes = Configuration::DefaultPrefetchBudgetBytes;
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
    [[nodiscard]] std::optional<std::chrono::steady_clock::duration> EarliestRenewalTimeRemaining() const;

  public:
    // Every io_context constructor takes the host-owned io_context that all Azure I/O runs on. The filesystem never
    // runs, stops or destroys it; the host must keep it alive and running for the filesystem's whole lifetime and must
    // not call into the filesystem from the threads running it.
    BasicBlobFilesystemImpl(
        boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
        const std::string& storageAccountKey, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BasicBlobFilesystemImpl(
        boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
        const std::string& servicePrincipalId, const std::string& servicePrincipalSecret, const std::string& tenantId,
        int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BasicBlobFilesystemImpl(
        boost::asio::io_context& ioContext, const std::string& name, const std::string& storageAccountUrl,
        const std::string& tenantId, const std::string& clientId, const std::string& serviceConnectionId,
        const std::string& accessToken, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BasicBlobFilesystemImpl(
        boost::asio::io_context& ioContext, Models::ChainedCredentialInfo primary,
        std::optional<Models::ChainedCredentialInfo> backup, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);
    BasicBlobFilesystemImpl(
        boost::asio::io_context& ioContext, Models::ServicePrincipalStorageInfo primary,
        std::optional<Models::ServicePrincipalStorageInfo> backup, int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize);

    // Builds the filesystem on an already-constructed runtime (and therefore any HTTP client). Used with shared-key
    // credentials. The tests pass a runtime that owns an in-memory fake and a short lock timeout, so that contention
    // does not take the production 20 seconds to give up.
    BasicBlobFilesystemImpl(
        std::shared_ptr<BasicClientRuntime<THttp>> runtime, const std::string& name,
        const std::string& storageAccountUrl, const std::string& storageAccountKey, int64_t dataFileInitialSize,
        int64_t dataFileBufferSize,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        std::optional<std::string_view> cachePath = {}, size_t maxCacheSize = Configuration::MaxCacheSize,
        std::optional<std::chrono::milliseconds> lockAcquireTimeout = {});

    // Stops lease renewal, then blocks until reads abandoned by RocksDB mid-flight have completed, so that their
    // completions never release the HTTP client or a file cache. Requires the host io_context to still be running.
    ~BasicBlobFilesystemImpl();
    BasicBlobFilesystemImpl(const BasicBlobFilesystemImpl&) = delete;
    BasicBlobFilesystemImpl& operator=(const BasicBlobFilesystemImpl&) = delete;
    BasicBlobFilesystemImpl(BasicBlobFilesystemImpl&&) = delete;
    BasicBlobFilesystemImpl& operator=(BasicBlobFilesystemImpl&&) = delete;

    [[nodiscard]] ReadableFileType CreateReadableFile(const std::string& filePath);
    // Caps the bytes held by prefetch buffers across all files opened afterwards; 0 declines every prefetch.
    void SetMaxPrefetchBytes(int64_t maxPrefetchBytes) { m_maxPrefetchBytes = maxPrefetchBytes; }
    [[nodiscard]] SequentialFileType CreateSequentialFile(const std::string& filePath);
    [[nodiscard]] WriteableFileType CreateWriteableFile(const std::string& filePath);
    [[nodiscard]] ReadWriteFileType CreateReadWriteFile(const std::string& filePath);
    [[nodiscard]] WriteableFileType ReopenWriteableFile(const std::string& filePath);
    [[nodiscard]] WriteableFileType ReuseWritableFile(const std::string& filePath);
    LoggerImpl CreateLogger(const std::string& filePath, int logLevel,
                            std::chrono::seconds cooldown = Configuration::LogRateLimiterCooldown);
    std::shared_ptr<LockFileType> LockFile(const std::string& filePath);
    void UnlockFile(LockFileType& lock);
    DirectoryType CreateDirectory(const std::string& directoryPath);

    [[nodiscard]] bool FileExists(const std::string& name);
    // True when at least one blob lives below the directory; stops at the first one rather than listing every page.
    [[nodiscard]] bool HasChildren(const std::string& directoryPath);
    std::vector<std::string> GetChildren(const std::string& directoryPath,
                                         int32_t sizeHint = 10000 /* hopefully plenty for now */);
    std::vector<BlobAttributes> GetChildrenFileAttributes(const std::string& directoryPath);
    [[nodiscard]] bool DeleteFile(const std::string& filePath) const;
    // Returns 0 when nothing is left under the directory; otherwise a non-zero value (not an exact count).
    [[nodiscard]] size_t DeleteDir(const std::string& directoryPath) const;
    // Shrinks the file; throws std::invalid_argument when `size` exceeds the current size (extending is unsupported).
    void Truncate(const std::string& filePath, int64_t size) const;
    [[nodiscard]] int64_t GetFileSize(const std::string& filePath) const;
    [[nodiscard]] uint64_t GetFileModificationTime(const std::string& filePath) const;
    size_t GetLeaseClientCount();
    // Test-only: blocks until every file cache's background downloader is idle, or `timeout` (a guard against hangs)
    // elapses; returns false on timeout.
    [[nodiscard]] bool WaitForFileCachesIdleForTesting(std::chrono::milliseconds timeout) const;
    void RenameFile(const std::string& fromFilePath, const std::string& toFilePath) const;

  private:
    BasicBlobFilesystemImpl(
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>>&& logger,
        std::shared_ptr<BasicClientRuntime<THttp>> runtime, int64_t dataFileInitialSize, int64_t dataFileBufferSize,
        std::optional<std::chrono::milliseconds> lockAcquireTimeout = {});
    void AddContainer(const BlobServiceClientOn<THttp>& serviceClient, const std::string& storageAccountUrl,
                      const std::string& name, std::optional<std::string_view> cachePath, size_t maxCacheSize);
    // Deletes the temporary blobs a rename left behind when it crashed or could not clean up after itself. Only safe
    // while holding the container's DB lock, since the lock holder is the only process that renames.
    void PurgeOrphanedRenameTemps(std::string_view prefix) const;
    template <class TInfo>
    void AddContainers(const TInfo& primary, const std::optional<TInfo>& backup,
                       std::optional<std::string_view> cachePath, size_t maxCacheSize);
    [[nodiscard]] const std::shared_ptr<BlobContainerClientOn<THttp>>& GetContainer(std::string_view prefix) const;
    // A path split into its storage-account prefix and blob name, with the blob client. The views point into the path
    // passed to ResolveBlob.
    struct ResolvedBlob {
        std::string_view Prefix;
        std::string_view RealPath;
        PageBlobClientOn<THttp> Blob;
    };
    [[nodiscard]] ResolvedBlob ResolveBlob(const std::string& path) const;
    void EnsureLiveness(std::source_location location = std::source_location::current()) const;
};
extern template class BasicBlobFilesystemImpl<::AVEVA::HttpClient>;

/// <summary>The production filesystem, over the real HTTP client. Non-template so it can be forward declared.</summary>
class BlobFilesystemImpl : public BasicBlobFilesystemImpl<::AVEVA::HttpClient> {
  public:
    using BasicBlobFilesystemImpl<::AVEVA::HttpClient>::BasicBlobFilesystemImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
