// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/Configuration.hpp"
#include "AVEVA/RocksDB/Plugin/Core/FileCache.hpp"

#include <boost/log/trivial.hpp>

#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class WriteableFileImpl {
    std::string m_name;
    int64_t m_bufferSize;
    std::shared_ptr<Core::BlobClient> m_blobClient;
    std::shared_ptr<Core::FileCache> m_fileCache;
    std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> m_logger;

    int64_t m_lastPageOffset;
    int64_t m_size;
    int64_t m_capacity;
    int64_t m_bufferOffset;
    bool m_closed;
    bool m_flushed;
    // Set while data has been appended that the blob's size metadata does not yet reflect.
    bool m_unsyncedSize = false;
    std::vector<char> m_buffer;

    // Shared with in-flight upload completions, which run on the host io_context and may outlive a move of this file.
    struct UploadTracker {
        std::mutex Mutex;
        std::condition_variable Done;
        size_t InFlight = 0;
        std::exception_ptr Error;
    };
    std::shared_ptr<UploadTracker> m_uploads = std::make_shared<UploadTracker>();

    // Upper bound on concurrent page uploads per file; bounds memory to this many buffer copies.
    static constexpr size_t MaxInFlightUploads = 4;

  public:
    // Size and capacity of the blob when the caller already knows them, which saves two GetProperties round trips.
    struct BlobState {
        int64_t Size;
        int64_t Capacity;
    };

    WriteableFileImpl(
        std::string_view name, std::shared_ptr<Core::BlobClient> blobClient, std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        int64_t bufferSize = Configuration::PageBlob::DefaultBufferSize);
    WriteableFileImpl(
        std::string_view name, std::shared_ptr<Core::BlobClient> blobClient, std::shared_ptr<Core::FileCache> fileCache,
        std::shared_ptr<boost::log::sources::severity_logger_mt<boost::log::trivial::severity_level>> logger,
        int64_t bufferSize, BlobState knownState);
    ~WriteableFileImpl();
    WriteableFileImpl(const WriteableFileImpl&) = delete;
    WriteableFileImpl& operator=(const WriteableFileImpl&) = delete;
    WriteableFileImpl(WriteableFileImpl&&) noexcept;
    WriteableFileImpl& operator=(WriteableFileImpl&&) noexcept;

    void Close();
    void Append(const std::span<const char> data);
    void Flush();
    // Starts uploading the buffered pages without waiting for them (see WriteableFile::RangeSync).
    void RangeSync();
    void Sync();
    void Truncate(int64_t size);
    [[nodiscard]] int64_t GetFileSize() const noexcept;
    [[nodiscard]] int64_t GetUniqueId(char* id, int64_t maxIdSize) const noexcept;

  private:
    void Expand(int64_t requiredCapacity);
    void StartFlush(bool includePartialPage);
    void StartUpload(std::vector<char> data, int64_t offset);
    void WaitForUploads(size_t maxRemaining);
    [[nodiscard]] bool HasUploadError() const noexcept;
    void DrainUploads() noexcept;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
