// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

// Part of BlobFilesystemImpl.ipp: included at its end, never on its own.
#pragma once

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
    BlobOperations::CreateOrReplace(*m_runtime, blob, initialSize);
    const int64_t capacity = initialSize;

    return WriteableFileType{realPath,
                             m_runtime,
                             std::make_shared<PageBlobClientOn<THttp>>(std::move(blob)),
                             BlobFilesystemDetail::FindCache(m_fileCaches, prefix),
                             m_logger,
                             bufferSize,
                             typename WriteableFileType::BlobState{0, capacity}};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::ReadWriteFileType
BasicBlobFilesystemImpl<THttp>::CreateReadWriteFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(*m_runtime, blob, Configuration::PageBlob::DefaultSize);

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
    const auto created = BlobOperations::CreateIfNotExists(*m_runtime, blob, initialSize);

    auto client = std::make_shared<PageBlobClientOn<THttp>>(std::move(blob));
    auto cache = BlobFilesystemDetail::FindCache(m_fileCaches, prefix);
    if (created) {
        // A blob this call just created is empty with a known capacity, so there is nothing to look up.
        return WriteableFileType{realPath,
                                 m_runtime,
                                 std::move(client),
                                 std::move(cache),
                                 m_logger,
                                 bufferSize,
                                 typename WriteableFileType::BlobState{0, initialSize}};
    }
    return WriteableFileType{realPath, m_runtime, std::move(client), std::move(cache), m_logger, bufferSize};
}

template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::WriteableFileType
BasicBlobFilesystemImpl<THttp>::ReuseWritableFile(const std::string& filePath) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);
    const auto [initialSize, bufferSize] =
        BlobFilesystemDetail::SizingFor(filePath, m_dataFileInitialSize, m_dataFileBufferSize);

    BlobOperations::CreateOrReplace(*m_runtime, blob, initialSize);

    return WriteableFileType{realPath,
                             m_runtime,
                             std::make_shared<PageBlobClientOn<THttp>>(std::move(blob)),
                             BlobFilesystemDetail::FindCache(m_fileCaches, prefix),
                             m_logger,
                             bufferSize,
                             typename WriteableFileType::BlobState{0, initialSize}};
}

template <typename THttp>
LoggerImpl BasicBlobFilesystemImpl<THttp>::CreateLogger(const std::string& filePath, const int logLevel,
                                                        const std::chrono::seconds cooldown) {
    EnsureLiveness();

    auto [prefix, realPath, blob] = ResolveBlob(filePath);

    BlobOperations::CreateIfNotExists(*m_runtime, blob, Configuration::PageBlob::DefaultSize);

    auto impl = std::make_unique<FileLogSink<WriteableFileType>>(std::make_unique<WriteableFileType>(
        realPath, m_runtime, std::make_shared<PageBlobClientOn<THttp>>(std::move(blob)), nullptr, m_logger,
        Configuration::PageBlob::DefaultBufferSize));
    return LoggerImpl{
        std::move(impl), logLevel,
        std::make_unique<LogRateLimiter>(
            std::vector<std::string>{"Stalling writes because we have", "Stopping writes because we have"}, cooldown)};
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
