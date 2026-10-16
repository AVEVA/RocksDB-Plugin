// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

// Part of BlobFilesystemImpl.ipp: included at its end, never on its own.
#pragma once

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// Deletes the blob and drops its cached copy so a later blob with the same name is never served stale bytes.
template <typename THttp> bool BasicBlobFilesystemImpl<THttp>::DeleteFile(const std::string& filePath) const {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(filePath);
    const BlobOperations::SizeInvalidation<decltype(client)> invalidateSize(*m_runtime, client);
    const auto res = CallResponse([&](auto token) { return client.DeleteIfExistsAsync(token); });

    auto cache = m_fileCaches.find(prefix);
    if (cache != m_fileCaches.end()) {
        cache->second->RemoveFile(realPath);
    }

    // A suppressed (not found) error means there was nothing to delete.
    return !res.Error().has_value();
}

// Removes every blob under the directory prefix. Object storage has no real directories, so the blobs are listed
// and deleted in batches to keep the request count low.
template <typename THttp> size_t BasicBlobFilesystemImpl<THttp>::DeleteDir(const std::string& directoryPath) const {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);
    const boost::scope::scope_exit invalidateSizes{[this] { m_runtime->InvalidateAllBlobSizes(); }};

    AzureClient::ListBlobsOptions options;
    options.Prefix = BlobFilesystemDetail::DirectoryPrefix(realPath);

    std::vector<std::string> blobs;
    BlobFilesystemDetail::ForEachBlob(
        *container, options, [&blobs](const AzureClient::Models::BlobItem& blob) { blobs.push_back(blob.Name); });

    if (!blobs.empty()) {
        // Batched deletes cut the request count by up to 256x. Every blob is attempted, then the first failure (if
        // any) is thrown so callers never mistake a partial delete for success.
        AzureClient::DeleteBlobsOptions deleteOptions;
        deleteOptions.MaxConcurrentRequests = Configuration::Transfer::MaxConcurrentDeleteBatches;
        const auto result = Call([&](auto token) { return container->DeleteBlobsAsync(blobs, deleteOptions, token); });

        std::optional<AzureClient::BlobStorageError> firstFailure;
        for (const auto& item : result.Items) {
            if (item.Error) {
                BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                    << "Failed to delete blob '" << item.Name << "': " << item.Error->Message;
                if (!firstFailure) {
                    firstFailure = item.Error;
                }
            }
        }
        if (firstFailure) {
            ThrowRequestFailed(*firstFailure);
        }
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
    BlobOperations::SetCapacity(*m_runtime, client, alignedSize);
    {
        boost::scope::scope_fail warn{[&] {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                << "'" << realPath << "' was resized to " << alignedSize
                << " bytes but its logical size could not be set to " << size
                << "; its recorded size may exceed its capacity until the truncate is retried";
        }};
        BlobOperations::SetSize(*m_runtime, client, size);
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
