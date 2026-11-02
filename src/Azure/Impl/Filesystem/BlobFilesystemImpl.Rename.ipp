// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

// Part of BlobFilesystemImpl.ipp: included at its end, never on its own.
#pragma once

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
/// <summary>
/// Copies the blob to its new name and carries over the logical file size, then deletes the source. Both paths must
/// live in the same storage account and container. A file of at most Transfer::MaxInlineRenameSize is downloaded
/// once and uploaded to the destination (see below); a larger one is copied inside the storage service (or, when the
/// service cannot authorize the source, in page-aligned chunks through this process).
///
/// Overwriting a small destination empties it and then writes it, so a crash in that short window leaves an empty
/// destination while the source is intact (BlobFilesystem rejects an empty CURRENT explicitly) and the rename can be
/// retried. Temporary blobs left by a crashed large rename are removed when this process next acquires the DB lock.
///
/// For a large file the storage service has no atomic blob replace, so an existing destination is only overwritten
/// after the
/// content has been fully copied to a temporary blob: a failure while copying leaves the destination and the
/// source untouched. The final commit (copy from the temporary blob over the destination) is not atomic, but it is
/// ordered so a torn commit is never mistaken for data: the destination is re-created empty with a logical size of
/// 0 (one request that also drops the old content), its pages are copied, and the real logical size is written
/// last. A failure in between therefore leaves an empty destination (BlobFilesystem rejects an empty CURRENT
/// explicitly) while the source is still intact, so the rename can be retried.
/// A destination that does not exist is written directly and removed again on failure.
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

    WithContainer(prefixAccountTo, [&](auto& container) {
        auto srcClient = container.GetPageBlobClient(std::string(realPathFrom));
        auto destClient = container.GetPageBlobClient(std::string(realPathTo));
        // Copied, because the clients move into the rename below.
        const std::string srcUrl = srcClient.ResourceUrl();
        const std::string destUrl = destClient.ResourceUrl();
        // Whatever happens below, a cached copy of either name may no longer match its blob: a failed commit can
        // leave the destination emptied or partly written, and a failed source delete leaves both names holding data.
        boost::scope::scope_exit dropCached{[&] {
            m_runtime->MetadataCache().Invalidate(srcUrl);
            m_runtime->MetadataCache().Invalidate(destUrl);
            if (const auto cache = m_fileCaches.find(prefixAccountTo); cache != m_fileCaches.end()) {
                cache->second->RemoveFile(std::string(realPathFrom));
                cache->second->RemoveFile(std::string(realPathTo));
            }
        }};

        // One blocking call for the whole rename: the steps below are coroutines, so the sequencing reads top to
        // bottom.
        auto tempName = std::string(realPathTo) + std::string(BlobFilesystemDetail::RenameTempMarker) +
                        boost::uuids::to_string(boost::uuids::random_generator()());
        auto tempClient = container.GetPageBlobClient(tempName);
        Block(container, RenameBlobAsync(RenameBlobs{.Source = std::move(srcClient),
                                                     .Destination = std::move(destClient),
                                                     .Temporary = std::move(tempClient),
                                                     .SourceName = std::string(realPathFrom),
                                                     .DestinationName = std::string(realPathTo),
                                                     .TemporaryName = std::move(tempName)}));
    });
}

template <typename THttp>
boost::asio::awaitable<void> BasicBlobFilesystemImpl<THttp>::RenameBlobAsync(RenameBlobs blobs) const {
    auto& srcClient = blobs.Source;
    auto& destClient = blobs.Destination;
    const std::string& realPathFrom = blobs.SourceName;
    const std::string& realPathTo = blobs.DestinationName;
    const auto [size, cap, etag, modified] = co_await BlobOperations::GetInfoAsync(srcClient);

    // The cleanup of a failed step awaits, which a catch handler cannot do, so each step records its failure, cleans
    // up, then rethrows.
    std::exception_ptr failure;

    if (size <= SignedBytes(Configuration::Transfer::MaxInlineRenameSize)) {
        // Small file: one download, then write the destination directly. Overwriting an existing destination still
        // goes through an emptied blob (there is no atomic replace), but that window is one upload wide instead of a
        // multi-request copy, and a failure leaves the source intact for a retry.
        constexpr auto PageSize = SignedPageSize;
        const auto paddedSize = static_cast<size_t>((size + PageSize - 1) / PageSize * PageSize);
        // The download overwrites [0, size); only the page-alignment tail needs zeroing.
        const auto data = std::make_unique_for_overwrite<char[]>(paddedSize);
        const std::span<char> buffer(data.get(), paddedSize);
        if (size > 0 && co_await BlobOperations::DownloadAsync(srcClient, buffer.first(static_cast<size_t>(size)),
                                                               Core::ByteOffset{}, CountFromSigned(size),
                                                               etag) != CountFromSigned(size)) {
            throw std::runtime_error("Unexpected end of blob while renaming '" + realPathFrom + "'");
        }
        std::ranges::fill(buffer.subspan(static_cast<size_t>(size)), '\0');

        const auto created =
            co_await BlobOperations::CreateIfNotExistsAsync(m_runtime, destClient, CountFromSigned(cap));
        if (created == BlobOperations::CreateOutcome::AlreadyExisted) {
            co_await BlobOperations::CreateOrReplaceAsync(m_runtime, destClient, CountFromSigned(cap));
        }
        try {
            if (paddedSize != 0) {
                co_await BlobOperations::UploadPagesAsync(m_runtime, destClient, std::span<const char>(buffer),
                                                          Core::ByteOffset{});
            }
            co_await BlobOperations::SetSizeAsync(m_runtime, destClient, CountFromSigned(size));
        } catch (...) {
            failure = std::current_exception();
        }
        // A destination that existed keeps whatever was written; only one created here is removed again.
        if (failure && created == BlobOperations::CreateOutcome::Created) {
            co_await DeleteQuietlyAsync(BlobOperations::Destination(destClient), realPathTo);
        }
    } else if (co_await BlobOperations::CreateIfNotExistsAsync(m_runtime, destClient, CountFromSigned(cap)) ==
               BlobOperations::CreateOutcome::Created) {
        // Nothing to lose at the destination, so write it in place and remove the partial blob on failure.
        try {
            co_await CopyIntoAsync(BlobOperations::Source(srcClient), BlobOperations::Destination(destClient),
                                   CountFromSigned(size), etag, realPathFrom);
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            co_await DeleteQuietlyAsync(BlobOperations::Destination(destClient), realPathTo);
        }
    } else {
        auto& tempClient = blobs.Temporary;
        co_await BlobOperations::CreateOrReplaceAsync(m_runtime, tempClient, CountFromSigned(cap));
        try {
            // Commit: replace the destination from the fully written temporary blob.
            // The ETag from the temporary blob's final write conditions the copy. The destination's logical size
            // stays 0 until the last step.
            const auto tempEtag =
                co_await CopyIntoAsync(BlobOperations::Source(srcClient), BlobOperations::Destination(tempClient),
                                       CountFromSigned(size), etag, realPathFrom);
            co_await BlobOperations::CreateOrReplaceAsync(m_runtime, destClient, CountFromSigned(cap));
            co_await CopyBlobPagesAsync(BlobOperations::Source(tempClient), BlobOperations::Destination(destClient),
                                        CountFromSigned(size), tempEtag, realPathFrom);
            co_await BlobOperations::SetSizeAsync(m_runtime, destClient, CountFromSigned(size));
        } catch (...) {
            failure = std::current_exception();
        }
        co_await DeleteQuietlyAsync(BlobOperations::Destination(tempClient), blobs.TemporaryName);
    }
    if (failure) {
        std::rethrow_exception(failure);
    }

    co_await BlobOperations::DeleteAsync(m_runtime, srcClient);
}

template <typename THttp>
boost::asio::awaitable<void> BasicBlobFilesystemImpl<THttp>::CopyBlobPagesAsync(
    const BlobOperations::Source<BlobClient> source, const BlobOperations::Destination<BlobClient> target,
    const Core::ByteCount size, const std::string etag, const std::string name) const {
    // Server-side copy comes first; it needs the source to be readable by the service with the client's credential.
    if (co_await BlobOperations::CopyPagesServerSideAsync(m_runtime, source, target, size, etag) ==
        BlobOperations::ServerSideCopy::Declined) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Server-side copy of " << name
            << " was refused (the source could not be authorized); copying through the client instead";
        co_await BlobOperations::CopyPagesAsync(m_runtime, source, target, size, etag);
    }
}

template <typename THttp>
boost::asio::awaitable<std::string>
BasicBlobFilesystemImpl<THttp>::CopyIntoAsync(const BlobOperations::Source<BlobClient> source,
                                              const BlobOperations::Destination<BlobClient> target,
                                              const Core::ByteCount size, std::string etag, std::string name) const {
    co_await CopyBlobPagesAsync(source, target, size, std::move(etag), std::move(name));
    co_return co_await BlobOperations::SetSizeAsync(m_runtime, target.Blob(), size);
}

template <typename THttp>
boost::asio::awaitable<void>
BasicBlobFilesystemImpl<THttp>::DeleteQuietlyAsync(const BlobOperations::Destination<BlobClient> blob,
                                                   const std::string name) const {
    try {
        co_await BlobOperations::DeleteAsync(m_runtime, blob.Blob());
    } catch (const std::exception& e) {
        BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
            << "Could not delete '" << name << "' while rolling back a rename: " << e.what();
    }
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
