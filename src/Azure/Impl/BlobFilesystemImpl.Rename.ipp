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

    const auto& container = GetContainer(prefixAccountTo);

    auto srcClient = container->GetPageBlobClient(std::string(realPathFrom));
    auto destClient = container->GetPageBlobClient(std::string(realPathTo));
    // Whatever happens below, a cached copy of either name may no longer match its blob: a failed commit can leave
    // the destination emptied or partly written, and a failed source delete leaves both names holding data.
    boost::scope::scope_exit dropCached{[&] {
        m_runtime->InvalidateBlobSize(srcClient.ResourceUrl());
        m_runtime->InvalidateBlobSize(destClient.ResourceUrl());
        if (const auto cache = m_fileCaches.find(prefixAccountTo); cache != m_fileCaches.end()) {
            cache->second->RemoveFile(std::string(realPathFrom));
            cache->second->RemoveFile(std::string(realPathTo));
        }
    }};

    const auto [size, cap, etag, modified] = BlobOperations::GetInfo(srcClient);

    // Copies the source's content (and logical size) into `target`, which must already exist with the source's
    // capacity. Server-side copy comes first; it needs the source to be readable by the service with the client's
    // credential. Returns the target's ETag after the final write.
    const auto copyInto = [&](auto& target) {
        if (!BlobOperations::CopyPagesServerSide(*m_runtime, srcClient, target, size, etag)) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                << "Server-side copy of " << realPathFrom
                << " was refused (the source could not be authorized); copying through the client instead";
            BlobOperations::CopyPages(*m_runtime, srcClient, target, size, etag);
        }
        return BlobOperations::SetSize(*m_runtime, target, size);
    };
    // Best-effort cleanup that never masks the failure being unwound.
    const auto deleteQuietly = [this](auto& blob, const std::string_view name) {
        try {
            BlobOperations::Delete(*m_runtime, blob);
        } catch (const std::exception& e) {
            BOOST_LOG_SEV(*m_logger, boost::log::trivial::severity_level::warning)
                << "Could not delete '" << name << "' while rolling back a rename: " << e.what();
        }
    };

    if (size <= Configuration::Transfer::MaxInlineRenameSize) {
        // Small file: one download, then write the destination directly. Overwriting an existing destination still
        // goes through an emptied blob (there is no atomic replace), but that window is one upload wide instead of a
        // multi-request copy, and a failure leaves the source intact for a retry.
        constexpr auto PageSize = Configuration::PageBlob::PageSize;
        const auto paddedSize = static_cast<size_t>((size + PageSize - 1) / PageSize * PageSize);
        // The download overwrites [0, size); only the page-alignment tail needs zeroing. The upload below is
        // synchronous, so the buffer can be owned by this scope instead of shared.
        const auto data = std::make_unique_for_overwrite<char[]>(paddedSize);
        const std::span<char> buffer(data.get(), paddedSize);
        if (size > 0 &&
            BlobOperations::Download(srcClient, buffer.first(static_cast<size_t>(size)), 0, size, etag) != size) {
            throw std::runtime_error("Unexpected end of blob while renaming '" + std::string(realPathFrom) + "'");
        }
        std::ranges::fill(buffer.subspan(static_cast<size_t>(size)), '\0');
        const auto writeInto = [&](auto& target) {
            if (paddedSize != 0) {
                BlobOperations::UploadPages(*m_runtime, target, std::span<const char>(buffer), 0);
            }
            BlobOperations::SetSize(*m_runtime, target, size);
        };
        if (BlobOperations::CreateIfNotExists(*m_runtime, destClient, cap)) {
            boost::scope::scope_fail rollback{[&] { deleteQuietly(destClient, realPathTo); }};
            writeInto(destClient);
        } else {
            BlobOperations::CreateOrReplace(*m_runtime, destClient, cap);
            writeInto(destClient);
        }
    } else if (BlobOperations::CreateIfNotExists(*m_runtime, destClient, cap)) {
        // Nothing to lose at the destination, so write it in place and remove the partial blob on failure.
        boost::scope::scope_fail rollback{[&] { deleteQuietly(destClient, realPathTo); }};
        copyInto(destClient);
    } else {
        const auto tempName = std::string(realPathTo) + std::string(BlobFilesystemDetail::RenameTempMarker) +
                              boost::uuids::to_string(boost::uuids::random_generator()());
        auto tempClient = container->GetPageBlobClient(tempName);
        BlobOperations::CreateOrReplace(*m_runtime, tempClient, cap);
        {
            boost::scope::scope_fail rollback{[&] { deleteQuietly(tempClient, tempName); }};
            // Commit: replace the destination from the fully written temporary blob.
            // The ETag from the temporary blob's final write conditions the copy. The destination's logical size
            // stays 0 until the last step.
            const auto tempEtag = copyInto(tempClient);
            BlobOperations::CreateOrReplace(*m_runtime, destClient, cap);
            if (!BlobOperations::CopyPagesServerSide(*m_runtime, tempClient, destClient, size, tempEtag)) {
                BlobOperations::CopyPages(*m_runtime, tempClient, destClient, size, tempEtag);
            }
            BlobOperations::SetSize(*m_runtime, destClient, size);
        }
        deleteQuietly(tempClient, tempName);
    }

    BlobOperations::Delete(*m_runtime, srcClient);
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
