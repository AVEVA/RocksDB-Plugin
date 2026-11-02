// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

// Part of BlobFilesystemImpl.ipp: included at its end, never on its own.
#pragma once

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename THttp>
typename BasicBlobFilesystemImpl<THttp>::DirectoryType
BasicBlobFilesystemImpl<THttp>::CreateDirectory(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    // Directories are virtual, but an unknown prefix must still fail here rather than on first use.
    WithContainer(prefix, [](auto&) {});
    return DirectoryType{realPath};
}

template <typename THttp> AzureClient::Existence BasicBlobFilesystemImpl<THttp>::FileExists(const std::string& name) {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(name);

    if (Call(client, client.ExistsAsync()) == AzureClient::Existence::Present) {
        return AzureClient::Existence::Present;
    }

    // Fallback: check if this is a directory
    // NOTE: This doesn't map 100% to how a filesystem would work because you can have empty
    // directories in any respectable fs. This probably won't matter for our use case.
    return HasChildren(name) == DirectoryContents::NonEmpty ? AzureClient::Existence::Present
                                                            : AzureClient::Existence::Absent;
}

// Stops at the first child instead of walking every page, which would cost a request per blob. Matches only children
// below "realPath/" so that "foo" does not match a sibling such as "foobar"; rename temporaries are not children.
template <typename THttp>
DirectoryContents BasicBlobFilesystemImpl<THttp>::HasChildren(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions options;
    options.Prefix = dirPrefix;
    options.MaxResults = 1;
    auto contents = DirectoryContents::Empty;
    WithContainer(prefix, [&](auto& container) {
        BlobFilesystemDetail::ForEachBlobUntil(
            container, std::move(options), [&](const AzureClient::Models::BlobItem& blob) {
                const auto isChild = BlobFilesystemDetail::ClassifyBlobName(blob.Name) !=
                                         BlobFilesystemDetail::BlobNameKind::RenameTemp &&
                                     !BlobFilesystemDetail::ChildName(blob.Name, dirPrefix).empty();
                contents = isChild ? DirectoryContents::NonEmpty : DirectoryContents::Empty;
                return isChild ? BlobFilesystemDetail::Traversal::Stop : BlobFilesystemDetail::Traversal::Continue;
            });
    });
    return contents;
}

template <typename THttp>
std::vector<std::string> BasicBlobFilesystemImpl<THttp>::GetChildren(const std::string& directoryPath,
                                                                     const ListPageSize pageSize) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);

    std::vector<std::string> children;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions opts;
    opts.Prefix = dirPrefix;
    opts.MaxResults = std::clamp(pageSize.Value(), std::uint32_t{1},
                                 static_cast<std::uint32_t>(Configuration::Transfer::MaxListPageSize));

    // Process all pages of results
    WithContainer(prefix, [&](auto& container) {
        BlobFilesystemDetail::ForEachBlob(container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
            if (BlobFilesystemDetail::ClassifyBlobName(blob.Name) == BlobFilesystemDetail::BlobNameKind::RenameTemp) {
                return;
            }
            if (const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix); !childName.empty()) {
                children.emplace_back(childName);
            }
        });
    });

    return children;
}

template <typename THttp>
std::vector<BlobAttributes>
BasicBlobFilesystemImpl<THttp>::GetChildrenFileAttributes(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    std::vector<BlobAttributes> attributes;

    AzureClient::ListBlobsOptions opts;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    opts.Prefix = dirPrefix;
    opts.MaxResults = static_cast<uint32_t>(Configuration::Transfer::MaxListPageSize);
    // The file size lives in blob metadata, so list it instead of issuing a GetProperties per blob.
    opts.Metadata = AzureClient::MetadataListing::Include;

    // Process all pages of results
    WithContainer(prefix, [&](auto& container) {
        BlobFilesystemDetail::ForEachBlob(container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
            const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix);
            if (childName.empty() ||
                BlobFilesystemDetail::ClassifyBlobName(blob.Name) == BlobFilesystemDetail::BlobNameKind::RenameTemp) {
                return;
            }

            attributes.emplace_back(BlobHelpers::FileSizeFromProperties(blob.Properties, blob.Name),
                                    std::string(childName));
        });
    });

    return attributes;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
