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
    return DirectoryType{m_runtime, GetContainer(prefix), realPath};
}

template <typename THttp> bool BasicBlobFilesystemImpl<THttp>::FileExists(const std::string& name) {
    EnsureLiveness();

    auto [prefix, realPath, client] = ResolveBlob(name);

    if (Call([&](auto token) { return client.ExistsAsync(token); })) {
        return true;
    }

    // Fallback: check if this is a directory
    // NOTE: This doesn't map 100% to how a filesystem would work because you can have empty
    // directories in any respectable fs. This probably won't matter for our use case.
    // Stop at the first child instead of walking every page, which would cost a request per blob.
    // Match only children below "realPath/" so that "foo" does not match a sibling such as "foobar".
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions options;
    options.Prefix = dirPrefix;
    options.MaxResults = 1;
    bool found = false;
    BlobFilesystemDetail::ForEachBlobUntil(*GetContainer(prefix), std::move(options),
                                           [&](const AzureClient::Models::BlobItem& blob) {
                                               found = !BlobFilesystemDetail::ChildName(blob.Name, dirPrefix).empty();
                                               return found;
                                           });
    return found;
}

template <typename THttp>
std::vector<std::string> BasicBlobFilesystemImpl<THttp>::GetChildren(const std::string& directoryPath,
                                                                     int32_t sizeHint) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);

    std::vector<std::string> children;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    AzureClient::ListBlobsOptions opts;
    opts.Prefix = dirPrefix;
    opts.MaxResults = BlobFilesystemDetail::ToListPageSize(sizeHint);

    // Process all pages of results
    BlobFilesystemDetail::ForEachBlob(*container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
        if (BlobFilesystemDetail::IsRenameTempName(blob.Name)) {
            return;
        }
        if (const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix); !childName.empty()) {
            children.emplace_back(childName);
        }
    });

    return children;
}

template <typename THttp>
std::vector<BlobAttributes>
BasicBlobFilesystemImpl<THttp>::GetChildrenFileAttributes(const std::string& directoryPath) {
    EnsureLiveness();

    const auto [prefix, realPath] = StorageAccount::StripPrefix(directoryPath);
    const auto& container = GetContainer(prefix);
    std::vector<BlobAttributes> attributes;

    AzureClient::ListBlobsOptions opts;
    const auto dirPrefix = BlobFilesystemDetail::DirectoryPrefix(realPath);
    opts.Prefix = dirPrefix;
    opts.MaxResults = static_cast<uint32_t>(Configuration::Transfer::MaxListPageSize);
    // The file size lives in blob metadata, so list it instead of issuing a GetProperties per blob.
    opts.IncludeMetadata = true;

    // Process all pages of results
    BlobFilesystemDetail::ForEachBlob(*container, std::move(opts), [&](const AzureClient::Models::BlobItem& blob) {
        const auto childName = BlobFilesystemDetail::ChildName(blob.Name, dirPrefix);
        if (childName.empty() || BlobFilesystemDetail::IsRenameTempName(blob.Name)) {
            return;
        }

        attributes.emplace_back(BlobHelpers::FileSizeFromProperties(blob.Properties, blob.Name),
                                std::string(childName));
    });

    return attributes;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
