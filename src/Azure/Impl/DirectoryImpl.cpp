// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/DirectoryImpl.hpp"

#include <algorithm>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
DirectoryImpl::DirectoryImpl(std::shared_ptr<ClientRuntime> runtime,
                             std::shared_ptr<AzureClient::BlobContainerClient> client, const std::string_view dirname)
    : m_runtime(std::move(runtime)), m_client(std::move(client)), m_name(dirname) {}

void DirectoryImpl::Fsync() {
    // TODO: figure out whether this is needed
}

size_t DirectoryImpl::GetUniqueId(char* id, size_t maxSize) const noexcept {
    size_t res = std::min(maxSize, m_name.length());
    std::copy_n(m_name.data(), res, id);
    return res;
}
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl
