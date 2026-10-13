// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
template <typename TContainer> class BasicDirectoryImpl {
    std::shared_ptr<ClientRuntimeBase> m_runtime;
    std::shared_ptr<TContainer> m_client;
    std::string m_name;

  public:
    BasicDirectoryImpl(std::shared_ptr<ClientRuntimeBase> runtime, std::shared_ptr<TContainer> client,
                       std::string_view dirname)
        : m_runtime(std::move(runtime)), m_client(std::move(client)), m_name(dirname) {}

    void Fsync() {
        // TODO: figure out whether this is needed
    }

    size_t GetUniqueId(char* id, size_t maxSize) const noexcept {
        size_t res = std::min(maxSize, m_name.length());
        std::copy_n(m_name.data(), res, id);
        return res;
    }
};

/// <summary>The production directory, over the real container client. Non-template so it can be forward
/// declared.</summary>
class DirectoryImpl : public BasicDirectoryImpl<BlobContainerClient> {
  public:
    using BasicDirectoryImpl<BlobContainerClient>::BasicDirectoryImpl;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl