// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/ClientRuntime.hpp"

#include <AVEVA/AzureClient/BlobContainerClient.hpp>

#include <memory>
#include <string>
#include <string_view>
namespace AVEVA::RocksDB::Plugin::Azure::Impl {
class DirectoryImpl {
    std::shared_ptr<ClientRuntime> m_runtime;
    std::shared_ptr<AzureClient::BlobContainerClient> m_client;
    std::string m_name;

  public:
    DirectoryImpl(std::shared_ptr<ClientRuntime> runtime, std::shared_ptr<AzureClient::BlobContainerClient> client,
                  std::string_view dirname);
    void Fsync();
    size_t GetUniqueId(char* id, size_t maxSize) const noexcept;
};
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl