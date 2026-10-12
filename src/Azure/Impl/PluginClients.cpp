// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>

namespace AVEVA::AzureClient {
template class BasicBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicBlockBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicPageBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicBlobContainerClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicBlobServiceClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
} // namespace AVEVA::AzureClient
