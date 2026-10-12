// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#include "AVEVA/RocksDB/Plugin/Azure/Impl/PluginClients.hpp"

#include <AVEVA/AzureClient/Detail/BlobClientImpl.hpp>
#include <AVEVA/AzureClient/Detail/BlobContainerClientImpl.hpp>
#include <AVEVA/AzureClient/Detail/BlobServiceClientImpl.hpp>
#include <AVEVA/AzureClient/Detail/BlockBlobClientImpl.hpp>
#include <AVEVA/AzureClient/Detail/PageBlobClientImpl.hpp>

namespace AVEVA::AzureClient {
template class BasicBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicBlockBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicPageBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicBlobContainerClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
template class BasicBlobServiceClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
} // namespace AVEVA::AzureClient
