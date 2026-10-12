// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 AVEVA

#pragma once
#include "AVEVA/RocksDB/Plugin/Azure/Impl/TokenCredentials.hpp"

#include <AVEVA/AzureClient/BlobClient.hpp>
#include <AVEVA/AzureClient/BlobContainerClient.hpp>
#include <AVEVA/AzureClient/BlobServiceClient.hpp>
#include <AVEVA/AzureClient/BlockBlobClient.hpp>
#include <AVEVA/AzureClient/PageBlobClient.hpp>
#include <AVEVA/HttpClient/HttpClient.hpp>

namespace AVEVA::RocksDB::Plugin::Azure::Impl {
// The Azure clients the plugin uses: the real HTTP transport authenticating with the plugin's credential.
using BlobClient = AzureClient::BasicBlobClient<::AVEVA::HttpClient, PluginCredential>;
using BlockBlobClient = AzureClient::BasicBlockBlobClient<::AVEVA::HttpClient, PluginCredential>;
using PageBlobClient = AzureClient::BasicPageBlobClient<::AVEVA::HttpClient, PluginCredential>;
using BlobContainerClient = AzureClient::BasicBlobContainerClient<::AVEVA::HttpClient, PluginCredential>;
using BlobServiceClient = AzureClient::BasicBlobServiceClient<::AVEVA::HttpClient, PluginCredential>;
} // namespace AVEVA::RocksDB::Plugin::Azure::Impl

// Instantiated once, in PluginClients.cpp, so the plugin does not recompile the client implementations per file.
namespace AVEVA::AzureClient {
extern template class BasicBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
extern template class BasicBlockBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
extern template class BasicPageBlobClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
extern template class BasicBlobContainerClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
extern template class BasicBlobServiceClient<::AVEVA::HttpClient, RocksDB::Plugin::Azure::Impl::PluginCredential>;
} // namespace AVEVA::AzureClient
